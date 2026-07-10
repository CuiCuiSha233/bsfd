package transport

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"sync"
	"time"

	"github.com/cuicuisha233/bsfd/protocol"
)

// Engine manages all P2P connections and data transfer.
type Engine struct {
	cfg        Config
	listenPort int
	listener   net.Listener

	mu    sync.RWMutex
	conns map[string][]*PeerConn

	localPeerID  string
	chunkHashes  []string

	chunkProvider func(chunkIndex int) ([]byte, error)
	cb            Callbacks

	stopCh   chan struct{}
	stopOnce sync.Once
}

// New creates a transfer engine.
func New(cfg Config) *Engine {
	return &Engine{
		cfg:        cfg,
		listenPort: cfg.ListenPort,
		conns:      make(map[string][]*PeerConn),
		stopCh:     make(chan struct{}),
	}
}

// SetChunkProvider registers a callback for serving chunk data.
func (e *Engine) SetChunkProvider(fn func(chunkIndex int) ([]byte, error)) { e.chunkProvider = fn }

// SetLocalPeerID stores this node's peer ID (used for Hello handshakes).
func (e *Engine) SetLocalPeerID(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.localPeerID = id
}

// LocalPeerID returns this node's peer ID.
func (e *Engine) LocalPeerID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.localPeerID
}

// SetChunkHashes stores expected chunk hashes for verification.
func (e *Engine) SetChunkHashes(h []string) { e.chunkHashes = h }

// SetCallbacks wires external listeners.
func (e *Engine) SetCallbacks(cb Callbacks) { e.cb = cb }

// Start begins accepting connections. Idempotent.
func (e *Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.listener != nil {
		return nil // already running
	}
	e.stopCh = make(chan struct{})
	e.stopOnce = sync.Once{}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", e.listenPort))
	if err != nil {
		return fmt.Errorf("transport: listen: %w", err)
	}
	e.listener = ln
	go e.acceptLoop()
	return nil
}

// Stop shuts down all connections and the listener. Idempotent.
func (e *Engine) Stop() {
	e.mu.Lock()
	var ln net.Listener
	var conns []*PeerConn
	e.stopOnce.Do(func() {
		close(e.stopCh)
		ln = e.listener
		e.listener = nil
		for _, pool := range e.conns {
			conns = append(conns, pool...)
		}
		e.conns = make(map[string][]*PeerConn)
	})
	e.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
	for _, pc := range conns {
		pc.Close()
	}
}

// Connect establishes up to PoolSize TCP connections to a peer.
func (e *Engine) Connect(peerID, ip string, port int) (*PeerConn, error) {
	e.mu.Lock()
	existing := e.conns[peerID]
	e.mu.Unlock()

	if len(existing) > 0 && len(existing) >= e.cfg.ConnPoolSize {
		return existing[0], nil
	}

	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	var first *PeerConn

	for i := len(existing); i < e.cfg.ConnPoolSize; i++ {
		tcp, err := net.DialTimeout("tcp", addr, e.cfg.ConnTimeout)
		if err != nil {
			if i == 0 {
				return nil, fmt.Errorf("transport: connect %s: %w", peerID, err)
			}
			break
		}
		configureTCP(tcp, e.cfg.TCPReadBufferSize, e.cfg.TCPWriteBufferSize)

		pc := newPeerConn(peerID, tcp, true, e.cfg)
		pc.chunkProvider = e.chunkProvider
		pc.chunkHashes = e.chunkHashes
		pc.cb = pcCallbacks{
			onChunkReceived: func(ci int, d []byte) {
				if e.cb.OnChunkReceived != nil {
					e.cb.OnChunkReceived(peerID, ci, d)
				}
			},
			onBlockReceived: func(ci, off int, d []byte) {
				if e.cb.OnBlockReceived != nil {
					e.cb.OnBlockReceived(peerID, ci, off, d)
				}
			},
			onDisconnect: func() { e.removeConn(peerID, pc) },
			onChunkRequested: func(ci int) {
				if e.cb.OnChunkRequested != nil {
					e.cb.OnChunkRequested(peerID, ci)
				}
			},
			onMessage: func(mt byte, p []byte) {
				if e.cb.OnMessage != nil {
					e.cb.OnMessage(peerID, mt, p)
				}
			},
		}

		e.mu.Lock()
		e.conns[peerID] = append(e.conns[peerID], pc)
		e.mu.Unlock()

		pc.Start()

		// 所有连接都要发 Hello 让对方识别
		{
			hello := protocol.Hello{PeerID: e.LocalPeerID()}
			if data, err := protocol.Encode(protocol.TypeHello, hello); err == nil {
				pc.sendRaw(data)
			}
		}

		if i == 0 {
			first = pc
		}
	}

	if first == nil && len(existing) > 0 {
		first = existing[0]
	}

	if len(existing) == 0 && first != nil && e.cb.OnPeerConnected != nil {
		e.cb.OnPeerConnected(peerID)
	}

	log.Printf("[transport] Connect: peer=%s pool=%d total=%d", peerID, e.poolSize(peerID), len(e.Peers()))
	return first, nil
}

// Send sends a JSON-encoded message to a peer (via first pool connection).
func (e *Engine) Send(peerID string, msgType byte, msg interface{}) error {
	pc := e.pick(peerID, 0)
	if pc == nil {
		return fmt.Errorf("transport: no connection to %s", peerID)
	}
	data, err := protocol.Encode(msgType, msg)
	if err != nil {
		return err
	}
	return pc.sendRaw(data)
}

// SendRaw sends pre-encoded frame bytes to a peer.
func (e *Engine) SendRaw(peerID string, frame []byte) error {
	pc := e.pick(peerID, 0)
	if pc == nil {
		return fmt.Errorf("transport: no connection to %s", peerID)
	}
	return pc.sendRaw(frame)
}

// SendChunkResponse sends a chunk data response to the peer on the
// same connection that would have handled the corresponding request,
// ensuring inflight tracking is properly cleaned up.
func (e *Engine) SendChunkResponse(peerID string, chunkIndex int, data []byte) error {
	pc := e.pick(peerID, chunkIndex)
	if pc == nil {
		return fmt.Errorf("transport: no connection to %s", peerID)
	}
	return pc.sendChunkResponse(chunkIndex, data)
}

// Broadcast sends a message to every connected peer.
func (e *Engine) Broadcast(msgType byte, msg interface{}) {
	data, err := protocol.Encode(msgType, msg)
	if err != nil {
		return
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, pool := range e.conns {
		for _, pc := range pool {
			pc.sendRaw(data)
		}
	}
}

// RequestChunk requests a chunk from a peer (hash-distributed across pool).
func (e *Engine) RequestChunk(peerID, fileHash string, chunkIndex int) error {
	pc := e.pick(peerID, chunkIndex)
	if pc == nil {
		return fmt.Errorf("transport: no connection to %s", peerID)
	}
	return pc.requestChunk(fileHash, chunkIndex)
}

// RequestBlock requests a block from a peer (hash-distributed across pool).
func (e *Engine) RequestBlock(peerID, fileHash string, chunkIndex, blockOffset, blockSize int) error {
	pc := e.pick(peerID, chunkIndex)
	if pc == nil {
		return fmt.Errorf("transport: no connection to %s", peerID)
	}
	return pc.requestBlock(fileHash, chunkIndex, blockOffset, blockSize)
}

// Peers returns all connected peer IDs.
func (e *Engine) Peers() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r := make([]string, 0, len(e.conns))
	for id, pool := range e.conns {
		if len(pool) > 0 {
			r = append(r, id)
		}
	}
	return r
}

// IsConnected checks if at least one connection exists to a peer.
func (e *Engine) IsConnected(peerID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.conns[peerID]) > 0
}

// Disconnect closes all connections to a peer.
func (e *Engine) Disconnect(peerID string) {
	e.mu.Lock()
	pool := e.conns[peerID]
	delete(e.conns, peerID)
	e.mu.Unlock()
	for _, pc := range pool {
		pc.Close()
	}
}

// ReKey remaps connections from oldID to newID (peer learned its real ID).
func (e *Engine) ReKey(oldID, newID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if pool, ok := e.conns[oldID]; ok {
		delete(e.conns, oldID)
		e.conns[newID] = append(e.conns[newID], pool...)
		log.Printf("[transport] ReKey: %s → %s", oldID, newID)
	}
}

// PeerRTT returns the minimum RTT across all connections to a peer.
func (e *Engine) PeerRTT(peerID string) time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var min time.Duration
	for _, pc := range e.conns[peerID] {
		if rtt := pc.avgRTT(); rtt > 0 && (min == 0 || rtt < min) {
			min = rtt
		}
	}
	return min
}

// pick selects a connection from the pool using hash(chunkIndex).
func (e *Engine) pick(peerID string, chunkIndex int) *PeerConn {
	e.mu.RLock()
	defer e.mu.RUnlock()
	pool := e.conns[peerID]
	if len(pool) == 0 {
		return nil
	}
	return pool[hashChunk(uint32(chunkIndex))%len(pool)]
}

func (e *Engine) poolSize(peerID string) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.conns[peerID])
}

func hashChunk(idx uint32) int {
	h := fnv.New32a()
	h.Write([]byte{byte(idx), byte(idx >> 8), byte(idx >> 16), byte(idx >> 24)})
	return int(h.Sum32())
}

func (e *Engine) acceptLoop() {
	for {
		select {
		case <-e.stopCh:
			return
		default:
		}
		conn, err := e.listener.Accept()
		if err != nil {
			select {
			case <-e.stopCh:
				return
			default:
				continue
			}
		}
		configureTCP(conn, e.cfg.TCPReadBufferSize, e.cfg.TCPWriteBufferSize)
		go e.handleIncoming(conn)
	}
}

func (e *Engine) handleIncoming(conn net.Conn) {
	fr := protocol.NewFrameReader(conn)
	msgType, payload, err := fr.ReadFrame()
	if err != nil {
		conn.Close()
		return
	}

	var peerID string
	if msgType == protocol.TypeHello {
		var h protocol.Hello
		if protocol.UnmarshalPayload(payload, &h) == nil {
			peerID = h.PeerID
		}
	}
	if peerID == "" {
		conn.Close()
		return
	}

	e.mu.Lock()
	if len(e.conns[peerID]) >= e.cfg.ConnPoolSize {
		e.mu.Unlock()
		conn.Close()
		return
	}
	wasNew := len(e.conns[peerID]) == 0

	pc := newPeerConn(peerID, conn, false, e.cfg)
	pc.chunkProvider = e.chunkProvider
	pc.chunkHashes = e.chunkHashes

	pc.cb = pcCallbacks{
		onChunkReceived: func(ci int, d []byte) {
			if e.cb.OnChunkReceived != nil {
				e.cb.OnChunkReceived(peerID, ci, d)
			}
		},
		onBlockReceived: func(ci, off int, d []byte) {
			if e.cb.OnBlockReceived != nil {
				e.cb.OnBlockReceived(peerID, ci, off, d)
			}
		},
		onDisconnect: func() { e.removeConn(peerID, pc) },
		onChunkRequested: func(ci int) {
			if e.cb.OnChunkRequested != nil {
				e.cb.OnChunkRequested(peerID, ci)
			}
		},
		onMessage: func(mt byte, p []byte) {
			if e.cb.OnMessage != nil {
				e.cb.OnMessage(peerID, mt, p)
			}
		},
	}

	e.conns[peerID] = append(e.conns[peerID], pc)
	e.mu.Unlock()

	pc.handleFirstMessage(msgType, payload)
	pc.Start()

	if wasNew && e.cb.OnPeerConnected != nil {
		e.cb.OnPeerConnected(peerID)
	}
}

func (e *Engine) removeConn(peerID string, pc *PeerConn) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pool := e.conns[peerID]
	for i, c := range pool {
		if c == pc {
			e.conns[peerID] = append(pool[:i], pool[i+1:]...)
			break
		}
	}
	if len(e.conns[peerID]) == 0 {
		delete(e.conns, peerID)
		if e.cb.OnPeerDisconnected != nil {
			e.cb.OnPeerDisconnected(peerID)
		}
	}
}

func configureTCP(conn net.Conn, rdBuf, wrBuf int) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
		tcp.SetReadBuffer(rdBuf)
		tcp.SetWriteBuffer(wrBuf)
	}
}
