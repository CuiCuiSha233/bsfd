package transport

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cuicuisha233/bsfd/protocol"
)

// PeerConn handles bidirectional communication with a peer over one TCP connection.
type PeerConn struct {
	peerID   string
	conn     net.Conn
	outgoing bool

	mu              sync.Mutex
	pendingRequests map[int]time.Time
	closed          bool

	avgRTTVal time.Duration
	rttCount int64

	chunkProvider func(chunkIndex int) ([]byte, error)
	chunkHashes   []string

	cb  pcCallbacks
	cfg Config

	stopCh chan struct{}
}

type pcCallbacks struct {
	onChunkReceived  func(chunkIndex int, data []byte)
	onBlockReceived  func(chunkIndex int, blockOffset int, data []byte)
	onDisconnect     func()
	onChunkRequested func(chunkIndex int)
	onMessage        func(msgType byte, payload []byte)
}

func newPeerConn(peerID string, conn net.Conn, outgoing bool, cfg Config) *PeerConn {
	return &PeerConn{
		peerID:          peerID,
		conn:            conn,
		outgoing:        outgoing,
		pendingRequests: make(map[int]time.Time),
		cfg:             cfg,
		stopCh:          make(chan struct{}),
	}
}

// Start launches the read, heartbeat, and timeout goroutines.
func (pc *PeerConn) Start() {
	go pc.readLoop()
	go pc.timeoutChecker()
	go pc.heartbeatLoop()
}

// Close shuts down the connection. Idempotent.
func (pc *PeerConn) Close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if !pc.closed {
		pc.closed = true
		close(pc.stopCh)
		if pc.conn != nil {
			pc.conn.Close()
		}
		if pc.cb.onDisconnect != nil {
			pc.cb.onDisconnect()
		}
	}
}

// avgRTT returns the EMA RTT.
func (pc *PeerConn) avgRTT() time.Duration {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.avgRTTVal
}

func (pc *PeerConn) sendRaw(data []byte) error { return pc.send(data) }

func (pc *PeerConn) send(data []byte) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.closed {
		return fmt.Errorf("transport: connection closed")
	}
	if pc.conn == nil {
		return fmt.Errorf("transport: connection not established")
	}
	pc.conn.SetWriteDeadline(time.Now().Add(pc.cfg.RequestTimeout))
	_, err := pc.conn.Write(data)
	return err
}

func (pc *PeerConn) requestChunk(fileHash string, chunkIndex int) error {
	pc.mu.Lock()
	if len(pc.pendingRequests) >= pc.cfg.MaxInflightRequests {
		pc.mu.Unlock()
		return fmt.Errorf("transport: max inflight requests for %s", pc.peerID)
	}
	pc.pendingRequests[chunkIndex] = time.Now()
	pc.mu.Unlock()

	data, err := protocol.Encode(protocol.TypeChunkRequest, protocol.ChunkRequest{
		FileHash: fileHash, ChunkIndex: chunkIndex,
	})
	if err != nil {
		return err
	}
	return pc.send(data)
}

func (pc *PeerConn) requestBlock(fileHash string, chunkIndex, blockOffset, blockSize int) error {
	data, err := protocol.Encode(protocol.TypeBlockRequest, protocol.BlockRequest{
		FileHash: fileHash, ChunkIndex: chunkIndex, BlockOffset: blockOffset, BlockSize: blockSize,
	})
	if err != nil {
		return err
	}
	return pc.send(data)
}

func (pc *PeerConn) handleFirstMessage(msgType byte, payload []byte) {
	pc.handleMessage(msgType, payload)
}

func (pc *PeerConn) handleMessage(msgType byte, payload []byte) {
	switch msgType {
	case protocol.TypeChunkRequest:
		var req protocol.ChunkRequest
		if protocol.UnmarshalPayload(payload, &req) == nil && pc.cb.onChunkRequested != nil {
			pc.cb.onChunkRequested(req.ChunkIndex)
		}

	case protocol.TypeChunkData:
		pc.handleChunkData(payload)

	case protocol.TypeChunkDataBin:
		pc.handleChunkDataBin(payload)

	case protocol.TypeBlockRequest:
		pc.handleBlockRequest(payload)

	case protocol.TypeBlockDataBin:
		pc.handleBlockDataBin(payload)

	default:
		if pc.cb.onMessage != nil {
			pc.cb.onMessage(msgType, payload)
		}
	}
}

func (pc *PeerConn) handleChunkData(payload []byte) {
	var cd protocol.ChunkData
	if protocol.UnmarshalPayload(payload, &cd) == nil && cd.FileHash != "" {
		pc.mu.Lock()
		delete(pc.pendingRequests, cd.ChunkIndex)
		pc.mu.Unlock()
		if len(cd.Data) > 0 && pc.cb.onChunkReceived != nil {
			pc.cb.onChunkReceived(cd.ChunkIndex, cd.Data)
		}
	}
}

func (pc *PeerConn) handleChunkDataBin(payload []byte) {
	if len(payload) < 4 {
		return
	}
	ci := int(binary.BigEndian.Uint32(payload[0:4]))
	data := payload[4:]

	pc.mu.Lock()
	if reqTime, ok := pc.pendingRequests[ci]; ok {
		pc.recordRTT(time.Since(reqTime))
	}
	delete(pc.pendingRequests, ci)
	pc.mu.Unlock()

	if pc.cb.onChunkReceived != nil {
		pc.cb.onChunkReceived(ci, data)
	}
}

func (pc *PeerConn) handleBlockRequest(payload []byte) {
	var req protocol.BlockRequest
	if protocol.UnmarshalPayload(payload, &req) != nil || pc.chunkProvider == nil {
		return
	}
	chunkData, err := pc.chunkProvider(req.ChunkIndex)
	if err != nil {
		return
	}
	start := req.BlockOffset
	end := start + req.BlockSize
	if end > len(chunkData) {
		end = len(chunkData)
	}
	blockData := chunkData[start:end]

	buf := make([]byte, 8+len(blockData))
	binary.BigEndian.PutUint32(buf[0:4], uint32(req.ChunkIndex))
	binary.BigEndian.PutUint32(buf[4:8], uint32(req.BlockOffset))
	copy(buf[8:], blockData)

	frame := protocol.EncodeRaw(protocol.TypeBlockDataBin, buf)
	if frame != nil {
		pc.sendRaw(frame)
	}
}

func (pc *PeerConn) handleBlockDataBin(payload []byte) {
	if len(payload) < 8 {
		return
	}
	ci := int(binary.BigEndian.Uint32(payload[0:4]))
	off := int(binary.BigEndian.Uint32(payload[4:8]))
	if pc.cb.onBlockReceived != nil {
		pc.cb.onBlockReceived(ci, off, payload[8:])
	}
}

func (pc *PeerConn) readLoop() {
	fr := protocol.NewFrameReader(pc.conn)
	for {
		select {
		case <-pc.stopCh:
			return
		default:
		}
		pc.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msgType, payload, err := fr.ReadFrame()
		if err != nil {
			pc.Close()
			return
		}
		pc.handleMessage(msgType, payload)
	}
}

func (pc *PeerConn) heartbeatLoop() {
	ticker := time.NewTicker(pc.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pc.stopCh:
			return
		case <-ticker.C:
			data, _ := protocol.Encode(protocol.TypeKeepAlive, protocol.KeepAlive{PeerID: pc.peerID})
			pc.send(data)
		}
	}
}

func (pc *PeerConn) timeoutChecker() {
	ticker := time.NewTicker(pc.cfg.TimeoutCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pc.stopCh:
			return
		case <-ticker.C:
			pc.mu.Lock()
			now := time.Now()
			for idx, reqTime := range pc.pendingRequests {
				if now.Sub(reqTime) > pc.cfg.RequestTimeout {
					delete(pc.pendingRequests, idx)
				}
			}
			pc.mu.Unlock()
		}
	}
}

func (pc *PeerConn) recordRTT(rtt time.Duration) {
	pc.rttCount++
	if pc.avgRTTVal == 0 {
		pc.avgRTTVal = rtt
	} else {
		const alpha = 0.125
		pc.avgRTTVal = time.Duration(float64(pc.avgRTTVal)*(1-alpha) + float64(rtt)*alpha)
	}
}
