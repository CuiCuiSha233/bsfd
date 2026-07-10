package discovery

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"

	"github.com/cuicuisha233/bsfd/protocol"
)

// Listener receives UDP heartbeats and maintains a peer registry.
type Listener struct {
	cfg    Config
	conn   *net.UDPConn
	stopCh chan struct{}
	stopOnce sync.Once
	mu       sync.RWMutex
	peers    map[string]*DiscoveredPeer

	onDiscover func(admin DiscoveredAdmin)
	onPeer     func(peer DiscoveredPeer)
}

// NewListener creates a UDP heartbeat listener.
func NewListener(cfg Config) *Listener {
	return &Listener{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		peers:  make(map[string]*DiscoveredPeer),
	}
}

// OnDiscover registers a callback fired when a new admin is discovered.
func (l *Listener) OnDiscover(fn func(admin DiscoveredAdmin)) { l.onDiscover = fn }

// OnPeer registers a callback fired on every heartbeat (for bitmap updates).
func (l *Listener) OnPeer(fn func(peer DiscoveredPeer)) { l.onPeer = fn }

// Start begins listening. Uses SO_REUSEADDR for multi-process support.
func (l *Listener) Start() error {
	addr := fmt.Sprintf(":%d", l.cfg.UDPPort)
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}
	pc, err := lc.ListenPacket(context.Background(), "udp4", addr)
	if err != nil {
		return fmt.Errorf("discovery: listen: %w", err)
	}
	conn, ok := pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return fmt.Errorf("discovery: not a UDPConn")
	}
	if err := conn.SetReadBuffer(l.cfg.ReadBufferSize); err != nil {
		conn.Close()
		return fmt.Errorf("discovery: set read buffer: %w", err)
	}
	l.conn = conn
	go l.listenLoop()
	return nil
}

// Stop shuts down the listener. Idempotent.
func (l *Listener) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
		if l.conn != nil {
			l.conn.Close()
		}
	})
}

// Admins returns all discovered admin nodes.
func (l *Listener) Admins() []DiscoveredAdmin {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var r []DiscoveredAdmin
	for _, p := range l.peers {
		if p.Role == "admin" {
			r = append(r, DiscoveredAdmin{Name: p.Name, IP: p.IP, TCPPort: p.TCPPort})
		}
	}
	return r
}

// Peers returns all discovered peers.
func (l *Listener) Peers() []DiscoveredPeer {
	l.mu.RLock()
	defer l.mu.RUnlock()
	r := make([]DiscoveredPeer, 0, len(l.peers))
	for _, p := range l.peers {
		r = append(r, *p)
	}
	return r
}

func (l *Listener) listenLoop() {
	buf := make([]byte, l.cfg.ReadBufferSize)
	for {
		select {
		case <-l.stopCh:
			return
		default:
		}
		n, remoteAddr, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-l.stopCh:
				return
			default:
				continue
			}
		}

		msgType, payloadLen, err := protocol.DecodeHeader(&bufferReader{buf: buf[:n]})
		if err != nil || msgType != protocol.TypeHeartbeat {
			continue
		}
		payloadData, err := protocol.DecodePayload(&bufferReader{buf: buf[protocol.HeaderLen:]}, payloadLen)
		if err != nil {
			continue
		}
		var hb protocol.Heartbeat
		if err := protocol.UnmarshalPayload(payloadData, &hb); err != nil {
			continue
		}

		// Ignore our own broadcasts
		if IsLocalIP(remoteAddr.IP) {
			continue
		}

		peer := &DiscoveredPeer{
			PeerID:          hb.PeerID,
			Name:            hb.Name,
			IP:              hb.IP,
			TCPPort:         hb.TCPPort,
			Role:            hb.Role,
			ChunkBitmap:     hb.ChunkBitmap,
			TotalChunks:     hb.TotalChunks,
			PreloadedHashes: hb.PreloadedHashes,
		}

		l.mu.Lock()
		_, exists := l.peers[hb.PeerID]
		l.peers[hb.PeerID] = peer
		l.mu.Unlock()

		if !exists && hb.Role == "admin" && l.onDiscover != nil {
			l.onDiscover(DiscoveredAdmin{Name: hb.Name, IP: hb.IP, TCPPort: hb.TCPPort})
		}
		if l.onPeer != nil {
			l.onPeer(*peer)
		}
	}
}
