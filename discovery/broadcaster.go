package discovery

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/cuicuisha233/bsfd/protocol"
)

// Broadcaster sends periodic UDP heartbeats on all interfaces.
type Broadcaster struct {
	cfg    Config
	peerID string
	name   string
	ip     string
	tcpPort   int
	role     string
	totalChunks int

	getBitmap          func() []byte
	getPreloadedHashes func() []string

	conns    []*net.UDPConn
	stopCh   chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
}

// NewBroadcaster creates a UDP heartbeat broadcaster.
func NewBroadcaster(cfg Config, peerID, name, ip string, tcpPort int, role string, totalChunks int, getBitmap func() []byte, getPreloadedHashes func() []string) *Broadcaster {
	return &Broadcaster{
		cfg:                cfg,
		peerID:             peerID,
		name:               name,
		ip:                 ip,
		tcpPort:            tcpPort,
		role:               role,
		totalChunks:        totalChunks,
		getBitmap:          getBitmap,
		getPreloadedHashes: getPreloadedHashes,
		stopCh:             make(chan struct{}),
	}
}

// Start begins broadcasting on all non-loopback interfaces.
func (b *Broadcaster) Start() error {
	ifaces, err := enumerateInterfaces()
	if err != nil {
		return fmt.Errorf("discovery: enumerate interfaces: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, iface := range ifaces {
		for _, addr := range iface.addrs {
			conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
			if err != nil {
				continue
			}
			if err := conn.SetWriteBuffer(b.cfg.ReadBufferSize); err != nil {
				conn.Close()
				continue
			}
			b.conns = append(b.conns, conn)
			go b.broadcastLoop(conn, addr)
		}
	}
	return nil
}

// Stop shuts down all broadcast goroutines. Idempotent.
func (b *Broadcaster) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, conn := range b.conns {
			conn.Close()
		}
		b.conns = nil
	})
}

func (b *Broadcaster) broadcastLoop(conn *net.UDPConn, addr *net.IPNet) {
	bcast := BroadcastAddr(addr)
	if bcast == nil {
		return
	}
	dest := &net.UDPAddr{IP: bcast, Port: b.cfg.UDPPort}

	ticker := time.NewTicker(b.cfg.BroadcastInterval)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			hb := protocol.Heartbeat{
				PeerID:      b.peerID,
				Name:        b.name,
				IP:          b.ip,
				TCPPort:     b.tcpPort,
				Role:        b.role,
				TotalChunks: b.totalChunks,
			}
			if b.getBitmap != nil {
				hb.ChunkBitmap = b.getBitmap()
			}
			if b.getPreloadedHashes != nil {
				hb.PreloadedHashes = b.getPreloadedHashes()
			}
			data, err := protocol.Encode(protocol.TypeHeartbeat, hb)
			if err != nil {
				continue
			}
			if _, err := conn.WriteToUDP(data, dest); err != nil {
				failures++
				if failures >= 3 {
					log.Printf("[WARN] discovery: broadcast failed %d times: %v", failures, err)
				}
			} else {
				failures = 0
			}
		}
	}
}
