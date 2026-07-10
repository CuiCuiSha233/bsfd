// Package transport implements the BSFD P2P transfer engine with TCP multiplexing,
// block-level pipelining, and RTT-aware dynamic concurrency.
package transport

import (
	"time"
)

// Config holds transport engine parameters.
type Config struct {
	ListenPort           int
	ConnPoolSize         int           // TCP connections per peer (default 4)
	ConnTimeout          time.Duration // dial timeout
	TCPReadBufferSize    int
	TCPWriteBufferSize   int
	HeartbeatInterval    time.Duration
	HeartbeatTimeout     time.Duration
	RequestTimeout       time.Duration
	TimeoutCheckInterval time.Duration
	MaxInflightRequests  int // per connection
	BlockSize            int // 16KB typical
	MaxRetries           int
}

// DefaultConfig returns production defaults for LAN use.
func DefaultConfig() Config {
	return Config{
		ListenPort:           26932,
		ConnPoolSize:         4,
		ConnTimeout:          10 * time.Second,
		TCPReadBufferSize:    1 << 20, // 1MB
		TCPWriteBufferSize:   1 << 20, // 1MB
		HeartbeatInterval:    10 * time.Second,
		HeartbeatTimeout:     30 * time.Second,
		RequestTimeout:       30 * time.Second,
		TimeoutCheckInterval: 5 * time.Second,
		MaxInflightRequests:  8,
		BlockSize:            16 * 1024, // 16KB
		MaxRetries:           3,
	}
}

// Callbacks wires engine events to the host application.
type Callbacks struct {
	OnChunkReceived    func(peerID string, chunkIndex int, data []byte)
	OnBlockReceived    func(peerID string, chunkIndex int, blockOffset int, data []byte)
	OnPeerConnected    func(peerID string)
	OnPeerDisconnected func(peerID string)
	OnChunkRequested   func(peerID string, chunkIndex int)
	OnMessage          func(peerID string, msgType byte, payload []byte)
}
