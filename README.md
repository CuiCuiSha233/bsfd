# BSFD - BitStorm File Distribution Protocol Engine

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-69%20passed-brightgreen)](#)

A lightweight, embeddable P2P file distribution protocol engine designed for high-throughput LAN environments. Zero external dependencies beyond the Go standard library.

## Features

- **UDP-based auto-discovery** — Nodes announce themselves via periodic heartbeats; clients discover admins automatically
- **TCP multiplexing** — 4 connections per peer with hash-based load distribution
- **Block-level pipelining** — 16 KB block transfers with dynamic concurrency windows
- **RTT-aware flow control** — EMA-smoothed round-trip time tracking with adaptive concurrency
- **Seed files (.bsfd)** — Portable metadata format for pre-deployment verification
- **Embeddable design** — Callback-driven API with no dependency on any GUI framework or filesystem

## Architecture

```
bsfd/
├── protocol/    Frame format (BSFD magic, type, payload), 13 message types
├── discovery/   UDP heartbeat broadcast, peer registry, network utilities
├── transport/   TCP connection pool, RTT EMA, block scheduler
└── storage/     Chunk store interface (implemented by host application)
```

### Wire Format (v1)

```
[4B Magic "BSFD"][1B Type][4B PayloadLen BE][PayloadLen bytes]
```

| Field      | Size | Description              |
|------------|------|--------------------------|
| Magic      | 4 B  | Always `BSFD`            |
| Type       | 1 B  | Message type (see below) |
| PayloadLen | 4 B  | Big-endian uint32        |
| Payload    | N B  | JSON or binary data      |

### Message Types

| Type | Name         | Direction     | Format |
|------|-------------|---------------|--------|
| 0x01 | Heartbeat   | Bidirectional | JSON   |
| 0x02 | Hello       | Client→Admin  | JSON   |
| 0x03 | FileInfo    | Admin→Client  | JSON   |
| 0x04 | StartDist   | Admin→Client  | JSON   |
| 0x10 | ChunkReq    | Peer→Peer     | JSON   |
| 0x11 | ChunkData   | Peer→Peer     | JSON   |
| 0x12 | ChunkDataBin| Peer→Peer     | Binary |
| 0x13 | BlockReq    | Peer→Peer     | JSON   |
| 0x14 | BlockDataBin| Peer→Peer     | Binary |
| 0x30 | Cancel      | Admin→Client  | JSON   |
| 0x31 | Finish      | Admin→Client  | JSON   |
| 0xFE | KeepAlive   | Bidirectional | JSON   |
| 0xFF | Disconnect  | Bidirectional | JSON   |

## Installation

```bash
go get github.com/cuicuisha233/bsfd
```

## Quick Start

```go
package main

import (
    "log"

    "github.com/cuicuisha233/bsfd/discovery"
    "github.com/cuicuisha233/bsfd/protocol"
    "github.com/cuicuisha233/bsfd/transport"
)

func main() {
    // === Discovery ===
    discCfg := discovery.DefaultConfig()
    listener := discovery.NewListener(discCfg)
    listener.OnDiscover(func(admin discovery.DiscoveredAdmin) {
        log.Printf("Discovered admin: %s at %s:%d", admin.Name, admin.IP, admin.TCPPort)
    })
    listener.Start()
    defer listener.Stop()

    broadcaster := discovery.NewBroadcaster(
        discCfg,
        "my-peer-id",
        discovery.Hostname(),
        discovery.LocalIPs()[0],
        26932,
        "client",
        0,
        nil,
        nil,
    )
    broadcaster.Start()
    defer broadcaster.Stop()

    // === Transport ===
    tCfg := transport.DefaultConfig()
    engine := transport.New(tCfg)
    engine.SetLocalPeerID("my-peer-id")
    engine.SetCallbacks(transport.Callbacks{
        OnPeerConnected: func(peerID string) {
            log.Printf("Peer connected: %s", peerID)
        },
        OnChunkReceived: func(peerID string, chunkIndex int, data []byte) {
            log.Printf("Received chunk %d from %s (%d bytes)", chunkIndex, peerID, len(data))
        },
    })
    engine.Start()
    defer engine.Stop()

    // === Protocol ===
    frame, _ := protocol.Encode(protocol.TypeHello, protocol.Hello{PeerID: "my-peer-id"})
    _ = frame
}
```

## Testing

```bash
# Run all tests
go test ./...

# With verbose output
go test -v ./...

# Benchmark protocol encoding
go test -bench=. ./protocol/
```

## Related Projects

- [BitStorm](https://github.com/cuicuisha233/BitStorm) — Full BitStorm desktop application powered by this engine

## License

MIT — see [LICENSE](LICENSE) for details.
