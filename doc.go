// Package bsfd implements the BitStorm File Distribution (BSFD) protocol engine.
//
// BSFD is a lightweight peer-to-peer file distribution protocol designed for
// high-throughput LAN environments. It features:
//
//   - UDP-based automatic node discovery
//   - TCP multiplexing with dynamic concurrency windows
//   - Block-level pipelined transfers with rarest-first scheduling
//   - Seed file (.bsfd) format for pre-deployment verification
//   - Zero external dependencies beyond the Go standard library
//
// The engine is designed to be embeddable: it exposes only interfaces and
// callbacks, with no dependency on any GUI framework or filesystem abstraction.
//
// Architecture:
//
//	protocol/  — frame format, message types, encoding
//	discovery/ — UDP heartbeat broadcast and peer discovery
//	transport/ — TCP peer connections, scheduling, flow control
//	storage/   — chunk store interface (implemented by the host application)
//
// Protocol version: 1
package bsfd
