// Package storage defines the ChunkStore interface that the host application
// must implement to provide persistent chunk storage for the BSFD engine.
package storage

// Store is the interface for persisting and retrieving file chunks.
// Implementations must be safe for concurrent use.
type Store interface {
	// SaveChunk persists a chunk. Returns an error if the chunk already exists
	// with different content.
	SaveChunk(index int, data []byte) error

	// GetChunk retrieves a previously saved chunk. Returns (nil, error) if not found.
	GetChunk(index int) ([]byte, error)

	// HasChunk returns true if the chunk at index is stored.
	HasChunk(index int) bool

	// Missing returns the indices of chunks not yet stored, given total count.
	Missing(total int) []int

	// Complete returns true when all chunks [0, total) are stored.
	Complete() bool

	// Total returns the configured total number of chunks.
	Total() int
}
