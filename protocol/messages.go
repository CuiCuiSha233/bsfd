// Package protocol defines all BSFD wire messages.
package protocol

// ----- Message type constants -----------------------------------------------

const (
	// Discovery layer — UDP
	TypeHeartbeat = 0x01 // bidirectional heartbeat (broadcast)
	TypeHello     = 0x02 // Client→Admin TCP handshake

	// Control layer — TCP
	TypeFileInfo  = 0x03 // Admin→Client: file metadata
	TypeStartDist = 0x04 // Admin→Client: begin download

	// Data layer — TCP (peer↔peer)
	TypeChunkRequest = 0x10 // request a full chunk
	TypeChunkData    = 0x11 // DEPRECATED — JSON-encoded chunk (base64 overhead)
	TypeChunkDataBin = 0x12 // binary-encoded chunk (4B index + raw data)
	TypeBlockRequest = 0x13 // request a block within a chunk
	TypeBlockDataBin = 0x14 // binary-encoded block (4B index + 4B offset + raw data)

	// Status layer — TCP
	TypeCancel = 0x30 // Admin→Client: cancel distribution
	TypeFinish = 0x31 // Admin→Client: distribution complete

	// Utility
	TypeKeepAlive  = 0xFE
	TypeDisconnect = 0xFF
)

// ----- Message structures ---------------------------------------------------

// SeedFile is the .bsfd metadata format (BitStorm File Descriptor).
type SeedFile struct {
	Version     int      `json:"version"`
	Type        string   `json:"type"` // "file" or "folder"
	FileName    string   `json:"fileName"`
	FileSize    int64    `json:"fileSize"`
	FileHash    string   `json:"fileHash"`
	ChunkSize   int      `json:"chunkSize"`
	TotalChunks int      `json:"totalChunks"`
	ChunkHashes []string `json:"chunkHashes"`
}

// Heartbeat broadcasts node presence on the LAN.
type Heartbeat struct {
	PeerID          string   `json:"peerId"`
	Name            string   `json:"name"`
	IP              string   `json:"ip"`
	TCPPort         int      `json:"tcpPort"`
	Role            string   `json:"role"` // "admin" | "client"
	ChunkBitmap     []byte   `json:"chunkBitmap"`
	TotalChunks     int      `json:"totalChunks"`
	PreloadedHashes []string `json:"preloadedHashes,omitempty"`
}

// Hello is the TCP handshake from client to admin.
type Hello struct {
	PeerID string `json:"peerId"`
}

// PeerInfo is a local registry entry describing a known peer.
type PeerInfo struct {
	ID              string   `json:"id"`
	IP              string   `json:"ip"`
	Port            int      `json:"port"`
	Hostname        string   `json:"hostname"`
	IsAdmin         bool     `json:"isAdmin"`
	IsSeeder        bool     `json:"isSeeder"`
	ChunkBitmap     []byte   `json:"chunkBitmap"`
	PreloadedHashes []string `json:"preloadedHashes,omitempty"`
}

// FileInfo carries file metadata to clients.
type FileInfo struct {
	FileHash    string   `json:"fileHash"`
	FileName    string   `json:"fileName"`
	FileSize    int64    `json:"fileSize"`
	ChunkSize   int      `json:"chunkSize"`
	TotalChunks int      `json:"totalChunks"`
	ChunkHashes []string `json:"chunkHashes"`
}

// StartDist signals clients to begin downloading.
type StartDist struct {
	FileHash string `json:"fileHash"`
	SavePath string `json:"savePath"`
}

// ChunkRequest asks a peer for a specific chunk.
type ChunkRequest struct {
	FileHash   string `json:"fileHash"`
	ChunkIndex int    `json:"chunkIndex"`
}

// BlockRequest asks a peer for a block within a chunk.
type BlockRequest struct {
	FileHash    string `json:"fileHash"`
	ChunkIndex  int    `json:"chunkIndex"`
	BlockOffset int    `json:"blockOffset"`
	BlockSize   int    `json:"blockSize"`
}

// ChunkData carries chunk bytes (JSON, deprecated).
type ChunkData struct {
	FileHash   string `json:"fileHash"`
	ChunkIndex int    `json:"chunkIndex"`
	Data       []byte `json:"data"`
}

// Cancel stops a distribution.
type Cancel struct {
	FileHash string `json:"fileHash"`
}

// Finish signals successful completion.
type Finish struct {
	FileHash string `json:"fileHash"`
}

// Disconnect notifies peer disconnection.
type Disconnect struct {
	PeerID string `json:"peerId"`
}

// KeepAlive is a TCP keep-alive message.
type KeepAlive struct {
	PeerID string `json:"peerId"`
}
