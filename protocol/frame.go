// Package protocol defines the BSFD wire protocol: frame format, message types,
// and binary encoding primitives.
//
// Frame format (v1):
//
//	[4B Magic "BSFD"][1B Type][4B PayloadLen BE][PayloadLen bytes]
//
// All multi-byte integers are big-endian. The maximum payload size is 32 MiB.
package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Frame constants.
const (
	MagicBytes = "BSFD"
	MagicLen   = 4
	HeaderLen  = 9         // Magic(4) + Type(1) + PayloadLen(4)
	MaxPayload = 32 << 20  // 32 MiB
)

// Protocol version (informational; not embedded in the wire format in v1).
const Version = 1

// Sentinels.
var (
	ErrInvalidMagic  = errors.New("bsfd: invalid magic bytes")
	ErrPayloadTooBig = errors.New("bsfd: payload exceeds maximum size")
	ErrShortHeader   = errors.New("bsfd: header too short")
)

// ----- Frame encoding -------------------------------------------------------

// Encode marshals msg as JSON and wraps it in a BSFD frame.
func Encode(msgType byte, msg interface{}) ([]byte, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("bsfd encode: marshal: %w", err)
	}
	return EncodeRaw(msgType, payload), nil
}

// EncodeRaw assembles a BSFD frame from pre-serialised payload bytes.
// Used for binary chunk / block data to avoid base64 overhead.
func EncodeRaw(msgType byte, payload []byte) []byte {
	if len(payload) > MaxPayload {
		return nil
	}
	buf := make([]byte, HeaderLen+len(payload))
	copy(buf[0:4], MagicBytes)
	buf[MagicLen] = msgType
	binary.BigEndian.PutUint32(buf[MagicLen+1:], uint32(len(payload)))
	copy(buf[HeaderLen:], payload)
	return buf
}

// ----- Frame decoding -------------------------------------------------------

// DecodeHeader reads and validates the frame header.  Returns (type, payloadLen, error).
func DecodeHeader(reader io.Reader) (byte, uint32, error) {
	header := make([]byte, HeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, 0, fmt.Errorf("bsfd: read header: %w", err)
	}
	if string(header[:MagicLen]) != MagicBytes {
		return 0, 0, ErrInvalidMagic
	}
	msgType := header[MagicLen]
	payloadLen := binary.BigEndian.Uint32(header[MagicLen+1:])
	if payloadLen > uint32(MaxPayload) {
		return 0, 0, ErrPayloadTooBig
	}
	return msgType, payloadLen, nil
}

// DecodePayload reads payloadLen bytes from reader.
func DecodePayload(reader io.Reader, payloadLen uint32) ([]byte, error) {
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("bsfd: read payload: %w", err)
	}
	return payload, nil
}

// DecodeMessage reads one complete frame: (type, payload, error).
func DecodeMessage(reader io.Reader) (byte, []byte, error) {
	msgType, payloadLen, err := DecodeHeader(reader)
	if err != nil {
		return 0, nil, err
	}
	payload, err := DecodePayload(reader, payloadLen)
	if err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// UnmarshalPayload unmarshals JSON payload into target.
func UnmarshalPayload(payload []byte, target interface{}) error {
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("bsfd: unmarshal: %w", err)
	}
	return nil
}

// ----- Buffered frame reader ------------------------------------------------

// FrameReader wraps an io.Reader and provides framed read.
type FrameReader struct {
	buf    [HeaderLen]byte
	reader io.Reader
}

// NewFrameReader creates a FrameReader over r.
func NewFrameReader(r io.Reader) *FrameReader { return &FrameReader{reader: r} }

// ReadFrame reads the next frame. Returns (type, payload, error).
func (fr *FrameReader) ReadFrame() (byte, []byte, error) {
	if _, err := io.ReadFull(fr.reader, fr.buf[:]); err != nil {
		return 0, nil, fmt.Errorf("bsfd: read frame header: %w", err)
	}
	if string(fr.buf[:MagicLen]) != MagicBytes {
		return 0, nil, ErrInvalidMagic
	}
	msgType := fr.buf[MagicLen]
	payloadLen := binary.BigEndian.Uint32(fr.buf[MagicLen+1:])
	if payloadLen > uint32(MaxPayload) {
		return 0, nil, ErrPayloadTooBig
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(fr.reader, payload); err != nil {
		return 0, nil, fmt.Errorf("bsfd: read frame payload: %w", err)
	}
	return msgType, payload, nil
}

// ----- Helpers --------------------------------------------------------------

// EncodeMessage is a convenience shorthand that JSON-marshals and wraps.
func EncodeMessage(msgType byte, msg interface{}) ([]byte, error) {
	return Encode(msgType, msg)
}

// BEBytes returns the big-endian uint32 encoding of v (4 bytes).
func BEBytes(v uint32) [4]byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b
}

// ReadBE reads a big-endian uint32 from the start of p.
func ReadBE(p []byte) uint32 { return binary.BigEndian.Uint32(p) }

// ReadPayload reads exactly n bytes into a new buffer.
func ReadPayload(r io.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

// JoinBytes is a zero-alloc concatenation when the result fits in a pooled buffer (caller-owned).
func JoinBytes(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}

// Buffer is a reusable byte buffer for assembling frames.
type Buffer struct{ bytes.Buffer }

// NewBuffer returns an empty Buffer.
func NewBuffer() *Buffer { return &Buffer{} }

// WriteBE writes a uint32 in big-endian order.
func (b *Buffer) WriteBE(v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	b.Write(tmp[:])
}
