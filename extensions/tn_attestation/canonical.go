package tn_attestation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// CanonicalPayload represents the eight attestation fields stored in result_canonical.
// The byte layout mirrors the SQL migration: fixed-width integers followed by
// length-prefixed blobs (little-endian 4-byte prefixes for variable sections).
//
// Layout:
//
//	1 byte    version
//	1 byte    algorithm
//	8 bytes   block height (big-endian)
//	4 + n     data provider (length-prefixed)
//	4 + m     stream ID (length-prefixed)
//	2 bytes   action ID (big-endian)
//	4 + k     arguments (length-prefixed)
//	4 + r     result (length-prefixed)
type CanonicalPayload struct {
	Version      uint8
	Algorithm    uint8
	BlockHeight  uint64
	DataProvider []byte
	StreamID     []byte
	ActionID     uint16
	Args         []byte
	Result       []byte

	raw []byte
}

// ParseCanonicalPayload decodes the canonical payload into structured fields.
// The function validates every length prefix and returns descriptive errors so
// future maintainers can diagnose storage corruption quickly.
func ParseCanonicalPayload(data []byte) (*CanonicalPayload, error) {
	if len(data) < 1+1+8+2 {
		return nil, fmt.Errorf("canonical payload too short: got %d bytes", len(data))
	}

	cursor := 0
	payload := &CanonicalPayload{
		Version:   data[cursor],
		Algorithm: data[cursor+1],
	}
	cursor += 2

	payload.BlockHeight = binary.BigEndian.Uint64(data[cursor : cursor+8])
	cursor += 8

	var err error
	if payload.DataProvider, cursor, err = readLengthPrefixed(data, cursor); err != nil {
		return nil, fmt.Errorf("decode data_provider: %w", err)
	}
	if payload.StreamID, cursor, err = readLengthPrefixed(data, cursor); err != nil {
		return nil, fmt.Errorf("decode stream_id: %w", err)
	}

	if len(data) < cursor+2 {
		return nil, fmt.Errorf("canonical payload truncated before action_id")
	}
	payload.ActionID = binary.BigEndian.Uint16(data[cursor : cursor+2])
	cursor += 2

	if payload.Args, cursor, err = readLengthPrefixed(data, cursor); err != nil {
		return nil, fmt.Errorf("decode args: %w", err)
	}
	if payload.Result, cursor, err = readLengthPrefixed(data, cursor); err != nil {
		return nil, fmt.Errorf("decode result: %w", err)
	}

	if cursor != len(data) {
		return nil, fmt.Errorf("canonical payload has %d trailing bytes", len(data)-cursor)
	}

	payload.raw = append(payload.raw[:0], data...) // ensure private copy
	return payload, nil
}

// SigningBytes returns the backing canonical bytes that must be covered by the
// validator's signature (fields 1 through 8). Callers should treat the slice as
// immutable.
func (p *CanonicalPayload) SigningBytes() []byte {
	return p.raw
}

// SigningDigest computes sha256(SigningBytes()) to match the on-chain verifier
// expectations. The digest is returned as a value to prevent accidental reuse of
// the backing slice.
func (p *CanonicalPayload) SigningDigest() [sha256.Size]byte {
	return sha256.Sum256(p.SigningBytes())
}

// readLengthPrefixed decodes a little-endian uint32 length followed by that many bytes.
func readLengthPrefixed(data []byte, cursor int) ([]byte, int, error) {
	if len(data) < cursor+4 {
		return nil, cursor, fmt.Errorf("truncated length prefix at offset %d", cursor)
	}

	length := binary.LittleEndian.Uint32(data[cursor : cursor+4])
	cursor += 4

	if len(data) < cursor+int(length) {
		return nil, cursor, fmt.Errorf("declared length %d exceeds remaining %d bytes", length, len(data)-cursor)
	}

	chunk := data[cursor : cursor+int(length)]
	cursor += int(length)
	return bytes.Clone(chunk), cursor, nil
}
