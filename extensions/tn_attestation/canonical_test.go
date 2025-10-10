package tn_attestation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCanonicalPayload_Success(t *testing.T) {
	version := uint8(1)
	algo := uint8(1)
	height := uint64(12345)
	actionID := uint16(9)
	dataProvider := []byte("provider-1")
	streamID := []byte("stream-xyz")
	args := []byte{0x01, 0x02, 0x03}
	result := []byte{0xAA, 0xBB}

	raw := buildCanonical(version, algo, height, dataProvider, streamID, actionID, args, result)

	payload, err := ParseCanonicalPayload(raw)
	require.NoError(t, err)
	require.NotNil(t, payload)

	require.Equal(t, version, payload.Version)
	require.Equal(t, algo, payload.Algorithm)
	require.Equal(t, height, payload.BlockHeight)
	require.Equal(t, dataProvider, payload.DataProvider)
	require.Equal(t, streamID, payload.StreamID)
	require.Equal(t, actionID, payload.ActionID)
	require.Equal(t, args, payload.Args)
	require.Equal(t, result, payload.Result)

	// Signing digest should equal sha256(raw)
	expectedDigest := sha256.Sum256(raw)
	require.Equal(t, expectedDigest, payload.SigningDigest())
	require.True(t, bytes.Equal(raw, payload.SigningBytes()))
}

func TestParseCanonicalPayload_TruncatedPrefix(t *testing.T) {
	base := buildCanonical(1, 1, 1, []byte("a"), []byte("b"), 1, []byte{0x01}, []byte{0x02})
	// Corrupt by chopping last byte
	corrupted := base[:len(base)-1]

	_, err := ParseCanonicalPayload(corrupted)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode result")
}

func TestParseCanonicalPayload_ExtraBytes(t *testing.T) {
	base := buildCanonical(1, 1, 1, []byte("a"), []byte("b"), 1, []byte{0x01}, []byte{0x02})
	extra := append(base, []byte{0xFF, 0xFF}...)

	_, err := ParseCanonicalPayload(extra)
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing bytes")
}

// buildCanonical mirrors the SQL encoder to generate canonical payloads.
func buildCanonical(version, algo uint8, height uint64, provider, stream []byte, actionID uint16, args, result []byte) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(version)
	buf.WriteByte(algo)

	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, height)
	buf.Write(heightBytes)

	writeLengthPrefixed(buf, provider)
	writeLengthPrefixed(buf, stream)

	actionBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(actionBytes, actionID)
	buf.Write(actionBytes)

	writeLengthPrefixed(buf, args)
	writeLengthPrefixed(buf, result)

	return buf.Bytes()
}

func writeLengthPrefixed(buf *bytes.Buffer, chunk []byte) {
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(len(chunk)))
	buf.Write(lengthBytes)
	buf.Write(chunk)
}
