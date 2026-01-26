//go:build kwiltest

package tn_utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

// TestComputeAttestationHash tests the compute_attestation_hash precompile
func TestComputeAttestationHash(t *testing.T) {
	t.Run("ValidComponents_ProducesCorrectHash", func(t *testing.T) {
		// Encode valid query components
		dataProvider := "0x1111111111111111111111111111111111111111"
		streamID := "stbtcusd00000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		args := []byte{0x00, 0x00, 0x00, 0x20} // Empty ABI-encoded bytes

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, args)
		require.NoError(t, err)

		// Call the handler
		var resultHash []byte
		err = computeAttestationHashHandler(nil, nil, []any{queryComponents}, func(result []any) error {
			resultHash = result[0].([]byte)
			return nil
		})

		require.NoError(t, err)
		require.Len(t, resultHash, 32, "hash should be 32 bytes")
	})

	t.Run("SameInputs_ProduceSameHash", func(t *testing.T) {
		// Test determinism
		dataProvider := "0x2222222222222222222222222222222222222222"
		streamID := "stteststream0000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		args := []byte{0x01, 0x02, 0x03}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, args)
		require.NoError(t, err)

		// Compute hash twice
		var hash1, hash2 []byte
		err = computeAttestationHashHandler(nil, nil, []any{queryComponents}, func(result []any) error {
			hash1 = result[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		err = computeAttestationHashHandler(nil, nil, []any{queryComponents}, func(result []any) error {
			hash2 = result[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, hash1, hash2, "same inputs should produce same hash")
	})

	t.Run("DifferentInputs_ProduceDifferentHashes", func(t *testing.T) {
		// Test that different components produce different hashes
		dataProvider := "0x3333333333333333333333333333333333333333"
		streamID1 := "ststream100000000000000000000000000" // Exactly 32 chars
		streamID2 := "ststream200000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		args := []byte{0x00}

		components1, err := encodeQueryComponents(dataProvider, streamID1, actionID, args)
		require.NoError(t, err)

		components2, err := encodeQueryComponents(dataProvider, streamID2, actionID, args)
		require.NoError(t, err)

		var hash1, hash2 []byte
		err = computeAttestationHashHandler(nil, nil, []any{components1}, func(result []any) error {
			hash1 = result[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		err = computeAttestationHashHandler(nil, nil, []any{components2}, func(result []any) error {
			hash2 = result[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		require.NotEqual(t, hash1, hash2, "different stream IDs should produce different hashes")
	})

	t.Run("EmptyQueryComponents_ReturnsError", func(t *testing.T) {
		err := computeAttestationHashHandler(nil, nil, []any{[]byte{}}, func(result []any) error {
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "query_components cannot be empty")
	})

	t.Run("InvalidABIEncoding_ReturnsError", func(t *testing.T) {
		// Pass invalid ABI data
		invalidABI := []byte{0xFF, 0xFF, 0xFF, 0xFF}

		err := computeAttestationHashHandler(nil, nil, []any{invalidABI}, func(result []any) error {
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to decode query_components")
	})

	t.Run("UnknownActionID_ReturnsError", func(t *testing.T) {
		// Encode with invalid action_id
		dataProvider := "0x4444444444444444444444444444444444444444"
		streamID := "sttest00000000000000000000000000000" // Exactly 32 chars
		invalidActionID := "unknown_action"
		args := []byte{0x00}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, invalidActionID, args)
		require.NoError(t, err)

		err = computeAttestationHashHandler(nil, nil, []any{queryComponents}, func(result []any) error {
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown action")
	})

	t.Run("KnownGoodTestVector", func(t *testing.T) {
		// Use a known good test vector to ensure hash computation matches expected format
		dataProvider := "0x1111111111111111111111111111111111111111"
		streamID := "stbtcusd00000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		args := []byte{0x00, 0x00, 0x00, 0x20} // Empty ABI-encoded bytes

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, args)
		require.NoError(t, err)

		// Compute hash
		var computedHash []byte
		err = computeAttestationHashHandler(nil, nil, []any{queryComponents}, func(result []any) error {
			computedHash = result[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		// Manually compute expected hash using attestation format
		expectedHash := computeExpectedAttestationHash(t, dataProvider, streamID, actionID, args)

		require.Equal(t, expectedHash, computedHash, "computed hash should match expected attestation format")
	})

	t.Run("AllSupportedActions_ProduceValidHashes", func(t *testing.T) {
		// Test all supported action IDs
		supportedActions := []string{
			"get_record",
			"get_index",
			"get_change_over_time",
			"get_last_record",
			"get_first_record",
		}

		dataProvider := "0x5555555555555555555555555555555555555555"
		streamID := "sttest00000000000000000000000000000" // Exactly 32 chars
		args := []byte{0x00}

		for _, actionID := range supportedActions {
			t.Run(actionID, func(t *testing.T) {
				queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, args)
				require.NoError(t, err)

				var hash []byte
				err = computeAttestationHashHandler(nil, nil, []any{queryComponents}, func(result []any) error {
					hash = result[0].([]byte)
					return nil
				})

				require.NoError(t, err, "action %s should succeed", actionID)
				require.Len(t, hash, 32, "hash should be 32 bytes for action %s", actionID)
			})
		}
	})
}

// Helper function to encode query components using ABI
func encodeQueryComponents(dataProvider, streamID, actionID string, args []byte) ([]byte, error) {
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, err
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, err
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, err
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, err
	}

	abiArgs := gethAbi.Arguments{
		{Type: addressType, Name: "data_provider"},
		{Type: bytes32Type, Name: "stream_id"},
		{Type: stringType, Name: "action_id"},
		{Type: bytesType, Name: "args"},
	}

	// Convert data provider to address
	dpAddr := gethCommon.HexToAddress(dataProvider)

	// Convert stream ID to bytes32 (pad with zeros on the right)
	var streamIDBytes [32]byte
	copy(streamIDBytes[:], []byte(streamID))

	// Pack the ABI
	encoded, err := abiArgs.Pack(dpAddr, streamIDBytes, actionID, args)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

// Helper function to compute expected hash using attestation format
// This replicates the logic in computeAttestationHashHandler for verification
func computeExpectedAttestationHash(t *testing.T, dataProvider, streamID, actionID string, args []byte) []byte {
	// Parse data provider address
	dpAddr := gethCommon.HexToAddress(dataProvider)

	// Convert stream ID to bytes32
	var streamIDBytes [32]byte
	copy(streamIDBytes[:], []byte(streamID))

	// Map action ID to number
	actionMap := map[string]uint16{
		"get_record":           1,
		"get_index":            2,
		"get_change_over_time": 3,
		"get_last_record":      4,
		"get_first_record":     5,
	}
	actionIDNum, ok := actionMap[actionID]
	require.True(t, ok, "unknown action_id: %s", actionID)

	// Build hash input
	buffer := new(bytes.Buffer)

	// Version (1 byte)
	buffer.WriteByte(1)

	// Algorithm (1 byte)
	buffer.WriteByte(0)

	// Length-prefixed data_provider (20 bytes)
	buffer.Write(lengthPrefixBytes(dpAddr.Bytes()))

	// Length-prefixed stream_id (32 bytes)
	buffer.Write(lengthPrefixBytes(streamIDBytes[:]))

	// Action ID as uint16 big-endian (2 bytes)
	var actionIDBytes [2]byte
	binary.BigEndian.PutUint16(actionIDBytes[:], actionIDNum)
	buffer.Write(actionIDBytes[:])

	// Length-prefixed args
	buffer.Write(lengthPrefixBytes(args))

	// Compute SHA256 hash
	hash := sha256.Sum256(buffer.Bytes())
	return hash[:]
}
