//go:build kwiltest

package tn_utils

import (
	"testing"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestDecodeQueryComponents(t *testing.T) {
	t.Run("ValidComponents_DecodesCorrectly", func(t *testing.T) {
		dataProvider := "0x1111111111111111111111111111111111111111"
		streamID := "stbtcusd00000000000000000000000000" // Exactly 32 chars
		actionID := "price_above_threshold"
		args := []byte{0x01, 0x02, 0x03, 0x04}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, args)
		require.NoError(t, err)

		var resDataProvider []byte
		var resStreamID []byte
		var resActionID string
		var resArgs []byte

		err = decodeQueryComponentsHandler(nil, nil, []any{queryComponents}, func(result []any) error {
			resDataProvider = result[0].([]byte)
			resStreamID = result[1].([]byte)
			resActionID = result[2].(string)
			resArgs = result[3].([]byte)
			return nil
		})

		require.NoError(t, err)
		
		// Verify data provider (as hex for comparison)
		require.Equal(t, gethCommon.HexToAddress(dataProvider).Bytes(), resDataProvider)
		
		// Verify stream ID
		var expectedStreamID [32]byte
		copy(expectedStreamID[:], []byte(streamID))
		require.Equal(t, expectedStreamID[:], resStreamID)
		
		// Verify action ID and args
		require.Equal(t, actionID, resActionID)
		require.Equal(t, args, resArgs)
	})

	t.Run("EmptyQueryComponents_ReturnsError", func(t *testing.T) {
		err := decodeQueryComponentsHandler(nil, nil, []any{[]byte{}}, func(result []any) error {
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "query_components cannot be empty")
	})

	t.Run("InvalidABIEncoding_ReturnsError", func(t *testing.T) {
		invalidABI := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		err := decodeQueryComponentsHandler(nil, nil, []any{invalidABI}, func(result []any) error {
			return nil
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to decode query_components")
	})
}
