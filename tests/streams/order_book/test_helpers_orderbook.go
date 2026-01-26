//go:build kwiltest

package order_book

import (
	"fmt"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/trufnetwork/node/extensions/tn_utils"
)

// encodeQueryComponentsForTests encodes query components using ABI format for testing
// This is a shared helper for all order_book tests
// If argsBytes is provided, it uses those args; otherwise creates default args for get_record
func encodeQueryComponentsForTests(dataProvider, streamID, actionID string, argsBytes []byte) ([]byte, error) {
	// If args not provided, encode default args for get_record action
	if argsBytes == nil {
		// For get_record action, args are: (data_provider, stream_id, from, to, frozen_at, use_cache)
		var err error
		argsBytes, err = tn_utils.EncodeActionArgs([]any{
			dataProvider,       // data_provider (TEXT)
			streamID,           // stream_id (TEXT)
			int64(0),           // from (INT8)
			int64(999999999),   // to (INT8) - far future
			nil,                // frozen_at (INT8, nullable)
			false,              // use_cache (BOOL)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to encode action args: %w", err)
		}
	}

	// Now encode the full query components as ABI
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create address type: %w", err)
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes32 type: %w", err)
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create string type: %w", err)
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes type: %w", err)
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
	encoded, err := abiArgs.Pack(dpAddr, streamIDBytes, actionID, argsBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to pack ABI: %w", err)
	}

	return encoded, nil
}
