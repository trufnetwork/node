package tn_utils

import (
	"fmt"
	"math/big"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
)

const dataPointTargetScale uint16 = 18

var (
	dataPointsABIArgs gethAbi.Arguments
	booleanABIArgs    gethAbi.Arguments
)

func init() {
	uint256Slice, err := gethAbi.NewType("uint256[]", "", nil)
	if err != nil {
		panic(fmt.Sprintf("tn_utils: failed to initialise uint256[] ABI type: %v", err))
	}
	int256Slice, err := gethAbi.NewType("int256[]", "", nil)
	if err != nil {
		panic(fmt.Sprintf("tn_utils: failed to initialise int256[] ABI type: %v", err))
	}
	dataPointsABIArgs = gethAbi.Arguments{
		{Type: uint256Slice},
		{Type: int256Slice},
	}

	// Boolean ABI type for binary action results
	boolType, err := gethAbi.NewType("bool", "", nil)
	if err != nil {
		panic(fmt.Sprintf("tn_utils: failed to initialise bool ABI type: %v", err))
	}
	booleanABIArgs = gethAbi.Arguments{
		{Type: boolType},
	}
}

// EncodeDataPointsABI converts the canonical result serialization produced by
// call_dispatch into ABI-encoded format.
//
// For numeric data actions (get_record, get_index, etc.):
//   - Returns abi.encode(uint256[] timestamps, int256[] values)
//
// For binary actions (price_above_threshold, value_in_range, etc.):
//   - Returns abi.encode(bool result)
//
// Binary results are auto-detected: single row with single boolean column.
func EncodeDataPointsABI(canonical []byte) ([]byte, error) {
	rows, err := DecodeQueryResultCanonical(canonical)
	if err != nil {
		return nil, err
	}

	// Check if this is a binary action result (single row, single boolean column)
	if isBooleanResult(rows) {
		return encodeBooleanResult(rows[0])
	}

	// Standard numeric datapoints encoding
	return encodeNumericDataPoints(rows)
}

// isBooleanResult checks if the result is from a binary action
// Binary actions return exactly 1 row with 1 boolean column
func isBooleanResult(rows []*common.Row) bool {
	if len(rows) != 1 {
		return false
	}
	if len(rows[0].Values) != 1 {
		return false
	}

	// Check if the value is a boolean
	switch rows[0].Values[0].(type) {
	case bool, *bool:
		return true
	default:
		return false
	}
}

// encodeBooleanResult encodes a single boolean result as abi.encode(bool)
func encodeBooleanResult(row *common.Row) ([]byte, error) {
	var result bool

	switch v := row.Values[0].(type) {
	case bool:
		result = v
	case *bool:
		if v == nil {
			return nil, fmt.Errorf("boolean result is NULL")
		}
		result = *v
	default:
		return nil, fmt.Errorf("expected boolean result, got %T", row.Values[0])
	}

	packed, err := booleanABIArgs.Pack(result)
	if err != nil {
		return nil, fmt.Errorf("abi encode boolean: %w", err)
	}
	return packed, nil
}

// encodeNumericDataPoints encodes numeric datapoints as abi.encode(uint256[], int256[])
func encodeNumericDataPoints(rows []*common.Row) ([]byte, error) {
	timestamps := make([]*big.Int, 0, len(rows))
	values := make([]*big.Int, 0, len(rows))

	for i, row := range rows {
		if len(row.Values) < 2 {
			return nil, fmt.Errorf("row %d: expected at least 2 columns, got %d", i, len(row.Values))
		}

		ts, err := extractTimestamp(row.Values[0], i)
		if err != nil {
			return nil, err
		}
		val, err := extractDataPointValue(row.Values[1], i)
		if err != nil {
			return nil, err
		}

		timestamps = append(timestamps, ts)
		values = append(values, val)
	}

	packed, err := dataPointsABIArgs.Pack(timestamps, values)
	if err != nil {
		return nil, fmt.Errorf("abi encode datapoints: %w", err)
	}
	return packed, nil
}

func extractTimestamp(raw any, rowIdx int) (*big.Int, error) {
	switch v := raw.(type) {
	case *int64:
		if v == nil {
			return nil, fmt.Errorf("row %d: timestamp is NULL", rowIdx)
		}
		if *v < 0 {
			return nil, fmt.Errorf("row %d: timestamp cannot be negative", rowIdx)
		}
		return new(big.Int).SetUint64(uint64(*v)), nil
	case int64:
		if v < 0 {
			return nil, fmt.Errorf("row %d: timestamp cannot be negative", rowIdx)
		}
		return new(big.Int).SetUint64(uint64(v)), nil
	default:
		return nil, fmt.Errorf("row %d: unsupported timestamp type %T", rowIdx, raw)
	}
}

func extractDataPointValue(raw any, rowIdx int) (*big.Int, error) {
	switch v := raw.(type) {
	case *types.Decimal:
		if v == nil {
			return nil, fmt.Errorf("row %d: value is NULL", rowIdx)
		}
		return decimalToScaledInt(v, dataPointTargetScale)
	case types.Decimal:
		return decimalToScaledInt(&v, dataPointTargetScale)
	default:
		return nil, fmt.Errorf("row %d: expected numeric(36,18) value, got %T", rowIdx, raw)
	}
}

func decimalToScaledInt(dec *types.Decimal, targetScale uint16) (*big.Int, error) {
	if dec.Scale() != targetScale {
		return nil, fmt.Errorf("expected decimal scale %d, got %d", targetScale, dec.Scale())
	}

	result := new(big.Int).Set(dec.BigInt())
	if dec.IsNegative() {
		result.Neg(result)
	}
	return result, nil
}
