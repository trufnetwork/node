// Package tn_utils provides reusable precompiles for dynamic dispatch and
// canonical serialization helpers.
//
// This package leverages Kwil's native encoding (types.EncodeValue) to ensure
// byte-for-byte identical serialization across all validators, supporting all
// Kwil data types automatically.
//
// Security Note:
// The call_dispatch precompile does NOT enforce any access control or allowlists.
// Callers are responsible for validating which actions can be dispatched before
// invoking this precompile.
//
// Encoding Format:
// Uses Kwil's types.EncodedValue format which includes type information and deterministic
// byte ordering (BigEndian for integers, length-prefixed for variable-length data).
package tn_utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
)

// EncodeActionArgs encodes action arguments into canonical bytes using Kwil's native encoding.
// Each argument is encoded with its type information for deterministic execution.
//
// Format: [arg_count:uint32][encoded_arg1][encoded_arg2]...
// Where each encoded_arg uses types.EncodedValue.MarshalBinary() format
//
// Supported types (via types.EncodeValue):
//   - nil
//   - int, int8, int16, int32, int64, uint, uint16, uint32, uint64
//   - string
//   - []byte
//   - bool
//   - [16]byte, types.UUID (UUID)
//   - types.Decimal
//   - Arrays of the above types (e.g., []string, []int64)
//
// Returns an error if any argument cannot be encoded by Kwil's type system.
func EncodeActionArgs(args []any) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write argument count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(args))); err != nil {
		return nil, fmt.Errorf("failed to write arg count: %w", err)
	}

	// Encode each argument using Kwil's native encoding
	for i, arg := range args {
		encodedVal, err := types.EncodeValue(arg)
		if err != nil {
			return nil, fmt.Errorf("failed to encode arg %d: %w", i, err)
		}

		// Serialize the EncodedValue
		argBytes, err := encodedVal.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal arg %d: %w", i, err)
		}

		// Write length-prefixed argument bytes
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(argBytes))); err != nil {
			return nil, fmt.Errorf("failed to write arg %d length: %w", i, err)
		}
		if _, err := buf.Write(argBytes); err != nil {
			return nil, fmt.Errorf("failed to write arg %d bytes: %w", i, err)
		}
	}

	return buf.Bytes(), nil
}

// DecodeActionArgs decodes canonical bytes back into action arguments.
// This is the inverse operation of EncodeActionArgs.
//
// Returns an error if:
//   - Data is too short to contain arg count
//   - Individual arguments fail to decode
//   - Type information is invalid
func DecodeActionArgs(data []byte) ([]any, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for arg count")
	}

	buf := bytes.NewReader(data)

	// Read argument count
	var argCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &argCount); err != nil {
		return nil, fmt.Errorf("failed to read arg count: %w", err)
	}

	args := make([]any, argCount)

	// Decode each argument
	for i := uint32(0); i < argCount; i++ {
		// Read argument length
		var argLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &argLen); err != nil {
			return nil, fmt.Errorf("failed to read arg %d length: %w", i, err)
		}

		// Read argument bytes
		argBytes := make([]byte, argLen)
		if _, err := io.ReadFull(buf, argBytes); err != nil {
			return nil, fmt.Errorf("failed to read arg %d bytes: %w", i, err)
		}

		// Unmarshal EncodedValue
		var encodedVal types.EncodedValue
		if err := encodedVal.UnmarshalBinary(argBytes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal arg %d: %w", i, err)
		}

		// Decode to Go value
		decodedVal, err := encodedVal.Decode()
		if err != nil {
			return nil, fmt.Errorf("failed to decode arg %d value: %w", i, err)
		}

		args[i] = decodedVal
	}

	return args, nil
}

// EncodeQueryResultCanonical encodes query results into canonical bytes using Kwil's native encoding.
// All validators executing the same query will produce identical bytes.
//
// Format: [row_count:uint32][row1][row2]...
// Each row: [col_count:uint32][encoded_col1][encoded_col2]...
// Where each encoded_col uses types.EncodeValue format
//
// The column order follows the query result set order. Kwil enforces deterministic ordering
// by automatically adding ORDER BY clauses to queries when needed.
//
// Returns an error if:
//   - Any value has an unsupported type
//   - Encoding operations fail
func EncodeQueryResultCanonical(rows []*common.Row) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write row count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(rows))); err != nil {
		return nil, fmt.Errorf("failed to write row count: %w", err)
	}

	// Encode each row
	for i, row := range rows {
		// Write column count
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(row.Values))); err != nil {
			return nil, fmt.Errorf("failed to write col count for row %d: %w", i, err)
		}

		// Encode each column value using Kwil's native encoding
		for j, value := range row.Values {
			encodedVal, err := types.EncodeValue(value)
			if err != nil {
				return nil, fmt.Errorf("failed to encode col %d of row %d: %w", j, i, err)
			}

			// Serialize the EncodedValue
			colBytes, err := encodedVal.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %d of row %d: %w", j, i, err)
			}

			// Write length-prefixed column bytes
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(colBytes))); err != nil {
				return nil, fmt.Errorf("failed to write col %d length for row %d: %w", j, i, err)
			}
			if _, err := buf.Write(colBytes); err != nil {
				return nil, fmt.Errorf("failed to write col %d bytes for row %d: %w", j, i, err)
			}
		}
	}

	return buf.Bytes(), nil
}

// DecodeQueryResultCanonical decodes canonical bytes back into query results.
// This is the inverse operation of EncodeQueryResultCanonical.
func DecodeQueryResultCanonical(data []byte) ([]*common.Row, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for row count")
	}

	buf := bytes.NewReader(data)

	// Read row count
	var rowCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &rowCount); err != nil {
		return nil, fmt.Errorf("failed to read row count: %w", err)
	}

	rows := make([]*common.Row, rowCount)

	for i := uint32(0); i < rowCount; i++ {
		row := &common.Row{}

		var colCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &colCount); err != nil {
			return nil, fmt.Errorf("failed to read col count for row %d: %w", i, err)
		}

		for j := uint32(0); j < colCount; j++ {
			var colLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &colLen); err != nil {
				return nil, fmt.Errorf("failed to read col %d length for row %d: %w", j, i, err)
			}

			colBytes := make([]byte, colLen)
			if _, err := io.ReadFull(buf, colBytes); err != nil {
				return nil, fmt.Errorf("failed to read col %d bytes for row %d: %w", j, i, err)
			}

			var encodedVal types.EncodedValue
			if err := encodedVal.UnmarshalBinary(colBytes); err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %d of row %d: %w", j, i, err)
			}

			decodedVal, err := encodedVal.Decode()
			if err != nil {
				return nil, fmt.Errorf("failed to decode col %d value of row %d: %w", j, i, err)
			}

			row.Values = append(row.Values, decodedVal)
		}

		rows[i] = row
	}

	return rows, nil
}
