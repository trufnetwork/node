package tn_utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

func buildPrecompile() precompiles.Precompile {
	return precompiles.Precompile{
		Methods: []precompiles.Method{
			callDispatchMethod(),
			byteaJoinMethod(),
			byteaLengthPrefixMethod(),
			byteaLengthPrefixManyMethod(),
			encodeUintMethod("encode_uint8", 8),
			encodeUintMethod("encode_uint16", 16),
			encodeUintMethod("encode_uint32", 32),
			encodeUintMethod("encode_uint64", 64),
		},
	}
}

func callDispatchMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "call_dispatch",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("action_name", types.TextType, false),
			precompiles.NewPrecompileValue("args_bytes", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("result_bytes", types.ByteaType, false),
			},
		},
		Handler: callDispatchHandler,
	}
}

func byteaJoinMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "bytea_join",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("chunks", types.ByteaArrayType, false),
			precompiles.NewPrecompileValue("delimiter", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("merged", types.ByteaType, false),
			},
		},
		Handler: byteaJoinHandler,
	}
}

func byteaLengthPrefixMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "bytea_length_prefix",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("chunk", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("prefixed", types.ByteaType, false),
			},
		},
		Handler: byteaLengthPrefixHandler,
	}
}

func byteaLengthPrefixManyMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "bytea_length_prefix_many",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("chunks", types.ByteaArrayType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("prefixed_chunks", types.ByteaArrayType, false),
			},
		},
		Handler: byteaLengthPrefixManyHandler,
	}
}

func encodeUintMethod(name string, bits int) precompiles.Method {
	return precompiles.Method{
		Name:            name,
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("value", types.IntType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("bytes", types.ByteaType, false),
			},
		},
		Handler: encodeUintHandler(bits),
	}
}

func callDispatchHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	actionName, ok := inputs[0].(string)
	if !ok {
		return fmt.Errorf("action_name must be string, got %T", inputs[0])
	}

	argsBytes, ok := inputs[1].([]byte)
	if !ok {
		return fmt.Errorf("args_bytes must be []byte, got %T", inputs[1])
	}

	args, err := DecodeActionArgs(argsBytes)
	if err != nil {
		return fmt.Errorf("failed to decode args for action '%s': %w", actionName, err)
	}

	var rows []*common.Row
	_, err = app.Engine.Call(ctx, app.DB, "main", actionName, args, func(row *common.Row) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return fmt.Errorf("action '%s' call failed: %w", actionName, err)
	}

	resultBytes, err := EncodeQueryResultCanonical(rows)
	if err != nil {
		return fmt.Errorf("failed to encode results from action '%s': %w", actionName, err)
	}

	return resultFn([]any{resultBytes})
}

func byteaJoinHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	chunks, err := toByteSliceArray(inputs[0])
	if err != nil {
		return err
	}

	delimiter, err := toByteSlice(inputs[1])
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	for i, chunk := range chunks {
		if i > 0 && len(delimiter) > 0 {
			buf.Write(delimiter)
		}
		if len(chunk) > 0 {
			buf.Write(chunk)
		}
	}

	return resultFn([]any{buf.Bytes()})
}

func byteaLengthPrefixHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	chunk, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return err
	}

	return resultFn([]any{lengthPrefixBytes(chunk)})
}

func byteaLengthPrefixManyHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	chunks, err := toByteSliceArray(inputs[0])
	if err != nil {
		return err
	}

	prefixed := make([][]byte, len(chunks))
	for i, chunk := range chunks {
		prefixed[i] = lengthPrefixBytes(chunk)
	}

	return resultFn([]any{prefixed})
}

func encodeUintHandler(bits int) precompiles.HandlerFunc {
	return func(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
		value, err := toInt64(inputs[0])
		if err != nil {
			return err
		}
		if value < 0 {
			return fmt.Errorf("value must be non-negative, got %d", value)
		}

		var encoded []byte
		switch bits {
		case 8:
			if value > math.MaxUint8 {
				return fmt.Errorf("value %d exceeds uint8 max", value)
			}
			encoded = []byte{byte(value)}
		case 16:
			if value > math.MaxUint16 {
				return fmt.Errorf("value %d exceeds uint16 max", value)
			}
			encoded = make([]byte, 2)
			binary.BigEndian.PutUint16(encoded, uint16(value))
		case 32:
			if value > math.MaxUint32 {
				return fmt.Errorf("value %d exceeds uint32 max", value)
			}
			encoded = make([]byte, 4)
			binary.BigEndian.PutUint32(encoded, uint32(value))
		case 64:
			encoded = make([]byte, 8)
			binary.BigEndian.PutUint64(encoded, uint64(value))
		default:
			return fmt.Errorf("unsupported integer size %d", bits)
		}

		return resultFn([]any{encoded})
	}
}

func toByteSliceArray(value any) ([][]byte, error) {
	switch v := value.(type) {
	case [][]byte:
		return v, nil
	case []any:
		result := make([][]byte, len(v))
		for i, elem := range v {
			if elem == nil {
				result[i] = nil
				continue
			}
			b, err := toByteSlice(elem)
			if err != nil {
				return nil, fmt.Errorf("chunks[%d]: %w", i, err)
			}
			result[i] = b
		}
		return result, nil
	default:
		return nil, fmt.Errorf("chunks must be [][]byte, got %T", value)
	}
}

func toByteSlice(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("expected []byte, got %T", value)
	}
}

func toByteSliceAllowNil(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return toByteSlice(value)
}

func lengthPrefixBytes(chunk []byte) []byte {
	if chunk == nil {
		chunk = []byte{}
	}
	prefixed := make([]byte, 4+len(chunk))
	binary.LittleEndian.PutUint32(prefixed[:4], uint32(len(chunk)))
	copy(prefixed[4:], chunk)
	return prefixed
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d exceeds int64 max", v)
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("expected integer type, got %T", value)
	}
}
