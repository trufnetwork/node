package tn_utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

func TestEncodeDecodeBasicTypes(t *testing.T) {
	original := []any{
		int64(42),
		int32(100),
		"hello",
		[]byte("world"),
		true,
		nil,
	}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

func TestEncodeDecodeArrays(t *testing.T) {
	original := []any{
		[]string{"hello", "world"},
		[]int64{1, 2, 3},
		[]bool{true, false, true},
	}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

func TestEncodeDecodeDecimal(t *testing.T) {
	original := []any{types.MustParseDecimal("123.456")}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

func TestEncodeDecodeUUID(t *testing.T) {
	uuid := types.UUID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	original := []any{uuid}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

func TestDeterministicEncoding(t *testing.T) {
	original := []any{int64(42), "hello", []byte("world")}

	encoded1, err := EncodeActionArgs(original)
	require.NoError(t, err)

	encoded2, err := EncodeActionArgs(original)
	require.NoError(t, err)

	assert.Equal(t, encoded1, encoded2)
}

func TestEmptyArgs(t *testing.T) {
	original := []any{}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

func TestLargeArray(t *testing.T) {
	original := []any{
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
	}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

func TestMixedTypes(t *testing.T) {
	dec := types.MustParseDecimal("999.999")
	uuid := types.UUID([16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1})

	original := []any{
		int64(-42),
		int32(-100),
		uint64(1234567890123456789),
		"complex string with émojis 🚀",
		[]byte{0x00, 0x01, 0x02, 0x03, 0xFF},
		true,
		false,
		nil,
		dec,
		uuid,
		[]int64{-1, 0, 1},
		[]string{"", "non-empty"},
		[][]byte{{}, {0xFF}},
	}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, normalizeDecodedSlice(original), normalizeDecodedSlice(decoded))
}

// Helpers -------------------------------------------------------------------

func normalizeDecodedSlice(values []any) []any {
	normalized := make([]any, len(values))
	for i, v := range values {
		normalized[i] = normalizeDecodedValue(v)
	}
	return normalized
}

func cloneByteSlices(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		if in[i] == nil {
			continue
		}
		out[i] = append([]byte(nil), in[i]...)
	}
	return out
}

func normalizeDecodedValue(val any) any {
	if val == nil {
		return nil
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		// Preserve decimal pointers so callers that rely on mutability keep references.
		if rv.Type().Elem() == reflect.TypeOf(types.Decimal{}) {
			return val
		}
		return normalizeDecodedValue(rv.Elem().Interface())
	}

	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint())
	case reflect.Bool:
		return rv.Bool()
	case reflect.String:
		return rv.String()
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, rv.Len())
			reflect.Copy(reflect.ValueOf(b), rv)
			return b
		}
		normalizedElems := make([]any, rv.Len())
		allString := true
		allInt := true
		allBool := true
		allBytes := true
		for i := 0; i < rv.Len(); i++ {
			norm := normalizeDecodedValue(rv.Index(i).Interface())
			normalizedElems[i] = norm
			switch norm.(type) {
			case string:
				allInt = false
				allBool = false
			case int64:
				allString = false
				allBool = false
			case bool:
				allString = false
				allInt = false
			case []byte:
				allString = false
				allInt = false
				allBool = false
			default:
				allString = false
				allInt = false
				allBool = false
				allBytes = false
			}
			if _, ok := norm.([]byte); !ok {
				allBytes = false
			}
		}
		switch {
		case allString:
			out := make([]string, len(normalizedElems))
			for i, elem := range normalizedElems {
				out[i] = elem.(string)
			}
			return out
		case allInt:
			out := make([]int64, len(normalizedElems))
			for i, elem := range normalizedElems {
				out[i] = elem.(int64)
			}
			return out
		case allBool:
			out := make([]bool, len(normalizedElems))
			for i, elem := range normalizedElems {
				out[i] = elem.(bool)
			}
			return out
		case allBytes:
			out := make([][]byte, len(normalizedElems))
			for i, elem := range normalizedElems {
				out[i] = elem.([]byte)
			}
			return out
		default:
			return normalizedElems
		}
	case reflect.Struct:
		return val
	default:
		return val
	}
}

func TestErrorHandling(t *testing.T) {
	t.Run("empty_data", func(t *testing.T) {
		_, err := DecodeActionArgs([]byte{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})

	t.Run("short_data", func(t *testing.T) {
		_, err := DecodeActionArgs([]byte{0x01, 0x00, 0x00})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})

	t.Run("invalid_length", func(t *testing.T) {
		data := []byte{
			0x01, 0x00, 0x00, 0x00,
			0xFF, 0xFF, 0xFF, 0xFF,
		}
		_, err := DecodeActionArgs(data)
		require.Error(t, err)
	})
}

func TestByteaJoinHandler(t *testing.T) {
	tests := []struct {
		name      string
		chunks    any
		delimiter any
		expected  []byte
	}{
		{
			name:      "basic join",
			chunks:    [][]byte{[]byte("foo"), []byte("bar")},
			delimiter: []byte("|"),
			expected:  []byte("foo|bar"),
		},
		{
			name:      "empty delimiter",
			chunks:    [][]byte{[]byte("a"), []byte("b")},
			delimiter: []byte{},
			expected:  []byte("ab"),
		},
		{
			name:      "single chunk",
			chunks:    [][]byte{[]byte("solo")},
			delimiter: []byte("|"),
			expected:  []byte("solo"),
		},
		{
			name:      "with nil chunks",
			chunks:    []any{[]byte("a"), nil, []byte("c")},
			delimiter: []byte("|"),
			expected:  []byte("a||c"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []any
			err := byteaJoinHandler(nil, nil, []any{tt.chunks, tt.delimiter}, func(out []any) error {
				result = out
				return nil
			})
			require.NoError(t, err)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expected, result[0])
		})
	}
}

func TestByteaJoinHandlerErrors(t *testing.T) {
	t.Run("invalid_chunks_type", func(t *testing.T) {
		err := byteaJoinHandler(nil, nil, []any{123, []byte("|")}, func([]any) error { return nil })
		require.Error(t, err)
	})

	t.Run("invalid_delimiter_type", func(t *testing.T) {
		err := byteaJoinHandler(nil, nil, []any{[][]byte{[]byte("a")}, 123}, func([]any) error { return nil })
		require.Error(t, err)
	})
}

func TestByteaLengthPrefixHandler(t *testing.T) {
	tests := []struct {
		name     string
		chunk    any
		expected []byte
	}{
		{
			name:     "nil chunk",
			chunk:    nil,
			expected: []byte{0, 0, 0, 0},
		},
		{
			name:     "non-empty",
			chunk:    []byte{0x01, 0x02},
			expected: []byte{2, 0, 0, 0, 0x01, 0x02},
		},
		{
			name:     "string input",
			chunk:    "abc",
			expected: []byte{3, 0, 0, 0, 'a', 'b', 'c'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out []any
			err := byteaLengthPrefixHandler(nil, nil, []any{tt.chunk}, func(result []any) error {
				out = result
				return nil
			})
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.Equal(t, tt.expected, out[0])
		})
	}
}

func TestByteaLengthPrefixManyHandler(t *testing.T) {
	input := [][]byte{
		[]byte("a"),
		nil,
		[]byte("bc"),
	}

	var out []any
	err := byteaLengthPrefixManyHandler(nil, nil, []any{input}, func(result []any) error {
		out = result
		return nil
	})
	require.NoError(t, err)
	require.Len(t, out, 1)

	prefixed, ok := out[0].([][]byte)
	require.True(t, ok)
	require.Len(t, prefixed, 3)

	assert.Equal(t, []byte{1, 0, 0, 0, 'a'}, prefixed[0])
	assert.Equal(t, []byte{0, 0, 0, 0}, prefixed[1])
	assert.Equal(t, []byte{2, 0, 0, 0, 'b', 'c'}, prefixed[2])
}

func TestEncodeUintHandlers(t *testing.T) {
	type testCase struct {
		name     string
		handler  precompiles.HandlerFunc
		input    any
		expected []byte
	}

	cases := []testCase{
		{
			name:     "uint8",
			handler:  encodeUintHandler(8),
			input:    int64(255),
			expected: []byte{0xFF},
		},
		{
			name:     "uint16",
			handler:  encodeUintHandler(16),
			input:    int64(0xABCD),
			expected: []byte{0xAB, 0xCD},
		},
		{
			name:     "uint32",
			handler:  encodeUintHandler(32),
			input:    int64(0x01020304),
			expected: []byte{0x01, 0x02, 0x03, 0x04},
		},
		{
			name:     "uint64",
			handler:  encodeUintHandler(64),
			input:    int64(0x0102030405060708),
			expected: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out []any
			err := tc.handler(nil, nil, []any{tc.input}, func(result []any) error {
				out = result
				return nil
			})
			require.NoError(t, err)
			require.Len(t, out, 1)
			assert.Equal(t, tc.expected, out[0])
		})
	}

	t.Run("value exceeds range", func(t *testing.T) {
		err := encodeUintHandler(8)(nil, nil, []any{int64(300)}, func([]any) error { return nil })
		require.Error(t, err)
	})

	t.Run("negative value", func(t *testing.T) {
		err := encodeUintHandler(16)(nil, nil, []any{int64(-1)}, func([]any) error { return nil })
		require.Error(t, err)
	})
}

func TestForceLastArgFalseHandler(t *testing.T) {
	t.Run("forces use_cache to false", func(t *testing.T) {
		// Simulate query action args: (data_provider, stream_id, from, to, frozen_at, use_cache=true)
		original := []interface{}{
			"data_provider",   // data_provider
			"stream_id",       // stream_id
			int64(1704067200), // from
			int64(1735689600), // to
			nil,               // frozen_at
			true,              // use_cache (will be forced to false)
		}

		// Encode args
		argsBytes, err := EncodeActionArgs(original)
		require.NoError(t, err)

		// Force last arg to false via handler
		var result []any
		err = forceLastArgFalseHandler(nil, nil, []any{argsBytes}, func(out []any) error {
			result = out
			return nil
		})
		require.NoError(t, err)
		require.Len(t, result, 1)

		modifiedArgsBytes := result[0].([]byte)

		// Decode to verify use_cache is now false
		decoded, err := DecodeActionArgs(modifiedArgsBytes)
		require.NoError(t, err)
		require.Len(t, decoded, 6)

		// Verify use_cache (last param) is now false (dereference pointer)
		assert.Equal(t, false, *decoded[5].(*bool))
	})

	t.Run("already false remains false", func(t *testing.T) {
		original := []interface{}{
			"provider",
			"stream",
			int64(100),
			int64(200),
			nil,
			false, // already false
		}

		argsBytes, err := EncodeActionArgs(original)
		require.NoError(t, err)

		var result []any
		err = forceLastArgFalseHandler(nil, nil, []any{argsBytes}, func(out []any) error {
			result = out
			return nil
		})
		require.NoError(t, err)

		modifiedArgsBytes := result[0].([]byte)
		decoded, err := DecodeActionArgs(modifiedArgsBytes)
		require.NoError(t, err)

		// Still false
		assert.Equal(t, false, *decoded[5].(*bool))
	})

	t.Run("empty args returns unchanged", func(t *testing.T) {
		emptyArgs, err := EncodeActionArgs([]interface{}{})
		require.NoError(t, err)

		var result []any
		err = forceLastArgFalseHandler(nil, nil, []any{emptyArgs}, func(out []any) error {
			result = out
			return nil
		})
		require.NoError(t, err)

		// Should return unchanged
		assert.Equal(t, emptyArgs, result[0].([]byte))
	})

	t.Run("single arg gets forced to false", func(t *testing.T) {
		original := []interface{}{true}

		argsBytes, err := EncodeActionArgs(original)
		require.NoError(t, err)

		var result []any
		err = forceLastArgFalseHandler(nil, nil, []any{argsBytes}, func(out []any) error {
			result = out
			return nil
		})
		require.NoError(t, err)

		modifiedArgsBytes := result[0].([]byte)
		decoded, err := DecodeActionArgs(modifiedArgsBytes)
		require.NoError(t, err)
		require.Len(t, decoded, 1)

		// Should be false now
		assert.Equal(t, false, *decoded[0].(*bool))
	})

	t.Run("invalid bytes returns error", func(t *testing.T) {
		err := forceLastArgFalseHandler(nil, nil, []any{[]byte{0x01, 0x02}}, func([]any) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode action args")
	})

	t.Run("non-bytes input returns error", func(t *testing.T) {
		err := forceLastArgFalseHandler(nil, nil, []any{"not bytes"}, func([]any) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "args_bytes must be []byte")
	})
}
