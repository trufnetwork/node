package tn_utils

import (
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

	assert.Equal(t, original, decoded)
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

	assert.Equal(t, original, decoded)
}

func TestEncodeDecodeDecimal(t *testing.T) {
	dec := &types.Decimal{}
	err := dec.SetString("123.456")
	require.NoError(t, err)

	original := []any{dec}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}

func TestEncodeDecodeUUID(t *testing.T) {
	uuid := types.UUID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	original := []any{uuid}

	encoded, err := EncodeActionArgs(original)
	require.NoError(t, err)

	decoded, err := DecodeActionArgs(encoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
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

	assert.Equal(t, original, decoded)
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

	assert.Equal(t, original, decoded)
}

func TestMixedTypes(t *testing.T) {
	dec := &types.Decimal{}
	err := dec.SetString("999.999")
	require.NoError(t, err)
	uuid := types.UUID([16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1})

	original := []any{
		int64(-42),
		int32(-100),
		uint64(18446744073709551615),
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

	assert.Equal(t, original, decoded)
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
