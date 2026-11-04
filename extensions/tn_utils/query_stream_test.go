package tn_utils

import (
	"encoding/binary"
	"encoding/hex"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

const (
	queryStreamProvider = "0x00000000000000000000000000000000000000a1"
	queryStreamID       = "stqueryargs000000000000000000000"
)

func queryStreamProviderBytes(t *testing.T) []byte {
	t.Helper()
	b, err := hex.DecodeString(queryStreamProvider[2:])
	require.NoError(t, err)
	return b
}

func encodeQueryArgs(t *testing.T, args ...any) []byte {
	t.Helper()
	b, err := EncodeActionArgs(args)
	require.NoError(t, err)
	return b
}

func TestValidateQueryStreamAcceptsArgsForTheSameStream(t *testing.T) {
	provider := queryStreamProviderBytes(t)

	for name, argProvider := range map[string]string{
		"lower case":            queryStreamProvider,
		"upper case hex digits": "0x" + strings.ToUpper(queryStreamProvider[2:]),
		"upper case prefix":     "0X" + queryStreamProvider[2:],
	} {
		t.Run(name, func(t *testing.T) {
			args := encodeQueryArgs(t, argProvider, queryStreamID, int64(1500), nil, false)
			require.NoError(t, validateQueryStream(provider, []byte(queryStreamID), args))
		})
	}

	t.Run("two args", func(t *testing.T) {
		args := encodeQueryArgs(t, queryStreamProvider, queryStreamID)
		require.NoError(t, validateQueryStream(provider, []byte(queryStreamID), args))
	})
}

func TestValidateQueryStreamRefusesArgsForAnotherStream(t *testing.T) {
	provider := queryStreamProviderBytes(t)
	otherStream := "stotherstream0000000000000000000"

	// An arg length that runs past the end of the payload.
	longArg := make([]byte, 8)
	binary.LittleEndian.PutUint32(longArg[0:], 1)
	binary.LittleEndian.PutUint32(longArg[4:], 1000)

	cases := []struct {
		name string
		args []byte
		want string
	}{
		{"another stream", encodeQueryArgs(t, queryStreamProvider, otherStream, int64(1500), nil, false), "stream_id in query args"},
		{"stream in another case", encodeQueryArgs(t, queryStreamProvider, strings.ToUpper(queryStreamID), int64(1500)), "stream_id in query args"},
		{"stream with a suffix", encodeQueryArgs(t, queryStreamProvider, queryStreamID+"0", int64(1500)), "stream_id in query args"},
		{"another provider", encodeQueryArgs(t, "0x00000000000000000000000000000000000000b2", queryStreamID, int64(1500)), "data_provider in query args"},
		{"provider without prefix", encodeQueryArgs(t, queryStreamProvider[2:], queryStreamID, int64(1500)), "data_provider in query args"},
		{"provider with a space", encodeQueryArgs(t, queryStreamProvider+" ", queryStreamID, int64(1500)), "data_provider in query args"},
		{"args swapped", encodeQueryArgs(t, queryStreamID, queryStreamProvider, int64(1500)), "data_provider in query args"},
		{"NULL provider", encodeQueryArgs(t, nil, queryStreamID, int64(1500)), "arg 1 (data_provider) must be TEXT, got NULL"},
		{"NULL stream", encodeQueryArgs(t, queryStreamProvider, nil, int64(1500)), "arg 2 (stream_id) must be TEXT, got NULL"},
		{"provider as bytes", encodeQueryArgs(t, provider, queryStreamID), "arg 1 (data_provider) must be TEXT"},
		{"stream as int", encodeQueryArgs(t, queryStreamProvider, int64(7)), "arg 2 (stream_id) must be TEXT"},
		{"one arg", encodeQueryArgs(t, queryStreamProvider), "got 1 args"},
		{"no args", encodeQueryArgs(t), "got 0 args"},
		{"not an args payload", []byte{0x01}, "failed to decode query args"},
		{"empty payload", []byte{}, "failed to decode query args"},
		{"count past the payload", []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "does not fit"},
		{"arg length past the payload", longArg, "exceeds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateQueryStream(provider, []byte(queryStreamID), tc.args)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateQueryStreamRefusesAnIncompleteQuery(t *testing.T) {
	provider := queryStreamProviderBytes(t)
	args := encodeQueryArgs(t, queryStreamProvider, queryStreamID, int64(1500))

	require.ErrorContains(t, validateQueryStream(provider[:19], []byte(queryStreamID), args), "20-byte address")
	require.ErrorContains(t, validateQueryStream(nil, []byte(queryStreamID), args), "20-byte address")
	require.ErrorContains(t, validateQueryStream(provider, nil, args), "stream_id is required")
	require.ErrorContains(t, validateQueryStream(provider, []byte{}, args), "stream_id is required")
}

func TestValidateQueryStreamHandler(t *testing.T) {
	provider := queryStreamProviderBytes(t)
	args := encodeQueryArgs(t, queryStreamProvider, queryStreamID, int64(1500), nil, false)
	noResult := func([]any) error {
		t.Fatal("validate_query_stream returns nothing")
		return nil
	}

	require.NoError(t, validateQueryStreamHandler(nil, nil, []any{provider, []byte(queryStreamID), args}, noResult))

	err := validateQueryStreamHandler(nil, nil, []any{provider, []byte("stotherstream0000000000000000000"), args}, noResult)
	require.ErrorContains(t, err, "stream_id in query args")
}

func TestValidateQueryStreamIsRegistered(t *testing.T) {
	var method *precompiles.Method
	for _, m := range buildPrecompile().Methods {
		if m.Name == "validate_query_stream" {
			m := m
			method = &m
		}
	}
	require.NotNil(t, method, "tn_utils must register validate_query_stream")
	require.ElementsMatch(t, []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC}, method.AccessModifiers)
	require.Nil(t, method.Returns)
	require.Len(t, method.Parameters, 3)
	for _, p := range method.Parameters {
		require.True(t, p.Type.Equals(types.ByteaType), "parameter %s must be BYTEA", p.Name)
		require.False(t, p.Nullable, "parameter %s must not be nullable", p.Name)
	}
}

// decodeQueryArgs has to read a payload the way call_dispatch does, or the
// check would pass args that the action then reads differently.
func TestDecodeQueryArgsMatchesDecodeActionArgs(t *testing.T) {
	decimal, err := types.ParseDecimalExplicit("12.500000000000000000", 36, 18)
	require.NoError(t, err)

	valid := encodeQueryArgs(t, queryStreamProvider, queryStreamID, int64(-5), nil, true, []byte{0x01, 0x02}, decimal, []string{"a", "b"})

	// Counts and lengths that the payload cannot hold, kept small enough for
	// DecodeActionArgs to try them.
	withCount := func(count uint32) []byte {
		p := append([]byte(nil), valid...)
		binary.LittleEndian.PutUint32(p, count)
		return p
	}
	longLength := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(longLength[4:], uint32(len(valid)))

	payloads := map[string][]byte{
		"valid":                   valid,
		"no args":                 encodeQueryArgs(t),
		"fewer args than counted": withCount(9),
		"count past the payload":  withCount(1000),
		"trailing bytes":          append(append([]byte(nil), valid...), 0xde, 0xad),
		"count only":              valid[:4],
		"cut in a value":          valid[:len(valid)-3],
		"long length":             longLength,
		"three bytes":             {0x01, 0x00, 0x00},
		"empty":                   {},
		"not a value":             {0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xff, 0xff},
	}

	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			want, wantErr := DecodeActionArgs(payload)
			got, gotErr := decodeQueryArgs(payload)
			if wantErr != nil {
				require.Error(t, gotErr, "DecodeActionArgs refused this payload: %v", wantErr)
				return
			}
			require.NoError(t, gotErr)
			require.Equal(t, want, got)
		})
	}
}

func TestDecodeQueryArgsRefusesAnOversizedCountWithoutAllocatingForIt(t *testing.T) {
	payload := []byte{0xff, 0xff, 0xff, 0xff}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := decodeQueryArgs(payload)
	runtime.ReadMemStats(&after)

	require.ErrorContains(t, err, "does not fit")
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}
