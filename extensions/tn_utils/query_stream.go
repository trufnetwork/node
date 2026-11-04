package tn_utils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// validateQueryStreamMethod checks that a query reads the stream it is for.
//
// An attestation, and a market that settles on one, is identified by a data
// provider, a stream, an action and the action's encoded args. The data
// provider and stream are given beside the args: request_attestation takes them
// as parameters and create_market reads them from the query components header.
// The action reads the stream named by its own first two args. This method
// requires the two to agree, so the stream an attestation or market is shown
// with is the stream its result comes from.
func validateQueryStreamMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "validate_query_stream",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("data_provider", types.ByteaType, false),
			precompiles.NewPrecompileValue("stream_id", types.ByteaType, false),
			precompiles.NewPrecompileValue("args_bytes", types.ByteaType, false),
		},
		Returns: nil, // void — errors if the args name another stream
		Handler: validateQueryStreamHandler,
	}
}

func validateQueryStreamHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	dataProvider, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return fmt.Errorf("data_provider: %w", err)
	}
	streamID, err := toByteSliceAllowNil(inputs[1])
	if err != nil {
		return fmt.Errorf("stream_id: %w", err)
	}
	argsBytes, err := toByteSliceAllowNil(inputs[2])
	if err != nil {
		return fmt.Errorf("args_bytes: %w", err)
	}
	return validateQueryStream(dataProvider, streamID, argsBytes)
}

// validateQueryStream returns an error unless argsBytes decodes and its first
// two args are dataProvider and streamID.
//
// Every attestation action is declared ($data_provider TEXT, $stream_id TEXT,
// ...) and reads LOWER($data_provider). So the first arg matches when it is the
// 0x-prefixed hex of dataProvider in either letter case, and the second when it
// is streamID byte for byte.
func validateQueryStream(dataProvider, streamID, argsBytes []byte) error {
	if len(dataProvider) != 20 {
		return fmt.Errorf("query data_provider must be a 20-byte address, got %d bytes", len(dataProvider))
	}
	if len(streamID) == 0 {
		return fmt.Errorf("query stream_id is required")
	}

	args, err := decodeQueryArgs(argsBytes)
	if err != nil {
		return fmt.Errorf("failed to decode query args: %w", err)
	}
	if len(args) < 2 {
		return fmt.Errorf("query args must start with data_provider and stream_id, got %d args", len(args))
	}

	argProvider, ok := textArg(args[0])
	if !ok {
		return fmt.Errorf("query arg 1 (data_provider) must be TEXT, got %s", argKind(args[0]))
	}
	argStream, ok := textArg(args[1])
	if !ok {
		return fmt.Errorf("query arg 2 (stream_id) must be TEXT, got %s", argKind(args[1]))
	}

	if !isHexAddress(argProvider, dataProvider) {
		return fmt.Errorf("data_provider in query args (%q) differs from the query's data_provider (0x%x)", argProvider, dataProvider)
	}
	if argStream != string(streamID) {
		return fmt.Errorf("stream_id in query args (%q) differs from the query's stream_id (%q)", argStream, streamID)
	}
	return nil
}

// decodeQueryArgs decodes an EncodeActionArgs payload. It accepts the payloads
// DecodeActionArgs accepts and returns the same values for them, and checks
// every count and length against the bytes that are present before it
// allocates anything for them.
func decodeQueryArgs(data []byte) ([]any, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for arg count")
	}
	argCount := binary.LittleEndian.Uint32(data)
	rest := data[4:]

	// Every arg starts with a 4-byte length.
	if uint64(argCount) > uint64(len(rest))/4 {
		return nil, fmt.Errorf("arg count %d does not fit in %d bytes", argCount, len(rest))
	}

	args := make([]any, argCount)
	for i := range args {
		if len(rest) < 4 {
			return nil, fmt.Errorf("failed to read arg %d length", i)
		}
		argLen := binary.LittleEndian.Uint32(rest)
		rest = rest[4:]
		if uint64(argLen) > uint64(len(rest)) {
			return nil, fmt.Errorf("arg %d length %d exceeds the %d bytes left", i, argLen, len(rest))
		}

		var encodedVal types.EncodedValue
		if err := encodedVal.UnmarshalBinary(rest[:argLen:argLen]); err != nil {
			return nil, fmt.Errorf("failed to unmarshal arg %d: %w", i, err)
		}
		rest = rest[argLen:]

		decodedVal, err := encodedVal.Decode()
		if err != nil {
			return nil, fmt.Errorf("failed to decode arg %d value: %w", i, err)
		}
		args[i] = decodedVal
	}

	return args, nil
}

// textArg returns the string held by a decoded TEXT arg. DecodeActionArgs
// returns TEXT as *string, and NULL as nil.
func textArg(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case *string:
		if s == nil {
			return "", false
		}
		return *s, true
	default:
		return "", false
	}
}

func argKind(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%T", v)
}

// isHexAddress reports whether s is addr written as 0x-prefixed hex, in either
// letter case.
func isHexAddress(s string, addr []byte) bool {
	if len(s) != 2+2*len(addr) || (s[:2] != "0x" && s[:2] != "0X") {
		return false
	}
	decoded, err := hex.DecodeString(s[2:])
	return err == nil && bytes.Equal(decoded, addr)
}
