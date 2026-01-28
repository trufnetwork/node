package tn_utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// buildPrecompile groups all tn_utils methods into a single precompile bundle so
// migrations can bind to them with `USE tn_utils AS ...`. Keeping the definition
// here makes the exported registration code in extension.go tiny and obvious.
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
			canonicalToDataPointsABIMethod(),
			forceLastArgFalseMethod(),
			parseAttestationBooleanMethod(),
			computeAttestationHashMethod(),
		},
	}
}

// callDispatchMethod exposes deterministic metered dispatch to another action.
// Arguments and results are transferred as canonical byte blobs so cross-validator
// comparisons remain byte-for-byte identical.
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

// byteaJoinMethod mirrors the behaviour of the Go canonical encoder: accepts a
// bytea array, tolerates NULL entries, and joins segments with an optional delimiter.
func byteaJoinMethod() precompiles.Method {
	// Mirrors the Go-side canonical concatenation: accepts BYTEA arrays, treats nil
	// chunks/delimiters as empty, and preserves deterministic ordering. We cannot
	// get the same behaviour with SQL's || operator (it is binary-only and NULL
	// propagates), so keeping this precompile ensures SQL migrations stay aligned
	// with the attestation encoder.
	return precompiles.Method{
		Name:            "bytea_join",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("chunks", types.ByteaArrayType, false),
			precompiles.NewPrecompileValue("delimiter", types.ByteaType, true),
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

// byteaLengthPrefixMethod length-prefixes a single chunk using little endian so
// validators can unambiguously slice the serialized payload.
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

// byteaLengthPrefixManyMethod maps length-prefixing across a bytea array; used
// for canonical payload construction where every field needs a length tag.
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

// encodeUintMethod registers a family of fixed-width unsigned integer encoders
// (8/16/32/64 bit). SQL only has signed ints, so we validate ranges before
// emitting the big-endian bytes expected by the attestation format.
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

func canonicalToDataPointsABIMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "canonical_to_datapoints_abi",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("canonical", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("encoded", types.ByteaType, false),
			},
		},
		Handler: canonicalToDataPointsABIHandler,
	}
}

// callDispatchHandler decodes action arguments, executes the target action inside
// the current engine context, canonicalises the resulting rows, and hands the
// bytes back to SQL. Any mismatch in decoding or execution bubbles up as an error.
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

func canonicalToDataPointsABIHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	canonical, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return err
	}
	if canonical == nil {
		canonical = []byte{}
	}

	encoded, err := EncodeDataPointsABI(canonical)
	if err != nil {
		return err
	}
	return resultFn([]any{encoded})
}

// byteaJoinHandler concatenates the provided chunks into a single bytea value,
// normalising nil delimiters/chunks to empty slices to stay deterministic.
func byteaJoinHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	chunks, err := toByteSliceArray(inputs[0])
	if err != nil {
		return err
	}

	delimiter, err := toByteSliceAllowNil(inputs[1])
	if err != nil {
		return err
	}
	if delimiter == nil {
		delimiter = []byte{}
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

// byteaLengthPrefixHandler prepends a 4-byte big-endian length header to the
// provided chunk and returns the combined byte slice.
func byteaLengthPrefixHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	chunk, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return err
	}

	return resultFn([]any{lengthPrefixBytes(chunk)})
}

// byteaLengthPrefixManyHandler applies lengthPrefixBytes to each element in a
// bytea array, returning the transformed array.
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

// encodeUintHandler validates the integer fits within the target width and
// serialises it using big-endian order. It supports the four unsigned widths
// needed for the canonical payload.
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
	binary.BigEndian.PutUint32(prefixed[:4], uint32(len(chunk)))
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

// forceLastArgFalseMethod decodes args, forces last parameter to false, and re-encodes.
// This is specifically for forcing use_cache=false in attestation query actions.
func forceLastArgFalseMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "force_last_arg_false",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("args_bytes", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("modified_args_bytes", types.ByteaType, false),
			},
		},
		Handler: forceLastArgFalseHandler,
	}
}

// forceLastArgFalseHandler decodes args, sets last param to false, re-encodes.
func forceLastArgFalseHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	argsBytes, ok := inputs[0].([]byte)
	if !ok {
		return fmt.Errorf("args_bytes must be []byte, got %T", inputs[0])
	}

	// Decode arguments
	args, err := DecodeActionArgs(argsBytes)
	if err != nil {
		return fmt.Errorf("failed to decode action args: %w", err)
	}

	// If no args or only one arg, return unchanged
	if len(args) == 0 {
		return resultFn([]any{argsBytes})
	}

	// Force last parameter to false (for use_cache)
	args[len(args)-1] = false

	// Re-encode modified args
	modifiedArgsBytes, err := EncodeActionArgs(args)
	if err != nil {
		return fmt.Errorf("failed to encode modified args: %w", err)
	}

	return resultFn([]any{modifiedArgsBytes})
}

// parseAttestationBooleanMethod extracts a boolean result from an attestation's
// result_canonical field. This is used for prediction market settlement where
// attestations return boolean outcomes (YES=true, NO=false).
func parseAttestationBooleanMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "parse_attestation_boolean",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("result_canonical", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("outcome", types.BoolType, false),
			},
		},
		Handler: parseAttestationBooleanHandler,
	}
}

// parseAttestationBooleanHandler parses result_canonical to extract a boolean outcome.
//
// This handler supports both:
// 1. Binary action results (action_id 6-9): Direct boolean encoded as abi.encode(bool)
// 2. Numeric results (action_id 1-5): Interpreted as value > 0 = TRUE, value == 0 = FALSE
//
// The result_canonical format is:
//   - version (uint8, 1 byte)
//   - algo (uint8, 1 byte)
//   - height (uint64, 8 bytes)
//   - length_prefix(data_provider) (4 bytes length + N bytes data)
//   - length_prefix(stream) (4 bytes length + N bytes data)
//   - action_id (uint16, 2 bytes)
//   - length_prefix(args) (4 bytes length + N bytes data)
//   - length_prefix(result_payload) (4 bytes length + N bytes data)
//
// We parse through the structure to reach result_payload, then decode based on action_id.
func parseAttestationBooleanHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	resultCanonical, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return fmt.Errorf("result_canonical must be bytea: %w", err)
	}
	if resultCanonical == nil || len(resultCanonical) == 0 {
		return fmt.Errorf("result_canonical cannot be empty")
	}

	// Parse the canonical format
	offset := 0

	// Skip version (1 byte)
	if offset+1 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for version")
	}
	offset += 1

	// Skip algo (1 byte)
	if offset+1 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for algo")
	}
	offset += 1

	// Skip height (8 bytes, big-endian uint64)
	if offset+8 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for height")
	}
	offset += 8

	// Skip length_prefix(data_provider)
	if offset+4 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for data_provider length")
	}
	dpLength := binary.BigEndian.Uint32(resultCanonical[offset : offset+4])
	offset += 4 + int(dpLength)
	if offset > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: data_provider data extends beyond buffer")
	}

	// Skip length_prefix(stream)
	if offset+4 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for stream length")
	}
	streamLength := binary.BigEndian.Uint32(resultCanonical[offset : offset+4])
	offset += 4 + int(streamLength)
	if offset > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: stream data extends beyond buffer")
	}

	// Read action_id (2 bytes, big-endian uint16) - we need this to determine decoding format
	if offset+2 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for action_id")
	}
	actionID := binary.BigEndian.Uint16(resultCanonical[offset : offset+2])
	offset += 2

	// Skip length_prefix(args)
	if offset+4 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for args length")
	}
	argsLength := binary.BigEndian.Uint32(resultCanonical[offset : offset+4])
	offset += 4 + int(argsLength)
	if offset > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: args data extends beyond buffer")
	}

	// Read length_prefix(result_payload)
	if offset+4 > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: too short for result_payload length")
	}
	resultLength := binary.BigEndian.Uint32(resultCanonical[offset : offset+4])
	offset += 4
	if offset+int(resultLength) > len(resultCanonical) {
		return fmt.Errorf("invalid result_canonical: result_payload data extends beyond buffer")
	}

	resultPayload := resultCanonical[offset : offset+int(resultLength)]

	// Check if this is a binary action (action_id 6-9)
	// Binary actions return abi.encode(bool) directly
	if IsBinaryAction(actionID) {
		return parseBinaryActionResult(resultPayload, resultFn)
	}

	// Validate action_id is in supported range before decoding
	if actionID < 1 || actionID > 9 {
		return fmt.Errorf("unsupported action_id %d", actionID)
	}

	// Numeric action (action_id 1-5) - decode as abi.encode(uint256[], int256[])
	return parseNumericActionResult(resultPayload, resultFn)
}

// parseBinaryActionResult decodes abi.encode(bool) and returns the boolean directly
func parseBinaryActionResult(resultPayload []byte, resultFn func([]any) error) error {
	// ABI-encoded bool is 32 bytes (padded)
	if len(resultPayload) != 32 {
		return fmt.Errorf("binary action result must be 32 bytes (abi-encoded bool), got %d", len(resultPayload))
	}

	// Decode using the boolean ABI args
	decoded, err := booleanABIArgs.Unpack(resultPayload)
	if err != nil {
		return fmt.Errorf("failed to decode boolean ABI result: %w", err)
	}

	if len(decoded) != 1 {
		return fmt.Errorf("expected 1 value from boolean decode, got %d", len(decoded))
	}

	outcome, ok := decoded[0].(bool)
	if !ok {
		return fmt.Errorf("decoded value is not boolean, got %T", decoded[0])
	}

	return resultFn([]any{outcome})
}

// parseNumericActionResult decodes abi.encode(uint256[], int256[]) and interprets as boolean
// value > 0 = TRUE (YES wins), value == 0 = FALSE (NO wins)
func parseNumericActionResult(resultPayload []byte, resultFn func([]any) error) error {
	decoded, err := dataPointsABIArgs.Unpack(resultPayload)
	if err != nil {
		return fmt.Errorf("failed to decode ABI result payload: %w", err)
	}

	if len(decoded) != 2 {
		return fmt.Errorf("expected 2 arrays (timestamps, values), got %d", len(decoded))
	}

	// Extract values array (second element)
	values, ok := decoded[1].([]*big.Int)
	if !ok {
		return fmt.Errorf("values must be []*big.Int, got %T", decoded[1])
	}

	if len(values) == 0 {
		return fmt.Errorf("result payload contains no values")
	}

	// Use the latest value for settlement (last element)
	// Prediction market pattern: value > 0 = YES (TRUE), value == 0 = NO (FALSE)
	latestValue := values[len(values)-1]
	outcome := latestValue.Sign() > 0

	return resultFn([]any{outcome})
}

// computeAttestationHashMethod computes the attestation-format hash from ABI-encoded query components.
// This ensures market hashes match attestation hashes, enabling automatic settlement.
func computeAttestationHashMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "compute_attestation_hash",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("query_components", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: true,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("hash", types.ByteaType, false),
			},
		},
		Handler: computeAttestationHashHandler,
	}
}

// computeAttestationHashHandler decodes ABI-encoded query components and computes
// the attestation hash using the same format as request_attestation.
func computeAttestationHashHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	queryComponents, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return err
	}

	if len(queryComponents) == 0 {
		return fmt.Errorf("query_components cannot be empty")
	}

	// Define ABI type for query components: (address, bytes32, string, bytes)
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return fmt.Errorf("failed to create address type: %w", err)
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return fmt.Errorf("failed to create bytes32 type: %w", err)
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return fmt.Errorf("failed to create string type: %w", err)
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return fmt.Errorf("failed to create bytes type: %w", err)
	}

	args := gethAbi.Arguments{
		{Type: addressType, Name: "data_provider"},
		{Type: bytes32Type, Name: "stream_id"},
		{Type: stringType, Name: "action_id"},
		{Type: bytesType, Name: "args"},
	}

	// Decode ABI
	decoded, err := args.Unpack(queryComponents)
	if err != nil {
		return fmt.Errorf("failed to decode query_components (expected ABI-encoded (address,bytes32,string,bytes)): %w", err)
	}

	if len(decoded) != 4 {
		return fmt.Errorf("expected 4 components, got %d", len(decoded))
	}

	// Extract components
	dataProvider, ok := decoded[0].(gethCommon.Address)
	if !ok {
		return fmt.Errorf("data_provider must be address, got %T", decoded[0])
	}

	streamID, ok := decoded[1].([32]byte)
	if !ok {
		return fmt.Errorf("stream_id must be bytes32, got %T", decoded[1])
	}

	actionIDStr, ok := decoded[2].(string)
	if !ok {
		return fmt.Errorf("action_id must be string, got %T", decoded[2])
	}

	argsBytes, ok := decoded[3].([]byte)
	if !ok {
		return fmt.Errorf("args must be bytes, got %T", decoded[3])
	}

	// Map action_id string to uint16 (must match attestation_actions table)
	actionIDNum, err := getActionIDNumber(actionIDStr)
	if err != nil {
		return fmt.Errorf("invalid action_id: %w", err)
	}

	// Build hash input using attestation format
	// Format: version(1) + algo(1) + length_prefix(data_provider) + length_prefix(stream_id) + action_id(2) + length_prefix(args)
	buffer := new(bytes.Buffer)

	// Version (1 byte) - always 0x01
	buffer.WriteByte(1)

	// Algorithm (1 byte) - always 0x00
	buffer.WriteByte(0)

	// Length-prefixed data_provider (20 bytes)
	dataProviderBytes := dataProvider.Bytes()
	buffer.Write(lengthPrefixBytes(dataProviderBytes))

	// Length-prefixed stream_id (32 bytes)
	buffer.Write(lengthPrefixBytes(streamID[:]))

	// Action ID as uint16 big-endian (2 bytes)
	var actionIDBytes [2]byte
	binary.BigEndian.PutUint16(actionIDBytes[:], actionIDNum)
	buffer.Write(actionIDBytes[:])

	// Length-prefixed args
	buffer.Write(lengthPrefixBytes(argsBytes))

	// Compute SHA256 hash
	hash := sha256.Sum256(buffer.Bytes())

	return resultFn([]any{hash[:]})
}

// getActionIDNumber maps action name to numeric ID (must match attestation_actions table)
func getActionIDNumber(actionName string) (uint16, error) {
	actionMap := map[string]uint16{
		// Numeric data actions (return TABLE(event_time INT8, value NUMERIC))
		"get_record":           1,
		"get_index":            2,
		"get_change_over_time": 3,
		"get_last_record":      4,
		"get_first_record":     5,
		// Binary actions (return TABLE(result BOOLEAN)) - for prediction market settlement
		"price_above_threshold": 6,
		"price_below_threshold": 7,
		"value_in_range":        8,
		"value_equals":          9,
	}

	id, ok := actionMap[actionName]
	if !ok {
		return 0, fmt.Errorf("unknown action: %s (valid actions: get_record, get_index, get_change_over_time, get_last_record, get_first_record, price_above_threshold, price_below_threshold, value_in_range, value_equals)", actionName)
	}
	return id, nil
}

// IsBinaryAction returns true if the action ID corresponds to a binary action
// that returns TABLE(result BOOLEAN) instead of TABLE(event_time INT8, value NUMERIC)
func IsBinaryAction(actionID uint16) bool {
	return actionID >= 6 && actionID <= 9
}
