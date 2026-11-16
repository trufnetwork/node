package tn_utils

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// The outcome actions (ids 13-16, migration 063) end every market: a real YES or NO, or INVALID
// once the print has not arrived by timestamp + window. Their signatures, none with a DEFAULT:
//
//	13 index_change_in_range_v2: data_provider, stream_id, timestamp, window, tolerance,
//	   time_interval, min_change, max_change
//	14 price_above_threshold_v2, 15 price_below_threshold_v2: data_provider, stream_id,
//	   timestamp, window, tolerance, threshold
//	16 value_in_range_v2: data_provider, stream_id, timestamp, window, tolerance, min_value,
//	   max_value
//
// data_provider and stream_id are TEXT; timestamp, window, tolerance and time_interval are INT8
// seconds; thresholds and bounds are NUMERIC(36,18).

// outcomeActionName returns the action name of an outcome action id, for error messages.
func outcomeActionName(actionID uint16) string {
	switch actionID {
	case IndexChangeInRangeV2ActionID:
		return "index_change_in_range_v2"
	case PriceAboveThresholdV2ActionID:
		return "price_above_threshold_v2"
	case PriceBelowThresholdV2ActionID:
		return "price_below_threshold_v2"
	case ValueInRangeV2ActionID:
		return "value_in_range_v2"
	default:
		return fmt.Sprintf("action %d", actionID)
	}
}

// outcomeArgCount returns how many args an outcome action declares.
func outcomeArgCount(actionID uint16) int {
	switch actionID {
	case IndexChangeInRangeV2ActionID:
		return 8
	case PriceAboveThresholdV2ActionID, PriceBelowThresholdV2ActionID:
		return 6
	case ValueInRangeV2ActionID:
		return 7
	default:
		return 0
	}
}

// validateOutcomeActionArgs checks the args of an outcome action before it is dispatched and when
// a market on it is created. The count and types must match the signature exactly, because the
// engine refuses any other when it calls the action, and a market whose query can never run would
// wait forever. The bounds make every market end: the deadline timestamp + window is reachable and
// fits in INT8, and every read stays within the 90-day range rule.
func validateOutcomeActionArgs(ctx *common.EngineContext, actionID uint16, rawArgs any) error {
	name := outcomeActionName(actionID)

	argsBytes, ok := rawArgs.([]byte)
	if !ok {
		return fmt.Errorf("args_bytes must be []byte, got %T", rawArgs)
	}

	// The same decoder validate_query_stream uses.
	args, err := decodeQueryArgs(argsBytes)
	if err != nil {
		return fmt.Errorf("failed to decode action args: %w", err)
	}
	if want := outcomeArgCount(actionID); len(args) != want {
		return fmt.Errorf("%s requires exactly %d args, got %d", name, want, len(args))
	}

	for i, param := range []string{"data_provider", "stream_id"} {
		if _, ok := textArg(args[i]); !ok {
			return fmt.Errorf("%s '%s' must be TEXT, got %s", name, param, argKind(args[i]))
		}
	}

	timestamp, err := outcomeIntArg(name, "timestamp", args[2])
	if err != nil {
		return err
	}
	window, err := outcomeIntArg(name, "window", args[3])
	if err != nil {
		return err
	}
	tolerance, err := outcomeIntArg(name, "tolerance", args[4])
	if err != nil {
		return err
	}

	if timestamp < 0 {
		return fmt.Errorf("%s 'timestamp' must not be negative, got %d", name, timestamp)
	}
	// A read-only call outside a block may carry no block time; only this check needs one.
	if now := outcomeBlockTimestamp(ctx); now > 0 && timestamp > now && timestamp-now > OutcomeMaxSpanSeconds {
		return fmt.Errorf("%s 'timestamp' %d is more than %d seconds after the block time %d",
			name, timestamp, OutcomeMaxSpanSeconds, now)
	}
	if window <= 0 || window > OutcomeMaxSpanSeconds {
		return fmt.Errorf("%s 'window' must be in (0, %d], got %d", name, OutcomeMaxSpanSeconds, window)
	}
	if tolerance < 0 || tolerance > OutcomeMaxToleranceSeconds {
		return fmt.Errorf("%s 'tolerance' must be in [0, %d], got %d", name, OutcomeMaxToleranceSeconds, tolerance)
	}
	// A print stamped anywhere in the tolerance must be able to arrive before the deadline.
	if window < tolerance {
		return fmt.Errorf("%s 'window' (%d) must not be less than 'tolerance' (%d)", name, window, tolerance)
	}

	switch actionID {
	case IndexChangeInRangeV2ActionID:
		interval, err := outcomeIntArg(name, "time_interval", args[5])
		if err != nil {
			return err
		}
		if interval <= 0 || interval > OutcomeMaxSpanSeconds {
			return fmt.Errorf("%s 'time_interval' must be in (0, %d], got %d", name, OutcomeMaxSpanSeconds, interval)
		}
		// The prior is read within tolerance of current - interval. A window 2 * tolerance wide
		// that is narrower than the interval holds at most one print of a series that prints
		// once per interval.
		if 2*tolerance >= interval {
			return fmt.Errorf("%s 'time_interval' (%d) must be more than twice 'tolerance' (%d)", name, interval, tolerance)
		}
		return validateOutcomeBounds(name, "min_change", "max_change", args[6], args[7])

	case PriceAboveThresholdV2ActionID, PriceBelowThresholdV2ActionID:
		threshold, err := outcomeNumericArg(name, "threshold", args[5])
		if err != nil {
			return err
		}
		// A NULL threshold makes the comparison NULL, which would resolve both actions NO.
		if threshold == nil {
			return fmt.Errorf("%s requires a 'threshold' parameter, got NULL", name)
		}
		return nil

	case ValueInRangeV2ActionID:
		return validateOutcomeBounds(name, "min_value", "max_value", args[5], args[6])

	default:
		return fmt.Errorf("unsupported action_id %d", actionID)
	}
}

// validateOutcomeBounds checks a [min, max) pair. A NULL bound leaves that tail open, but not
// both, and min must be below max when both are set, or the answer is fixed before any print.
func validateOutcomeBounds(name, minParam, maxParam string, rawMin, rawMax any) error {
	lo, err := outcomeNumericArg(name, minParam, rawMin)
	if err != nil {
		return err
	}
	hi, err := outcomeNumericArg(name, maxParam, rawMax)
	if err != nil {
		return err
	}
	if lo == nil && hi == nil {
		return fmt.Errorf("%s requires '%s' or '%s', got both NULL", name, minParam, maxParam)
	}
	if lo == nil || hi == nil {
		return nil
	}
	order, err := lo.Cmp(hi)
	if err != nil {
		return fmt.Errorf("%s cannot compare '%s' and '%s': %w", name, minParam, maxParam, err)
	}
	if order >= 0 {
		return fmt.Errorf("%s '%s' (%s) must be less than '%s' (%s)", name, minParam, lo, maxParam, hi)
	}
	return nil
}

// outcomeIntArg returns a required INT8 arg. Decoded args carry INT8 as *int64 and NULL as nil.
func outcomeIntArg(name, param string, v any) (int64, error) {
	switch n := v.(type) {
	case nil:
		return 0, fmt.Errorf("%s requires a '%s' parameter, got NULL", name, param)
	case *int64:
		if n == nil {
			return 0, fmt.Errorf("%s requires a '%s' parameter, got NULL", name, param)
		}
		return *n, nil
	case int64:
		return n, nil
	default:
		return 0, fmt.Errorf("%s '%s' must be INT8, got %T", name, param, v)
	}
}

// outcomeNumericArg returns a NUMERIC(36,18) arg, or nil when it is NULL. Precision and scale must
// match the parameter, since the engine refuses any other.
func outcomeNumericArg(name, param string, v any) (*types.Decimal, error) {
	var d *types.Decimal
	switch n := v.(type) {
	case nil:
		return nil, nil
	case *types.Decimal:
		if n == nil {
			return nil, nil
		}
		d = n
	default:
		return nil, fmt.Errorf("%s '%s' must be NUMERIC(36,18), got %T", name, param, v)
	}
	if d.Precision() != 36 || d.Scale() != 18 {
		return nil, fmt.Errorf("%s '%s' must be NUMERIC(36,18), got NUMERIC(%d,%d)", name, param, d.Precision(), d.Scale())
	}
	// NaN decodes as NUMERIC(36,18) and would fix the answer before any print.
	if d.NaN() {
		return nil, fmt.Errorf("%s '%s' must be a number, got NaN", name, param)
	}
	return d, nil
}

// outcomeBlockTimestamp returns the block time the engine context carries, or 0 when it carries
// none.
func outcomeBlockTimestamp(ctx *common.EngineContext) int64 {
	if ctx == nil || ctx.TxContext == nil || ctx.TxContext.BlockContext == nil {
		return 0
	}
	return ctx.TxContext.BlockContext.Timestamp
}

// parseAttestationOutcomeMethod reads the result of an outcome action from an attestation's
// result_canonical: 1 YES, 0 NO, -1 INVALID. parse_attestation_boolean keeps refusing these ids, so
// a reader that does not know the third state fails instead of misreading it.
func parseAttestationOutcomeMethod() precompiles.Method {
	return precompiles.Method{
		Name:            "parse_attestation_outcome",
		AccessModifiers: []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC},
		Parameters: []precompiles.PrecompileValue{
			precompiles.NewPrecompileValue("result_canonical", types.ByteaType, false),
		},
		Returns: &precompiles.MethodReturn{
			IsTable: false,
			Fields: []precompiles.PrecompileValue{
				precompiles.NewPrecompileValue("outcome", types.IntType, false),
			},
		},
		Handler: parseAttestationOutcomeHandler,
	}
}

// outcomeYesValue and outcomeInvalidValue are 1 and -1 as NUMERIC(36,18) values in the data
// points encoding, which scales by 1e18.
var (
	outcomeYesValue     = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	outcomeInvalidValue = new(big.Int).Neg(outcomeYesValue)
)

// parseAttestationOutcomeHandler walks result_canonical to the action id and the result payload,
// accepts only outcome actions, and maps the payload's single value to 1, 0 or -1.
func parseAttestationOutcomeHandler(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	canonical, err := toByteSliceAllowNil(inputs[0])
	if err != nil {
		return fmt.Errorf("result_canonical must be bytea: %w", err)
	}
	if len(canonical) == 0 {
		return fmt.Errorf("result_canonical cannot be empty")
	}

	actionID, payload, err := outcomeCanonicalFields(canonical)
	if err != nil {
		return err
	}
	if !IsOutcomeAction(actionID) {
		return fmt.Errorf("unsupported action_id %d", actionID)
	}

	outcome, err := decodeOutcomePayload(payload)
	if err != nil {
		return err
	}
	return resultFn([]any{outcome})
}

// outcomeCanonicalFields returns the action id and the result payload of result_canonical:
//
//	version u8 | algo u8 | height u64 | lp(data_provider) | lp(stream) | action_id u16 | lp(args) | lp(result_payload)
//
// where lp is a 4-byte big-endian length followed by that many bytes.
func outcomeCanonicalFields(canonical []byte) (uint16, []byte, error) {
	r := &canonicalReader{buf: canonical}

	for _, f := range []struct {
		name string
		size int
	}{{"version", 1}, {"algo", 1}, {"height", 8}} {
		if _, ok := r.next(f.size); !ok {
			return 0, nil, fmt.Errorf("invalid result_canonical: too short for %s", f.name)
		}
	}
	for _, field := range []string{"data_provider", "stream"} {
		if _, err := r.lengthPrefixed(field); err != nil {
			return 0, nil, err
		}
	}

	idBytes, ok := r.next(2)
	if !ok {
		return 0, nil, fmt.Errorf("invalid result_canonical: too short for action_id")
	}
	actionID := binary.BigEndian.Uint16(idBytes)

	if _, err := r.lengthPrefixed("args"); err != nil {
		return 0, nil, err
	}
	payload, err := r.lengthPrefixed("result_payload")
	if err != nil {
		return 0, nil, err
	}
	return actionID, payload, nil
}

// canonicalReader reads result_canonical front to back.
type canonicalReader struct {
	buf []byte
	off int
}

// next returns the next n bytes, or false when fewer remain.
func (r *canonicalReader) next(n int) ([]byte, bool) {
	if n < 0 || n > len(r.buf)-r.off {
		return nil, false
	}
	out := r.buf[r.off : r.off+n]
	r.off += n
	return out, true
}

// lengthPrefixed returns the bytes of the next length-prefixed field.
func (r *canonicalReader) lengthPrefixed(field string) ([]byte, error) {
	head, ok := r.next(4)
	if !ok {
		return nil, fmt.Errorf("invalid result_canonical: too short for %s length", field)
	}
	n := binary.BigEndian.Uint32(head)
	if uint64(n) > uint64(len(r.buf)-r.off) {
		return nil, fmt.Errorf("invalid result_canonical: %s data extends beyond buffer", field)
	}
	data, _ := r.next(int(n))
	return data, nil
}

// decodeOutcomePayload decodes abi.encode(uint256[] event_times, int256[] values), requires
// exactly one data point, and maps its value 1e18, 0 or -1e18 to 1, 0 or -1.
func decodeOutcomePayload(payload []byte) (int64, error) {
	decoded, err := dataPointsABIArgs.Unpack(payload)
	if err != nil {
		return 0, fmt.Errorf("outcome action result: failed to decode ABI payload: %w", err)
	}
	if len(decoded) != 2 {
		return 0, fmt.Errorf("outcome action result: expected 2 arrays (event times, values), got %d", len(decoded))
	}
	eventTimes, ok := decoded[0].([]*big.Int)
	if !ok {
		return 0, fmt.Errorf("outcome action result: event times must be []*big.Int, got %T", decoded[0])
	}
	values, ok := decoded[1].([]*big.Int)
	if !ok {
		return 0, fmt.Errorf("outcome action result: values must be []*big.Int, got %T", decoded[1])
	}
	if len(eventTimes) != 1 || len(values) != 1 {
		return 0, fmt.Errorf("outcome action result must hold exactly one data point, got %d event times and %d values",
			len(eventTimes), len(values))
	}

	value := values[0]
	switch {
	case value.Cmp(outcomeYesValue) == 0:
		return 1, nil
	case value.Sign() == 0:
		return 0, nil
	case value.Cmp(outcomeInvalidValue) == 0:
		return -1, nil
	default:
		return 0, fmt.Errorf("outcome action result must be 1e18, 0 or -1e18, got %s", value)
	}
}
