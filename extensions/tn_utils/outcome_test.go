package tn_utils

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

var outcomeActions = map[uint16]string{
	13: "index_change_in_range_v2",
	14: "price_above_threshold_v2",
	15: "price_below_threshold_v2",
	16: "value_in_range_v2",
}

// TestOutcomeActionRegistry keeps the ids, the name map and both settlement predicates in
// agreement. A name that maps to another id would give markets a hash no attestation matches.
func TestOutcomeActionRegistry(t *testing.T) {
	require.Equal(t, 13, IndexChangeInRangeV2ActionID)
	require.Equal(t, 14, PriceAboveThresholdV2ActionID)
	require.Equal(t, 15, PriceBelowThresholdV2ActionID)
	require.Equal(t, 16, ValueInRangeV2ActionID)

	for id, name := range outcomeActions {
		got, err := getActionIDNumber(name)
		require.NoError(t, err, "%s should be a known action", name)
		require.Equal(t, id, got, "%s must map to id %d", name, id)
		require.True(t, IsOutcomeAction(id), "%s (id %d) should be an outcome action", name, id)
		require.False(t, IsBinaryAction(id), "%s (id %d) must not settle as a boolean", name, id)
		require.Equal(t, name, outcomeActionName(id))
	}

	// Ids 1-12 keep their meaning: none is an outcome action, and the boolean set is unchanged.
	binarySet := map[uint16]bool{6: true, 7: true, 8: true, 9: true, 12: true}
	for id := uint16(0); id <= 12; id++ {
		require.False(t, IsOutcomeAction(id), "id %d is not an outcome action", id)
		require.Equal(t, binarySet[id], IsBinaryAction(id), "IsBinaryAction(%d) changed", id)
	}
	for _, id := range []uint16{17, 255, 0xFFFF} {
		require.False(t, IsOutcomeAction(id), "id %d is not an outcome action", id)
	}

	_, err := getActionIDNumber("index_change_in_range_v3")
	require.Error(t, err)
}

func TestParseAttestationOutcomeIsRegistered(t *testing.T) {
	var found *precompiles.Method
	for _, m := range buildPrecompile().Methods {
		if m.Name == "parse_attestation_outcome" {
			m := m
			found = &m
		}
	}
	require.NotNil(t, found, "parse_attestation_outcome must be in the tn_utils bundle")
	require.ElementsMatch(t, []precompiles.Modifier{precompiles.VIEW, precompiles.PUBLIC}, found.AccessModifiers)
	require.Len(t, found.Parameters, 1)
	require.True(t, found.Parameters[0].Type.EqualsStrict(types.ByteaType))
	require.NotNil(t, found.Returns)
	require.False(t, found.Returns.IsTable)
	require.Len(t, found.Returns.Fields, 1)
	require.Equal(t, "outcome", found.Returns.Fields[0].Name)
	require.True(t, found.Returns.Fields[0].Type.EqualsStrict(types.IntType))
	require.False(t, found.Returns.Fields[0].Nullable)
}

// outcomePayload builds a result payload the way request_attestation does: the action's rows are
// encoded canonically by call_dispatch, then turned into data points by EncodeDataPointsABI.
func outcomePayload(t *testing.T, eventTime int64, value string) []byte {
	t.Helper()
	rows := []*common.Row{{Values: []any{eventTime, types.MustParseDecimalExplicit(value, 36, 18)}}}
	canonical, err := EncodeQueryResultCanonical(rows)
	require.NoError(t, err)
	payload, err := EncodeDataPointsABI(canonical)
	require.NoError(t, err)
	return payload
}

// rawDataPoints packs event times and values directly, for payloads the encoder never produces.
func rawDataPoints(t *testing.T, eventTimes, values []*big.Int) []byte {
	t.Helper()
	payload, err := dataPointsABIArgs.Pack(eventTimes, values)
	require.NoError(t, err)
	return payload
}

// resultCanonical lays out result_canonical as request_attestation stores it.
func resultCanonical(actionID uint16, payload []byte) []byte {
	out := []byte{1, 0} // version, algo
	out = binary.BigEndian.AppendUint64(out, 3196705)
	out = append(out, lengthPrefixBytes(make([]byte, 20))...)
	out = append(out, lengthPrefixBytes([]byte("st8f1e62d3a130572ec468dda082f889"))...)
	out = binary.BigEndian.AppendUint16(out, actionID)
	out = append(out, lengthPrefixBytes([]byte{0, 0, 0, 0})...)
	out = append(out, lengthPrefixBytes(payload)...)
	return out
}

func parseOutcome(input any) (int64, error) {
	var got int64
	err := parseAttestationOutcomeHandler(nil, nil, []any{input}, func(row []any) error {
		got = row[0].(int64)
		return nil
	})
	return got, err
}

func TestParseAttestationOutcome(t *testing.T) {
	t.Run("YesNoInvalidForEveryOutcomeAction", func(t *testing.T) {
		cases := map[string]int64{
			"1.000000000000000000":  1,
			"0.000000000000000000":  0,
			"-1.000000000000000000": -1,
		}
		for id := range outcomeActions {
			for value, want := range cases {
				got, err := parseOutcome(resultCanonical(id, outcomePayload(t, 1759276800, value)))
				require.NoError(t, err, "id %d value %s", id, value)
				require.Equal(t, want, got, "id %d value %s", id, value)
			}
		}
	})

	t.Run("EmptyInput", func(t *testing.T) {
		for _, input := range []any{nil, []byte{}} {
			_, err := parseOutcome(input)
			require.ErrorContains(t, err, "result_canonical cannot be empty")
		}
	})

	t.Run("NotBytea", func(t *testing.T) {
		_, err := parseOutcome(int64(1))
		require.ErrorContains(t, err, "result_canonical must be bytea")
	})

	t.Run("EveryTruncationIsRefused", func(t *testing.T) {
		full := resultCanonical(13, outcomePayload(t, 1759276800, "1"))
		for n := 1; n < len(full); n++ {
			_, err := parseOutcome(full[:n])
			require.ErrorContains(t, err, "invalid result_canonical", "prefix of %d bytes", n)
		}
	})

	t.Run("LengthBeyondBuffer", func(t *testing.T) {
		bad := []byte{1, 0}
		bad = binary.BigEndian.AppendUint64(bad, 1)
		bad = binary.BigEndian.AppendUint32(bad, 0xFFFFFFFF)
		bad = append(bad, make([]byte, 64)...)
		_, err := parseOutcome(bad)
		require.ErrorContains(t, err, "invalid result_canonical: data_provider data extends beyond buffer")
	})

	t.Run("OtherActionIDsAreUnsupported", func(t *testing.T) {
		payload := outcomePayload(t, 1759276800, "1")
		for _, id := range []uint16{0, 1, 4, 5, 6, 9, 10, 11, 12, 17, 255, 0xFFFF} {
			_, err := parseOutcome(resultCanonical(id, payload))
			require.ErrorContains(t, err, "unsupported action_id", "id %d", id)
		}
	})

	t.Run("BadPayloads", func(t *testing.T) {
		one := big.NewInt(1)
		e18 := new(big.Int).Set(outcomeYesValue)
		boolTrue, err := booleanABIArgs.Pack(true)
		require.NoError(t, err)

		cases := map[string][]byte{
			"empty":               {},
			"garbage":             {0xde, 0xad, 0xbe, 0xef},
			"abi bool":            boolTrue,
			"no data points":      rawDataPoints(t, []*big.Int{}, []*big.Int{}),
			"two data points":     rawDataPoints(t, []*big.Int{one, one}, []*big.Int{e18, e18}),
			"two values one time": rawDataPoints(t, []*big.Int{one}, []*big.Int{e18, e18}),
			"one value two times": rawDataPoints(t, []*big.Int{one, one}, []*big.Int{e18}),
			"value 2":             outcomePayload(t, 1, "2"),
			"value -2":            outcomePayload(t, 1, "-2"),
			"value 0.5":           outcomePayload(t, 1, "0.5"),
			"value 1e-18":         outcomePayload(t, 1, "0.000000000000000001"),
			"value 1 unscaled":    rawDataPoints(t, []*big.Int{one}, []*big.Int{one}),
			"value -1 unscaled":   rawDataPoints(t, []*big.Int{one}, []*big.Int{big.NewInt(-1)}),
			"value 1e18 plus 1":   rawDataPoints(t, []*big.Int{one}, []*big.Int{new(big.Int).Add(e18, one)}),
			"value -1e18 minus 1": rawDataPoints(t, []*big.Int{one}, []*big.Int{new(big.Int).Sub(outcomeInvalidValue, one)}),
		}
		for name, payload := range cases {
			_, err := parseOutcome(resultCanonical(14, payload))
			require.ErrorContains(t, err, "outcome action result", name)
		}
	})

	t.Run("BooleanParserStillRefusesOutcomeIDs", func(t *testing.T) {
		for id := range outcomeActions {
			err := parseAttestationBooleanHandler(nil, nil, []any{resultCanonical(id, outcomePayload(t, 1, "1"))},
				func([]any) error { return nil })
			require.ErrorContains(t, err, "unsupported action_id", "id %d", id)
		}
	})
}

const (
	testNow      int64 = 1763251200 // 2025-11-16T00:00:00Z
	testDay      int64 = 24 * 60 * 60
	testProvider       = "0x1111111111111111111111111111111111111111"
	testStream         = "st8f1e62d3a130572ec468dda082f889"
)

func blockAt(ts int64) *common.EngineContext {
	return &common.EngineContext{TxContext: &common.TxContext{BlockContext: &common.BlockContext{Timestamp: ts}}}
}

func dec(s string) *types.Decimal { return types.MustParseDecimalExplicit(s, 36, 18) }

// validOutcomeArgs returns a set of args each outcome action accepts.
func validOutcomeArgs(id uint16) []any {
	head := []any{testProvider, testStream, testNow - 30*testDay, 90 * testDay, 2 * testDay}
	switch id {
	case IndexChangeInRangeV2ActionID:
		return append(head, 365*testDay, dec("2"), dec("3"))
	case PriceAboveThresholdV2ActionID, PriceBelowThresholdV2ActionID:
		return append(head, dec("100"))
	case ValueInRangeV2ActionID:
		return append(head, dec("1"), dec("2"))
	}
	panic("not an outcome action")
}

func withArg(args []any, i int, v any) []any {
	out := append([]any(nil), args...)
	out[i] = v
	return out
}

type refusal struct {
	args []any
	want string
}

func validateOutcome(t *testing.T, ctx *common.EngineContext, id int64, args []any) error {
	t.Helper()
	argsBytes, err := EncodeActionArgs(args)
	require.NoError(t, err)
	return validateAttestationDateRangeHandler(ctx, nil, []any{id, argsBytes}, nil)
}

func TestValidateOutcomeActionArgs(t *testing.T) {
	const tenYears = OutcomeMaxSpanSeconds
	const maxTol = OutcomeMaxToleranceSeconds
	require.Equal(t, int64(315360000), tenYears)
	require.Equal(t, int64(3888000), maxTol)

	for id, name := range outcomeActions {
		id, name := id, name
		base := validOutcomeArgs(id)
		ctx := blockAt(testNow)

		t.Run(name+"/accepts", func(t *testing.T) {
			accept := map[string][]any{
				"defaults":                base,
				"timestamp 0":             withArg(base, 2, int64(0)),
				"timestamp at now + 10 y": withArg(base, 2, testNow+tenYears),
				"window 10 y":             withArg(base, 3, tenYears),
				"tolerance 0":             withArg(base, 4, int64(0)),
				"tolerance 45 d = window": withArg(withArg(base, 3, maxTol), 4, maxTol),
			}
			switch id {
			case IndexChangeInRangeV2ActionID:
				accept["open low tail"] = withArg(base, 6, nil)
				accept["open high tail"] = withArg(base, 7, nil)
				accept["negative bounds"] = withArg(withArg(base, 6, dec("-3")), 7, dec("-2"))
				accept["interval 10 y"] = withArg(base, 5, tenYears)
				accept["interval just over 2 tol"] = withArg(base, 5, 4*testDay+1)
			case PriceAboveThresholdV2ActionID, PriceBelowThresholdV2ActionID:
				accept["negative threshold"] = withArg(base, 5, dec("-5.5"))
				accept["zero threshold"] = withArg(base, 5, dec("0"))
			case ValueInRangeV2ActionID:
				accept["open low tail"] = withArg(base, 5, nil)
				accept["open high tail"] = withArg(base, 6, nil)
			}
			for label, args := range accept {
				require.NoError(t, validateOutcome(t, ctx, int64(id), args), label)
			}

			// Without a block time only the timestamp's upper bound is skipped.
			far := withArg(base, 2, testNow+3*tenYears)
			for _, c := range []*common.EngineContext{nil, {}, {TxContext: &common.TxContext{}}, blockAt(0), blockAt(-1)} {
				require.NoError(t, validateOutcome(t, c, int64(id), far))
				require.ErrorContains(t, validateOutcome(t, c, int64(id), withArg(base, 3, int64(0))), "'window'")
			}
		})

		t.Run(name+"/refuses", func(t *testing.T) {
			refuse := map[string]refusal{
				"one arg short":           {base[:len(base)-1], "requires exactly"},
				"one arg extra":           {append(append([]any(nil), base...), int64(1)), "requires exactly"},
				"no args":                 {[]any{}, "requires exactly"},
				"provider NULL":           {withArg(base, 0, nil), "'data_provider' must be TEXT"},
				"provider INT8":           {withArg(base, 0, int64(1)), "'data_provider' must be TEXT"},
				"stream NULL":             {withArg(base, 1, nil), "'stream_id' must be TEXT"},
				"timestamp NULL":          {withArg(base, 2, nil), "'timestamp' parameter"},
				"timestamp NUMERIC":       {withArg(base, 2, dec("1")), "'timestamp' must be INT8"},
				"timestamp TEXT":          {withArg(base, 2, "1"), "'timestamp' must be INT8"},
				"timestamp negative":      {withArg(base, 2, int64(-1)), "'timestamp' must not be negative"},
				"timestamp past now+10 y": {withArg(base, 2, testNow+tenYears+1), "after the block time"},
				"timestamp max int":       {withArg(base, 2, int64(1<<63-1)), "after the block time"},
				"window NULL":             {withArg(base, 3, nil), "'window' parameter"},
				"window BOOL":             {withArg(base, 3, true), "'window' must be INT8"},
				"window 0":                {withArg(base, 3, int64(0)), "'window' must be in"},
				"window negative":         {withArg(base, 3, int64(-5)), "'window' must be in"},
				"window over 10 y":        {withArg(base, 3, tenYears+1), "'window' must be in"},
				"tolerance NULL":          {withArg(base, 4, nil), "'tolerance' parameter"},
				"tolerance negative":      {withArg(base, 4, int64(-1)), "'tolerance' must be in"},
				"tolerance over 45 d":     {withArg(base, 4, maxTol+1), "'tolerance' must be in"},
				"window below tolerance":  {withArg(withArg(base, 3, 10*testDay), 4, 10*testDay+1), "must not be less than 'tolerance'"},
			}
			switch id {
			case IndexChangeInRangeV2ActionID:
				refuse["interval NULL"] = refusal{withArg(base, 5, nil), "'time_interval' parameter"}
				refuse["interval NUMERIC"] = refusal{withArg(base, 5, dec("365")), "'time_interval' must be INT8"}
				refuse["interval 0"] = refusal{withArg(base, 5, int64(0)), "'time_interval' must be in"}
				refuse["interval negative"] = refusal{withArg(base, 5, int64(-1)), "'time_interval' must be in"}
				refuse["interval over 10 y"] = refusal{withArg(base, 5, tenYears+1), "'time_interval' must be in"}
				refuse["interval equal to 2 tol"] = refusal{withArg(base, 5, 4*testDay), "more than twice 'tolerance'"}
				refuse["bounds both NULL"] = refusal{withArg(withArg(base, 6, nil), 7, nil), "got both NULL"}
				refuse["min equals max"] = refusal{withArg(withArg(base, 6, dec("2")), 7, dec("2")), "must be less than"}
				refuse["min above max"] = refusal{withArg(withArg(base, 6, dec("3")), 7, dec("2")), "must be less than"}
				refuse["min INT8"] = refusal{withArg(base, 6, int64(2)), "'min_change' must be NUMERIC(36,18)"}
				refuse["max other precision"] = refusal{withArg(base, 7, types.MustParseDecimalExplicit("3", 10, 2)), "'max_change' must be NUMERIC(36,18), got NUMERIC(10,2)"}
				refuse["min NaN, max NULL"] = refusal{withArg(withArg(base, 6, dec("NaN")), 7, nil), "'min_change' must be a number, got NaN"}
			case PriceAboveThresholdV2ActionID, PriceBelowThresholdV2ActionID:
				refuse["threshold NULL"] = refusal{withArg(base, 5, nil), "'threshold' parameter, got NULL"}
				refuse["threshold INT8"] = refusal{withArg(base, 5, int64(100)), "'threshold' must be NUMERIC(36,18)"}
				refuse["threshold TEXT"] = refusal{withArg(base, 5, "100"), "'threshold' must be NUMERIC(36,18)"}
				refuse["threshold other scale"] = refusal{withArg(base, 5, types.MustParseDecimalExplicit("100", 36, 0)), "got NUMERIC(36,0)"}
				refuse["threshold NaN"] = refusal{withArg(base, 5, dec("NaN")), "'threshold' must be a number, got NaN"}
			case ValueInRangeV2ActionID:
				refuse["bounds both NULL"] = refusal{withArg(withArg(base, 5, nil), 6, nil), "got both NULL"}
				refuse["min equals max"] = refusal{withArg(withArg(base, 5, dec("1")), 6, dec("1")), "must be less than"}
				refuse["min above max"] = refusal{withArg(withArg(base, 5, dec("2")), 6, dec("1")), "must be less than"}
				refuse["max TEXT"] = refusal{withArg(base, 6, "2"), "'max_value' must be NUMERIC(36,18)"}
				refuse["max NaN, min NULL"] = refusal{withArg(withArg(base, 5, nil), 6, dec("NaN")), "'max_value' must be a number, got NaN"}
			}
			for label, c := range refuse {
				err := validateOutcome(t, ctx, int64(id), c.args)
				require.ErrorContains(t, err, c.want, label)
			}
		})

		t.Run(name+"/malformed bytes", func(t *testing.T) {
			err := validateAttestationDateRangeHandler(ctx, nil, []any{int64(id), "not bytes"}, nil)
			require.ErrorContains(t, err, "args_bytes must be []byte")

			err = validateAttestationDateRangeHandler(ctx, nil, []any{int64(id), []byte{1, 2}}, nil)
			require.ErrorContains(t, err, "failed to decode action args")

			// A count the bytes cannot hold is refused.
			err = validateAttestationDateRangeHandler(ctx, nil, []any{int64(id), []byte{0xff, 0xff, 0xff, 0xff}}, nil)
			require.ErrorContains(t, err, "failed to decode action args")
		})
	}
}

// TestValidateAttestationDateRangeOtherIDsUnchanged checks that the new branch is keyed on ids
// 13-16 alone: the same v2 args under any other id take the path they took before.
func TestValidateAttestationDateRangeOtherIDsUnchanged(t *testing.T) {
	ctx := blockAt(testNow)
	garbage := []byte{0xde, 0xad}

	// Ids that were never validated still pass anything, including ids that would wrap to 13-16
	// as a uint16.
	for _, id := range []int64{-1, 0, 4, 5, 6, 7, 8, 9, 17, 255, 65536 + 13, 65536 + 16} {
		err := validateAttestationDateRangeHandler(ctx, nil, []any{id, garbage}, nil)
		require.NoError(t, err, "id %d", id)
	}

	for _, id := range []int64{1, 2, 3} {
		err := validateAttestationDateRangeHandler(ctx, nil, []any{id, garbage}, nil)
		require.ErrorContains(t, err, "not allowed for attestation", "id %d", id)
	}

	// Id 12 still checks only its interval at index 4.
	args, err := EncodeActionArgs([]any{testProvider, testStream, testNow, nil, int64(-1)})
	require.NoError(t, err)
	err = validateAttestationDateRangeHandler(ctx, nil, []any{int64(12), args}, nil)
	require.ErrorContains(t, err, "index_change_in_range 'time_interval' must be positive")

	// The v2 args for id 13 read as id 12 put the tolerance (2 d) where the interval goes, so they pass.
	args, err = EncodeActionArgs(validOutcomeArgs(13))
	require.NoError(t, err)
	require.NoError(t, validateAttestationDateRangeHandler(ctx, nil, []any{int64(12), args}, nil))
}
