//go:build kwiltest

package maa

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// The two component keys used across the tests — these are the golden-vector inputs
// (2MAA-Plan.md §5.4): restricted = 0x11*20, unrestricted = 0x22*20.
const (
	restrictedHex   = "0x1111111111111111111111111111111111111111"
	unrestrictedHex = "0x2222222222222222222222222222222222222222"

	// Frozen golden vector A (2MAA-Plan.md §5.4): bps 250, allow-list {ob_place_order(0xcc), ob_cancel_order},
	// salt 0xab*32. rule_id is the full 32-byte identifier; maa_address is the derived 20-byte wallet.
	goldenRuleIDHex = "a0b517da759b794e2484dc8b9dba8f5211a53dcdf26448f19c7c68699ff7bcf1"
	goldenMAAHex    = "84da4dbca14d429c719d65a0bb76bd7fa3c5c349"
)

func TestMAA(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_RuleStore",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testMAACreateRuleJoinAndGetters(t),
			testMAAValidation(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func repeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func dec(t *testing.T, s string) *kwilTypes.Decimal {
	t.Helper()
	d, err := kwilTypes.ParseDecimalExplicit(s, 78, 0)
	require.NoError(t, err)
	return d
}

// callAs invokes an action with @caller set to the given address, returning the action error (res.Error).
func callAs(ctx context.Context, platform *kwilTesting.Platform, caller util.EthereumAddress, action string, args []any, rowFn func(*common.Row) error) error {
	if rowFn == nil {
		rowFn = func(*common.Row) error { return nil }
	}
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       caller.Bytes(),
		Caller:       caller.Address(),
		TxID:         platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", action, args, rowFn)
	if err != nil {
		return err
	}
	return res.Error
}

// createDefaultRule registers the golden-vector rule signed by `restricted`, returns the rule_id bytes.
func createDefaultRule(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, restricted util.EthereumAddress, feeBps int64, salt []byte) []byte {
	t.Helper()
	var ruleID []byte
	err := callAs(ctx, platform, restricted, "maa_create_rule", []any{
		salt,                                          // $salt
		"bps",                                         // $fee_mode
		feeBps,                                        // $fee_bps
		dec(t, "0"),                                   // $fee_flat
		[]string{"main", "main"},                      // $namespaces
		[]string{"ob_place_order", "ob_cancel_order"}, // $actions
		[][]byte{repeat(0xcc, 32), nil},               // $body_hashes
	}, func(row *common.Row) error {
		ruleID = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})
	require.NoError(t, err, "maa_create_rule should succeed")
	require.Len(t, ruleID, 32, "rule_id must be 32 bytes (untruncated identifier)")
	return ruleID
}

// joinRule has `funder` join an existing rule_id, returns the derived 20-byte maa_address.
func joinRule(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, funder util.EthereumAddress, ruleID []byte) []byte {
	t.Helper()
	var maa []byte
	err := callAs(ctx, platform, funder, "maa_join", []any{ruleID}, func(row *common.Row) error {
		maa = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})
	require.NoError(t, err, "maa_join should succeed")
	require.Len(t, maa, 20, "maa_address must be 20 bytes")
	return maa
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func testMAACreateRuleJoinAndGetters(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		// 1) Create the rule (restricted signs). Must reproduce golden-vector A rule_id.
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		wantRuleID, err := hex.DecodeString(goldenRuleIDHex)
		require.NoError(t, err)
		require.Equal(t, wantRuleID, ruleID, "maa_create_rule must reproduce golden-vector A rule_id")

		// 2) Join the rule (unrestricted funder signs). Must reproduce golden-vector A maa_address.
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		wantMAA, err := hex.DecodeString(goldenMAAHex)
		require.NoError(t, err)
		require.Equal(t, wantMAA, maa, "maa_join must reproduce golden-vector A maa_address")

		// maa_is_known(maa) -> true; maa_is_known(random) -> false.
		var known bool
		require.NoError(t, callAs(ctx, platform, restricted, "maa_is_known", []any{maa},
			func(row *common.Row) error { known = row.Values[0].(bool); return nil }))
		require.True(t, known)
		known = true
		require.NoError(t, callAs(ctx, platform, restricted, "maa_is_known", []any{repeat(0x99, 20)},
			func(row *common.Row) error { known = row.Values[0].(bool); return nil }))
		require.False(t, known, "unknown address must report not-known")

		// maa_get_rule(rule_id) -> field checks (no bridge/token/enabled/unrestricted on the rule).
		var restrField, feeMode string
		var feeBps int64
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_rule", []any{ruleID},
			func(row *common.Row) error {
				restrField = row.Values[1].(string)
				feeMode = row.Values[3].(string)
				feeBps = row.Values[4].(int64)
				return nil
			}))
		require.Equal(t, restrictedHex, restrField)
		require.Equal(t, "bps", feeMode)
		require.Equal(t, int64(250), feeBps)

		// maa_get_instance(maa) -> MAA maps to both keys + rule_id.
		var instMAA, instRule, instRestr, instUnrestr string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_instance", []any{maa},
			func(row *common.Row) error {
				instMAA = row.Values[0].(string)
				instRule = row.Values[1].(string)
				instRestr = row.Values[2].(string)
				instUnrestr = row.Values[3].(string)
				return nil
			}))
		require.Equal(t, "0x"+goldenMAAHex, instMAA)
		require.Equal(t, "0x"+goldenRuleIDHex, instRule)
		require.Equal(t, restrictedHex, instRestr)
		require.Equal(t, unrestrictedHex, instUnrestr)

		// maa_get_allowed_actions(rule_id) -> 2 rows, canonically ordered (cancel before place).
		var acts []string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_allowed_actions", []any{ruleID},
			func(row *common.Row) error { acts = append(acts, row.Values[1].(string)); return nil }))
		require.Equal(t, []string{"ob_cancel_order", "ob_place_order"}, acts)

		// maa_get_events(rule_id) -> CREATE_RULE (restricted) then JOIN (unrestricted).
		var evtTypes, evtRoles []string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_events", []any{ruleID, int64(100), int64(0)},
			func(row *common.Row) error {
				evtTypes = append(evtTypes, row.Values[2].(string))
				evtRoles = append(evtRoles, row.Values[3].(string))
				return nil
			}))
		require.Equal(t, []string{"CREATE_RULE", "JOIN"}, evtTypes)
		require.Equal(t, []string{"restricted", "unrestricted"}, evtRoles)
		return nil
	}
}

func testMAAValidation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		// Create the canonical rule so collisions below have something to hit.
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))

		// Duplicate rule identity (same restricted/rules/salt) must be rejected.
		require.Error(t, callAs(ctx, platform, restricted, "maa_create_rule", []any{
			repeat(0xab, 32), "bps", int64(250), dec(t, "0"),
			[]string{"main", "main"}, []string{"ob_place_order", "ob_cancel_order"},
			[][]byte{repeat(0xcc, 32), nil},
		}, nil), "duplicate rule must be rejected")

		// fee_bps out of range (>10000 = >100%) must be rejected.
		require.Error(t, callAs(ctx, platform, restricted, "maa_create_rule", []any{
			repeat(0x02, 32), "bps", int64(10001), dec(t, "0"),
			[]string{}, []string{}, [][]byte{},
		}, nil), "fee_bps > 10000 must be rejected")

		// Duplicate (namespace, action) in the allow-list must be rejected.
		require.Error(t, callAs(ctx, platform, restricted, "maa_create_rule", []any{
			repeat(0x03, 32), "bps", int64(0), dec(t, "0"),
			[]string{"main", "main"}, []string{"ob_place_order", "ob_place_order"},
			[][]byte{nil, nil},
		}, nil), "duplicate (namespace, action) must be rejected")

		// maa_join on an unknown rule_id must be rejected.
		require.Error(t, callAs(ctx, platform, unrestricted, "maa_join", []any{repeat(0xee, 32)}, nil),
			"join of unknown rule_id must be rejected")

		// Self-delegation: the rule creator (restricted) joining its own rule must be rejected.
		require.Error(t, callAs(ctx, platform, restricted, "maa_join", []any{ruleID}, nil),
			"funder == rule creator must be rejected")

		// First real join succeeds; a second identical join (same funder + rule) must be rejected.
		_ = joinRule(t, ctx, platform, unrestricted, ruleID)
		require.Error(t, callAs(ctx, platform, unrestricted, "maa_join", []any{ruleID}, nil),
			"double join must be rejected")
		return nil
	}
}
