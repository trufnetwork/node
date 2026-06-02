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

// Two component keys used across the tests.
const (
	restrictedHex   = "0x1111111111111111111111111111111111111111"
	unrestrictedHex = "0x2222222222222222222222222222222222222222"
)

func TestMAA(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_RuleStore",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testMAACreateMatchesGoldenVectorAndGetters(t),
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

// createDefaultMAA registers an MAA signed by `restricted`, returns the derived address bytes.
func createDefaultMAA(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, restricted util.EthereumAddress, feeBps int64) []byte {
	t.Helper()
	var addr []byte
	err := callAs(ctx, platform, restricted, "maa_create", []any{
		unrestrictedHex,                    // $unrestricted_addr
		repeat(0xab, 32),                   // $salt
		"eth_truf",                         // $bridge (token TRUF is derived from this)
		"bps",                              // $fee_mode
		feeBps,                             // $fee_bps
		dec(t, "0"),                        // $fee_flat
		[]string{"main", "main"},           // $namespaces
		[]string{"ob_place_order", "ob_cancel_order"}, // $actions
		[][]byte{repeat(0xcc, 32), nil},    // $body_hashes
	}, func(row *common.Row) error {
		addr = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})
	require.NoError(t, err, "maa_create should succeed")
	require.Len(t, addr, 20, "maa_address must be 20 bytes")
	return addr
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func testMAACreateMatchesGoldenVectorAndGetters(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		platform.Deployer = restricted.Bytes()

		addr := createDefaultMAA(t, ctx, platform, restricted, 250)

		// The on-chain derivation MUST match the frozen golden vector A
		// (5RulesHash-Preimage-Spec.md §4) — same inputs, same address.
		wantA, err := hex.DecodeString("79ce248b31fc0d2016a175b36f79c5726b40387a")
		require.NoError(t, err)
		require.Equal(t, wantA, addr, "maa_create must reproduce golden-vector A address")

		// maa_is_known(addr) -> true
		var known bool
		require.NoError(t, callAs(ctx, platform, restricted, "maa_is_known", []any{addr},
			func(row *common.Row) error { known = row.Values[0].(bool); return nil }))
		require.True(t, known)

		// maa_is_known(random) -> false
		known = true
		require.NoError(t, callAs(ctx, platform, restricted, "maa_is_known", []any{repeat(0x99, 20)},
			func(row *common.Row) error { known = row.Values[0].(bool); return nil }))
		require.False(t, known, "unknown address must report not-known")

		// maa_get_rule(addr) -> field checks
		var restrField, unrestrField, bridgeField, tokenField, feeMode string
		var feeBps int64
		var enabled bool
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_rule", []any{addr},
			func(row *common.Row) error {
				restrField = row.Values[2].(string)
				unrestrField = row.Values[3].(string)
				bridgeField = row.Values[5].(string)
				tokenField = row.Values[6].(string)
				feeMode = row.Values[7].(string)
				feeBps = row.Values[8].(int64)
				enabled = row.Values[10].(bool)
				return nil
			}))
		require.Equal(t, restrictedHex, restrField)
		require.Equal(t, unrestrictedHex, unrestrField)
		require.Equal(t, "eth_truf", bridgeField)
		require.Equal(t, "TRUF", tokenField, "token must be derived from bridge, not caller-supplied")
		require.Equal(t, "bps", feeMode)
		require.Equal(t, int64(250), feeBps)
		require.True(t, enabled)

		// maa_get_allowed_actions(addr) -> 2 rows, canonically ordered (cancel before place)
		var acts []string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_allowed_actions", []any{addr},
			func(row *common.Row) error { acts = append(acts, row.Values[1].(string)); return nil }))
		require.Equal(t, []string{"ob_cancel_order", "ob_place_order"}, acts)

		// maa_get_events(addr) -> exactly one CREATE event, actor_role restricted
		var evtTypes, evtRoles []string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_events", []any{addr, int64(100), int64(0)},
			func(row *common.Row) error {
				evtTypes = append(evtTypes, row.Values[1].(string))
				evtRoles = append(evtRoles, row.Values[2].(string))
				return nil
			}))
		require.Equal(t, []string{"CREATE"}, evtTypes)
		require.Equal(t, []string{"restricted"}, evtRoles)
		return nil
	}
}

func testMAAValidation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		owner := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		// Create the canonical MAA so the duplicate check below has something to collide with.
		_ = createDefaultMAA(t, ctx, platform, restricted, 250)

		// Duplicate identity (same restricted/unrestricted/rules/salt) must be rejected.
		require.Error(t, callAs(ctx, platform, restricted, "maa_create", []any{
			unrestrictedHex, repeat(0xab, 32), "eth_truf", "bps", int64(250), dec(t, "0"),
			[]string{"main", "main"}, []string{"ob_place_order", "ob_cancel_order"},
			[][]byte{repeat(0xcc, 32), nil},
		}, nil), "duplicate MAA must be rejected")

		// restricted == unrestricted must be rejected (signer is `owner`, unrestricted also owner).
		require.Error(t, callAs(ctx, platform, owner, "maa_create", []any{
			unrestrictedHex, repeat(0x01, 32), "eth_truf", "bps", int64(0), dec(t, "0"),
			[]string{}, []string{}, [][]byte{},
		}, nil), "restricted == unrestricted must be rejected")

		// fee_bps out of range must be rejected.
		require.Error(t, callAs(ctx, platform, restricted, "maa_create", []any{
			unrestrictedHex, repeat(0x02, 32), "eth_truf", "bps", int64(10001), dec(t, "0"),
			[]string{}, []string{}, [][]byte{},
		}, nil), "fee_bps > 10000 must be rejected")

		// Duplicate (namespace, action) in the allow-list must be rejected (PK + canonical-set integrity).
		require.Error(t, callAs(ctx, platform, restricted, "maa_create", []any{
			unrestrictedHex, repeat(0x03, 32), "eth_truf", "bps", int64(0), dec(t, "0"),
			[]string{"main", "main"}, []string{"ob_place_order", "ob_place_order"},
			[][]byte{nil, nil},
		}, nil), "duplicate (namespace, action) must be rejected")

		// Unsupported bridge must be rejected (token is derived from bridge, not caller-supplied).
		require.Error(t, callAs(ctx, platform, restricted, "maa_create", []any{
			unrestrictedHex, repeat(0x04, 32), "eth_dai", "bps", int64(0), dec(t, "0"),
			[]string{}, []string{}, [][]byte{},
		}, nil), "unsupported bridge must be rejected")
		return nil
	}
}
