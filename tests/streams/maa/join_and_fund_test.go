//go:build kwiltest

package maa

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestMAAJoinAndFund(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_JoinAndFund",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testMAAJoinAndFundHappyPath(t),
			testMAAJoinAndFundValidation(t),
			testMAAJoinAndFundAtomicity(t),
			testMAAJoinAndFundDuplicateAndSecondFunder(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// joinAndFund calls maa_join_and_fund as `funder`, returning the derived MAA address and
// the action error (if any) so guard tests can assert on it.
func joinAndFund(ctx context.Context, platform *kwilTesting.Platform, funder util.EthereumAddress, ruleID []byte, bridge string, amount *kwilTypes.Decimal) ([]byte, error) {
	var maa []byte
	err := callAs(ctx, platform, funder, "maa_join_and_fund", []any{ruleID, bridge, amount}, func(row *common.Row) error {
		maa = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})
	return maa, err
}

// maaIsKnown reads the maa_is_known flag for an address.
func maaIsKnown(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, caller util.EthereumAddress, maa []byte) bool {
	t.Helper()
	var known bool
	require.NoError(t, callAs(ctx, platform, caller, "maa_is_known", []any{maa}, func(row *common.Row) error {
		known = row.Values[0].(bool)
		return nil
	}))
	return known
}

// fundAddressAt credits `to` on the hoodi_tt2 bridge at an explicit ordered-sync point,
// chained to `prev`. A second deposit on the same bridge instance MUST chain to the
// previous point or ordered-sync never processes it.
func fundAddressAt(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, to string, amount string, point int64, prev *int64) {
	t.Helper()
	require.NoError(t, testerc20.InjectERC20Transfer(
		ctx, platform, wdChain, wdEscrow, wdERC20, to, to, amount, point, prev,
	))
}

// goldenMAA returns golden-vector A's derived maa_address (restricted 0x11*20,
// unrestricted 0x22*20, salt 0xab*32) as bytes.
func goldenMAA(t *testing.T) []byte {
	t.Helper()
	maa, err := hex.DecodeString(goldenMAAHex)
	require.NoError(t, err)
	return maa
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// testMAAJoinAndFundHappyPath: one transaction both registers the funder's MAA and moves the
// funding from the funder to it — the instance exists, the funder is debited, the wallet is
// credited, and the join is audited, all from a single action call.
func testMAAJoinAndFundHappyPath(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))

		// The FUNDER holds 100 tokens; the join-and-fund moves 40 of them into the new wallet.
		orderedsync.ForTestingReset()
		fundAddress(t, ctx, platform, unrestrictedHex, "100000000000000000000", 1)

		maa, err := joinAndFund(ctx, platform, unrestricted, ruleID, wdBridge, dec(t, "40000000000000000000"))
		require.NoError(t, err, "maa_join_and_fund should succeed")
		require.Equal(t, goldenMAA(t), maa, "the composite action must derive the same MAA as maa_join (golden vector A)")

		// The join leg registered the instance.
		require.True(t, maaIsKnown(t, ctx, platform, unrestricted, maa), "the MAA must be registered")

		// The fund leg moved the tokens: funder debited, wallet credited.
		maaAddr := addrFromBytes(maa)
		require.Equal(t, "60000000000000000000", balance(t, ctx, platform, unrestrictedHex), "funder is debited the funding amount")
		require.Equal(t, "40000000000000000000", balance(t, ctx, platform, maaAddr.Address()), "agent wallet is credited the funding amount")

		// The funded balance is visible through the MAA balance surface too.
		var got string
		require.NoError(t, callAs(ctx, platform, unrestricted, "maa_get_balance", []any{maa, wdBridge},
			func(row *common.Row) error { got = row.Values[0].(*kwilTypes.Decimal).String(); return nil }))
		require.Equal(t, "40000000000000000000", got)

		// The join is audited exactly as a plain maa_join would be.
		var evtTypes []string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_events", []any{ruleID, int64(100), int64(0)},
			func(row *common.Row) error {
				evtTypes = append(evtTypes, row.Values[2].(string))
				return nil
			}))
		require.Contains(t, evtTypes, "JOIN", "join-and-fund must record the JOIN audit event")
		return nil
	}
}

// testMAAJoinAndFundValidation: guards that reject BEFORE the join leg writes anything —
// non-positive amount, unknown rule, and the rule creator funding itself.
func testMAAJoinAndFundValidation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))

		orderedsync.ForTestingReset()
		fundAddress(t, ctx, platform, unrestrictedHex, "100000000000000000000", 1)

		// A zero, negative, or NULL fund leg is rejected up front. NULL exercises the
		// `IS NULL` half of the guard specifically: `NULL <= 0` is NULL (not true), so only
		// the explicit null-check stops a NULL from reaching the transfer precompile.
		_, err := joinAndFund(ctx, platform, unrestricted, ruleID, wdBridge, dec(t, "0"))
		require.ErrorContains(t, err, "Funding amount must be positive")
		_, err = joinAndFund(ctx, platform, unrestricted, ruleID, wdBridge, dec(t, "-1"))
		require.ErrorContains(t, err, "Funding amount must be positive")
		err = callAs(ctx, platform, unrestricted, "maa_join_and_fund", []any{ruleID, wdBridge, nil}, nil)
		require.ErrorContains(t, err, "Funding amount must be positive", "a NULL amount must be rejected by the IS NULL guard")
		require.False(t, maaIsKnown(t, ctx, platform, unrestricted, goldenMAA(t)), "a rejected amount must not register the MAA")

		// An unknown rule is rejected by the join leg's first lookup.
		_, err = joinAndFund(ctx, platform, unrestricted, repeat(0x55, 32), wdBridge, dec(t, "1"))
		require.ErrorContains(t, err, "unknown rule_id")

		// The rule creator cannot fund itself into its own rule.
		_, err = joinAndFund(ctx, platform, restricted, ruleID, wdBridge, dec(t, "1"))
		require.ErrorContains(t, err, "unrestricted must differ from the rule creator")

		// Nothing moved in any of the rejected calls.
		require.Equal(t, "100000000000000000000", balance(t, ctx, platform, unrestrictedHex), "no rejected call may debit the funder")
		return nil
	}
}

// testMAAJoinAndFundAtomicity: the reason this action exists — a fund leg that fails AFTER
// the join leg has written the instance row must roll back the join too. Production wraps
// every transaction's route body in a nested DB transaction that is rolled back when the
// route errors; emulate exactly that boundary with a savepoint, as the withdrawal
// atomicity test does.
func testMAAJoinAndFundAtomicity(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))

		// The funder holds only 10 tokens.
		orderedsync.ForTestingReset()
		fundAddress(t, ctx, platform, unrestrictedHex, "10000000000000000000", 1)

		// 1) Insufficient balance: the join leg succeeds, then the transfer leg fails.
		spTx, err := platform.DB.BeginTx(ctx)
		require.NoError(t, err)
		// Guard every failure path: a failed require aborts this function (Goexit still
		// runs defers), and without this the savepoint would leak open on the shared
		// session. A second Rollback after the explicit one below is a no-op.
		defer func() { _ = spTx.Rollback(ctx) }()
		err = callAsOn(ctx, platform, spTx, unrestricted, "maa_join_and_fund",
			[]any{ruleID, wdBridge, dec(t, "40000000000000000000")})
		require.Error(t, err, "funding beyond the funder's balance must abort the whole action")
		require.NoError(t, spTx.Rollback(ctx))
		require.False(t, maaIsKnown(t, ctx, platform, unrestricted, goldenMAA(t)),
			"the join leg must be confined to the rolled-back tx")
		require.Equal(t, "10000000000000000000", balance(t, ctx, platform, unrestrictedHex), "failed activation must move nothing")

		// 2) Invalid bridge: same shape — join writes, dispatch rejects, both roll back.
		spTx2, err := platform.DB.BeginTx(ctx)
		require.NoError(t, err)
		defer func() { _ = spTx2.Rollback(ctx) }()
		err = callAsOn(ctx, platform, spTx2, unrestricted, "maa_join_and_fund",
			[]any{ruleID, "no_such_bridge", dec(t, "1")})
		require.ErrorContains(t, err, "Invalid bridge")
		require.NoError(t, spTx2.Rollback(ctx))
		require.False(t, maaIsKnown(t, ctx, platform, unrestricted, goldenMAA(t)),
			"a bad bridge must not leave a joined-but-unfunded wallet")

		// 3) Nothing half-committed survives: the same funder can still activate cleanly.
		maa, err := joinAndFund(ctx, platform, unrestricted, ruleID, wdBridge, dec(t, "10000000000000000000"))
		require.NoError(t, err, "activation must succeed after the rolled-back attempts")
		maaAddr := addrFromBytes(maa)
		require.Equal(t, "10000000000000000000", balance(t, ctx, platform, maaAddr.Address()))
		require.Equal(t, "0", balance(t, ctx, platform, unrestrictedHex))
		return nil
	}
}

// testMAAJoinAndFundDuplicateAndSecondFunder: the same funder cannot activate twice (top-ups
// are plain transfers), while a different funder joining the same rule derives its own
// distinct wallet — one rule, many funders, many MAAs.
func testMAAJoinAndFundDuplicateAndSecondFunder(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		funder2 := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))

		// Two funders on the SAME bridge instance: the second deposit must chain to the first.
		orderedsync.ForTestingReset()
		fundAddress(t, ctx, platform, unrestrictedHex, "100000000000000000000", 1)
		point1 := int64(1)
		fundAddressAt(t, ctx, platform, funder2.Address(), "100000000000000000000", 2, &point1)

		maa1, err := joinAndFund(ctx, platform, unrestricted, ruleID, wdBridge, dec(t, "30000000000000000000"))
		require.NoError(t, err)
		maa1Addr := addrFromBytes(maa1)

		// A second activation by the same funder is rejected and moves nothing more.
		_, err = joinAndFund(ctx, platform, unrestricted, ruleID, wdBridge, dec(t, "30000000000000000000"))
		require.ErrorContains(t, err, "already joined this rule")
		require.Equal(t, "70000000000000000000", balance(t, ctx, platform, unrestrictedHex), "the funder is debited exactly once")
		require.Equal(t, "30000000000000000000", balance(t, ctx, platform, maa1Addr.Address()), "the wallet holds exactly one funding")

		// A different funder activates its own wallet on the same rule.
		maa2, err := joinAndFund(ctx, platform, funder2, ruleID, wdBridge, dec(t, "20000000000000000000"))
		require.NoError(t, err, "a second funder must be able to join-and-fund the same rule")
		require.Len(t, maa2, 20)
		require.NotEqual(t, maa1, maa2, "each funder derives a distinct MAA")
		require.True(t, maaIsKnown(t, ctx, platform, funder2, maa2))
		maa2Addr := addrFromBytes(maa2)
		require.Equal(t, "80000000000000000000", balance(t, ctx, platform, funder2.Address()))
		require.Equal(t, "20000000000000000000", balance(t, ctx, platform, maa2Addr.Address()))
		return nil
	}
}
