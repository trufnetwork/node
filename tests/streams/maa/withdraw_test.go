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

// The withdrawal tests run against the hoodi_tt2 (USDC) test bridge — one of the per-token
// instances an agent wallet can hold. Balances are 18-decimal in the test harness.
const (
	wdBridge = "hoodi_tt2"
	wdChain  = "hoodi"
	wdEscrow = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	wdERC20  = "0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd"
)

func TestMAAWithdraw(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_Withdraw",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testMAAWithdrawInternalBps(t),
			testMAAWithdrawGuards(t),
			testMAAWithdrawFlatFeeClamped(t),
			testMAABridgeOut(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func addrFromBytes(b []byte) util.EthereumAddress {
	return util.Unsafe_NewEthereumAddressFromString("0x" + hex.EncodeToString(b))
}

// fundAddress credits `to` on the hoodi_tt2 bridge with `amount` base units.
func fundAddress(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, to string, amount string, point int64) {
	t.Helper()
	require.NoError(t, testerc20.InjectERC20Transfer(
		ctx, platform, wdChain, wdEscrow, wdERC20, to, to, amount, point, nil,
	))
}

// balance reads an address's hoodi_tt2 ledger balance as a string ("0" when there is no row).
func balance(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, addr string) string {
	t.Helper()
	b, err := testerc20.GetUserBalance(ctx, platform, wdBridge, addr)
	require.NoError(t, err)
	if b == "" || b == "<nil>" {
		return "0"
	}
	return b
}

// setupFundedMAA creates a rule (restricted), joins it (unrestricted), funds the derived MAA
// with `fund` base units, and returns the MAA address and the created rule ID. The ERC20
// extension is initialized first.
func setupFundedMAA(
	t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	restricted, unrestricted util.EthereumAddress, feeBps int64, salt []byte, fund string,
) (maa []byte, ruleID []byte) {
	t.Helper()
	require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

	ruleID = createDefaultRule(t, ctx, platform, restricted, feeBps, salt)
	maa = joinRule(t, ctx, platform, unrestricted, ruleID)

	orderedsync.ForTestingReset()
	maaAddr := addrFromBytes(maa)
	fundAddress(t, ctx, platform, maaAddr.Address(), fund, 1)
	return maa, ruleID
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// testMAAWithdrawInternalBps: the owner withdraws the whole funded balance internally; the
// agent earns the bps commission, the owner receives the remainder, the MAA is drained.
func testMAAWithdrawInternalBps(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		// 100 tokens funded; 250 bps (2.5%) commission.
		maa, ruleID := setupFundedMAA(t, ctx, platform, restricted, unrestricted, 250, repeat(0xab, 32), "100000000000000000000")
		maaAddr := addrFromBytes(maa)

		// maa_get_balance reports the funded amount.
		var got string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_balance", []any{maa, wdBridge},
			func(row *common.Row) error { got = row.Values[0].(*kwilTypes.Decimal).String(); return nil }))
		require.Equal(t, "100000000000000000000", got)

		// Withdraw the full 100 tokens, running AS the MAA (@caller = MAA, as the route would set it).
		require.NoError(t, callAs(ctx, platform, maaAddr, "maa_withdraw", []any{wdBridge, dec(t, "100000000000000000000")}, nil),
			"owner withdrawal as the MAA must succeed")

		// commission = 100e18 * 250 / 10000 = 2.5e18; payout = 97.5e18; MAA drained.
		require.Equal(t, "2500000000000000000", balance(t, ctx, platform, restrictedHex), "agent earns the commission")
		require.Equal(t, "97500000000000000000", balance(t, ctx, platform, unrestrictedHex), "owner receives the remainder")
		require.Equal(t, "0", balance(t, ctx, platform, maaAddr.Address()), "agent wallet is drained")

		// A WITHDRAW audit event is recorded for this rule, carrying the gross amount.
		var evtTypes []string
		var withdrawAmount string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_events", []any{ruleID, int64(100), int64(0)},
			func(row *common.Row) error {
				evtType := row.Values[2].(string)
				evtTypes = append(evtTypes, evtType)
				if evtType == "WITHDRAW" && row.Values[7] != nil {
					withdrawAmount = row.Values[7].(*kwilTypes.Decimal).String()
				}
				return nil
			}))
		require.Contains(t, evtTypes, "WITHDRAW")
		require.Equal(t, "100000000000000000000", withdrawAmount, "the WITHDRAW event records the gross amount")
		return nil
	}
}

// testMAAWithdrawGuards: a non-MAA caller and an over-balance withdrawal must both fail closed
// and move nothing.
func testMAAWithdrawGuards(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		maa, _ := setupFundedMAA(t, ctx, platform, restricted, unrestricted, 250, repeat(0xab, 32), "100000000000000000000")
		maaAddr := addrFromBytes(maa)

		// A normal signer (not a known MAA) cannot withdraw — @caller is not an agent wallet.
		require.Error(t, callAs(ctx, platform, unrestricted, "maa_withdraw", []any{wdBridge, dec(t, "1")}, nil),
			"a non-MAA caller must not be able to withdraw")
		require.Equal(t, "100000000000000000000", balance(t, ctx, platform, maaAddr.Address()), "balance unchanged after rejected withdraw")

		// Withdrawing more than the free balance must fail.
		require.Error(t, callAs(ctx, platform, maaAddr, "maa_withdraw", []any{wdBridge, dec(t, "100000000000000000001")}, nil),
			"over-balance withdrawal must be rejected")
		require.Equal(t, "100000000000000000000", balance(t, ctx, platform, maaAddr.Address()), "balance unchanged after rejected over-withdraw")

		// A non-positive amount must be rejected.
		require.Error(t, callAs(ctx, platform, maaAddr, "maa_withdraw", []any{wdBridge, dec(t, "0")}, nil),
			"zero-amount withdrawal must be rejected")
		return nil
	}
}

// testMAAWithdrawFlatFeeClamped: a flat-fee rule where the fee exceeds the withdrawal is clamped
// to the gross — the agent gets everything, the owner gets nothing, and the move still balances.
func testMAAWithdrawFlatFeeClamped(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		// Flat fee of 10 tokens.
		var ruleID []byte
		require.NoError(t, callAs(ctx, platform, restricted, "maa_create_rule", []any{
			repeat(0x77, 32), "flat", int64(0), dec(t, "10000000000000000000"),
			[]string{}, []string{}, [][]byte{},
		}, func(row *common.Row) error {
			ruleID = append([]byte(nil), row.Values[0].([]byte)...)
			return nil
		}))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)

		orderedsync.ForTestingReset()
		// Fund only 4 tokens — less than the 10-token flat fee.
		fundAddress(t, ctx, platform, maaAddr.Address(), "4000000000000000000", 1)

		require.NoError(t, callAs(ctx, platform, maaAddr, "maa_withdraw", []any{wdBridge, dec(t, "4000000000000000000")}, nil),
			"withdrawal below the flat fee must still succeed (fee clamped)")

		// Commission clamped to the 4-token gross; owner gets nothing; MAA drained.
		require.Equal(t, "4000000000000000000", balance(t, ctx, platform, restrictedHex), "flat fee clamped to the gross")
		require.Equal(t, "0", balance(t, ctx, platform, unrestrictedHex), "owner receives nothing when the fee consumes the gross")
		require.Equal(t, "0", balance(t, ctx, platform, maaAddr.Address()), "agent wallet is drained")
		return nil
	}
}

// testMAABridgeOut: the owner bridges the payout off to L1 while the agent still earns its
// commission internally. The MAA is debited in full and the commission lands with the agent.
func testMAABridgeOut(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		maa, _ := setupFundedMAA(t, ctx, platform, restricted, unrestricted, 250, repeat(0xab, 32), "100000000000000000000")
		maaAddr := addrFromBytes(maa)

		// Bridge the whole balance off; recipient defaults to the owner.
		require.NoError(t, callAs(ctx, platform, maaAddr, "maa_bridge_out", []any{wdBridge, dec(t, "100000000000000000000"), nil}, nil),
			"bridge-out as the MAA must succeed")

		// Commission lands with the agent internally; the MAA is fully debited; the payout left
		// the ledger to L1, so the owner's internal balance is untouched.
		require.Equal(t, "2500000000000000000", balance(t, ctx, platform, restrictedHex), "agent earns the commission on bridge-out")
		require.Equal(t, "0", balance(t, ctx, platform, maaAddr.Address()), "agent wallet is drained")
		require.Equal(t, "0", balance(t, ctx, platform, unrestrictedHex), "the payout was bridged to L1, not credited internally")
		return nil
	}
}
