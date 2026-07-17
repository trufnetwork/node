//go:build kwiltest

package order_book

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// hoodi_tt2 (the dev "USDC" bridge) registration values — see
// internal/migrations/erc20-bridge/000-extension.sql. hoodi_tt ("TRUF") reuses the
// testTRUF* constants from market_creation_test.go.
const (
	richlistUSDCChain  = "hoodi"
	richlistUSDCEscrow = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	richlistUSDCERC20  = "0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd"

	// Distinct, unambiguous holder addresses (returned lowercased by the action).
	rlWalletA = "0xAAaAAaAaAAaAAaAAAAAAaaAAaAAAAaAaaaAaAaaA"
	rlWalletB = "0xBBbBBbBBbBbbBbbbbBbBBBBBBBbBBbbbBBbBBBBb"
	rlWalletC = "0xCcCCccCCCCCCCCCcCCcCcCCCCCccCCcCccCCcCCc"
)

// Chained ordered-sync points per bridge instance (reset per test fn).
var (
	rlTrufPoint int64 = 7000
	rlUsdcPoint int64 = 8000
	rlLastTruf  *int64
	rlLastUsdc  *int64
)

func seedTrufBalance(ctx context.Context, platform *kwilTesting.Platform, wallet, amount string) error {
	rlTrufPoint++
	p := rlTrufPoint
	err := testerc20.InjectERC20Transfer(ctx, platform, testTRUFChain, testTRUFEscrow, testTRUFERC20,
		wallet, wallet, amount, p, rlLastTruf)
	if err == nil {
		rlLastTruf = &p
	}
	return err
}

func seedUsdcBalance(ctx context.Context, platform *kwilTesting.Platform, wallet, amount string) error {
	rlUsdcPoint++
	p := rlUsdcPoint
	err := testerc20.InjectERC20Transfer(ctx, platform, richlistUSDCChain, richlistUSDCEscrow, richlistUSDCERC20,
		wallet, wallet, amount, p, rlLastUsdc)
	if err == nil {
		rlLastUsdc = &p
	}
	return err
}

type richlistRow struct {
	address string
	balance string
}

func callGetOrderedBalances(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, token string, ascending bool, limit int, minBalance any) ([]richlistRow, error) {
	var rows []richlistRow
	err := callActionQueries(ctx, platform, signer, "get_ordered_balances",
		[]any{token, ascending, int64(limit), minBalance},
		func(row *common.Row) error {
			rows = append(rows, richlistRow{
				address: row.Values[0].(string),
				balance: row.Values[1].(*types.Decimal).String(),
			})
			return nil
		})
	return rows, err
}

func richlistReader() *util.EthereumAddress {
	// Reads are signed by an arbitrary wallet that holds nothing itself.
	r := util.Unsafe_NewEthereumAddressFromString("0xC1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1")
	return &r
}

// TestRichlist exercises get_ordered_balances (migration 053, trufscan #185):
// a read-only richlist of a token's wallets ordered by balance.
func TestRichlist(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_RICHLIST",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testRichlistDescending(t),
			testRichlistAscending(t),
			testRichlistLimit(t),
			testRichlistThreshold(t),
			testRichlistUsdcIsolatedFromTruf(t),
			testRichlistUnknownToken(t),
			testRichlistEmpty(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func richlistSetup(ctx context.Context, platform *kwilTesting.Platform) error {
	rlLastTruf = nil
	rlLastUsdc = nil
	return erc20bridge.ForTestingInitializeExtension(ctx, platform)
}

// Descending (default): largest balance first.
func testRichlistDescending(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletA, "300"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletB, "100"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletC, "200"))

		rows, err := callGetOrderedBalances(ctx, platform, richlistReader(), "TRUF", false, 20, nil)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		require.Equal(t, []string{"300", "200", "100"}, []string{rows[0].balance, rows[1].balance, rows[2].balance})
		// Address is returned as lowercase 0x-hex.
		require.Equal(t, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", rows[0].address)
		return nil
	}
}

// Ascending: smallest balance first. Token is case-insensitive.
func testRichlistAscending(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletA, "300"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletB, "100"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletC, "200"))

		rows, err := callGetOrderedBalances(ctx, platform, richlistReader(), "truf", true, 20, nil)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		require.Equal(t, []string{"100", "200", "300"}, []string{rows[0].balance, rows[1].balance, rows[2].balance})
		return nil
	}
}

// limit returns exactly the top-N, and a limit above the hard cap does not error.
func testRichlistLimit(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletA, "300"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletB, "100"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletC, "200"))

		top2, err := callGetOrderedBalances(ctx, platform, richlistReader(), "TRUF", false, 2, nil)
		require.NoError(t, err)
		require.Len(t, top2, 2)
		require.Equal(t, []string{"300", "200"}, []string{top2[0].balance, top2[1].balance})

		// A limit above the hard cap of 50 is clamped, not rejected.
		all, err := callGetOrderedBalances(ctx, platform, richlistReader(), "TRUF", false, 1000, nil)
		require.NoError(t, err)
		require.Len(t, all, 3)
		return nil
	}
}

// min_balance filters out wallets below the threshold.
func testRichlistThreshold(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletA, "300"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletB, "100"))
		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletC, "200"))

		threshold := types.MustParseDecimalExplicit("150", 78, 0)
		rows, err := callGetOrderedBalances(ctx, platform, richlistReader(), "TRUF", false, 20, threshold)
		require.NoError(t, err)
		require.Len(t, rows, 2, "only wallets with balance >= 150")
		require.Equal(t, []string{"300", "200"}, []string{rows[0].balance, rows[1].balance})
		return nil
	}
}

// USDC reads the hoodi_tt2 instance and does not leak TRUF (hoodi_tt) balances —
// the reward_id keys the two ledgers apart.
func testRichlistUsdcIsolatedFromTruf(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		require.NoError(t, seedTrufBalance(ctx, platform, rlWalletA, "999"))
		require.NoError(t, seedUsdcBalance(ctx, platform, rlWalletB, "700"))
		require.NoError(t, seedUsdcBalance(ctx, platform, rlWalletC, "500"))

		rows, err := callGetOrderedBalances(ctx, platform, richlistReader(), "USDC", false, 20, nil)
		require.NoError(t, err)
		require.Len(t, rows, 2, "only the two USDC holders, not the TRUF holder")
		require.Equal(t, []string{"700", "500"}, []string{rows[0].balance, rows[1].balance})
		return nil
	}
}

// An unrecognized token errors clearly rather than returning an empty list.
func testRichlistUnknownToken(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		_, err := callGetOrderedBalances(ctx, platform, richlistReader(), "DOGE", false, 20, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported token")
		return nil
	}
}

// A token with no holders returns an empty result (not an error).
func testRichlistEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		require.NoError(t, richlistSetup(ctx, platform))

		rows, err := callGetOrderedBalances(ctx, platform, richlistReader(), "TRUF", false, 20, nil)
		require.NoError(t, err)
		require.Empty(t, rows)
		return nil
	}
}
