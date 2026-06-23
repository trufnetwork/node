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
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestPortfolioByWallet exercises the address-parameterized portfolio getters
// (get_positions_by_wallet / get_collateral_by_wallet) added in migration 051.
//
// Unlike get_user_positions / get_user_collateral (which read @caller), these
// take the wallet as an argument so an owner can read an agent wallet's (MAA)
// portfolio without holding its key. Every test signs the read as a DIFFERENT
// wallet than the one whose portfolio it asks for — that is the property the
// feature exists for.
func TestPortfolioByWallet(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_PORTFOLIO_BY_WALLET",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testGetPositionsByWalletEmpty(t),
			testGetPositionsByWalletReadsAnotherWallet(t),
			testGetPositionsByWalletAcceptsBareHex(t),
			testGetPositionsByWalletRejectsMalformedAddress(t),
			testGetCollateralByWalletEmpty(t),
			testGetCollateralByWalletReadsAnotherWallet(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// An address that never traded returns an empty result, even though the reader
// is a different (also-non-participant) wallet.
func testGetPositionsByWalletEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil
		lastTrufBalancePointQueries = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		reader := util.Unsafe_NewEthereumAddressFromString("0xC1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1")
		neverTraded := "0xD2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2D2"

		var positions []UserPosition
		err = callGetPositionsByWallet(ctx, platform, &reader, neverTraded, func(row *common.Row) error {
			positions = append(positions, UserPosition{})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, positions, "a wallet that never traded should have no positions")

		return nil
	}
}

// A trader places a buy order; a DIFFERENT reader asks for the trader's
// positions by address and sees the buy order — while the reader's own
// @caller-scoped positions stay empty.
func testGetPositionsByWalletReadsAnotherWallet(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil
		lastTrufBalancePointQueries = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		trader := util.Unsafe_NewEthereumAddressFromString("0xC3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3")
		reader := util.Unsafe_NewEthereumAddressFromString("0xC4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4")

		err = giveBalanceQueries(ctx, platform, trader.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &trader)

		err = callPlaceBuyOrderQueries(ctx, platform, &trader, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		// The reader (a different wallet) reads the trader's positions by address.
		var positions []UserPosition
		err = callGetPositionsByWallet(ctx, platform, &reader, trader.Address(), func(row *common.Row) error {
			positions = append(positions, UserPosition{
				QueryID:      int(row.Values[0].(int64)),
				PositionType: row.Values[4].(string),
			})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, positions, 1, "reader should see the trader's single buy order")
		require.Equal(t, "buy_order", positions[0].PositionType)
		require.Equal(t, queryID, positions[0].QueryID)

		// Proof this is NOT @caller-scoped: the reader's own portfolio is empty.
		var readerOwn []UserPosition
		err = callGetUserPositions(ctx, platform, &reader, func(row *common.Row) error {
			readerOwn = append(readerOwn, UserPosition{})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, readerOwn, "the reader has no positions of its own")

		return nil
	}
}

// The getter accepts the wallet with OR without a 0x prefix (the bot may hold
// either form), returning the same positions.
func testGetPositionsByWalletAcceptsBareHex(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil
		lastTrufBalancePointQueries = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		trader := util.Unsafe_NewEthereumAddressFromString("0xC9C9C9C9C9C9C9C9C9C9C9C9C9C9C9C9C9C9C9C9")
		reader := util.Unsafe_NewEthereumAddressFromString("0xCACACACACACACACACACACACACACACACACACACACA")

		err = giveBalanceQueries(ctx, platform, trader.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &trader)

		err = callPlaceBuyOrderQueries(ctx, platform, &trader, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		// Strip the 0x prefix — the getter must still resolve the same wallet.
		bareHex := trader.Address()[2:]

		var positions []UserPosition
		err = callGetPositionsByWallet(ctx, platform, &reader, bareHex, func(row *common.Row) error {
			positions = append(positions, UserPosition{PositionType: row.Values[4].(string)})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, positions, 1, "bare hex (no 0x) must resolve the same wallet")
		require.Equal(t, "buy_order", positions[0].PositionType)

		return nil
	}
}

// A malformed wallet argument is rejected (the normalization must not silently
// resolve a garbage address to an empty portfolio).
func testGetPositionsByWalletRejectsMalformedAddress(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil
		lastTrufBalancePointQueries = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		reader := util.Unsafe_NewEthereumAddressFromString("0xCBCBCBCBCBCBCBCBCBCBCBCBCBCBCBCBCBCBCBCB")

		err = callGetPositionsByWallet(ctx, platform, &reader, "0x1234", func(row *common.Row) error {
			return nil
		})
		require.Error(t, err, "a too-short wallet address must be rejected")

		return nil
	}
}

// An address that never traded reports zero collateral, read by a different wallet.
func testGetCollateralByWalletEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil
		lastTrufBalancePointQueries = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		reader := util.Unsafe_NewEthereumAddressFromString("0xC5C5C5C5C5C5C5C5C5C5C5C5C5C5C5C5C5C5C5C5")
		neverTraded := "0xD6D6D6D6D6D6D6D6D6D6D6D6D6D6D6D6D6D6D6D6"

		var total string
		err = callGetCollateralByWallet(ctx, platform, &reader, neverTraded, testExtensionNameQueries, func(row *common.Row) error {
			total = row.Values[0].(*types.Decimal).String()
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, "0", total, "a wallet that never traded has no locked collateral")

		return nil
	}
}

// A trader locks collateral in a buy order; a different reader reads the
// trader's collateral by address and sees the locked amount.
func testGetCollateralByWalletReadsAnotherWallet(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil
		lastTrufBalancePointQueries = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		trader := util.Unsafe_NewEthereumAddressFromString("0xC7C7C7C7C7C7C7C7C7C7C7C7C7C7C7C7C7C7C7C7")
		reader := util.Unsafe_NewEthereumAddressFromString("0xC8C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8C8")

		err = giveBalanceQueries(ctx, platform, trader.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &trader)

		// Buy 100 @ 55 = 55 tokens locked.
		err = callPlaceBuyOrderQueries(ctx, platform, &trader, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		var total, buyLocked, shareValue string
		err = callGetCollateralByWallet(ctx, platform, &reader, trader.Address(), testExtensionNameQueries, func(row *common.Row) error {
			total = row.Values[0].(*types.Decimal).String()
			buyLocked = row.Values[1].(*types.Decimal).String()
			shareValue = row.Values[2].(*types.Decimal).String()
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, "55000000000000000000", buyLocked, "55 tokens locked in the buy order")
		require.Equal(t, "0", shareValue, "no shares held")
		require.Equal(t, "55000000000000000000", total, "total equals buy-order collateral")

		return nil
	}
}

// ============================================================================
// Helpers for the address-parameterized getters
// ============================================================================

func callGetPositionsByWallet(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, walletAddress string, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_positions_by_wallet", []any{walletAddress}, resultFn)
}

func callGetCollateralByWallet(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, walletAddress string, bridge string, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_collateral_by_wallet", []any{walletAddress, bridge}, resultFn)
}
