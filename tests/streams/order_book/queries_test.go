//go:build kwiltest

package order_book

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants - match existing erc20-bridge configuration
const (
	testChainQueries         = "sepolia"
	testEscrowQueries        = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	testERC20Queries         = "0x2222222222222222222222222222222222222222"
	testExtensionNameQueries = "sepolia_bridge"
)

var (
	queriesPointCounter     int64  = 200 // Start from 200 to avoid conflicts
	lastBalancePointQueries *int64       // For chaining balance deposits
)

func TestQueries(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_11_Queries",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			// get_order_book tests
			testGetOrderBookEmpty(t),
			testGetOrderBookWithBuyOrders(t),
			testGetOrderBookWithSellOrders(t),
			testGetOrderBookMixed(t),
			testGetOrderBookExcludesHoldings(t),
			testGetOrderBookSortingFIFO(t),

			// get_user_positions tests
			testGetUserPositionsEmpty(t),
			testGetUserPositionsWithHoldings(t),
			testGetUserPositionsWithOrders(t),
			testGetUserPositionsMixed(t),

			// get_market_depth tests
			testGetMarketDepthEmpty(t),
			testGetMarketDepthAggregation(t),

			// get_best_prices tests
			testGetBestPricesNoOrders(t),
			testGetBestPricesOnlyBuy(t),
			testGetBestPricesOnlySell(t),
			testGetBestPricesBothSides(t),

			// get_user_collateral tests
			testGetUserCollateralEmpty(t),
			testGetUserCollateralWithBuyOrders(t),
			testGetUserCollateralWithShares(t),
			testGetUserCollateralMixed(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// ============================================================================
// get_order_book Tests
// ============================================================================

func testGetOrderBookEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Give balance
		err = giveBalanceQueries(ctx, platform, user.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Query empty order book
		var orders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			orders = append(orders, OrderBookEntry{
				ParticipantID: int(row.Values[0].(int64)),
				Price:         int(row.Values[1].(int64)),
				Amount:        row.Values[2].(int64),
				LastUpdated:   row.Values[3].(int64),
				WalletAddress: row.Values[4].(string),
			})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, orders, "empty order book should return no results")

		return nil
	}
}

func testGetOrderBookWithBuyOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Place buy order
		err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		// Query order book
		var orders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			orders = append(orders, OrderBookEntry{
				Price:  int(row.Values[1].(int64)),
				Amount: row.Values[2].(int64),
			})
			return nil
		})
		require.NoError(t, err)

		require.Len(t, orders, 1)
		require.Equal(t, -55, orders[0].Price, "buy order should have negative price")
		require.Equal(t, int64(100), orders[0].Amount)

		return nil
	}
}

func testGetOrderBookWithSellOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Place split order (creates holdings + sell order)
		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 60, 100, nil)
		require.NoError(t, err)

		// Query NO order book (should have sell order at price 40)
		var orders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user, queryID, false, func(row *common.Row) error {
			orders = append(orders, OrderBookEntry{
				Price:  int(row.Values[1].(int64)),
				Amount: row.Values[2].(int64),
			})
			return nil
		})
		require.NoError(t, err)

		require.Len(t, orders, 1)
		require.Equal(t, 40, orders[0].Price, "sell order should have positive price")
		require.Equal(t, int64(100), orders[0].Amount)

		return nil
	}
}

func testGetOrderBookMixed(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		err = giveBalanceQueries(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceQueries(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user1)

		// User1: Buy order
		err = callPlaceBuyOrderQueries(ctx, platform, &user1, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		// User2: Split order (sell NO at 40)
		err = callPlaceSplitOrderQueries(ctx, platform, &user2, queryID, 60, 200, nil)
		require.NoError(t, err)

		// Query YES order book
		var yesOrders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user1, queryID, true, func(row *common.Row) error {
			yesOrders = append(yesOrders, OrderBookEntry{
				Price: int(row.Values[1].(int64)),
			})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, yesOrders, 1)
		require.Equal(t, -55, yesOrders[0].Price)

		// Query NO order book
		var noOrders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user1, queryID, false, func(row *common.Row) error {
			noOrders = append(noOrders, OrderBookEntry{
				Price: int(row.Values[1].(int64)),
			})
			return nil
		})
		require.NoError(t, err)
		require.Len(t, noOrders, 1)
		require.Equal(t, 40, noOrders[0].Price)

		return nil
	}
}

func testGetOrderBookExcludesHoldings(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Split order creates YES holdings + NO sell
		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 60, 100, nil)
		require.NoError(t, err)

		// Query YES order book - should be EMPTY (holdings excluded)
		var yesOrders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			yesOrders = append(yesOrders, OrderBookEntry{})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, yesOrders, "holdings (price=0) should be excluded")

		return nil
	}
}

func testGetOrderBookSortingFIFO(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		err = giveBalanceQueries(ctx, platform, user.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Place orders at different prices
		// Note: Same participant + same price = merged orders (amount summed)
		prices := []int{58, 55, 56, 55} // Two at 55 will merge
		for _, price := range prices {
			err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, true, price, 50, nil)
			require.NoError(t, err)
		}

		// Query order book
		var orders []OrderBookEntry
		err = callGetOrderBook(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			orders = append(orders, OrderBookEntry{
				Price:       int(row.Values[1].(int64)),
				Amount:      row.Values[2].(int64),
				LastUpdated: row.Values[3].(int64),
			})
			return nil
		})
		require.NoError(t, err)

		// Should have 3 orders: 55 (merged, 100 shares), 56, 58
		require.Len(t, orders, 3)

		// Best price first (merged order at -55 with 100 shares)
		require.Equal(t, -55, orders[0].Price)
		require.Equal(t, int64(100), orders[0].Amount, "two 50-share orders at -55 should merge to 100")

		// Next prices in order
		require.Equal(t, -56, orders[1].Price)
		require.Equal(t, -58, orders[2].Price)

		return nil
	}
}

// ============================================================================
// get_user_positions Tests
// ============================================================================

func testGetUserPositionsEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Query user with no positions
		var positions []UserPosition
		err = callGetUserPositions(ctx, platform, &user, func(row *common.Row) error {
			positions = append(positions, UserPosition{})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, positions)

		return nil
	}
}

func testGetUserPositionsWithHoldings(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Split order creates holdings
		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 60, 100, nil)
		require.NoError(t, err)

		// Query positions
		var positions []UserPosition
		err = callGetUserPositions(ctx, platform, &user, func(row *common.Row) error {
			positions = append(positions, UserPosition{
				Price:        int(row.Values[2].(int64)),
				PositionType: row.Values[4].(string),
			})
			return nil
		})
		require.NoError(t, err)

		// Should have 2 positions: YES holding + NO sell
		require.Len(t, positions, 2)

		// Find holding
		var hasHolding bool
		for _, pos := range positions {
			if pos.PositionType == "holding" && pos.Price == 0 {
				hasHolding = true
				break
			}
		}
		require.True(t, hasHolding, "should have a holding")

		return nil
	}
}

func testGetUserPositionsWithOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Buy order
		err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		// Query positions
		var positions []UserPosition
		err = callGetUserPositions(ctx, platform, &user, func(row *common.Row) error {
			positions = append(positions, UserPosition{
				PositionType: row.Values[4].(string),
			})
			return nil
		})
		require.NoError(t, err)

		require.Len(t, positions, 1)
		require.Equal(t, "buy_order", positions[0].PositionType)

		return nil
	}
}

func testGetUserPositionsMixed(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

		err = giveBalanceQueries(ctx, platform, user.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Buy + split
		err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, false, 45, 50, nil)
		require.NoError(t, err)

		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 60, 100, nil)
		require.NoError(t, err)

		// Query positions
		var positions []UserPosition
		err = callGetUserPositions(ctx, platform, &user, func(row *common.Row) error {
			positions = append(positions, UserPosition{
				PositionType: row.Values[4].(string),
			})
			return nil
		})
		require.NoError(t, err)

		// Should have 3: buy, holding, sell
		require.Len(t, positions, 3)

		typeCount := make(map[string]int)
		for _, pos := range positions {
			typeCount[pos.PositionType]++
		}
		require.Equal(t, 1, typeCount["buy_order"])
		require.Equal(t, 1, typeCount["holding"])
		require.Equal(t, 1, typeCount["sell_order"])

		return nil
	}
}

// ============================================================================
// get_market_depth Tests
// ============================================================================

func testGetMarketDepthEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

		err = giveBalanceQueries(ctx, platform, user.Address(), "100000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		var depths []MarketDepth
		err = callGetMarketDepth(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			depths = append(depths, MarketDepth{})
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, depths)

		return nil
	}
}

func testGetMarketDepthAggregation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")

		err = giveBalanceQueries(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceQueries(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user1)

		// 2 buy orders at price 55
		err = callPlaceBuyOrderQueries(ctx, platform, &user1, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		err = callPlaceBuyOrderQueries(ctx, platform, &user2, queryID, true, 55, 50, nil)
		require.NoError(t, err)

		// Query depth
		var depths []MarketDepth
		err = callGetMarketDepth(ctx, platform, &user1, queryID, true, func(row *common.Row) error {
			depths = append(depths, MarketDepth{
				Price:     int(row.Values[0].(int64)),
				BuyVolume: row.Values[1].(int64),
			})
			return nil
		})
		require.NoError(t, err)

		require.Len(t, depths, 1, "should aggregate same price")
		require.Equal(t, 55, depths[0].Price)
		require.Equal(t, int64(150), depths[0].BuyVolume, "100+50")

		return nil
	}
}

// ============================================================================
// get_best_prices Tests
// ============================================================================

func testGetBestPricesNoOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

		err = giveBalanceQueries(ctx, platform, user.Address(), "100000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		type BestPrices struct {
			BestBid *int
			BestAsk *int
		}

		var result BestPrices
		err = callGetBestPrices(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			if row.Values[0] != nil {
				bid := int(row.Values[0].(int64))
				result.BestBid = &bid
			}
			if row.Values[1] != nil {
				ask := int(row.Values[1].(int64))
				result.BestAsk = &ask
			}
			return nil
		})
		require.NoError(t, err)
		require.Nil(t, result.BestBid)
		require.Nil(t, result.BestAsk)

		return nil
	}
}

func testGetBestPricesOnlyBuy(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x1010101010101010101010101010101010101010")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		var bestBid *int
		err = callGetBestPrices(ctx, platform, &user, queryID, true, func(row *common.Row) error {
			if row.Values[0] != nil {
				bid := int(row.Values[0].(int64))
				bestBid = &bid
			}
			return nil
		})
		require.NoError(t, err)

		require.NotNil(t, bestBid)
		require.Equal(t, 55, *bestBid)

		return nil
	}
}

func testGetBestPricesOnlySell(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x2020202020202020202020202020202020202020")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Split creates sell order
		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 60, 100, nil)
		require.NoError(t, err)

		var bestAsk *int
		err = callGetBestPrices(ctx, platform, &user, queryID, false, func(row *common.Row) error {
			if row.Values[1] != nil {
				ask := int(row.Values[1].(int64))
				bestAsk = &ask
			}
			return nil
		})
		require.NoError(t, err)

		require.NotNil(t, bestAsk)
		require.Equal(t, 40, *bestAsk)

		return nil
	}
}

func testGetBestPricesBothSides(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x3030303030303030303030303030303030303030")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x4040404040404040404040404040404040404040")

		err = giveBalanceQueries(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceQueries(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user1)

		// User1: Buy @55
		err = callPlaceBuyOrderQueries(ctx, platform, &user1, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		// User2: Split to get holdings, then sell @60
		err = callPlaceSplitOrderQueries(ctx, platform, &user2, queryID, 50, 50, nil)
		require.NoError(t, err)

		err = callPlaceSellOrderQueries(ctx, platform, &user2, queryID, true, 60, 50, nil)
		require.NoError(t, err)

		var bestBid, bestAsk, spread *int
		err = callGetBestPrices(ctx, platform, &user1, queryID, true, func(row *common.Row) error {
			if row.Values[0] != nil {
				bid := int(row.Values[0].(int64))
				bestBid = &bid
			}
			if row.Values[1] != nil {
				ask := int(row.Values[1].(int64))
				bestAsk = &ask
			}
			if row.Values[2] != nil {
				s := int(row.Values[2].(int64))
				spread = &s
			}
			return nil
		})
		require.NoError(t, err)

		require.NotNil(t, bestBid)
		require.NotNil(t, bestAsk)
		require.NotNil(t, spread)
		require.Equal(t, 55, *bestBid)
		require.Equal(t, 60, *bestAsk)
		require.Equal(t, 5, *spread)

		return nil
	}
}

// ============================================================================
// get_user_collateral Tests
// ============================================================================

func testGetUserCollateralEmpty(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x5050505050505050505050505050505050505050")

		var total string
		err = callGetUserCollateral(ctx, platform, &user, func(row *common.Row) error {
			total = row.Values[0].(*types.Decimal).String()
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, "0", total)

		return nil
	}
}

func testGetUserCollateralWithBuyOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x6060606060606060606060606060606060606060")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Buy: 100 @ 55 = 55 tokens
		err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, true, 55, 100, nil)
		require.NoError(t, err)

		var buyLocked string
		err = callGetUserCollateral(ctx, platform, &user, func(row *common.Row) error {
			buyLocked = row.Values[1].(*types.Decimal).String()
			return nil
		})
		require.NoError(t, err)

		// 55 * 10^18
		require.Equal(t, "55000000000000000000", buyLocked)

		return nil
	}
}

func testGetUserCollateralWithShares(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x7070707070707070707070707070707070707070")

		err = giveBalanceQueries(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Split: 100 shares = 200 total (YES + NO)
		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 60, 100, nil)
		require.NoError(t, err)

		var shareValue string
		err = callGetUserCollateral(ctx, platform, &user, func(row *common.Row) error {
			shareValue = row.Values[2].(*types.Decimal).String()
			return nil
		})
		require.NoError(t, err)

		// 200 * 10^18
		require.Equal(t, "200000000000000000000", shareValue)

		return nil
	}
}

func testGetUserCollateralMixed(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePointQueries = nil // Reset for this test

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0x8080808080808080808080808080808080808080")

		err = giveBalanceQueries(ctx, platform, user.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryID, _ := createTestMarketQueries(t, ctx, platform, &user)

		// Buy: 100 @ 50 = 50 tokens
		err = callPlaceBuyOrderQueries(ctx, platform, &user, queryID, true, 50, 100, nil)
		require.NoError(t, err)

		// Split: 150 = 300 shares
		err = callPlaceSplitOrderQueries(ctx, platform, &user, queryID, 55, 150, nil)
		require.NoError(t, err)

		var total, buyLocked, shareValue string
		err = callGetUserCollateral(ctx, platform, &user, func(row *common.Row) error {
			total = row.Values[0].(*types.Decimal).String()
			buyLocked = row.Values[1].(*types.Decimal).String()
			shareValue = row.Values[2].(*types.Decimal).String()
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, "50000000000000000000", buyLocked)
		require.Equal(t, "300000000000000000000", shareValue)
		require.Equal(t, "350000000000000000000", total)

		return nil
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

func giveBalanceQueries(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	queriesPointCounter++
	currentPoint := queriesPointCounter

	err := testerc20.InjectERC20Transfer(
		ctx, platform,
		testChainQueries,
		testEscrowQueries,
		testERC20Queries,
		wallet, wallet,
		amountStr,
		currentPoint,
		lastBalancePointQueries, // Chain to previous
	)

	if err == nil {
		lastBalancePointQueries = &currentPoint // Update for next call
	}
	return err
}

func createTestMarketQueries(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress) (int, []byte) {
	queryData := []byte(fmt.Sprintf("test:stream:%d", time.Now().UnixNano()))
	queryHash := sha256.Sum256(queryData)
	settleTime := time.Now().Add(1 * time.Hour).Unix()

	var queryID int64
	err := callActionQueries(ctx, platform, signer, "create_market",
		[]any{testExtensionNameQueries, queryHash[:], settleTime, int64(5), int64(20)},
		func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
	require.NoError(t, err)

	return int(queryID), queryHash[:]
}

func callGetOrderBook(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, outcome bool, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_order_book",
		[]any{queryID, outcome}, resultFn)
}

func callGetUserPositions(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_user_positions", []any{}, resultFn)
}

func callGetMarketDepth(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, outcome bool, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_market_depth",
		[]any{queryID, outcome}, resultFn)
}

func callGetBestPrices(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, outcome bool, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_best_prices",
		[]any{queryID, outcome}, resultFn)
}

func callGetUserCollateral(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "get_user_collateral", []any{}, resultFn)
}

func callPlaceBuyOrderQueries(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, outcome bool, price, amount int, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "place_buy_order",
		[]any{queryID, outcome, price, int64(amount)}, resultFn)
}

func callPlaceSellOrderQueries(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, outcome bool, price, amount int, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "place_sell_order",
		[]any{queryID, outcome, price, int64(amount)}, resultFn)
}

func callPlaceSplitOrderQueries(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, truePrice, amount int, resultFn func(*common.Row) error) error {
	return callActionQueries(ctx, platform, signer, "place_split_limit_order",
		[]any{queryID, truePrice, int64(amount)}, resultFn)
}

func callActionQueries(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, actionName string, args []any, resultFn func(*common.Row) error) error {
	// Generate leader key for fee transfers
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
			Proposer:  pub,
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", actionName, args, resultFn)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

// ============================================================================
// Helper Types
// ============================================================================

type OrderBookEntry struct {
	ParticipantID int
	Price         int
	Amount        int64
	LastUpdated   int64
	WalletAddress string
}

type UserPosition struct {
	QueryID      int
	Outcome      bool
	Price        int
	Amount       int64
	PositionType string
}

type MarketDepth struct {
	Price      int
	BuyVolume  int64
	SellVolume int64
}
