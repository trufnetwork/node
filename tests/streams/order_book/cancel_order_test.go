//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestCancelOrder is the main test suite for cancel_order functionality
func TestCancelOrder(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_05_CancelOrder",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testCancelBuyOrderSuccess(t),
			testCancelSellOrderSuccess(t),
			testCancelBuyOrderMultiplePrices(t),
			testCancelOrderNotFound(t),
			// testCancelOrderMarketSettled(t), // TODO: Enable after Issue 7 (settlement) is implemented
			testCancelOrderMarketNotFound(t),
			testCancelOrderInvalidPrice(t),
			testCancelOrderHoldings(t),
			testCancelBuyOrderVerifyRefund(t),
			testCancelSellOrderVerifyShares(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testCancelBuyOrderSuccess tests successful cancellation of a buy order
func testCancelBuyOrderSuccess(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to initialize ERC20 extension")

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000027", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err, "create_market should succeed")

		// Place buy order: 10 YES @ $0.56
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 10)
		require.NoError(t, err, "place_buy_order should succeed")

		// Verify order exists
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 1, "should have 1 buy order")
		require.Equal(t, int16(-56), positions[0].Price, "price should be -56")

		// Cancel the buy order
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, -56)
		require.NoError(t, err, "cancel_order should succeed")

		// Verify order is deleted
		positions, err = getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 0, "order should be deleted after cancel")

		return nil
	}
}

// testCancelSellOrderSuccess tests successful cancellation of a sell order
func testCancelSellOrderSuccess(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000028", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order to get shares
		// This creates YES holdings + NO sell order @ $0.44
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 10)
		require.NoError(t, err, "place_split_limit_order should succeed")

		// Verify we have NO sell order @ 44
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have YES holding + NO sell order")

		// Find the NO sell order
		var hasSellOrder bool
		for _, pos := range positions {
			if !pos.Outcome && pos.Price == 44 {
				hasSellOrder = true
				require.Equal(t, int64(10), pos.Amount)
			}
		}
		require.True(t, hasSellOrder, "should have NO sell order @ 44")

		// Cancel the sell order
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), false, 44)
		require.NoError(t, err, "cancel_order should succeed")

		// Verify sell order is deleted and shares moved to holdings
		positions, err = getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Should now have YES holding (10) + NO holding (10)
		var yesHolding, noHolding int64
		for _, pos := range positions {
			if pos.Price == 0 {
				if pos.Outcome {
					yesHolding = pos.Amount
				} else {
					noHolding = pos.Amount
				}
			}
		}

		require.Equal(t, int64(10), yesHolding, "YES holdings should be 10")
		require.Equal(t, int64(10), noHolding, "NO holdings should be 10 (returned from cancelled sell)")

		return nil
	}
}

// testCancelBuyOrderMultiplePrices tests cancelling specific orders when user has multiple
func testCancelBuyOrderMultiplePrices(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000029", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place 3 buy orders at different prices
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 10) // @ $0.50
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 20) // @ $0.56
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 60, 5) // @ $0.60
		require.NoError(t, err)

		// Verify all 3 orders exist
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 3, "should have 3 buy orders")

		// Cancel only the middle order (@ $0.56)
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, -56)
		require.NoError(t, err)

		// Verify only 2 orders remain
		positions, err = getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have 2 orders after cancel")

		// Verify the correct order was cancelled
		var has50, has56, has60 bool
		for _, pos := range positions {
			if pos.Price == -50 {
				has50 = true
			}
			if pos.Price == -56 {
				has56 = true
			}
			if pos.Price == -60 {
				has60 = true
			}
		}

		require.True(t, has50, "order @ -50 should still exist")
		require.False(t, has56, "order @ -56 should be cancelled")
		require.True(t, has60, "order @ -60 should still exist")

		return nil
	}
}

// testCancelOrderNotFound tests error when order doesn't exist
func testCancelOrderNotFound(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000030", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place one order to create participant record
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 10)
		require.NoError(t, err)

		// Try to cancel order that doesn't exist (different price)
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, -56)
		require.Error(t, err, "should fail when order doesn't exist")
		require.Contains(t, err.Error(), "Order not found", "error should mention order not found")

		return nil
	}
}

// testCancelOrderMarketSettled tests error when market is already settled
func testCancelOrderMarketSettled(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market with settle time in the past
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000031", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place buy order
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 10)
		require.NoError(t, err)

		// Manually mark market as settled (simulate settlement)
		err = markMarketAsSettled(ctx, platform, int(marketID), true)
		require.NoError(t, err)

		// Try to cancel order on settled market
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, -56)
		require.Error(t, err, "should fail when market is settled")
		require.Contains(t, err.Error(), "Cannot cancel orders on settled market", "error should mention settled market")

		return nil
	}
}

// testCancelOrderMarketNotFound tests error when market doesn't exist
func testCancelOrderMarketNotFound(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Try to cancel order on non-existent market
		err = callCancelOrder(ctx, platform, &userAddr, 99999, true, -56)
		require.Error(t, err, "should fail when market doesn't exist")
		require.Contains(t, err.Error(), "Market does not exist", "error should mention market not found")

		return nil
	}
}

// testCancelOrderInvalidPrice tests error with invalid price values
func testCancelOrderInvalidPrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000032", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test price out of range (< -99)
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, -100)
		require.Error(t, err, "should fail with price < -99")

		// Test price out of range (> 99)
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), false, 100)
		require.Error(t, err, "should fail with price > 99")

		return nil
	}
}

// testCancelOrderHoldings tests error when trying to cancel holdings (price=0)
func testCancelOrderHoldings(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000033", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Try to cancel holdings (price=0)
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, 0)
		require.Error(t, err, "should fail when trying to cancel holdings")
		require.Contains(t, err.Error(), "Holdings (price=0) cannot be cancelled", "error should mention holdings")

		return nil
	}
}

// testCancelBuyOrderVerifyRefund tests that collateral is properly refunded
func testCancelBuyOrderVerifyRefund(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000034", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Get balance before buy order
		balanceBefore, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Place buy order: 10 YES @ $0.56 (costs 5.6 TRUF)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 10)
		require.NoError(t, err)

		// Get balance after buy order (should be 5.6 TRUF less)
		balanceAfterBuy, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterBuy := new(big.Int).Sub(balanceBefore, toWei("5.6"))
		require.Equal(t, 0, expectedAfterBuy.Cmp(balanceAfterBuy),
			fmt.Sprintf("Balance after buy should be 5.6 TRUF less. Before: %s, After: %s, Expected: %s",
				balanceBefore.String(), balanceAfterBuy.String(), expectedAfterBuy.String()))

		// Cancel the order
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), true, -56)
		require.NoError(t, err)

		// Get balance after cancel (should be back to balanceBefore)
		balanceAfterCancel, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		require.Equal(t, 0, balanceBefore.Cmp(balanceAfterCancel),
			fmt.Sprintf("Balance after cancel should equal balance before buy. Before: %s, After: %s",
				balanceBefore.String(), balanceAfterCancel.String()))

		return nil
	}
}

// testCancelSellOrderVerifyShares tests that shares are returned to holdings
func testCancelSellOrderVerifyShares(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 200 TRUF (market creation costs 2 TRUF, split order costs 100 TRUF)
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000035", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order: 100 shares @ $0.56/$0.44 (costs 100 TRUF)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Verify initial state: YES holding (100) + NO sell @ 44 (100)
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2)

		var initialNoSell int64
		for _, pos := range positions {
			if !pos.Outcome && pos.Price == 44 {
				initialNoSell = pos.Amount
			}
		}
		require.Equal(t, int64(100), initialNoSell, "should have 100 NO shares for sale")

		// Cancel NO sell order
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), false, 44)
		require.NoError(t, err)

		// Verify shares returned to holdings
		positions, err = getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		var yesHolding, noHolding, noSell int64
		for _, pos := range positions {
			if pos.Outcome && pos.Price == 0 {
				yesHolding = pos.Amount
			}
			if !pos.Outcome && pos.Price == 0 {
				noHolding = pos.Amount
			}
			if !pos.Outcome && pos.Price == 44 {
				noSell = pos.Amount
			}
		}

		require.Equal(t, int64(100), yesHolding, "YES holdings should be 100")
		require.Equal(t, int64(100), noHolding, "NO holdings should be 100 (returned from cancelled sell)")
		require.Equal(t, int64(0), noSell, "NO sell order should be deleted")

		return nil
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

// callCancelOrder calls the cancel_order action
func callCancelOrder(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, marketID int, outcome bool, price int) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	res, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"",
		"cancel_order",
		[]any{marketID, outcome, price},
		nil,
	)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

// markMarketAsSettled marks a market as settled (for testing)
func markMarketAsSettled(ctx context.Context, platform *kwilTesting.Platform, marketID int, winningOutcome bool) error {
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	return platform.Engine.Execute(
		engineCtx,
		platform.DB,
		"UPDATE ob_queries SET settled = true, winning_outcome = $outcome WHERE id = $id",
		map[string]any{"$id": marketID, "$outcome": winningOutcome},
		nil,
	)
}
