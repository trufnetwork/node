//go:build kwiltest

package order_book

import (
	"context"
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

// TestPlaceSplitLimitOrder is the main test suite for split limit order placement
func TestPlaceSplitLimitOrder(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_04_PlaceSplitLimitOrder",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// Category A: Validation Tests
			testSplitOrderMarketNotFound(t),
			testSplitOrderMarketSettled(t),
			testSplitOrderInvalidPrice(t),
			testSplitOrderInvalidAmount(t),
			testSplitOrderInsufficientBalance(t),

			// Category B: Success Path Tests
			testSplitOrderSuccessful(t),
			testSplitOrderMultipleDifferentPrices(t),
			testSplitOrderMultipleSamePrice(t),
			testSplitOrderBalanceChanges(t),
			testSplitOrderPositionVerification(t),

			// Category C: Matching Tests (Blocked by Issue 6)
			// TODO: Enable after Issue 6 (Matching Engine) is complete
			// testSplitOrderMatchesBuyOrder(t),
			// testSplitOrderPartialMatch(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testSplitOrderMarketNotFound tests error when market doesn't exist
func testSplitOrderMarketNotFound(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Try to split on non-existent market
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, 99999, 56, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Market does not exist")

		return nil
	}
}

// testSplitOrderMarketSettled tests error when market is already settled
func testSplitOrderMarketSettled(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market in the future (valid), then we'll manually settle it
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000018", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// TODO (Issue 7 - Settlement): This is a TEST-ONLY shortcut that manually
		// marks the market as settled by directly updating the database.
		// In production, settlement should happen through a proper settlement action
		// that will be implemented in Issue 7, which will:
		//   1. Request attestation from TN oracle
		//   2. Verify the outcome (TRUE/FALSE)
		//   3. Cancel all outstanding orders
		//   4. Payout winners (minus 2% fee)
		//   5. Distribute fees to LPs and network
		//   6. Mark market as settled
		// For now, we bypass this entire flow just to test the "market settled" validation.
		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        userAddr.Bytes(),
			Caller:        userAddr.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		err = platform.Engine.Execute(
			engineCtx,
			platform.DB,
			"UPDATE ob_queries SET settled = true WHERE id = $id",
			map[string]any{"$id": marketID},
			nil,
		)
		require.NoError(t, err, "should mark market as settled")

		// Try to split on settled market
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Market has already settled")

		return nil
	}
}

// testSplitOrderInvalidPrice tests various invalid price scenarios
func testSplitOrderInvalidPrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000019", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test price = 0 (invalid)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 0, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "true_price must be between 1 and 99")

		// Test price = 100 (invalid)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 100, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "true_price must be between 1 and 99")

		// Test price > 100 (invalid)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 150, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "true_price must be between 1 and 99")

		return nil
	}
}

// testSplitOrderInvalidAmount tests various invalid amount scenarios
func testSplitOrderInvalidAmount(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000020", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test amount = 0 (invalid)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "amount must be positive")

		// Test amount < 0 (invalid)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, -10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "amount must be positive")

		// Test amount > 1 billion (invalid)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 2000000000)
		require.Error(t, err)
		require.Contains(t, err.Error(), "amount exceeds maximum allowed")

		return nil
	}
}

// testSplitOrderInsufficientBalance tests error when user has insufficient balance
func testSplitOrderInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user only 50 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "50000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000021", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Try to split 100 shares (requires 100 TRUF, but user only has 50)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Insufficient balance")

		return nil
	}
}

// testSplitOrderSuccessful tests successful split order placement
func testSplitOrderSuccessful(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 200 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000022", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split order: 100 shares @ $0.56 YES / $0.44 NO
		// Collateral: 100 × 10^18 = 100 TRUF
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Verify positions created
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have 2 positions (YES holding + NO sell)")

		var yesHolding, noSell *Position
		for i := range positions {
			if positions[i].Outcome && positions[i].Price == 0 {
				yesHolding = &positions[i]
			} else if !positions[i].Outcome && positions[i].Price == 44 {
				noSell = &positions[i]
			}
		}

		require.NotNil(t, yesHolding, "YES holding should exist")
		require.NotNil(t, noSell, "NO sell order should exist")

		require.Equal(t, int64(100), yesHolding.Amount, "YES holding amount")
		require.Equal(t, int64(100), noSell.Amount, "NO sell order amount")

		return nil
	}
}

// testSplitOrderMultipleDifferentPrices tests multiple split orders at different prices
func testSplitOrderMultipleDifferentPrices(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 300 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "300000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000023", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split order 1: 50 @ $0.50/$0.50
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 50, 50)
		require.NoError(t, err)

		// Place split order 2: 60 @ $0.60/$0.40
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 60, 60)
		require.NoError(t, err)

		// Place split order 3: 70 @ $0.30/$0.70
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 30, 70)
		require.NoError(t, err)

		// Verify positions: should have 4 positions
		// - 1 YES holding (accumulated: 50+60+70 = 180)
		// - 3 NO sell orders at different prices (50, 40, 70)
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 4, "should have 4 positions")

		var yesHolding *Position
		noSellOrders := make(map[int16]int64)

		for i := range positions {
			if positions[i].Outcome && positions[i].Price == 0 {
				yesHolding = &positions[i]
			} else if !positions[i].Outcome && positions[i].Price > 0 {
				noSellOrders[positions[i].Price] = positions[i].Amount
			}
		}

		require.NotNil(t, yesHolding, "YES holding should exist")
		require.Equal(t, int64(180), yesHolding.Amount, "YES holding should accumulate")

		require.Equal(t, int64(50), noSellOrders[50], "NO sell @ 50")
		require.Equal(t, int64(60), noSellOrders[40], "NO sell @ 40")
		require.Equal(t, int64(70), noSellOrders[70], "NO sell @ 70")

		return nil
	}
}

// testSplitOrderMultipleSamePrice tests UPSERT accumulation at same price
func testSplitOrderMultipleSamePrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 300 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "300000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000024", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split order 1: 50 @ $0.56/$0.44
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 50)
		require.NoError(t, err)

		// Place split order 2: 30 @ $0.56/$0.44 (same price)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 30)
		require.NoError(t, err)

		// Place split order 3: 20 @ $0.56/$0.44 (same price)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 20)
		require.NoError(t, err)

		// Verify positions: should have 2 positions (UPSERT accumulation)
		// - 1 YES holding @ price=0 (50+30+20 = 100)
		// - 1 NO sell @ price=44 (50+30+20 = 100)
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have 2 positions (accumulated)")

		var yesHolding, noSell *Position
		for i := range positions {
			if positions[i].Outcome && positions[i].Price == 0 {
				yesHolding = &positions[i]
			} else if !positions[i].Outcome && positions[i].Price == 44 {
				noSell = &positions[i]
			}
		}

		require.NotNil(t, yesHolding, "YES holding should exist")
		require.NotNil(t, noSell, "NO sell order should exist")

		require.Equal(t, int64(100), yesHolding.Amount, "YES holding should accumulate to 100")
		require.Equal(t, int64(100), noSell.Amount, "NO sell should accumulate to 100")

		return nil
	}
}

// testSplitOrderBalanceChanges verifies balance changes after split order
func testSplitOrderBalanceChanges(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 200 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000025", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Get USDC balance before (split orders lock USDC, not TRUF)
		balanceBefore, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Place split order: 75 shares
		// Collateral: 75 × 10^18 = 75 USDC
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 75)
		require.NoError(t, err)

		// Get USDC balance after
		balanceAfter, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Verify USDC balance decreased by 75 (collateral locked)
		expectedDecrease := toWei("75")
		actualDecrease := new(big.Int).Sub(balanceBefore, balanceAfter)
		require.Equal(t, expectedDecrease.String(), actualDecrease.String(),
			"USDC balance should decrease by 75 (collateral)")

		return nil
	}
}

// testSplitOrderPositionVerification verifies both positions are created correctly
func testSplitOrderPositionVerification(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000026", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split order: 50 @ $0.35/$0.65
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 35, 50)
		require.NoError(t, err)

		// Verify positions in detail
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have exactly 2 positions")

		var yesHolding, noSell *Position
		for i := range positions {
			if positions[i].Outcome && positions[i].Price == 0 {
				yesHolding = &positions[i]
			} else if !positions[i].Outcome && positions[i].Price == 65 {
				noSell = &positions[i]
			}
		}

		// Verify YES holding
		require.NotNil(t, yesHolding, "YES holding should exist")
		require.True(t, yesHolding.Outcome, "YES holding outcome should be TRUE")
		require.Equal(t, int16(0), yesHolding.Price, "YES holding price should be 0")
		require.Equal(t, int64(50), yesHolding.Amount, "YES holding amount should be 50")

		// Verify NO sell order
		require.NotNil(t, noSell, "NO sell order should exist")
		require.False(t, noSell.Outcome, "NO sell outcome should be FALSE")
		require.Equal(t, int16(65), noSell.Price, "NO sell price should be 65 (100 - 35)")
		require.Equal(t, int64(50), noSell.Amount, "NO sell amount should be 50")

		return nil
	}
}

// Helper: Call the place_split_limit_order action
func callPlaceSplitLimitOrder(
	ctx context.Context,
	platform *kwilTesting.Platform,
	signer *util.EthereumAddress,
	queryID int,
	truePrice int,
	amount int64,
) error {
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
		"place_split_limit_order",
		[]any{queryID, truePrice, amount},
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
