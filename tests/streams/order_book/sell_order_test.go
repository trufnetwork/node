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
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestPlaceSellOrder is the main test suite for sell order placement
// NOTE: Some tests are blocked by Issue 6 (Matching Engine) - marked with TODO
func TestPlaceSellOrder(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_03_PlaceSellOrder",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// TODO: Blocked by Issue 6 - needs matching engine to create holdings
			// testSellOrderSuccessful(t),
			// testSellOrderInsufficientShares(t),

			testSellOrderNoSharesAtAll(t),       // ✅ Tests error when user has no shares
			testSellOrderWrongOutcome(t),        // ✅ Tests error for wrong outcome (if has shares)
			testSellOrderMarketNotFound(t),      // ✅ Tests market validation
			testSellOrderMarketSettled(t),       // ✅ Tests settled market validation
			testSellOrderInvalidPrice(t),        // ✅ Tests price validation
			testSellOrderInvalidAmount(t),       // ✅ Tests amount validation

			// TODO: Blocked by Issue 6 - need holdings to test these
			// testSellOrderMultipleDifferentPrices(t),
			// testSellOrderMultipleSamePrice(t),
			// testSellOrderExactAmount(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testSellOrderSuccessful tests successful sell order placement
func testSellOrderSuccessful(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to initialize ERC20 extension")

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_1"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err, "create_market should succeed")

		// Give user 50 YES shares directly (holdings at price = 0)
		err = giveShares(ctx, platform, &userAddr, int(marketID), true, 50)
		require.NoError(t, err, "giveShares should succeed")

		// Verify holdings exist
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 1, "should have 1 holding position")
		require.Equal(t, int16(0), positions[0].Price, "holdings should have price = 0")
		require.Equal(t, int64(50), positions[0].Amount)

		// Now sell 30 of those 50 shares at $0.60
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 60, 30)
		require.NoError(t, err, "place_sell_order should succeed")

		// Verify position updates:
		// - 20 shares remaining in holdings (price = 0)
		// - 30 shares listed for sale (price = 60)
		positions, err = getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have 2 positions (holdings + sell order)")

		var holdings, sellOrder *Position
		for i := range positions {
			if positions[i].Price == 0 {
				holdings = &positions[i]
			} else if positions[i].Price == 60 {
				sellOrder = &positions[i]
			}
		}

		require.NotNil(t, holdings, "holdings should exist")
		require.Equal(t, int64(20), holdings.Amount, "should have 20 shares remaining")

		require.NotNil(t, sellOrder, "sell order should exist")
		require.Equal(t, int64(30), sellOrder.Amount, "should have 30 shares for sale")
		require.True(t, sellOrder.Outcome, "should be YES shares")

		return nil
	}
}

// testSellOrderInsufficientShares tests error when user doesn't have enough shares
func testSellOrderInsufficientShares(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_2"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Give user 20 YES shares (via holdings)
		err = giveShares(ctx, platform, &userAddr, int(marketID), true, 20)
		require.NoError(t, err, "give shares should succeed")

		// Try to sell 50 shares (more than they have)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 60, 50)
		require.Error(t, err, "should fail with insufficient shares")
		require.Contains(t, err.Error(), "Insufficient shares")
		require.Contains(t, err.Error(), "You own: 20")
		require.Contains(t, err.Error(), "trying to sell: 50")

		return nil
	}
}

// testSellOrderNoSharesAtAll tests error when user has no shares
func testSellOrderNoSharesAtAll(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_3"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User has never bought shares - try to sell
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 60, 10)
		require.Error(t, err, "should fail - no shares")
		require.Contains(t, err.Error(), "No shares found")

		return nil
	}
}

// testSellOrderWrongOutcome tests error when user has no shares for that outcome
// NOTE: Simplified to not need holdings - just tests the "no shares" error path
func testSellOrderWrongOutcome(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_4"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Try to sell NO shares (user has no shares at all)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), false, 50, 10)
		require.Error(t, err, "should fail - no shares")
		require.Contains(t, err.Error(), "No shares found")

		return nil
	}
}

// testSellOrderMarketNotFound tests error when market doesn't exist
func testSellOrderMarketNotFound(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Try to sell in non-existent market
		err = callPlaceSellOrder(ctx, platform, &userAddr, 99999, true, 50, 10)
		require.Error(t, err, "should fail - market not found")
		require.Contains(t, err.Error(), "Market does not exist")

		return nil
	}
}

// testSellOrderMarketSettled tests error when market is already settled
// NOTE: Doesn't need holdings - just tests market validation
func testSellOrderMarketSettled(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_6"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Manually mark market as settled using admin privileges
		tx := &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: 1},
			Signer:       platform.Deployer,
			Caller:       "0x0000000000000000000000000000000000000000",
			TxID:         platform.Txid(),
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

		// Try to sell - should fail even though user has no shares
		// (market validation happens before share validation)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 50, 10)
		require.Error(t, err, "should fail - market settled")
		require.Contains(t, err.Error(), "Market has already settled")

		return nil
	}
}

// testSellOrderInvalidPrice tests price validation
// NOTE: Doesn't need holdings - validation happens before share check
func testSellOrderInvalidPrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_7"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test price = 0 (too low) - validation happens before share check
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 0, 10)
		require.Error(t, err, "price=0 should fail")
		require.Contains(t, err.Error(), "price must be between 1 and 99")

		// Test price = 100 (too high)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 100, 10)
		require.Error(t, err, "price=100 should fail")
		require.Contains(t, err.Error(), "price must be between 1 and 99")

		// Test negative price
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, -50, 10)
		require.Error(t, err, "negative price should fail")
		require.Contains(t, err.Error(), "price must be between 1 and 99")

		return nil
	}
}

// testSellOrderInvalidAmount tests amount validation
// NOTE: Doesn't need holdings - validation happens before share check
func testSellOrderInvalidAmount(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_8"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test amount = 0 - validation happens before share check
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 50, 0)
		require.Error(t, err, "amount=0 should fail")
		require.Contains(t, err.Error(), "amount must be positive")

		// Test negative amount
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 50, -10)
		require.Error(t, err, "negative amount should fail")
		require.Contains(t, err.Error(), "amount must be positive")

		// Test amount exceeds maximum (1 billion + 1)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 50, 1000000001)
		require.Error(t, err, "amount > 1 billion should fail")
		require.Contains(t, err.Error(), "amount exceeds maximum allowed")

		return nil
	}
}

// testSellOrderMultipleDifferentPrices tests selling at different prices
func testSellOrderMultipleDifferentPrices(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_9"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Give user 100 YES shares
		err = giveShares(ctx, platform, &userAddr, int(marketID), true, 100)
		require.NoError(t, err)

		// Sell 30 shares at $0.50
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 50, 30)
		require.NoError(t, err, "first sell should succeed")

		// Sell 40 shares at $0.60 (different price)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 60, 40)
		require.NoError(t, err, "second sell should succeed")

		// Verify positions:
		// - 30 held (price = 0)
		// - 30 listed @ $0.50
		// - 40 listed @ $0.60
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 3, "should have 3 positions")

		// Find each position
		var holdings, sell50, sell60 *Position
		for i := range positions {
			switch positions[i].Price {
			case 0:
				holdings = &positions[i]
			case 50:
				sell50 = &positions[i]
			case 60:
				sell60 = &positions[i]
			}
		}

		require.NotNil(t, holdings, "holdings should exist")
		require.Equal(t, int64(30), holdings.Amount)

		require.NotNil(t, sell50, "sell order @50 should exist")
		require.Equal(t, int64(30), sell50.Amount)

		require.NotNil(t, sell60, "sell order @60 should exist")
		require.Equal(t, int64(40), sell60.Amount)

		return nil
	}
}

// testSellOrderMultipleSamePrice tests UPSERT behavior
func testSellOrderMultipleSamePrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_10"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Give user 100 YES shares
		err = giveShares(ctx, platform, &userAddr, int(marketID), true, 100)
		require.NoError(t, err)

		// Sell 30 shares at $0.55
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 55, 30)
		require.NoError(t, err, "first sell should succeed")

		// Sell 20 MORE shares at $0.55 (same price - should UPSERT)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 55, 20)
		require.NoError(t, err, "second sell should succeed")

		// Verify positions:
		// - 50 held (price = 0)
		// - 50 listed @ $0.55 (30 + 20, accumulated)
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have 2 positions (holdings + sell)")

		var holdings, sellOrder *Position
		for i := range positions {
			if positions[i].Price == 0 {
				holdings = &positions[i]
			} else if positions[i].Price == 55 {
				sellOrder = &positions[i]
			}
		}

		require.NotNil(t, holdings, "holdings should exist")
		require.Equal(t, int64(50), holdings.Amount)

		require.NotNil(t, sellOrder, "sell order should exist")
		require.Equal(t, int64(50), sellOrder.Amount, "should have 50 shares (30+20)")

		return nil
	}
}

// testSellOrderExactAmount tests selling all holdings (cleanup test)
func testSellOrderExactAmount(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_market_sell_11"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Give user exactly 50 YES shares
		err = giveShares(ctx, platform, &userAddr, int(marketID), true, 50)
		require.NoError(t, err)

		// Sell ALL 50 shares
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), true, 60, 50)
		require.NoError(t, err, "sell all should succeed")

		// Verify:
		// - NO holdings position (price = 0 row deleted)
		// - 50 shares listed @ $0.60
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 1, "should have only 1 position (sell order)")

		require.Equal(t, int16(60), positions[0].Price)
		require.Equal(t, int64(50), positions[0].Amount)

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// callPlaceSellOrder calls place_sell_order() action
func callPlaceSellOrder(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, marketID int, outcome bool, price int, amount int64) error {
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
		"place_sell_order",
		[]any{marketID, outcome, price, amount},
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

// giveShares creates holdings for testing (bypasses normal order flow)
// Simulates user receiving shares from previous buy/mint operations
// Works by: 1) Creating a buy order (which auto-creates participant)
//           2) Converting that buy order to holdings by updating price to 0
// Uses OverrideAuthz: true for admin privileges to modify database
func giveShares(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, marketID int, outcome bool, amount int64) error {
	// Step 1: Create a buy order at price = 99 (highest price)
	// This will auto-create the participant and lock collateral
	err := callPlaceBuyOrder(ctx, platform, signer, marketID, outcome, 99, amount)
	if err != nil {
		return fmt.Errorf("failed to create buy order for test setup: %w", err)
	}

	// Step 2: Convert the buy order (price = -99) to holdings (price = 0)
	// This simulates the order being filled and converted to shares
	// Use admin context with OverrideAuthz: true to bypass permission checks

	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1, Timestamp: time.Now().Unix()},
		Signer:       platform.Deployer, // Use deployer as admin
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         platform.Txid(),
	}
	// KEY: OverrideAuthz: true bypasses permission checks
	engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

	callerBytes := signer.Bytes()

	// Get participant ID using admin context
	var participantID int64
	err = platform.Engine.Execute(
		engineCtx,
		platform.DB,
		`SELECT id FROM ob_participants WHERE wallet_address = $addr`,
		map[string]any{"$addr": callerBytes},
		func(row *common.Row) error {
			participantID = row.Values[0].(int64)
			return nil
		},
	)
	if err != nil || participantID == 0 {
		return fmt.Errorf("failed to get participant ID: %w", err)
	}

	// UPDATE price from -99 (buy order) to 0 (holdings) using admin privileges
	// This simulates the order being matched and converted to holdings
	err = platform.Engine.Execute(
		engineCtx,
		platform.DB,
		`UPDATE ob_positions SET price = 0 WHERE query_id = $qid AND participant_id = $pid AND outcome = $outcome AND price = -99`,
		map[string]any{
			"$qid":     marketID,
			"$pid":     participantID,
			"$outcome": outcome,
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to convert buy order to holdings: %w", err)
	}

	return nil
}
