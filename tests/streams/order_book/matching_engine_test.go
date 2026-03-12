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
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// giveBalanceChained gives balance (BOTH TRUF and USDC) with proper linked-list chaining for ordered-sync
func giveBalanceChained(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	// Inject TRUF balance (for market creation fees)
	trufBalancePointCounter++
	trufPoint := trufBalancePointCounter

	from := ensureNonZeroAddress(wallet)

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTRUFChain,
		testTRUFEscrow,
		testTRUFERC20,
		from,
		wallet,
		amountStr,
		trufPoint,
		lastTrufBalancePoint, // Chain to previous TRUF point
	)


	if err != nil {
		return fmt.Errorf("failed to inject TRUF: %w", err)
	}

	// Update TRUF lastPoint for next call
	p := trufPoint
	lastTrufBalancePoint = &p

	// Inject USDC balance (for market collateral)
	err = giveUSDCBalanceChained(ctx, platform, wallet, amountStr)
	if err != nil {
		return err
	}

	return nil
}

// TestMatchingEngine tests all three match types: direct, mint, and burn
func TestMatchingEngine(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_06_MatchingEngine",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// Category A: Direct Match Tests
			testDirectMatchFullMatch(t),
			testDirectMatchPartialFill(t),

			// Category B: Mint Match Tests
			testMintMatchFullMatch(t),
			testMintMatchPartialFill(t),

			// Category C: Burn Match Tests
			testBurnMatchFullMatch(t),
			testBurnMatchPartialFill(t),

			// Category D: Multiple Round Tests
			testDirectMatchMultipleRounds(t),

			// Category E: Price-Crossing Tests
			testDirectMatchPriceCrossing(t),
			testDirectMatchPriceCrossingSweep(t),

			// Category F: Edge Cases
			testNoMatchingOrders(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Category A: Direct Match Tests
// =============================================================================

// testDirectMatchFullMatch tests direct match where buy and sell are same amount
func testDirectMatchFullMatch(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give balances to both users using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000036", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Buy 100 YES @ $0.56
		err = callPlaceBuyOrder(ctx, platform, &user1Addr, int(marketID), true, 56, 100)
		require.NoError(t, err)

		// User2: Create shares and sell 100 YES @ $0.56
		err = callPlaceSplitLimitOrder(ctx, platform, &user2Addr, int(marketID), 56, 100)
		require.NoError(t, err)

		err = callPlaceSellOrder(ctx, platform, &user2Addr, int(marketID), true, 56, 100)
		require.NoError(t, err)

		// Verify: Get all positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Find user1's positions (should have 100 YES holdings)
		var user1YesHoldings *Position
		for i := range positions {
			// User1 is participant 1, User2 is participant 2
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == 0 {
				user1YesHoldings = &positions[i]
			}
		}

		require.NotNil(t, user1YesHoldings, "User1 should have YES holdings")
		require.Equal(t, int64(100), user1YesHoldings.Amount)

		return nil
	}
}

// =============================================================================
// Category B: Mint Match Tests
// =============================================================================

// testMintMatchFullMatch tests mint match where opposite buy orders create shares
func testMintMatchFullMatch(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Give balances to both users using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000037", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Buy 100 YES @ $0.56
		err = callPlaceBuyOrder(ctx, platform, &user1Addr, int(marketID), true, 56, 100)
		require.NoError(t, err)

		// User2: Buy 100 NO @ $0.44 (complementary: 56 + 44 = 100)
		err = callPlaceBuyOrder(ctx, platform, &user2Addr, int(marketID), false, 44, 100)
		require.NoError(t, err) // Should trigger mint match

		// Verify: Get all positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Find user1's YES holdings and user2's NO holdings
		var user1YesHoldings, user2NoHoldings *Position
		for i := range positions {
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == 0 {
				user1YesHoldings = &positions[i]
			}
			if positions[i].ParticipantID == 2 && !positions[i].Outcome && positions[i].Price == 0 {
				user2NoHoldings = &positions[i]
			}
		}

		require.NotNil(t, user1YesHoldings, "User1 should have YES holdings")
		require.Equal(t, int64(100), user1YesHoldings.Amount)
		require.NotNil(t, user2NoHoldings, "User2 should have NO holdings")
		require.Equal(t, int64(100), user2NoHoldings.Amount)

		return nil
	}
}

// =============================================================================
// Category C: Burn Match Tests
// =============================================================================

// testBurnMatchFullMatch tests burn match where opposite sell orders destroy shares
func testBurnMatchFullMatch(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Give balances to both users using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000038", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Split limit @ 60 creates YES holdings + NO sell @ 40
		// (User1 wants to go long on YES, so holds YES and sells NO)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1Addr, int(marketID), 60, 100)
		require.NoError(t, err)

		// Cancel the NO sell order so we only have YES holdings
		err = callCancelOrder(ctx, platform, &user1Addr, int(marketID), false, 40)
		require.NoError(t, err)

		// Get balance before burn
		balance1Before, err := getUSDCBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err)

		// User1: Sell the YES holdings @ $0.60
		err = callPlaceSellOrder(ctx, platform, &user1Addr, int(marketID), true, 60, 100)
		require.NoError(t, err)

		// Get User2's initial balance BEFORE split limit
		balance2Initial, err := getUSDCBalance(ctx, platform, user2Addr.Address())
		require.NoError(t, err)

		// User2: Split limit @ 60 creates YES holdings + NO sell @ 40
		// The NO sell @ 40 should immediately trigger burn match with User1's YES sell @ 60
		err = callPlaceSplitLimitOrder(ctx, platform, &user2Addr, int(marketID), 60, 100)
		require.NoError(t, err)

		// Verify: Positions should be cleared
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Should have no sell orders (all burned)
		hasSellOrders := false
		for i := range positions {
			if positions[i].Price == 60 || positions[i].Price == 40 {
				hasSellOrders = true
			}
		}
		require.False(t, hasSellOrders, "Sell orders should be burned")

		// Verify collateral returned
		balance1After, err := getUSDCBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err)

		balance2After, err := getUSDCBalance(ctx, platform, user2Addr.Address())
		require.NoError(t, err)

		// User1 should receive 60 USDC (100 shares × $0.60)
		payout1 := new(big.Int).Sub(balance1After, balance1Before)
		expectedPayout1 := new(big.Int).Mul(big.NewInt(60), big.NewInt(1e18))
		require.Equal(t, expectedPayout1.String(), payout1.String(),
			fmt.Sprintf("User1 should receive 60 USDC, got %s", payout1.String()))

		// User2 locked 100 USDC for split, got back 40 USDC from burn
		// Net result: balance should decrease by 60 USDC
		netChange2 := new(big.Int).Sub(balance2After, balance2Initial)
		expectedNetChange2 := new(big.Int).Mul(big.NewInt(-60), big.NewInt(1e18))
		require.Equal(t, expectedNetChange2.String(), netChange2.String(),
			fmt.Sprintf("User2 net change should be -60 USDC (locked 100, received 40), got %s", netChange2.String()))

		return nil
	}
}

// testDirectMatchPartialFill tests direct match where buy amount > sell amount
func testDirectMatchPartialFill(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Give balances to both users using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000039", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Buy 100 YES @ $0.56
		err = callPlaceBuyOrder(ctx, platform, &user1Addr, int(marketID), true, 56, 100)
		require.NoError(t, err)

		// User2: Create and sell only 60 YES @ $0.56 (partial fill)
		err = callPlaceSplitLimitOrder(ctx, platform, &user2Addr, int(marketID), 56, 60)
		require.NoError(t, err)

		err = callPlaceSellOrder(ctx, platform, &user2Addr, int(marketID), true, 56, 60)
		require.NoError(t, err)

		// Verify: Get all positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// User1 should have 60 YES holdings (matched) + 40 YES buy order remaining
		var user1YesHoldings, user1YesBuyOrder *Position
		for i := range positions {
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == 0 {
				user1YesHoldings = &positions[i]
			}
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == -56 {
				user1YesBuyOrder = &positions[i]
			}
		}

		require.NotNil(t, user1YesHoldings, "User1 should have YES holdings")
		require.Equal(t, int64(60), user1YesHoldings.Amount, "User1 should have 60 YES holdings")
		require.NotNil(t, user1YesBuyOrder, "User1 should still have buy order")
		require.Equal(t, int64(40), user1YesBuyOrder.Amount, "User1 should have 40 YES remaining in buy order")

		return nil
	}
}

// =============================================================================
// Category B: Mint Match Tests (continued)
// =============================================================================

// testMintMatchPartialFill tests mint match where YES amount < NO amount
func testMintMatchPartialFill(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

		// Give balances to both users using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000040", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Buy 60 YES @ $0.56 (smaller amount)
		err = callPlaceBuyOrder(ctx, platform, &user1Addr, int(marketID), true, 56, 60)
		require.NoError(t, err)

		// User2: Buy 100 NO @ $0.44 (larger amount, partial fill)
		err = callPlaceBuyOrder(ctx, platform, &user2Addr, int(marketID), false, 44, 100)
		require.NoError(t, err)

		// Verify: Get all positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// User1 should have 60 YES holdings (fully matched)
		// User2 should have 60 NO holdings + 40 NO buy order remaining
		var user1YesHoldings, user2NoHoldings, user2NoBuyOrder *Position
		for i := range positions {
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == 0 {
				user1YesHoldings = &positions[i]
			}
			if positions[i].ParticipantID == 2 && !positions[i].Outcome && positions[i].Price == 0 {
				user2NoHoldings = &positions[i]
			}
			if positions[i].ParticipantID == 2 && !positions[i].Outcome && positions[i].Price == -44 {
				user2NoBuyOrder = &positions[i]
			}
		}

		require.NotNil(t, user1YesHoldings, "User1 should have YES holdings")
		require.Equal(t, int64(60), user1YesHoldings.Amount)
		require.NotNil(t, user2NoHoldings, "User2 should have NO holdings")
		require.Equal(t, int64(60), user2NoHoldings.Amount)
		require.NotNil(t, user2NoBuyOrder, "User2 should still have NO buy order")
		require.Equal(t, int64(40), user2NoBuyOrder.Amount)

		return nil
	}
}

// =============================================================================
// Category C: Burn Match Tests (continued)
// =============================================================================

// testBurnMatchPartialFill tests burn match where YES amount < NO amount
func testBurnMatchPartialFill(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

		// Give balances to both users using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000041", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Split limit @ 60 creates 60 YES holdings + NO sell @ 40
		err = callPlaceSplitLimitOrder(ctx, platform, &user1Addr, int(marketID), 60, 60)
		require.NoError(t, err)

		// Cancel the NO sell order
		err = callCancelOrder(ctx, platform, &user1Addr, int(marketID), false, 40)
		require.NoError(t, err)

		// Get balance before burn
		balance1Before, err := getUSDCBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err)

		// User1: Sell 60 YES @ $0.60
		err = callPlaceSellOrder(ctx, platform, &user1Addr, int(marketID), true, 60, 60)
		require.NoError(t, err)

		// Get User2's initial balance
		balance2Initial, err := getUSDCBalance(ctx, platform, user2Addr.Address())
		require.NoError(t, err)

		// User2: Split limit @ 60 creates 100 YES holdings + 100 NO sell @ 40
		// Only 60 should burn (partial fill)
		err = callPlaceSplitLimitOrder(ctx, platform, &user2Addr, int(marketID), 60, 100)
		require.NoError(t, err)

		// Verify: Get positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// User2 should still have 40 NO sell order remaining
		var user2NoSellOrder *Position
		for i := range positions {
			if positions[i].ParticipantID == 2 && !positions[i].Outcome && positions[i].Price == 40 {
				user2NoSellOrder = &positions[i]
			}
		}
		require.NotNil(t, user2NoSellOrder, "User2 should have 40 NO remaining in sell order")
		require.Equal(t, int64(40), user2NoSellOrder.Amount)

		// Verify collateral returned
		balance1After, err := getUSDCBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err)

		balance2After, err := getUSDCBalance(ctx, platform, user2Addr.Address())
		require.NoError(t, err)

		// User1 should receive 36 USDC (60 shares × $0.60)
		payout1 := new(big.Int).Sub(balance1After, balance1Before)
		expectedPayout1 := new(big.Int).Mul(big.NewInt(36), big.NewInt(1e18))
		require.Equal(t, expectedPayout1.String(), payout1.String())

		// User2 locked 100 USDC for split, got back 24 USDC from burn (60 shares × $0.40)
		// Net change: -76 USDC
		netChange2 := new(big.Int).Sub(balance2After, balance2Initial)
		expectedNetChange2 := new(big.Int).Mul(big.NewInt(-76), big.NewInt(1e18))
		require.Equal(t, expectedNetChange2.String(), netChange2.String())

		return nil
	}
}

// =============================================================================
// Category D: Multiple Round Tests
// =============================================================================

// testDirectMatchMultipleRounds tests multiple orders matched sequentially
func testDirectMatchMultipleRounds(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
		user3Addr := util.Unsafe_NewEthereumAddressFromString("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
		user4Addr := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")

		// Give balances using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user3Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user4Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000042", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Buy 100 YES @ $0.56 (will be matched across 3 sell orders)
		err = callPlaceBuyOrder(ctx, platform, &user1Addr, int(marketID), true, 56, 100)
		require.NoError(t, err)

		// User2: Sell 30 YES @ $0.56 (first match)
		err = callPlaceSplitLimitOrder(ctx, platform, &user2Addr, int(marketID), 56, 30)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user2Addr, int(marketID), true, 56, 30)
		require.NoError(t, err)

		// User3: Sell 40 YES @ $0.56 (second match)
		err = callPlaceSplitLimitOrder(ctx, platform, &user3Addr, int(marketID), 56, 40)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user3Addr, int(marketID), true, 56, 40)
		require.NoError(t, err)

		// User4: Sell 30 YES @ $0.56 (third match, fully fills User1)
		err = callPlaceSplitLimitOrder(ctx, platform, &user4Addr, int(marketID), 56, 30)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user4Addr, int(marketID), true, 56, 30)
		require.NoError(t, err)

		// Verify: Get positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// User1 should have 100 YES holdings (fully matched)
		var user1YesHoldings *Position
		for i := range positions {
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == 0 {
				user1YesHoldings = &positions[i]
			}
		}

		require.NotNil(t, user1YesHoldings, "User1 should have YES holdings")
		require.Equal(t, int64(100), user1YesHoldings.Amount, "User1 should have 100 YES from 3 matches")

		// No buy orders should remain
		hasBuyOrders := false
		for i := range positions {
			if positions[i].Price < 0 {
				hasBuyOrders = true
			}
		}
		require.False(t, hasBuyOrders, "All buy orders should be matched")

		return nil
	}
}

// =============================================================================
// Category E: Edge Cases
// =============================================================================

// testNoMatchingOrders tests that no-op occurs when no matching orders exist
func testNoMatchingOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// CRITICAL: Initialize ERC20 extension singleton FIRST
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1Addr := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000002")
		user2Addr := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000003")

		// Give balances using chained deposits
		err = giveBalanceChained(ctx, platform, user1Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2Addr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1Addr.Address(), "sttest00000000000000000000000043", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &user1Addr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User1: Buy YES @ $0.56
		err = callPlaceBuyOrder(ctx, platform, &user1Addr, int(marketID), true, 56, 100)
		require.NoError(t, err)

		// User2: Sell YES @ $0.65 (different price, no match)
		// Note: Split limit @ 65 creates YES holdings + NO sell @ 35
		// Then selling YES @ 65 won't burn because 65 + 35 = 100 would burn User2's own orders
		// So we create shares WITHOUT split limit to avoid self-burn
		err = callPlaceSplitLimitOrder(ctx, platform, &user2Addr, int(marketID), 65, 100)
		require.NoError(t, err)

		// Cancel the NO sell order to prevent self-burn
		err = callCancelOrder(ctx, platform, &user2Addr, int(marketID), false, 35)
		require.NoError(t, err)

		err = callPlaceSellOrder(ctx, platform, &user2Addr, int(marketID), true, 65, 100)
		require.NoError(t, err)

		// Verify: Both orders should remain unmatched
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Should have both buy and sell orders
		var user1BuyOrder, user2SellOrder *Position
		for i := range positions {
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == -56 {
				user1BuyOrder = &positions[i]
			}
			if positions[i].ParticipantID == 2 && positions[i].Outcome && positions[i].Price == 65 {
				user2SellOrder = &positions[i]
			}
		}

		require.NotNil(t, user1BuyOrder, "User1 buy order should remain")
		require.Equal(t, int64(100), user1BuyOrder.Amount)
		require.NotNil(t, user2SellOrder, "User2 sell order should remain")
		require.Equal(t, int64(100), user2SellOrder.Amount)

		return nil
	}
}

// =============================================================================
// Category E: Price-Crossing Tests
// =============================================================================

// testDirectMatchPriceCrossing tests that buy@52 matches sell@51 (price-crossing)
// The match executes at sell price and buyer is refunded the difference.
func testDirectMatchPriceCrossing(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		buyer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000004")
		seller := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000005")

		err = giveBalanceChained(ctx, platform, buyer.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, seller.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(buyer.Address(), "sttest00000000000000000000000044", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &buyer, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Seller: Create shares and place sell@51
		err = callPlaceSplitLimitOrder(ctx, platform, &seller, int(marketID), 51, 100)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &seller, int(marketID), true, 51, 100)
		require.NoError(t, err)

		// Record buyer's USDC balance before buy
		buyerBalanceBefore, err := getUSDCBalance(ctx, platform, buyer.Address())
		require.NoError(t, err)

		// Buyer: Buy YES @ $0.52 — should cross and match sell@51
		err = callPlaceBuyOrder(ctx, platform, &buyer, int(marketID), true, 52, 50)
		require.NoError(t, err)

		// Verify: buyer should have 50 YES holdings
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Seller acts first (participant 1), buyer acts second (participant 2)
		var buyerYesHoldings *Position
		var sellerSellRemaining *Position
		for i := range positions {
			if positions[i].ParticipantID == 2 && positions[i].Outcome && positions[i].Price == 0 {
				buyerYesHoldings = &positions[i]
			}
			if positions[i].ParticipantID == 1 && positions[i].Outcome && positions[i].Price == 51 {
				sellerSellRemaining = &positions[i]
			}
		}

		require.NotNil(t, buyerYesHoldings, "Buyer should have YES holdings from price-crossing match")
		require.Equal(t, int64(50), buyerYesHoldings.Amount, "Buyer should have 50 YES shares")

		// Seller's sell order should be partially filled (100 - 50 = 50 remaining)
		require.NotNil(t, sellerSellRemaining, "Seller should have remaining sell order")
		require.Equal(t, int64(50), sellerSellRemaining.Amount, "Seller should have 50 remaining")

		// No buy order should remain (fully matched)
		hasBuyOrders := false
		for i := range positions {
			if positions[i].Price < 0 && positions[i].Outcome {
				hasBuyOrders = true
			}
		}
		require.False(t, hasBuyOrders, "Buy order should be fully consumed")

		// Verify buyer got price improvement refund
		// Buyer locked: 50 × 52 × 10^16 = 26 × 10^18 (26 TRUF)
		// Seller received: 50 × 51 × 10^16 = 25.5 × 10^18 (25.5 TRUF)
		// Buyer refund: 50 × 1 × 10^16 = 0.5 × 10^18 (0.5 TRUF)
		// Net cost to buyer: 25.5 TRUF (not 26 TRUF)
		buyerBalanceAfter, err := getUSDCBalance(ctx, platform, buyer.Address())
		require.NoError(t, err)

		// Balance decrease = amount actually paid = 50 shares × $0.51 = 25.5 TRUF
		balanceDecrease := new(big.Int).Sub(buyerBalanceBefore, buyerBalanceAfter)
		expectedCost := new(big.Int).Mul(big.NewInt(50*51), new(big.Int).Exp(big.NewInt(10), big.NewInt(16), nil))
		require.Equal(t, expectedCost.String(), balanceDecrease.String(),
			"Buyer should pay at sell price (51), not buy price (52)")

		return nil
	}
}

// testDirectMatchPriceCrossingSweep tests sweeping across multiple price levels
// Buy@55 should match sell@48, sell@50, sell@52 (cheapest first)
func testDirectMatchPriceCrossingSweep(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		buyer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000006")
		seller1 := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000007")
		seller2 := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000008")
		seller3 := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000009")

		err = giveBalanceChained(ctx, platform, buyer.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, seller1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, seller2.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, seller3.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(buyer.Address(), "sttest00000000000000000000000045", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()
		var marketID int64
		err = callCreateMarket(ctx, platform, &buyer, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Three sellers at different prices
		// Seller1: sell YES @ 48 (30 shares)
		err = callPlaceSplitLimitOrder(ctx, platform, &seller1, int(marketID), 48, 30)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &seller1, int(marketID), true, 48, 30)
		require.NoError(t, err)

		// Seller2: sell YES @ 50 (40 shares)
		err = callPlaceSplitLimitOrder(ctx, platform, &seller2, int(marketID), 50, 40)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &seller2, int(marketID), true, 50, 40)
		require.NoError(t, err)

		// Seller3: sell YES @ 52 (50 shares)
		err = callPlaceSplitLimitOrder(ctx, platform, &seller3, int(marketID), 52, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &seller3, int(marketID), true, 52, 50)
		require.NoError(t, err)

		// Buyer: Buy 100 YES @ $0.55 — should sweep sell@48 (30), sell@50 (40), sell@52 (30 of 50)
		err = callPlaceBuyOrder(ctx, platform, &buyer, int(marketID), true, 55, 100)
		require.NoError(t, err)

		// Verify positions
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Participant order: seller1=1, seller2=2, seller3=3, buyer=4
		var buyerYesHoldings *Position
		for i := range positions {
			if positions[i].ParticipantID == 4 && positions[i].Outcome && positions[i].Price == 0 {
				buyerYesHoldings = &positions[i]
			}
		}

		require.NotNil(t, buyerYesHoldings, "Buyer should have YES holdings from sweep")
		require.Equal(t, int64(100), buyerYesHoldings.Amount,
			"Buyer should have 100 YES shares (30@48 + 40@50 + 30@52)")

		// Seller3 should have 20 remaining (50 - 30 = 20)
		var seller3Remaining *Position
		for i := range positions {
			if positions[i].ParticipantID == 3 && positions[i].Outcome && positions[i].Price == 52 {
				seller3Remaining = &positions[i]
			}
		}
		require.NotNil(t, seller3Remaining, "Seller3 should have remaining sell order")
		require.Equal(t, int64(20), seller3Remaining.Amount, "Seller3 should have 20 remaining at price 52")

		// No buy orders should remain
		hasBuyOrders := false
		for i := range positions {
			if positions[i].Price < 0 && positions[i].Outcome {
				hasBuyOrders = true
			}
		}
		require.False(t, hasBuyOrders, "Buy order should be fully consumed by sweep")

		return nil
	}
}
