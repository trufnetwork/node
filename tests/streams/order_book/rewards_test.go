//go:build kwiltest

package order_book

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

func callSampleLPRewards(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, block int64, resultFn func(*common.Row) error) error {
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
		"sample_lp_rewards",
		[]any{queryID, block},
		resultFn,
	)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return fmt.Errorf("%s", res.Error.Error())
	}
	return nil
}

func getRewards(ctx context.Context, platform *kwilTesting.Platform, queryID int, block int64) (map[int]float64, error) {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
		},
		Signer:        []byte("anonymous"),
		Caller:        "anonymous",
		TxID:          platform.Txid(),
		Authenticator: "anonymous",
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	rewards := make(map[int]float64)
	err := platform.Engine.Execute(
		engineCtx,
		platform.DB,
		"SELECT participant_id, reward_percent FROM ob_rewards WHERE query_id = $qid AND block = $block ORDER BY participant_id",
		map[string]any{"$qid": queryID, "$block": block},
		func(row *common.Row) error {
			pid := int(row.Values[0].(int64))
			// reward_percent is NUMERIC(5,2) which comes as *types.Decimal
			decimal := row.Values[1].(*types.Decimal)
			rewardStr := decimal.String()
			reward, err := strconv.ParseFloat(rewardStr, 64)
			if err != nil {
				return fmt.Errorf("failed to parse reward %q: %w", rewardStr, err)
			}
			rewards[pid] = reward
			return nil
		},
	)
	return rewards, err
}

// TestRewardsSampling is the main test suite for LP rewards sampling
func TestRewards(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_08_Rewards",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// Category A: Validation Tests
			testSampleRewardsMarketSettled(t),
			testSampleRewardsNoOrderBook(t),
			testSampleRewardsIncompleteOrderBook(t),

			// Category B: Midpoint and Spread Tests
			testSampleRewardsSpread5Cents(t),
			testSampleRewardsSpread4Cents(t),
			testSampleRewardsSpread3Cents(t),
			testSampleRewardsIneligibleMarket(t),

			// Category C: Scoring Tests
			testSampleRewardsSingleLP(t),
			testSampleRewardsTwoLPs(t),
			testSampleRewardsMultipleLPs(t),

			// Category D: Edge Cases
			testSampleRewardsNoQualifyingOrders(t),

			// Category E: Constraint Logic Tests
			testConstraintSellBuyPair(t),
			testConstraintNoDuplicates(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testSampleRewardsMarketSettled tests error when trying to sample a settled market
func testSampleRewardsMarketSettled(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_settled"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Manually settle the market (test shortcut)
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
		require.NoError(t, err)

		// Try to sample settled market
		err = callSampleLPRewards(ctx, platform, &userAddr, int(marketID), 1000, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Market is already settled")

		return nil
	}
}

// testSampleRewardsNoOrderBook tests sampling when no orders exist
func testSampleRewardsNoOrderBook(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_no_orders"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Sample should succeed but produce no rewards (empty order book)
		err = callSampleLPRewards(ctx, platform, &userAddr, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Verify no rewards were inserted
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewards, "No rewards should be generated for empty order book")

		return nil
	}
}

// testSampleRewardsIncompleteOrderBook tests when only bid or ask exists (not both)
func testSampleRewardsIncompleteOrderBook(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_incomplete"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order at 60¢ (creates YES holdings + NO sell @ 40¢)
		// This creates a sell order but no buy order, so midpoint calculation fails
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 60, 100)
		require.NoError(t, err)

		// Sample should succeed but produce no rewards (incomplete order book)
		err = callSampleLPRewards(ctx, platform, &userAddr, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Verify no rewards
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewards, "No rewards for incomplete order book")

		return nil
	}
}

// testSampleRewardsSpread5Cents tests dynamic spread = 5¢ (midpoint 36-50 or 50-64)
func testSampleRewardsSpread5Cents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_spread_5"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book with midpoint around 48¢ (distance from 50 = 2, spread = 5¢)
		// User1: Split @ 48¢ → YES holdings + NO sell @ 52¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 48, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Verify spread was 5¢ (we can't check spread directly, but rewards should be generated)
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		t.Logf("Spread 5¢ rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsSpread4Cents tests dynamic spread = 4¢ (midpoint distance 30-59)
func testSampleRewardsSpread4Cents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_spread_4"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book with midpoint around 35¢ (distance from 50 = 15, spread = 4¢)
		// User1: Split @ 35¢ → YES holdings + NO sell @ 65¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 35, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 2000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 2000)
		require.NoError(t, err)
		t.Logf("Spread 4¢ rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsSpread3Cents tests dynamic spread = 3¢ (midpoint distance 60-79)
func testSampleRewardsSpread3Cents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_spread_3"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book with midpoint around 20¢ (distance from 50 = 30, spread = 3¢)
		// User1: Split @ 20¢ → YES holdings + NO sell @ 80¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 20, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 3000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 3000)
		require.NoError(t, err)
		t.Logf("Spread 3¢ rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsIneligibleMarket tests markets too certain for rewards (distance 80-99)
func testSampleRewardsIneligibleMarket(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_ineligible"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book with midpoint around 5¢ (distance from 50 = 45, ineligible)
		// User1: Split @ 5¢ → YES holdings + NO sell @ 95¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 5, 100)
		require.NoError(t, err)

		// Sample should succeed but produce no rewards (ineligible spread)
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 4000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 4000)
		require.NoError(t, err)
		require.Empty(t, rewards, "No rewards for ineligible market (too certain)")

		return nil
	}
}

// testSampleRewardsSingleLP tests single LP gets 100% of rewards
func testSampleRewardsSingleLP(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_single_lp"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book depth for midpoint calculation
		// Split @ 50 with 300 → 300 TRUE holdings + 300 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)

		// Establish bid and ask for midpoint
		// TRUE BUY @ 46 (establishes best bid)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		// TRUE SELL @ 52 (establishes best ask, uses 200 holdings)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 200)
		require.NoError(t, err)

		// User1: Create paired SELL+BUY orders for LP rewards
		// Sell YES @ 48¢ + Buy NO @ 52¢ (complementary, liquidity provision)
		// Uses remaining 100 TRUE holdings from split
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 48, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 52, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 5000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 5000)
		require.NoError(t, err)
		require.Len(t, rewards, 1, "Should have 1 LP")

		// Single LP should get 100%
		for _, percent := range rewards {
			require.InDelta(t, 100.0, percent, 0.01, "Single LP should get 100% of rewards")
		}

		return nil
	}
}

// testSampleRewardsTwoLPs tests reward distribution between two LPs
func testSampleRewardsTwoLPs(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give both users balance (MUST use chained helper!)
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_two_lps"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book depth
		// User1: Split @ 50 with 400 → 400 TRUE holdings + 400 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 400)
		require.NoError(t, err)

		// Establish bid and ask for midpoint
		// TRUE BUY @ 44 (establishes best bid)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 44, 50)
		require.NoError(t, err)
		// TRUE SELL @ 52 (establishes best ask, uses 300 holdings)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 300)
		require.NoError(t, err)

		// User1: Paired SELL+BUY orders YES @ 46¢ + NO @ 54¢
		// Uses remaining 100 TRUE holdings from split
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 46, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 54, 100)
		require.NoError(t, err)

		// User2: Paired SELL+BUY orders YES @ 47¢ + NO @ 53¢ (closer to midpoint)
		// Split @ 50 to get TRUE holdings
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 47, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 53, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 6000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 6000)
		require.NoError(t, err)
		require.Len(t, rewards, 2, "Should have 2 LPs")

		// Total should sum to 100%
		total := 0.0
		for _, percent := range rewards {
			total += percent
		}
		require.InDelta(t, 100.0, total, 0.01, "Rewards should sum to 100%")

		t.Logf("Two LP rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsMultipleLPs tests reward distribution among multiple LPs
func testSampleRewardsMultipleLPs(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xcccccccccccccccccccccccccccccccccccccccc")
		user3 := util.Unsafe_NewEthereumAddressFromString("0xdddddddddddddddddddddddddddddddddddddddd")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give all users balance (MUST use chained helper!)
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user3.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_multiple_lps"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create order book depth
		// User1: Split @ 50 with 400 → 400 TRUE holdings + 400 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 400)
		require.NoError(t, err)

		// Establish bid and ask for midpoint
		// TRUE BUY @ 44 (establishes best bid)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 44, 50)
		require.NoError(t, err)
		// TRUE SELL @ 52 (establishes best ask, uses 300 holdings)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 300)
		require.NoError(t, err)

		// User1: Paired SELL+BUY orders YES @ 46¢ + NO @ 54¢ (farthest from midpoint, 4¢ away)
		// Uses remaining 100 TRUE holdings from split
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 46, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 54, 100)
		require.NoError(t, err)

		// User2: Paired SELL+BUY orders YES @ 47¢ + NO @ 53¢ (middle distance, 3¢ away, larger amount)
		// Split @ 50 to get TRUE holdings
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 200)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 47, 200)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 53, 200)
		require.NoError(t, err)

		// User3: Paired SELL+BUY orders YES @ 48¢ + NO @ 52¢ (closest to midpoint, 2¢ away)
		// Split @ 50 to get TRUE holdings
		err = callPlaceSplitLimitOrder(ctx, platform, &user3, int(marketID), 50, 100)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user3, int(marketID), true, 48, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user3, int(marketID), false, 52, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 7000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 7000)
		require.NoError(t, err)
		require.Len(t, rewards, 3, "Should have 3 LPs")

		// Total should sum to 100%
		total := 0.0
		for _, percent := range rewards {
			total += percent
		}
		require.InDelta(t, 100.0, total, 0.01, "Rewards should sum to 100%")

		t.Logf("Multiple LP rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsNoQualifyingOrders tests when orders exist but none qualify (too wide spread)
func testSampleRewardsNoQualifyingOrders(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xffffffffffffffffffffffffffffffffffffffff")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give users balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_rewards_no_qualifying"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create orders with very wide spread (won't qualify for rewards)
		// User1: Split @ 10¢ → YES holdings + NO sell @ 90¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 10, 100)
		require.NoError(t, err)

		// User2: Split @ 90¢ → YES holdings + NO sell @ 10¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 90, 100)
		require.NoError(t, err)

		// Midpoint will be around 50¢ with spread distance > threshold
		// Sample should produce no rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 8000, nil)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 8000)
		require.NoError(t, err)
		require.Empty(t, rewards, "No rewards when orders don't qualify (spread too wide)")

		return nil
	}
}
// =============================================================================
// Category E: Constraint Logic Tests
// =============================================================================

// testConstraintSellBuyPair verifies the constraint correctly matches SELL+BUY pairs
func testConstraintSellBuyPair(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xc111111111111111111111111111111111111111")
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryHash := sha256.Sum256([]byte("test_constraint_sell_buy"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var queryID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create complete order book with SELL+BUY pair
		// Split @ 40 → TRUE holdings + FALSE SELL @ 60
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(queryID), 40, 100)
		require.NoError(t, err)

		// TRUE BUY @ 42 (establishes bid for midpoint)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(queryID), true, 42, 50)
		require.NoError(t, err)

		// TRUE SELL @ 48
		err = callPlaceSellOrder(ctx, platform, &user1, int(queryID), true, 48, 100)
		require.NoError(t, err)

		// FALSE BUY @ 52 (matches constraint with TRUE SELL @ 48)
		// Constraint: yes_price == 100 + no_price → 48 == 100 + (-52) ✅
		err = callPlaceBuyOrder(ctx, platform, &user1, int(queryID), false, 52, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(queryID), 9000, nil)
		require.NoError(t, err)

		// Verify rewards generated
		rewards, err := getRewards(ctx, platform, int(queryID), 9000)
		require.NoError(t, err)
		require.NotEmpty(t, rewards, "SELL+BUY pair should receive rewards")

		// Should have exactly 1 participant
		require.Len(t, rewards, 1, "Should have 1 LP")

		// Should get 100% of rewards (only LP)
		for _, percent := range rewards {
			require.InDelta(t, 100.0, percent, 0.01, "Single LP should get 100%")
		}

		return nil
	}
}

// testConstraintNoDuplicates verifies the constraint prevents duplicate rows from self-join
func testConstraintNoDuplicates(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user2 := util.Unsafe_NewEthereumAddressFromString("0xc222222222222222222222222222222222222222")
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryHash := sha256.Sum256([]byte("test_constraint_no_dup"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var queryID int64
		err = callCreateMarket(ctx, platform, &user2, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create same SELL+BUY pair
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(queryID), 40, 100)
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &user2, int(queryID), true, 42, 50)
		require.NoError(t, err)

		err = callPlaceSellOrder(ctx, platform, &user2, int(queryID), true, 48, 100)
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &user2, int(queryID), false, 52, 100)
		require.NoError(t, err)

		// Sample rewards
		err = callSampleLPRewards(ctx, platform, &user2, int(queryID), 10000, nil)
		require.NoError(t, err)

		// Count reward rows - should be exactly 1, not 2
		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        []byte("anonymous"),
			Caller:        "anonymous",
			TxID:          platform.Txid(),
			Authenticator: "anonymous",
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		var count int
		err = platform.Engine.Execute(
			engineCtx,
			platform.DB,
			`SELECT COUNT(*) FROM ob_rewards WHERE query_id = $query_id AND block = $block`,
			map[string]any{
				"$query_id": queryID,
				"$block":    int64(10000),
			},
			func(row *common.Row) error {
				count = int(row.Values[0].(int64))
				return nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Should have exactly 1 reward row (no duplicates from self-join)")

		return nil
	}
}
