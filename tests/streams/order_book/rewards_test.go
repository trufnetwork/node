//go:build kwiltest

package order_book

import (
	"context"
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

// triggerDirectSampling allows testing individual market sampling (e.g. for error cases)
func triggerDirectSampling(ctx context.Context, platform *kwilTesting.Platform, queryID int, block int64) error {
	res, err := platform.Engine.CallWithoutEngineCtx(ctx, platform.DB, "main", "sample_lp_rewards", []any{int64(queryID), block}, nil)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

// triggerBatchSampling simulates the EndBlockHook calling the PRIVATE sample_all_active_lp_rewards action.
func triggerBatchSampling(ctx context.Context, platform *kwilTesting.Platform, block int64) error {
	return triggerBatchSamplingWithLogs(ctx, platform, block, nil)
}

// triggerBatchSamplingWithLogs is like triggerBatchSampling but captures NOTICE logs for debugging.
func triggerBatchSamplingWithLogs(ctx context.Context, platform *kwilTesting.Platform, block int64, t *testing.T) error {
	// We use CallWithoutEngineCtx because internal hooks run without an external caller/signer.
	// This matches the new batch logic in tn_lp_rewards.go.
	res, err := platform.Engine.CallWithoutEngineCtx(
		ctx,
		platform.DB,
		"main",
		"sample_all_active_lp_rewards",
		[]any{block, int64(1000)},
		nil,
	)
	if err != nil {
		return err
	}
	if t != nil && res != nil && len(res.Logs) > 0 {
		for i, log := range res.Logs {
			t.Logf("NOTICE[%d]: %s", i, log)
		}
	}
	if res.Error != nil {
		return res.Error
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
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000046", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
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

		// Try to sample settled market directly
		err = triggerDirectSampling(ctx, platform, int(marketID), 1000)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Market is already settled")

		return nil
	}
}

// testSampleRewardsNoOrderBook tests sampling when no orders exist
func testSampleRewardsNoOrderBook(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000047", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Sample should succeed but produce no rewards (empty order book)
		err = triggerBatchSampling(ctx, platform, 1000)
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
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000048", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order at 60¢ (creates YES holdings + NO sell @ 40¢)
		// This gives us bid-side only: YES buy order is missing, and more importantly
		// YES sell order is missing. Spec-aligned midpoint requires YES sell, so
		// sampling returns early with no rewards.
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 60, 100)
		require.NoError(t, err)

		// Also add a YES buy to have at least the bid side
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 58, 50)
		require.NoError(t, err)
		// Still no YES sell → midpoint can't be calculated → no rewards

		// Sample should succeed but produce no rewards (no YES sell = incomplete order book)
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)

		// Verify no rewards
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewards, "No rewards for incomplete order book (missing YES sell)")

		return nil
	}
}

// testSampleRewardsSpread5Cents tests dynamic spread = 5¢ (midpoint distance from 50 < 15)
func testSampleRewardsSpread5Cents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000049", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create two-sided order book with balanced TRUE-side and FALSE-side pairs.
		// LEAST(TRUE-side, FALSE-side) scoring requires BOTH types of pairs.
		// CRITICAL: LP pair buy prices must be BELOW existing sell prices of same
		// outcome to avoid the matching engine consuming them on placement.
		//
		// Step 1: Split @ 50 for holdings + NO sell @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 200)
		require.NoError(t, err)
		// Creates: YES holdings (TRUE, price=0, amount=200) + NO sell @ 50 (FALSE, price=50, amount=200)

		// Step 2: Establish bid and ask for midpoint
		// YES buy @ 46 (bid)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		// YES sell @ 54 (ask, from holdings: 200→150)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 54, 50)
		require.NoError(t, err)

		// Step 3: TRUE-side LP pair: YES sell @ 51 + NO buy @ 49
		// Pair: p1.price=51 = 100+(-49) ✓, amounts 100=100
		// NO buy @ 49 does NOT match NO sell @ 50 (sell 50 > buy 49 → no direct match)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 51, 100)
		require.NoError(t, err)
		// holdings: 150→50
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// Step 4: FALSE-side LP pair: NO sell @ 50 + YES buy @ 50
		// Pair: p1.price=50 = 100+(-50) ✓, amounts 200=200
		// YES buy @ 50 does NOT match YES sell @ 51 (sell 51 > buy 50 → no direct match)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 200)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=51 → midpoint=(51+50)/2=50
		// spread_base=|50-50|=0 → spread=5

		// Sample rewards with NOTICE logging
		err = triggerBatchSamplingWithLogs(ctx, platform, 1000, t)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.NotEmpty(t, rewards, "Should generate rewards with 5¢ spread")
		t.Logf("Spread 5¢ rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsSpread4Cents tests dynamic spread = 4¢ (midpoint distance 30-59)
func testSampleRewardsSpread4Cents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000050", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create two-sided order book with midpoint around 35¢
		// spread_base = |35 - 65| = 30 → spread = 4¢
		// CRITICAL: Buy prices chosen below sell prices to avoid matching engine consumption.
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 35, 200)
		require.NoError(t, err)
		// YES buy @ 33 (bid side)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 33, 50)
		require.NoError(t, err)
		// YES sell @ 37 (ask side, from holdings: 200→150)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 37, 50)
		require.NoError(t, err)

		// TRUE-side LP pair: YES sell @ 36 + NO buy @ 64 (36 = 100 + (-64))
		// NO buy @ 64 does NOT match NO sell @ 65 (sell 65 > buy 64 → no match)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 36, 100)
		require.NoError(t, err)
		// holdings: 150→50
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 64, 100)
		require.NoError(t, err)

		// FALSE-side LP pair: NO sell @ 65 (from split, amount=200) + YES buy @ 35
		// YES buy @ 35 does NOT match YES sell @ 36 (sell 36 > buy 35 → no match)
		// Pair: p1.price=65 = 100+(-35) ✓, amounts 200=200
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 35, 200)
		require.NoError(t, err)
		// Final midpoint: best bid=-35, lowest sell=36 → midpoint=(36+35)/2=35
		// spread_base=|35-65|=30 → spread=4

		// Sample rewards
		err = triggerBatchSamplingWithLogs(ctx, platform, 2000, t)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 2000)
		require.NoError(t, err)
		require.NotEmpty(t, rewards, "Should generate rewards with 4¢ spread")
		t.Logf("Spread 4¢ rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsSpread3Cents tests dynamic spread = 3¢ (midpoint distance 60-79)
func testSampleRewardsSpread3Cents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000051", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create two-sided order book with midpoint around 20¢
		// spread_base = |20 - 80| = 60 → spread = 3¢
		// CRITICAL: Buy prices chosen below sell prices to avoid matching engine consumption.
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 20, 200)
		require.NoError(t, err)
		// YES buy @ 18 (bid side)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 18, 50)
		require.NoError(t, err)
		// YES sell @ 22 (ask side, from holdings: 200→150)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 22, 50)
		require.NoError(t, err)

		// TRUE-side LP pair: YES sell @ 21 + NO buy @ 79 (21 = 100 + (-79))
		// NO buy @ 79 does NOT match NO sell @ 80 (sell 80 > buy 79 → no match)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 21, 100)
		require.NoError(t, err)
		// holdings: 150→50
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 79, 100)
		require.NoError(t, err)

		// FALSE-side LP pair: NO sell @ 80 (from split, amount=200) + YES buy @ 20
		// YES buy @ 20 does NOT match YES sell @ 21 (sell 21 > buy 20 → no match)
		// Pair: p1.price=80 = 100+(-20) ✓, amounts 200=200
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 20, 200)
		require.NoError(t, err)
		// Final midpoint: best bid=-20, lowest sell=21 → midpoint=(21+20)/2=20
		// spread_base=|20-80|=60 → spread=3

		// Sample rewards
		err = triggerBatchSamplingWithLogs(ctx, platform, 3000, t)
		require.NoError(t, err)

		rewards, err := getRewards(ctx, platform, int(marketID), 3000)
		require.NoError(t, err)
		require.NotEmpty(t, rewards, "Should generate rewards with 3¢ spread")
		t.Logf("Spread 3¢ rewards: %+v", rewards)

		return nil
	}
}

// testSampleRewardsIneligibleMarket tests markets too certain for rewards (distance 80-99)
func testSampleRewardsIneligibleMarket(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000052", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create two-sided order book with midpoint around 10¢
		// spread_base = |10 - 90| = 80 → INELIGIBLE (>= 80)
		// Split @ 10¢ → YES holdings + NO sell @ 90¢
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 10, 200)
		require.NoError(t, err)
		// YES buy @ 8 (bid side)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 8, 50)
		require.NoError(t, err)
		// YES sell @ 12 (ask side, from holdings)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 12, 100)
		require.NoError(t, err)
		// Midpoint = (12 + 8) / 2 = 10. spread_base = |10 - 90| = 80 → INELIGIBLE

		// Sample should succeed but produce no rewards (ineligible spread)
		err = triggerBatchSampling(ctx, platform, 4000)
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
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance (1000 TRUF for TRUE-side + FALSE-side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000053", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
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
		// TRUE SELL @ 54 (establishes best ask, uses 200 holdings: 300→100)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 54, 200)
		require.NoError(t, err)

		// User1: TRUE-side LP pair: YES sell @ 51 + NO buy @ 49
		// Pair: p1.price=51 = 100+(-49) ✓, amounts 100=100
		// NO buy @ 49 does NOT match NO sell @ 50 (sell 50 > buy 49 → no match)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 51, 100)
		require.NoError(t, err)
		// holdings: 100→0
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// User1: FALSE-side LP pair: NO sell @ 50 (from split, amount=300) + YES buy @ 50
		// YES buy @ 50 does NOT match YES sell @ 51 (sell 51 > buy 50 → no match)
		// Pair: p1.price=50 = 100+(-50) ✓, amounts 300=300
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 300)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=51 → midpoint=50, spread=5

		// Sample rewards
		err = triggerBatchSampling(ctx, platform, 5000)
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
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give both users balance (1000 TRUF for TRUE-side + FALSE-side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000054", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
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
		// TRUE SELL @ 56 (establishes best ask, uses 200 holdings: 400→200)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 56, 200)
		require.NoError(t, err)

		// User2: Split @ 50 with 100 → 100 TRUE holdings + 100 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)

		// User1: TRUE-side LP pair: YES sell @ 52 + NO buy @ 48
		// NO buy @ 48 does NOT match NO sell @ 50 (sell 50 > buy 48 → no match)
		// Pair: 52 = 100+(-48) ✓, amounts 100=100
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 100)
		require.NoError(t, err)
		// holdings: 200→100
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 48, 100)
		require.NoError(t, err)

		// User2: TRUE-side LP pair: YES sell @ 51 + NO buy @ 49 (closer to midpoint)
		// NO buy @ 49 does NOT match NO sell @ 50 (sell 50 > buy 49 → no match)
		// Pair: 51 = 100+(-49) ✓, amounts 100=100
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 51, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// User1: FALSE-side LP pair: NO sell @ 50 (amount=400) + YES buy @ 50
		// YES buy @ 50 does NOT match YES sell @ 51 (sell 51 > buy 50 → no match)
		// Pair: 50 = 100+(-50) ✓, amounts 400=400
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 400)
		require.NoError(t, err)

		// User2: FALSE-side LP pair: NO sell @ 50 (amount=100) + YES buy @ 50
		// Pair: 50 = 100+(-50) ✓, amounts 100=100
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), true, 50, 100)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=51 → midpoint=50, spread=5

		// Sample rewards
		err = triggerBatchSampling(ctx, platform, 6000)
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
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xcccccccccccccccccccccccccccccccccccccccc")
		user3 := util.Unsafe_NewEthereumAddressFromString("0xdddddddddddddddddddddddddddddddddddddddd")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give all users balance (1000 TRUF for TRUE-side + FALSE-side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user3.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000055", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
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
		// TRUE SELL @ 56 (establishes best ask, uses 200 holdings: 400→200)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 56, 200)
		require.NoError(t, err)

		// User2: Split @ 50 with 200 → 200 TRUE holdings + 200 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 200)
		require.NoError(t, err)

		// User3: Split @ 50 with 100 → 100 TRUE holdings + 100 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user3, int(marketID), 50, 100)
		require.NoError(t, err)

		// User1: TRUE-side LP pair: YES sell @ 54 + NO buy @ 46 (farthest, 3¢ from midpoint)
		// NO buy @ 46 does NOT match NO sell @ 50 (sell 50 > buy 46 → no match)
		// Pair: 54 = 100+(-46) ✓, amounts 100=100
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 54, 100)
		require.NoError(t, err)
		// holdings: 200→100
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 46, 100)
		require.NoError(t, err)

		// User2: TRUE-side LP pair: YES sell @ 53 + NO buy @ 47 (middle, 2¢ from midpoint)
		// NO buy @ 47 does NOT match NO sell @ 50 (sell 50 > buy 47 → no match)
		// Pair: 53 = 100+(-47) ✓, amounts 200=200
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 53, 200)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 47, 200)
		require.NoError(t, err)

		// User3: TRUE-side LP pair: YES sell @ 52 + NO buy @ 48 (closest, 1¢ from midpoint)
		// NO buy @ 48 does NOT match NO sell @ 50 (sell 50 > buy 48 → no match)
		// Pair: 52 = 100+(-48) ✓, amounts 100=100
		err = callPlaceSellOrder(ctx, platform, &user3, int(marketID), true, 52, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user3, int(marketID), false, 48, 100)
		require.NoError(t, err)

		// User1: FALSE-side LP pair: NO sell @ 50 (amount=400) + YES buy @ 50
		// YES buy @ 50 does NOT match any YES sell (52, 53, 54, 56 all > 50 → no match)
		// Pair: 50 = 100+(-50) ✓, amounts 400=400
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 400)
		require.NoError(t, err)

		// User2: FALSE-side LP pair: NO sell @ 50 (amount=200) + YES buy @ 50
		// Pair: 50 = 100+(-50) ✓, amounts 200=200
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), true, 50, 200)
		require.NoError(t, err)

		// User3: FALSE-side LP pair: NO sell @ 50 (amount=100) + YES buy @ 50
		// Pair: 50 = 100+(-50) ✓, amounts 100=100
		err = callPlaceBuyOrder(ctx, platform, &user3, int(marketID), true, 50, 100)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=52 → midpoint=(52+50)/2=51
		// spread_base=|51-49|=2 → spread=5

		// Sample rewards
		err = triggerBatchSampling(ctx, platform, 7000)
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
		lastBalancePoint = nil
		lastTrufBalancePoint = nil // Reset for this test
		user1 := util.Unsafe_NewEthereumAddressFromString("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000056", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create two-sided order book with midpoint at 50¢ (spread = 5¢)
		// Then create LP pairs far from midpoint that won't qualify
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 200)
		require.NoError(t, err)
		// YES buy @ 48 (bid side)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 48, 50)
		require.NoError(t, err)
		// YES sell @ 52 (ask side, from holdings: 200→150)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 50)
		require.NoError(t, err)
		// Midpoint = (52 + 48) / 2 = 50. spread_base = |50 - 50| = 0 → spread = 5

		// TRUE-side LP pair far from midpoint: YES sell @ 58 + NO buy @ 42
		// |50 - 58| = 8 > spread 5 → won't qualify
		// NO buy @ 42 does NOT match NO sell @ 50 (sell 50 > buy 42 → no match)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 58, 50)
		require.NoError(t, err)
		// holdings: 150→100
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 42, 50)
		require.NoError(t, err)
		// No FALSE-side pair needed: LEAST(0, any) = 0 regardless

		// Sample should produce no rewards (pair too far from midpoint)
		err = triggerBatchSampling(ctx, platform, 8000)
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
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xc111111111111111111111111111111111111111")
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000044", "get_record", []byte{0x01})


		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var queryID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create complete order book with SELL+BUY pair
		// Split @ 50 → TRUE holdings + FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(queryID), 50, 100)
		require.NoError(t, err)

		// TRUE BUY @ 46 (establishes bid for midpoint)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(queryID), true, 46, 50)
		require.NoError(t, err)

		// TRUE SELL @ 54 (establishes ask for midpoint, holdings: 100→50)
		err = callPlaceSellOrder(ctx, platform, &user1, int(queryID), true, 54, 50)
		require.NoError(t, err)

		// TRUE-side LP pair: YES sell @ 51 + NO buy @ 49
		// NO buy @ 49 does NOT match NO sell @ 50 (sell 50 > buy 49 → no match)
		// Pair: 51 = 100+(-49) ✓, amounts 50=50
		err = callPlaceSellOrder(ctx, platform, &user1, int(queryID), true, 51, 50)
		require.NoError(t, err)
		// holdings: 50→0
		err = callPlaceBuyOrder(ctx, platform, &user1, int(queryID), false, 49, 50)
		require.NoError(t, err)

		// FALSE-side LP pair: NO sell @ 50 (from split, amount=100) + YES buy @ 50
		// YES buy @ 50 does NOT match YES sell @ 51 (sell 51 > buy 50 → no match)
		// Pair: 50 = 100+(-50) ✓, amounts 100=100
		err = callPlaceBuyOrder(ctx, platform, &user1, int(queryID), true, 50, 100)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=51 → midpoint=50, spread=5

		// Sample rewards
		err = triggerBatchSampling(ctx, platform, 9000)
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
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user2 := util.Unsafe_NewEthereumAddressFromString("0xc222222222222222222222222222222222222222")
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(user2.Address(), "sttest00000000000000000000000045", "get_record", []byte{0x01})


		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var queryID int64
		err = callCreateMarket(ctx, platform, &user2, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Create SELL+BUY pair with same pattern as constraint test
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(queryID), 50, 100)
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &user2, int(queryID), true, 46, 50)
		require.NoError(t, err)

		err = callPlaceSellOrder(ctx, platform, &user2, int(queryID), true, 54, 50)
		require.NoError(t, err)

		// TRUE-side LP pair: YES sell @ 51 + NO buy @ 49
		err = callPlaceSellOrder(ctx, platform, &user2, int(queryID), true, 51, 50)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(queryID), false, 49, 50)
		require.NoError(t, err)

		// FALSE-side LP pair: NO sell @ 50 (amount=100) + YES buy @ 50
		err = callPlaceBuyOrder(ctx, platform, &user2, int(queryID), true, 50, 100)
		require.NoError(t, err)

		// Sample rewards
		err = triggerBatchSampling(ctx, platform, 10000)
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
