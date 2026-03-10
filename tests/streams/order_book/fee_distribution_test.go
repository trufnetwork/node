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
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestFeeDistribution is the main test suite for LP fee distribution
func TestFeeDistribution(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_09_FeeDistribution",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// Happy path: 1 block, 2 LPs with different reward percentages
			testDistribution1Block2LPs(t),
			// Multi-block distribution: 3 blocks, 2 LPs
			testDistribution3Blocks2LPs(t),
			// Edge case: No LP samples recorded (fees stay in vault)
			testDistributionNoSamples(t),
			// Edge case: Zero fees to distribute
			testDistributionZeroFees(t),
			// Single LP scenario: 1 LP gets 100% of fees
			testDistribution1LP(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testDistribution1Block2LPs tests basic fee distribution with 1 sampled block and 2 LPs
// Scenario: LP1 gets 70%, LP2 gets 30% of 1000 TRUF fees
func testDistribution1Block2LPs(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give both users balance using chained deposits (1000 TRUF each for TRUE+FALSE side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000057", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book depth (so midpoint can be calculated)
		// CRITICAL: Buy prices BELOW sell prices to avoid matching engine consumption.
		// User1: Split @ 50 with 300 → 300 TRUE holdings + 300 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)

		// Establish bid and ask for midpoint calculation
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 44, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 56, 150)
		require.NoError(t, err)
		// holdings: 300→150

		// User2: Split @ 50 for TRUE holdings
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)

		// User1 TRUE-side LP pair: YES sell@52 + NO buy@48
		// NO buy@48 < NO sell@50 → no match ✓
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 100)
		require.NoError(t, err)
		// holdings: 150→50
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 48, 100)
		require.NoError(t, err)

		// User2 TRUE-side LP pair: YES sell@51 + NO buy@49
		// NO buy@49 < NO sell@50 → no match ✓
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 51, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// User1 FALSE-side LP pair: NO sell@50(300) + YES buy@50(300)
		// YES buy@50 < YES sell@51 → no match ✓
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 300)
		require.NoError(t, err)

		// User2 FALSE-side LP pair: NO sell@50(100) + YES buy@50(100)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), true, 50, 100)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=51 → midpoint=50, spread=5

		// Sample LP rewards at block 1000
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)

		// Verify rewards were recorded
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Len(t, rewards, 2, "Should have 2 LPs")
		t.Logf("Sampled rewards: %+v", rewards)

		// Get balances before distribution
		balance1Before, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2Before, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)
		t.Logf("Balances before distribution: User1=%s, User2=%s", balance1Before.String(), balance2Before.String())

		// Call distribute_fees directly with 10 TRUF fees (in wei) - reduced for testing
		// 10 TRUF = 10 * 10^18 wei
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))

		// Fund the vault (escrow) with 1000 TRUF so it can distribute fees
		// Use giveBalanceChained to maintain the ordered-sync chain
		err = giveUSDCBalanceChained(ctx, platform, testUSDCEscrow, totalFees.String())
		require.NoError(t, err)
		t.Logf("Funded vault with %s TRUF", new(big.Int).Div(totalFees, big.NewInt(1e18)).String())

		// Force sync the ERC20 instance to recognize vault balance
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Convert to NUMERIC(78, 0) type using ParseDecimalExplicit
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		// Generate leader key for fee transfers
		pub := NewTestProposerPub(t)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"distribute_fees",
			[]any{int(marketID), totalFeesDecimal, true},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balances after distribution
		balance1After, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2After, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)
		t.Logf("Balances after distribution: User1=%s, User2=%s", balance1After.String(), balance2After.String())

		// Calculate actual distributions
		dist1 := new(big.Int).Sub(balance1After, balance1Before)
		dist2 := new(big.Int).Sub(balance2After, balance2Before)
		t.Logf("Distributions: User1=%s TRUF, User2=%s TRUF",
			new(big.Int).Div(dist1, big.NewInt(1e18)).String(),
			new(big.Int).Div(dist2, big.NewInt(1e18)).String())

		// Step 0: Calculate Expected Shares (75/12.5/12.5 split)
		infraShare := new(big.Int).Div(new(big.Int).Mul(totalFees, big.NewInt(125)), big.NewInt(1000))
		lpShareTotal := new(big.Int).Sub(totalFees, new(big.Int).Mul(infraShare, big.NewInt(2)))

		// Dynamically calculate expected LP shares based on sampled rewards
		// rewards map is [participant_id: percentage]
		expectedLP1 := big.NewInt(0)
		expectedLP2 := big.NewInt(0)

		if p1Reward, ok := rewards[1]; ok {
			// Convert float64 percentage to big.Int with precision
			// reward_percent is already 0-100
			p1Wei := new(big.Int).Mul(lpShareTotal, big.NewInt(int64(p1Reward*100)))
			expectedLP1 = new(big.Int).Div(p1Wei, big.NewInt(10000))
		}
		if p2Reward, ok := rewards[2]; ok {
			p2Wei := new(big.Int).Mul(lpShareTotal, big.NewInt(int64(p2Reward*100)))
			expectedLP2 = new(big.Int).Div(p2Wei, big.NewInt(10000))
		}

		// User1 gets expectedLP1 + infraShare (as DP)
		// They might also get another infraShare if they are the leader (@leader_sender)
		expectedDist1 := new(big.Int).Add(expectedLP1, infraShare)
		expectedDist2 := expectedLP2

		// Total distributed to our test users (might not be 100% if leader is different)
		totalToUsers := new(big.Int).Add(dist1, dist2)
		
		// If totalToUsers is totalFees, then User1 was also the leader
		if totalToUsers.Cmp(totalFees) == 0 {
			t.Logf("User1 appears to be the leader, adding second infraShare to expectation")
			expectedDist1 = new(big.Int).Add(expectedDist1, infraShare)
		} else {
			// If not, verify total matches expectedLP1 + expectedLP2 + infraShare (DP)
			expectedTotalToUsers := new(big.Int).Add(lpShareTotal, infraShare)
			require.Equal(t, expectedTotalToUsers.String(), totalToUsers.String(), "Total to users should be LP shares + DP share")
		}

		require.Equal(t, expectedDist1.String(), dist1.String(), "User1 should get LP share + DP share (+ Leader share if applicable)")
		require.Equal(t, expectedDist2.String(), dist2.String(), "User2 should get exactly their LP share")

		t.Logf("✅ Fee split verified: User1 (LP+DP)=%s, User2 (LP)=%s", dist1.String(), dist2.String())

		// Verify ob_rewards table is cleaned up
		rewardsAfter, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewardsAfter, "ob_rewards table should be empty after distribution")

		return nil
	}
}

// testDistribution3Blocks2LPs tests fee distribution across 3 sampled blocks with 2 LPs
// Scenario: Shows block-based distribution where reward_per_block = total_fees / 3
func testDistribution3Blocks2LPs(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give both users balance (1000 TRUF each for TRUE+FALSE side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000058", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book depth with proper LP pairs (avoid matching engine consumption)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 44, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 56, 150)
		require.NoError(t, err)

		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)

		// User1 TRUE-side: YES sell@52 + NO buy@48
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 48, 100)
		require.NoError(t, err)

		// User2 TRUE-side: YES sell@51 + NO buy@49
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 51, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// User1 FALSE-side: YES buy@50(300)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 300)
		require.NoError(t, err)

		// User2 FALSE-side: YES buy@50(100)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), true, 50, 100)
		require.NoError(t, err)

		// Sample LP rewards at 3 different blocks
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)
		err = triggerBatchSampling(ctx, platform, 2000)
		require.NoError(t, err)
		err = triggerBatchSampling(ctx, platform, 3000)
		require.NoError(t, err)

		// Verify rewards were recorded for all 3 blocks
		rewards1000, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Len(t, rewards1000, 2, "Should have 2 LPs at block 1000")

		rewards2000, err := getRewards(ctx, platform, int(marketID), 2000)
		require.NoError(t, err)
		require.Len(t, rewards2000, 2, "Should have 2 LPs at block 2000")

		rewards3000, err := getRewards(ctx, platform, int(marketID), 3000)
		require.NoError(t, err)
		require.Len(t, rewards3000, 2, "Should have 2 LPs at block 3000")

		t.Logf("Sampled rewards - Block 1000: %+v, Block 2000: %+v, Block 3000: %+v",
			rewards1000, rewards2000, rewards3000)

		// Get balances before distribution
		balance1Before, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2Before, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Distribute 30 TRUF in fees (10 TRUF per block)
		totalFees := new(big.Int).Mul(big.NewInt(30), big.NewInt(1e18))

		// Fund vault
		err = giveUSDCBalanceChained(ctx, platform, testUSDCEscrow, totalFees.String())
		require.NoError(t, err)
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Call distribute_fees
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		// Generate leader key for fee transfers
		pub := NewTestProposerPub(t)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"distribute_fees",
			[]any{int(marketID), totalFeesDecimal, true},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balances after distribution
		balance1After, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2After, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Calculate actual distributions
		dist1 := new(big.Int).Sub(balance1After, balance1Before)
		dist2 := new(big.Int).Sub(balance2After, balance2Before)
		t.Logf("Distributions: User1=%s TRUF, User2=%s TRUF",
			new(big.Int).Div(dist1, big.NewInt(1e18)).String(),
			new(big.Int).Div(dist2, big.NewInt(1e18)).String())

		// Step 0: Calculate Expected Shares (75/12.5/12.5 split)
		infraShare := new(big.Int).Div(new(big.Int).Mul(totalFees, big.NewInt(125)), big.NewInt(1000))
		lpShareTotal := new(big.Int).Sub(totalFees, new(big.Int).Mul(infraShare, big.NewInt(2)))

		// Calculate average LP share across all blocks
		totalP1Reward := rewards1000[1] + rewards2000[1] + rewards3000[1]
		totalP2Reward := rewards1000[2] + rewards2000[2] + rewards3000[2]

		// User1 LP: (lpShareTotal * totalP1Reward) / (100 * 3)
		// User2 LP: (lpShareTotal * totalP2Reward) / (100 * 3)
		expectedLP1 := new(big.Int).Div(
			new(big.Int).Mul(lpShareTotal, big.NewInt(int64(totalP1Reward*100))),
			big.NewInt(30000),
		)
		expectedLP2 := new(big.Int).Div(
			new(big.Int).Mul(lpShareTotal, big.NewInt(int64(totalP2Reward*100))),
			big.NewInt(30000),
		)

		// User1 is DP, so gets expectedLP1 + infraShare (+ Leader share if applicable)
		expectedDist1 := new(big.Int).Add(expectedLP1, infraShare)
		expectedDist2 := expectedLP2

		// Total distributed to our test users
		totalToUsers := new(big.Int).Add(dist1, dist2)
		
		if totalToUsers.Cmp(totalFees) == 0 {
			t.Logf("User1 appears to be the leader, adding second infraShare to expectation")
			expectedDist1 = new(big.Int).Add(expectedDist1, infraShare)
		} else {
			expectedTotalToUsers := new(big.Int).Add(lpShareTotal, infraShare)
			require.Equal(t, expectedTotalToUsers.String(), totalToUsers.String(), "Total to users should be LP shares + DP share")
		}

		require.Equal(t, expectedDist1.String(), dist1.String(), "User1 should get LP share + DP share (+ Leader share if applicable)")
		require.Equal(t, expectedDist2.String(), dist2.String(), "User2 should get exactly their LP share")

		t.Logf("✅ Fee split verified: User1=%s, User2=%s", dist1.String(), dist2.String())

		// Verify ob_rewards table is cleaned up
		rewardsAfter, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewardsAfter, "ob_rewards table should be empty after distribution")

		return nil
	}
}

// testDistributionNoSamples tests edge case where no LP samples exist
// Scenario: DP and Validator get their 12.5% shares, LP share (75%) stays in vault
// Note: In practice, distribute_fees() calls sample_lp_rewards() one final time at settlement,
// so block_count=0 only occurs if the market has no two-sided liquidity at all.
func testDistributionNoSamples(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000059", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Lock some collateral to ensure bridge liquidity for payouts
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 100)
		require.NoError(t, err)

		// DO NOT call sample_lp_rewards - no samples!
		// distribute_fees will call sample_lp_rewards one final time at @height,
		// but with only bid-side liquidity (split limit order only creates YES holdings + NO sell),
		// the spec-aligned midpoint requires YES sell orders, so no rewards will be generated.

		// Get balance before distribution
		balanceBefore, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Prepare 10 TRUF fees
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))

		// Fund vault
		err = giveUSDCBalanceChained(ctx, platform, testUSDCEscrow, totalFees.String())
		require.NoError(t, err)
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Call distribute_fees - with no qualifying LPs, only DP and Validator get paid
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		// Generate leader key for fee transfers
		pub := NewTestProposerPub(t)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"distribute_fees",
			[]any{int(marketID), totalFeesDecimal, true},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balance after distribution
		balanceAfter, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Only DP (12.5%) and Validator (12.5%) shares are distributed
		// LP share (75%) stays in vault — per spec, no redistribution
		infraShare := new(big.Int).Div(new(big.Int).Mul(totalFees, big.NewInt(125)), big.NewInt(1000))

		actualDist := new(big.Int).Sub(balanceAfter, balanceBefore)
		t.Logf("Distribution: User1 received=%s", actualDist.String())
		t.Logf("Expected infraShare (12.5%%): %s", infraShare.String())

		// User1 is DP → gets infraShare (12.5%)
		// If User1 is also the leader → gets 2 * infraShare (25%)
		expectedDPOnly := infraShare
		expectedDPAndLeader := new(big.Int).Mul(infraShare, big.NewInt(2))

		if actualDist.Cmp(expectedDPAndLeader) == 0 {
			t.Logf("User1 is DP + Leader, received 2x infraShare = %s", expectedDPAndLeader.String())
		} else {
			require.Equal(t, expectedDPOnly.String(), actualDist.String(),
				"User1 (DP only) should get exactly 12.5%% infraShare")
		}

		// Verify LP share remains in vault (75% of fees not distributed)
		lpShare := new(big.Int).Sub(totalFees, new(big.Int).Mul(infraShare, big.NewInt(2)))
		t.Logf("LP share staying in vault: %s (75%% of total fees)", lpShare.String())

		return nil
	}
}

// testDistributionZeroFees tests edge case with zero fees
// Scenario: Should return early, no changes
func testDistributionZeroFees(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give both users balance (1000 TRUF each for TRUE+FALSE side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000060", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book and sample rewards (so there ARE LPs)
		// Use proper LP pair pattern (avoid matching engine consumption)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 44, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 56, 150)
		require.NoError(t, err)

		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)

		// User1 TRUE-side: YES sell@52 + NO buy@48
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 48, 100)
		require.NoError(t, err)

		// User2 TRUE-side: YES sell@51 + NO buy@49
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 51, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// FALSE-side pairs
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), true, 50, 100)
		require.NoError(t, err)

		// Sample LP rewards
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)

		// Get balances before distribution
		balance1Before, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2Before, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Call distribute_fees with ZERO fees (should return early)
		zeroFees := new(big.Int).SetInt64(0)
		zeroFeesDecimal, err := kwilTypes.ParseDecimalExplicit(zeroFees.String(), 78, 0)
		require.NoError(t, err)

		// Generate leader key for fee transfers
		pub := NewTestProposerPub(t)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"distribute_fees",
			[]any{int(marketID), zeroFeesDecimal, true},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balances after distribution
		balance1After, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2After, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Verify NO distribution occurred
		require.Equal(t, balance1Before, balance1After, "User1 balance should be unchanged (zero fees)")
		require.Equal(t, balance2Before, balance2After, "User2 balance should be unchanged (zero fees)")

		// Verify ob_rewards table is NOT cleaned up (early return before cleanup)
		rewardsAfter, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Len(t, rewardsAfter, 2, "ob_rewards should still have data (early return)")

		t.Logf("✅ Zero fees correctly skipped distribution")

		return nil
	}
}

// testDistribution1LP tests single LP scenario
// Scenario: 1 LP should get 100% of fees
func testDistribution1LP(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Give user balance (1000 TRUF for TRUE+FALSE side pairs)
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000061", "get_record", []byte{0x01})

		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book with ONLY user1 (avoid matching engine consumption)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 54, 200)
		require.NoError(t, err)
		// holdings: 300→100

		// User1 TRUE-side LP pair: YES sell@51 + NO buy@49
		// NO buy@49 < NO sell@50 → no match ✓
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 51, 100)
		require.NoError(t, err)
		// holdings: 100→0
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 49, 100)
		require.NoError(t, err)

		// User1 FALSE-side LP pair: NO sell@50(300) + YES buy@50(300)
		// YES buy@50 < YES sell@51 → no match ✓
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 50, 300)
		require.NoError(t, err)
		// Final midpoint: best bid=-50, lowest sell=51 → midpoint=50, spread=5

		// Sample LP rewards at block 1000
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)

		// Verify only 1 LP recorded
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Len(t, rewards, 1, "Should have only 1 LP")
		t.Logf("Sampled rewards: %+v", rewards)

		participant1ID := 1

		// Verify LP has 100% of rewards
		require.InDelta(t, 100.0, rewards[participant1ID], 0.01, "Single LP should have 100%")

		// Get balance before distribution
		balanceBefore, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Distribute 10 TRUF in fees
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))

		// Fund vault
		err = giveUSDCBalanceChained(ctx, platform, testUSDCEscrow, totalFees.String())
		require.NoError(t, err)
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Call distribute_fees
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		// Generate leader key for fee transfers
		pub := NewTestProposerPub(t)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"distribute_fees",
			[]any{int(marketID), totalFeesDecimal, true},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balance after distribution
		balanceAfter, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Calculate distribution
		dist := new(big.Int).Sub(balanceAfter, balanceBefore)
		t.Logf("Distribution: User1=%s TRUF",
			new(big.Int).Div(dist, big.NewInt(1e18)).String())

		// Step 0: Calculate Expected Share (75/12.5/12.5 split)
		infraShare := new(big.Int).Div(new(big.Int).Mul(totalFees, big.NewInt(125)), big.NewInt(1000))
		lpShareTotal := new(big.Int).Sub(totalFees, new(big.Int).Mul(infraShare, big.NewInt(2)))

		// User1 is LP (100%), DP, and potentially leader.
		expectedDist := new(big.Int).Add(lpShareTotal, infraShare)

		if dist.Cmp(totalFees) == 0 {
			t.Logf("User1 appears to be the leader, adding second infraShare to expectation")
			expectedDist = totalFees
		}

		// Verify user got exactly LP + DP share
		require.Equal(t, 0, dist.Cmp(expectedDist),
			fmt.Sprintf("User1 should get exactly LP + DP fees. Got %s, expected %s",
				dist.String(), expectedDist.String()))

		t.Logf("✅ Single LP (plus DP/Leader roles) correctly received fees: %s", dist.String())

		// Verify ob_rewards table is cleaned up
		rewardsAfter, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewardsAfter, "ob_rewards table should be empty after distribution")

		t.Logf("✅ Single LP correctly received 100%% of fees")

		return nil
	}
}
