//go:build kwiltest

package order_book

import (
	"context"
	"crypto/sha256"
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

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give both users balance using chained deposits
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_distribution_1block_2lps"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book depth (so midpoint can be calculated)
		// User1: Split @ 50 with 300 → 300 TRUE holdings + 300 FALSE SELL @ 50
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)

		// Establish bid and ask for midpoint calculation
		// TRUE BUY @ 46 (establishes best bid)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		// TRUE SELL @ 52 (establishes best ask, uses 200 holdings)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 200)
		require.NoError(t, err)

		// User1: Create paired SELL+BUY orders for LP rewards
		// Sell YES @ 48¢ + Buy NO @ 52¢
		// Uses remaining 100 TRUE holdings from split
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 48, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 52, 100)
		require.NoError(t, err)

		// User2: Create paired SELL+BUY orders closer to midpoint (higher score)
		// Split @ 50 to get TRUE holdings
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)
		// Sell YES @ 49¢ + Buy NO @ 51¢ (tighter spread, higher score)
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 49, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 51, 100)
		require.NoError(t, err)

		// Sample LP rewards at block 1000
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Verify rewards were recorded
		rewards, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Len(t, rewards, 2, "Should have 2 LPs")
		t.Logf("Sampled rewards: %+v", rewards)

		// Get participant IDs (user1=1, user2=2 since user1 created market first)
		participant1ID := 1
		participant2ID := 2

		// Verify percentages sum to ~100%
		total := rewards[participant1ID] + rewards[participant2ID]
		require.InDelta(t, 100.0, total, 0.01, "Rewards should sum to 100%")

		// Get balances before distribution
		balance1Before, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2Before, err := getBalance(ctx, platform, user2.Address())
		require.NoError(t, err)
		t.Logf("Balances before distribution: User1=%s, User2=%s", balance1Before.String(), balance2Before.String())

		// Call distribute_fees directly with 10 TRUF fees (in wei) - reduced for testing
		// 10 TRUF = 10 * 10^18 wei
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))

		// Fund the vault (escrow) with 1000 TRUF so it can distribute fees
		// Use giveBalanceChained to maintain the ordered-sync chain
		err = giveBalanceChained(ctx, platform, testEscrow, totalFees.String())
		require.NoError(t, err)
		t.Logf("Funded vault with %s TRUF", new(big.Int).Div(totalFees, big.NewInt(1e18)).String())

		// Force sync the ERC20 instance to recognize vault balance
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Convert to NUMERIC(78, 0) type using ParseDecimalExplicit
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
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
			[]any{int(marketID), totalFeesDecimal},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balances after distribution
		balance1After, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2After, err := getBalance(ctx, platform, user2.Address())
		require.NoError(t, err)
		t.Logf("Balances after distribution: User1=%s, User2=%s", balance1After.String(), balance2After.String())

		// Calculate actual distributions
		dist1 := new(big.Int).Sub(balance1After, balance1Before)
		dist2 := new(big.Int).Sub(balance2After, balance2Before)
		t.Logf("Distributions: User1=%s TRUF, User2=%s TRUF",
			new(big.Int).Div(dist1, big.NewInt(1e18)).String(),
			new(big.Int).Div(dist2, big.NewInt(1e18)).String())

		// ZERO-LOSS VERIFICATION: Total distributed must equal total fees exactly
		totalDistributed := new(big.Int).Add(dist1, dist2)
		require.Equal(t, totalFees, totalDistributed,
			fmt.Sprintf("Zero-loss verification failed: distributed %s, expected %s",
				totalDistributed.String(), totalFees.String()))

		// Verify proportional distribution
		// User1: (10e18 * 64) / 100 = 6.4e18 (exactly, no truncation with these percentages)
		// User2: (10e18 * 36) / 100 = 3.6e18 (exactly, no truncation)
		// Total: 6.4e18 + 3.6e18 = 10e18 ✅ (zero loss, but no dust because math is exact)
		expectedDist1 := new(big.Int).Mul(big.NewInt(64), new(big.Int).Div(totalFees, big.NewInt(100))) // 64% = 6.4 TRUF
		expectedDist2 := new(big.Int).Mul(big.NewInt(36), new(big.Int).Div(totalFees, big.NewInt(100))) // 36% = 3.6 TRUF

		require.Equal(t, expectedDist1, dist1, "User1 should get exactly 64%")
		require.Equal(t, expectedDist2, dist2, "User2 should get exactly 36%")

		t.Logf("✅ Zero-loss distribution verified: User1=%s%%, User2=%s%%, Total=%s wei",
			new(big.Int).Div(new(big.Int).Mul(dist1, big.NewInt(100)), totalFees).String(),
			new(big.Int).Div(new(big.Int).Mul(dist2, big.NewInt(100)), totalFees).String(),
			totalDistributed.String())

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

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give both users balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_distribution_3blocks"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book depth
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 200)
		require.NoError(t, err)

		// User1: Create LP orders
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 48, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 52, 100)
		require.NoError(t, err)

		// User2: Create LP orders
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 49, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 51, 100)
		require.NoError(t, err)

		// Sample LP rewards at 3 different blocks
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 2000, nil)
		require.NoError(t, err)
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 3000, nil)
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

		// Get participant IDs
		participant1ID := 1
		participant2ID := 2

		// Get balances before distribution
		balance1Before, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2Before, err := getBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Distribute 30 TRUF in fees (10 TRUF per block)
		totalFees := new(big.Int).Mul(big.NewInt(30), big.NewInt(1e18))

		// Fund vault
		err = giveBalanceChained(ctx, platform, testEscrow, totalFees.String())
		require.NoError(t, err)
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Call distribute_fees
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
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
			[]any{int(marketID), totalFeesDecimal},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balances after distribution
		balance1After, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2After, err := getBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Calculate actual distributions
		dist1 := new(big.Int).Sub(balance1After, balance1Before)
		dist2 := new(big.Int).Sub(balance2After, balance2Before)
		t.Logf("Distributions: User1=%s TRUF, User2=%s TRUF",
			new(big.Int).Div(dist1, big.NewInt(1e18)).String(),
			new(big.Int).Div(dist2, big.NewInt(1e18)).String())

		// ZERO-LOSS VERIFICATION: Total distributed must equal total fees exactly
		totalDistributed := new(big.Int).Add(dist1, dist2)
		require.Equal(t, totalFees, totalDistributed,
			fmt.Sprintf("Zero-loss verification failed: distributed %s, expected %s",
				totalDistributed.String(), totalFees.String()))

		// Verify proportional distribution
		// Total percentages: User1 = 64+64+64 = 192%, User2 = 36+36+36 = 108%
		// User1: (30e18 * 192) / (100 * 3) = 19.2e18 (exactly, no truncation)
		// User2: (30e18 * 108) / (100 * 3) = 10.8e18 (exactly, no truncation)
		// Total: 19.2e18 + 10.8e18 = 30e18 ✅ (zero loss)
		totalPct1 := rewards1000[participant1ID] + rewards2000[participant1ID] + rewards3000[participant1ID]
		totalPct2 := rewards1000[participant2ID] + rewards2000[participant2ID] + rewards3000[participant2ID]

		// Calculate expected distributions
		// totalPct1 = 192.00, totalPct2 = 108.00
		// (30e18 * 192) / (100 * 3) = 19.2e18
		expectedDist1 := new(big.Int).Div(
			new(big.Int).Mul(totalFees, big.NewInt(int64(totalPct1))),
			new(big.Int).Mul(big.NewInt(100), big.NewInt(3)),
		)
		expectedDist2 := new(big.Int).Div(
			new(big.Int).Mul(totalFees, big.NewInt(int64(totalPct2))),
			new(big.Int).Mul(big.NewInt(100), big.NewInt(3)),
		)

		require.Equal(t, expectedDist1, dist1,
			fmt.Sprintf("User1 should get (30 TRUF * %v) / 300 = %s",
				totalPct1, expectedDist1.String()))
		require.Equal(t, expectedDist2, dist2,
			fmt.Sprintf("User2 should get (30 TRUF * %v) / 300 = %s",
				totalPct2, expectedDist2.String()))

		t.Logf("✅ Zero-loss distribution verified: ALL %s wei distributed (User1: %v%%, User2: %v%%)",
			totalFees.String(), totalPct1, totalPct2)

		// Verify ob_rewards table is cleaned up
		rewardsAfter, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewardsAfter, "ob_rewards table should be empty after distribution")

		return nil
	}
}

// testDistributionNoSamples tests edge case where no LP samples exist
// Scenario: Fees should remain in vault (safe accumulation)
func testDistributionNoSamples(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_distribution_no_samples"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// DO NOT call sample_lp_rewards - no samples!

		// Get balance before distribution
		balanceBefore, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Prepare 10 TRUF fees
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))

		// Fund vault
		err = giveBalanceChained(ctx, platform, testEscrow, totalFees.String())
		require.NoError(t, err)
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Call distribute_fees (should return early - no samples)
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
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
			[]any{int(marketID), totalFeesDecimal},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balance after distribution
		balanceAfter, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Verify NO distribution occurred
		require.Equal(t, balanceBefore, balanceAfter, "User balance should be unchanged (no samples)")

		// Verify vault still has the fees (they weren't distributed)
		vaultBalance, err := getBalance(ctx, platform, testEscrow)
		require.NoError(t, err)
		require.True(t, vaultBalance.Cmp(totalFees) >= 0, "Vault should retain fees when no samples exist")

		t.Logf("✅ Fees correctly stayed in vault (no LPs to reward)")

		return nil
	}
}

// testDistributionZeroFees tests edge case with zero fees
// Scenario: Should return early, no changes
func testDistributionZeroFees(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give both users balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_distribution_zero_fees"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book and sample rewards (so there ARE LPs)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 200)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 48, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 52, 100)
		require.NoError(t, err)

		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 50, 100)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), true, 49, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 51, 100)
		require.NoError(t, err)

		// Sample LP rewards
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Get balances before distribution
		balance1Before, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2Before, err := getBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Call distribute_fees with ZERO fees (should return early)
		zeroFees := new(big.Int).SetInt64(0)
		zeroFeesDecimal, err := kwilTypes.ParseDecimalExplicit(zeroFees.String(), 78, 0)
		require.NoError(t, err)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
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
			[]any{int(marketID), zeroFeesDecimal},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balances after distribution
		balance1After, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		balance2After, err := getBalance(ctx, platform, user2.Address())
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

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Give user balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryHash := sha256.Sum256([]byte("test_distribution_1lp"))
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryHash[:], settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Create order book with ONLY user1 (no user2)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 300)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 46, 50)
		require.NoError(t, err)
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 52, 200)
		require.NoError(t, err)

		// User1: Create paired SELL+BUY orders for LP rewards
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 48, 100)
		require.NoError(t, err)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), false, 52, 100)
		require.NoError(t, err)

		// Sample LP rewards at block 1000
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
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
		balanceBefore, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Distribute 10 TRUF in fees
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))

		// Fund vault
		err = giveBalanceChained(ctx, platform, testEscrow, totalFees.String())
		require.NoError(t, err)
		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		require.NoError(t, err)

		// Call distribute_fees
		totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
		require.NoError(t, err)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
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
			[]any{int(marketID), totalFeesDecimal},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			t.Fatalf("distribute_fees error: %v", res.Error)
		}

		// Get balance after distribution
		balanceAfter, err := getBalance(ctx, platform, user1.Address())
		require.NoError(t, err)

		// Calculate distribution
		dist := new(big.Int).Sub(balanceAfter, balanceBefore)
		t.Logf("Distribution: User1=%s TRUF",
			new(big.Int).Div(dist, big.NewInt(1e18)).String())

		// Verify user got ~100% of fees (allow 1% tolerance for rounding)
		tolerance := new(big.Int).Div(totalFees, big.NewInt(100))
		diff := new(big.Int).Sub(dist, totalFees)
		diff.Abs(diff)
		require.True(t, diff.Cmp(tolerance) <= 0,
			fmt.Sprintf("Single LP should get all fees. Got %s, expected %s (diff %s)",
				dist.String(), totalFees.String(), diff.String()))

		// Verify ob_rewards table is cleaned up
		rewardsAfter, err := getRewards(ctx, platform, int(marketID), 1000)
		require.NoError(t, err)
		require.Empty(t, rewardsAfter, "ob_rewards table should be empty after distribution")

		t.Logf("✅ Single LP correctly received 100%% of fees")

		return nil
	}
}
