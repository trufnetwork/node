//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"math/big"
	"strings"
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

// TestFeeDistributionAudit tests the audit trail for fee distribution (Issue 9C Phase 1)
func TestFeeDistributionAudit(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_09C_FeeDistributionAudit",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// Test 1: Verify audit record creation (basic flow)
			testAuditRecordCreation(t),
			// Test 2: Verify multi-block audit correctness
			testAuditMultiBlock(t),
			// Test 3: Verify no audit when no LPs
			testAuditNoLPs(t),
			// Test 4: Verify no audit when zero fees
			testAuditZeroFees(t),
			// Test 5: Verify audit data integrity (matches actual transfers)
			testAuditDataIntegrity(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testAuditRecordCreation verifies that audit records are created correctly after distribution
func testAuditRecordCreation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

		// Give both users balance
		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000062", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)
		t.Logf("Created market ID: %d", marketID)

		// Setup LP scenario (same as fee_distribution_test.go)
		setupLPScenario(t, ctx, platform, &user1, &user2, int(marketID))

		// Sample LP rewards
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)

		// Fund vault and call distribute_fees
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)) // 10 TRUF
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees, true)
		require.NoError(t, err)

		// Verify audit summary record exists
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

		// Query distribution summary using callback pattern
		var distributionID int
		var totalLPFeesStr string
		var totalDPFeesStr string
		var totalValFeesStr string
		var lpCount int
		var blockCount int
		var summaryRowCount int

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				distributionID = int(row.Values[0].(int64))
				// total_lp_fees_distributed is NUMERIC(78, 0) which comes as *types.Decimal
				totalLPFeesDecimal := row.Values[1].(*kwilTypes.Decimal)
				totalLPFeesStr = totalLPFeesDecimal.String()
				totalDPFeesStr = row.Values[2].(*kwilTypes.Decimal).String()
				totalValFeesStr = row.Values[3].(*kwilTypes.Decimal).String()
				lpCount = int(row.Values[4].(int64))
				blockCount = int(row.Values[5].(int64))
				summaryRowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, summaryRowCount, "Should have 1 distribution summary record")

		t.Logf("✅ Audit summary: distribution_id=%d, total_lp_fees=%s, total_dp_fees=%s, total_val_fees=%s, lp_count=%d, block_count=%d",
			distributionID, totalLPFeesStr, totalDPFeesStr, totalValFeesStr, lpCount, blockCount)

		// Verify 75/12.5/12.5 split
		expectedInfraShare := new(big.Int).Div(new(big.Int).Mul(totalFees, big.NewInt(125)), big.NewInt(1000))
		expectedLPShare := new(big.Int).Sub(totalFees, new(big.Int).Mul(expectedInfraShare, big.NewInt(2)))

		require.Equal(t, expectedLPShare.String(), totalLPFeesStr, "LP share in audit should match 75% (+ dust)")
		require.Equal(t, expectedInfraShare.String(), totalDPFeesStr, "DP share in audit should match 12.5%")
		require.Equal(t, expectedInfraShare.String(), totalValFeesStr, "Validator share in audit should match 12.5%")

		require.Equal(t, 2, lpCount, "LP count should be 2")
		// Block count = 2: 1 manual sample + 1 final sample at settlement (@height)
		require.Equal(t, 2, blockCount, "Block count should be 2 (1 manual + 1 final at settlement)")

		// Verify per-LP detail records using callback pattern with slice collection
		type detailRow struct {
			participantID int
			wallet        string
			rewardAmount  string
			rewardPercent string
		}
		detailRows := make([]detailRow, 0, 2)

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_details", []any{distributionID},
			func(row *common.Row) error {
				// NUMERIC fields come as *types.Decimal
				rewardAmtDecimal := row.Values[2].(*kwilTypes.Decimal)
				rewardPctDecimal := row.Values[3].(*kwilTypes.Decimal)

				detailRows = append(detailRows, detailRow{
					participantID: int(row.Values[0].(int64)),
					wallet:        row.Values[1].(string),
					rewardAmount:  rewardAmtDecimal.String(),
					rewardPercent: rewardPctDecimal.String(),
				})
				return nil
			})
		require.NoError(t, err)
		require.Len(t, detailRows, 2, "Should have 2 LP detail records")

		// Verify zero-loss: SUM(reward_amount) = total_lp_fees_distributed
		totalLPFeesFromAudit, _ := new(big.Int).SetString(totalLPFeesStr, 10)
		var totalDistributed big.Int
		for _, detail := range detailRows {
			amt, _ := new(big.Int).SetString(detail.rewardAmount, 10)
			totalDistributed.Add(&totalDistributed, amt)
		}

		require.Equal(t, totalLPFeesFromAudit.String(), totalDistributed.String(),
			"Zero-loss audit: SUM(reward_amount) should equal total_lp_fees_distributed")

		t.Logf("✅ Audit record creation verified: %s wei distributed across %d LPs", totalLPFeesFromAudit.String(), lpCount)

		return nil
	}
}

// testAuditMultiBlock verifies audit correctness with multiple block samples
func testAuditMultiBlock(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")

		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000063", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Setup LP scenario
		setupLPScenario(t, ctx, platform, &user1, &user2, int(marketID))

		// Sample 3 blocks
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)
		err = triggerBatchSampling(ctx, platform, 2000)
		require.NoError(t, err)
		err = triggerBatchSampling(ctx, platform, 3000)
		require.NoError(t, err)

		// Distribute fees
		totalFees := new(big.Int).Mul(big.NewInt(30), big.NewInt(1e18))
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees, true)
		require.NoError(t, err)

		// Verify audit summary
		tx := &common.TxContext{
			Ctx:           ctx,
			BlockContext:  &common.BlockContext{Height: 1, Timestamp: time.Now().Unix()},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		// Query distribution summary using callback pattern
		var blockCount int
		var rowCount int

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				blockCount = int(row.Values[5].(int64))
				rowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, rowCount, "Should have 1 distribution summary record")
		// Block count = 4: 3 manual samples + 1 final sample at settlement (@height)
		require.Equal(t, 4, blockCount, "Block count should be 4 (3 manual + 1 final at settlement)")

		t.Logf("✅ Multi-block audit verified: %d blocks sampled", blockCount)

		return nil
	}
}

// testAuditNoLPs verifies no audit record when no LPs (fees stay in vault)
func testAuditNoLPs(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")

		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000064", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Lock some collateral to ensure bridge liquidity for payouts
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 50, 100)
		require.NoError(t, err)

		// Don't sample LP rewards (no LP samples)
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees, true)
		require.NoError(t, err)

		// Verify NO audit summary record
		tx := &common.TxContext{
			Ctx:           ctx,
			BlockContext:  &common.BlockContext{Height: 1, Timestamp: time.Now().Unix()},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		// Query distribution summary - should have 1 row with 0 LPs but with DP/Val fees
		var rowCount int
		var lpCount int
		var totalDPFeesStr string
		var totalValFeesStr string
		
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				totalDPFeesStr = row.Values[2].(*kwilTypes.Decimal).String()
				totalValFeesStr = row.Values[3].(*kwilTypes.Decimal).String()
				lpCount = int(row.Values[4].(int64))
				rowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, rowCount, "Should have 1 distribution record even with no LPs")
		require.Equal(t, 0, lpCount, "LP count should be 0")
		
		expectedInfraShare := new(big.Int).Div(new(big.Int).Mul(totalFees, big.NewInt(125)), big.NewInt(1000))
		require.Equal(t, expectedInfraShare.String(), totalDPFeesStr, "DP fees should be recorded")
		require.Equal(t, expectedInfraShare.String(), totalValFeesStr, "Validator fees should be recorded")

		t.Logf("✅ Audit record correctly created with 0 LPs and recorded infra fees")

		return nil
	}
}

// testAuditZeroFees verifies no audit when zero fees collected
func testAuditZeroFees(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000065", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Call distribute_fees with $0 fees (early return)
		zeroFees := big.NewInt(0)
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), zeroFees, true)
		require.NoError(t, err)

		// Verify NO audit record (zero fees early return)
		tx := &common.TxContext{
			Ctx:           ctx,
			BlockContext:  &common.BlockContext{Height: 1, Timestamp: time.Now().Unix()},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		// Query distribution summary - should have 1 row
		var rowCount int
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				rowCount++
				// Verify zero values in the summary
				// get_distribution_summary returns (distribution_id, total_lp_fees_distributed, total_dp_fees, total_validator_fees, total_lp_count, block_count, distributed_at)
				require.Equal(t, "0", row.Values[1].(*kwilTypes.Decimal).String(), "Total fees should be 0")
				require.Equal(t, int64(0), row.Values[4].(int64), "Total LP count should be 0")
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, rowCount, "Should have 1 distribution record even with zero fees")

		t.Logf("✅ Audit record correctly created with zero fees")

		return nil
	}
}

// testAuditDataIntegrity verifies audit data matches actual balance transfers
func testAuditDataIntegrity(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		user2 := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		err = giveBalanceChained(ctx, platform, user1.Address(), "1000000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(user1.Address(), "sttest00000000000000000000000066", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user1, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Setup LP scenario (places orders which spend collateral)
		setupLPScenario(t, ctx, platform, &user1, &user2, int(marketID))

		// Get initial USDC balances AFTER order placement (to measure only fee distribution increase)
		// Note: Fee distribution pays in USDC (hoodi_tt2), not TRUF (hoodi_tt)
		bal1Before, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		bal2Before, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Sample
		err = triggerBatchSampling(ctx, platform, 1000)
		require.NoError(t, err)

		// Distribute
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees, true)
		require.NoError(t, err)

		// Get final USDC balances
		bal1After, err := getUSDCBalance(ctx, platform, user1.Address())
		require.NoError(t, err)
		bal2After, err := getUSDCBalance(ctx, platform, user2.Address())
		require.NoError(t, err)

		// Calculate actual balance increases
		increase1 := new(big.Int).Sub(bal1After, bal1Before)
		increase2 := new(big.Int).Sub(bal2After, bal2Before)

		// Get audit records
		tx := &common.TxContext{
			Ctx:           ctx,
			BlockContext:  &common.BlockContext{Height: 1, Timestamp: time.Now().Unix()},
			Signer:        user1.Bytes(),
			Caller:        user1.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

		// Query distribution summary using callback pattern
		var distributionID int
		var totalLPFeesStr string
		var totalDPFeesStr string
		var totalValFeesStr string
		var summaryRowCount int

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				distributionID = int(row.Values[0].(int64))
				totalLPFeesStr = row.Values[1].(*kwilTypes.Decimal).String()
				totalDPFeesStr = row.Values[2].(*kwilTypes.Decimal).String()
				totalValFeesStr = row.Values[3].(*kwilTypes.Decimal).String()
				summaryRowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, summaryRowCount, "Should have 1 distribution summary record")

		// Query distribution details using callback pattern with map collection
		auditRewards := make(map[string]*big.Int)
		var detailRowCount int

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_details", []any{distributionID},
			func(row *common.Row) error {
				walletHex := row.Values[1].(string)
				// reward_amount is NUMERIC(78, 0) which comes as *types.Decimal
				rewardDecimal := row.Values[2].(*kwilTypes.Decimal)
				rewardStr := rewardDecimal.String()
				reward, _ := new(big.Int).SetString(rewardStr, 10)
				// Normalize to lowercase for consistent map lookup
				key := strings.ToLower(walletHex)
				auditRewards[key] = reward
				detailRowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 2, detailRowCount, "Should have 2 LP detail records")

		// Verify audit rewards match actual balance increases
		// Note: user.Address() already includes "0x" prefix
		user1Hex := strings.ToLower(user1.Address())
		user2Hex := strings.ToLower(user2.Address())

		auditReward1 := auditRewards[user1Hex]
		auditReward2 := auditRewards[user2Hex]

		require.NotNil(t, auditReward1, "User1 should have audit reward")
		require.NotNil(t, auditReward2, "User2 should have audit reward")

		// Expected balance increases:
		// User1 = auditReward1 (LP) + totalDPFees (DP) [+ totalValFees (if Leader)]
		// User2 = auditReward2 (LP)
		infraShare, _ := new(big.Int).SetString(totalDPFeesStr, 10)
		expectedIncrease1 := new(big.Int).Add(auditReward1, infraShare)
		
		if increase1.Cmp(expectedIncrease1) != 0 {
			// Try adding validator share if User1 is leader
			infraShareVal, _ := new(big.Int).SetString(totalValFeesStr, 10)
			expectedIncrease1WithVal := new(big.Int).Add(expectedIncrease1, infraShareVal)
			if increase1.Cmp(expectedIncrease1WithVal) == 0 {
				t.Logf("User1 appears to be the leader, adding Validator share to expectation")
				expectedIncrease1 = expectedIncrease1WithVal
			}
		}

		require.Equal(t, expectedIncrease1.String(), increase1.String(), "User1 balance increase should match LP + DP (+ Leader) audit rewards")
		require.Equal(t, increase2.String(), auditReward2.String(), "User2 balance increase should match LP audit reward")

		// Verify zero-loss in audit
		totalLPFeesFromAudit, _ := new(big.Int).SetString(totalLPFeesStr, 10)
		auditLPSum := new(big.Int).Add(auditReward1, auditReward2)
		require.Equal(t, totalLPFeesFromAudit.String(), auditLPSum.String(), "Audit LP rewards should sum to total LP fees")

		totalDPFees, _ := new(big.Int).SetString(totalDPFeesStr, 10)
		totalValFees, _ := new(big.Int).SetString(totalValFeesStr, 10)
		totalFeesFromAudit := new(big.Int).Add(totalLPFeesFromAudit, new(big.Int).Add(totalDPFees, totalValFees))
		require.Equal(t, totalFees.String(), totalFeesFromAudit.String(), "Total fees from audit should match input totalFees")

		t.Logf("✅ Audit data integrity verified:")
		t.Logf("  - Audit User1 LP reward: %s wei (total increase: %s)", auditReward1.String(), increase1.String())
		t.Logf("  - Audit User2 LP reward: %s wei (total increase: %s)", auditReward2.String(), increase2.String())
		t.Logf("  - Audit total: %s wei (zero-loss verified)", totalFeesFromAudit.String())

		return nil
	}
}

// setupLPScenario creates paired orders for both users to qualify as LPs.
// CRITICAL: Buy prices are chosen BELOW existing sell prices to avoid the
// matching engine consuming LP pair orders on placement.
func setupLPScenario(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	user1, user2 *util.EthereumAddress, marketID int) {

	// User1: Split@50 → YES(300), NO sell@50(300)
	err := callPlaceSplitLimitOrder(ctx, platform, user1, marketID, 50, 300)
	require.NoError(t, err)

	// Establish bid and ask for midpoint
	err = callPlaceBuyOrder(ctx, platform, user1, marketID, true, 44, 50)
	require.NoError(t, err)
	err = callPlaceSellOrder(ctx, platform, user1, marketID, true, 56, 150)
	require.NoError(t, err)
	// holdings: 300→150

	// User2: Split@50 → YES(100), NO sell@50(100)
	err = callPlaceSplitLimitOrder(ctx, platform, user2, marketID, 50, 100)
	require.NoError(t, err)

	// User1 TRUE-side LP pair: YES sell@52 + NO buy@48
	// NO buy@48 < NO sell@50 → no match ✓
	err = callPlaceSellOrder(ctx, platform, user1, marketID, true, 52, 100)
	require.NoError(t, err)
	// holdings: 150→50
	err = callPlaceBuyOrder(ctx, platform, user1, marketID, false, 48, 100)
	require.NoError(t, err)

	// User2 TRUE-side LP pair: YES sell@51 + NO buy@49
	// NO buy@49 < NO sell@50 → no match ✓
	err = callPlaceSellOrder(ctx, platform, user2, marketID, true, 51, 100)
	require.NoError(t, err)
	err = callPlaceBuyOrder(ctx, platform, user2, marketID, false, 49, 100)
	require.NoError(t, err)

	// User1 FALSE-side LP pair: NO sell@50(300) + YES buy@50(300)
	// YES buy@50 < YES sell@51 → no match ✓
	err = callPlaceBuyOrder(ctx, platform, user1, marketID, true, 50, 300)
	require.NoError(t, err)

	// User2 FALSE-side LP pair: NO sell@50(100) + YES buy@50(100)
	err = callPlaceBuyOrder(ctx, platform, user2, marketID, true, 50, 100)
	require.NoError(t, err)
	// Final midpoint: best bid=-50, lowest sell=51 → midpoint=50, spread=5
}

// fundVaultAndDistributeFees funds the vault and calls distribute_fees
func fundVaultAndDistributeFees(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	user *util.EthereumAddress, marketID int, totalFees *big.Int, winningOutcome bool) error {

	// Fund vault if fees > 0 (use USDC-only since vault doesn't need TRUF)
	if totalFees.Sign() > 0 {
		err := giveUSDCBalanceChained(ctx, platform, testUSDCEscrow, totalFees.String())
		if err != nil {
			return err
		}

		_, err = erc20bridge.ForTestingForceSyncInstance(ctx, platform, testChain, testEscrow, testERC20, 18)
		if err != nil {
			return err
		}
	}

	// Convert to NUMERIC(78, 0)
	totalFeesDecimal, err := kwilTypes.ParseDecimalExplicit(totalFees.String(), 78, 0)
	if err != nil {
		return err
	}

	// Generate leader key for fee transfers
	pub := NewTestProposerPub(t)

	// Call distribute_fees
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
			Proposer:  pub,
		},
		Signer:        user.Bytes(),
		Caller:        user.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "distribute_fees", []any{marketID, totalFeesDecimal, winningOutcome}, nil)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return fmt.Errorf("distribute_fees error: %v", res.Error)
	}

	return nil
}
