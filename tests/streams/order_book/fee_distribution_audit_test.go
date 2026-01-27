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
		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
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
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Fund vault and call distribute_fees
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)) // 10 TRUF
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees)
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
		var totalFeesStr string
		var lpCount int
		var blockCount int
		var summaryRowCount int

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				distributionID = int(row.Values[0].(int64))
				// total_fees_distributed is NUMERIC(78, 0) which comes as *types.Decimal
				totalFeesDecimal := row.Values[1].(*kwilTypes.Decimal)
				totalFeesStr = totalFeesDecimal.String()
				lpCount = int(row.Values[2].(int64))
				blockCount = int(row.Values[3].(int64))
				summaryRowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, summaryRowCount, "Should have 1 distribution summary record")

		t.Logf("✅ Audit summary: distribution_id=%d, total_fees=%s, lp_count=%d, block_count=%d",
			distributionID, totalFeesStr, lpCount, blockCount)

		require.Equal(t, 2, lpCount, "LP count should be 2")
		require.Equal(t, 1, blockCount, "Block count should be 1")

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

		// Verify zero-loss: SUM(reward_amount) = total_fees_distributed
		totalFeesFromAudit, _ := new(big.Int).SetString(totalFeesStr, 10)
		var totalDistributed big.Int
		for _, detail := range detailRows {
			amt, _ := new(big.Int).SetString(detail.rewardAmount, 10)
			totalDistributed.Add(&totalDistributed, amt)
		}

		require.Equal(t, totalFeesFromAudit.String(), totalDistributed.String(),
			"Zero-loss audit: SUM(reward_amount) should equal total_fees_distributed")

		t.Logf("✅ Audit record creation verified: %s wei distributed across %d LPs", totalFeesFromAudit.String(), lpCount)

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

		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
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
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 2000, nil)
		require.NoError(t, err)
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 3000, nil)
		require.NoError(t, err)

		// Distribute fees
		totalFees := new(big.Int).Mul(big.NewInt(30), big.NewInt(1e18))
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees)
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
				blockCount = int(row.Values[3].(int64))
				rowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 1, rowCount, "Should have 1 distribution summary record")
		require.Equal(t, 3, blockCount, "Block count should be 3 (3 samples)")

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

		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
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

		// Don't sample LP rewards (no LP samples)
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees)
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

		// Query distribution summary - should have 0 rows
		var rowCount int
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				rowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 0, rowCount, "Should have 0 distribution records (no LPs)")

		t.Logf("✅ No audit record created when no LPs (fees stayed in vault)")

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

		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
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
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), zeroFees)
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

		// Query distribution summary - should have 0 rows
		var rowCount int
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				rowCount++
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, 0, rowCount, "Should have 0 distribution records (zero fees)")

		t.Logf("✅ No audit record created when zero fees collected")

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

		err = giveBalanceChained(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = giveBalanceChained(ctx, platform, user2.Address(), "500000000000000000000")
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
		err = callSampleLPRewards(ctx, platform, &user1, int(marketID), 1000, nil)
		require.NoError(t, err)

		// Distribute
		totalFees := new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))
		err = fundVaultAndDistributeFees(t, ctx, platform, &user1, int(marketID), totalFees)
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
		var totalFeesStr string
		var summaryRowCount int

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_distribution_summary", []any{int(marketID)},
			func(row *common.Row) error {
				distributionID = int(row.Values[0].(int64))
				// total_fees_distributed is NUMERIC(78, 0) which comes as *types.Decimal
				totalFeesDecimal := row.Values[1].(*kwilTypes.Decimal)
				totalFeesStr = totalFeesDecimal.String()
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
		require.Equal(t, increase1.String(), auditReward1.String(), "Audit reward1 should match balance increase")
		require.Equal(t, increase2.String(), auditReward2.String(), "Audit reward2 should match balance increase")

		// Verify zero-loss in audit
		totalFeesFromAudit, _ := new(big.Int).SetString(totalFeesStr, 10)
		auditSum := new(big.Int).Add(auditReward1, auditReward2)
		require.Equal(t, totalFeesFromAudit.String(), auditSum.String(), "Audit rewards should sum to total fees")

		t.Logf("✅ Audit data integrity verified:")
		t.Logf("  - Audit User1 reward: %s wei (balance increase: %s)", auditReward1.String(), increase1.String())
		t.Logf("  - Audit User2 reward: %s wei (balance increase: %s)", auditReward2.String(), increase2.String())
		t.Logf("  - Audit total: %s wei (zero-loss verified)", auditSum.String())

		return nil
	}
}

// setupLPScenario creates paired orders for both users to qualify as LPs
func setupLPScenario(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	user1, user2 *util.EthereumAddress, marketID int) {

	err := callPlaceSplitLimitOrder(ctx, platform, user1, marketID, 50, 300)
	require.NoError(t, err)

	err = callPlaceBuyOrder(ctx, platform, user1, marketID, true, 46, 50)
	require.NoError(t, err)
	err = callPlaceSellOrder(ctx, platform, user1, marketID, true, 52, 200)
	require.NoError(t, err)

	err = callPlaceSellOrder(ctx, platform, user1, marketID, true, 48, 100)
	require.NoError(t, err)
	err = callPlaceBuyOrder(ctx, platform, user1, marketID, false, 52, 100)
	require.NoError(t, err)

	err = callPlaceSplitLimitOrder(ctx, platform, user2, marketID, 50, 100)
	require.NoError(t, err)
	err = callPlaceSellOrder(ctx, platform, user2, marketID, true, 49, 100)
	require.NoError(t, err)
	err = callPlaceBuyOrder(ctx, platform, user2, marketID, false, 51, 100)
	require.NoError(t, err)
}

// fundVaultAndDistributeFees funds the vault and calls distribute_fees
func fundVaultAndDistributeFees(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	user *util.EthereumAddress, marketID int, totalFees *big.Int) error {

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

	// Call distribute_fees
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
		},
		Signer:        user.Bytes(),
		Caller:        user.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "distribute_fees", []any{marketID, totalFeesDecimal}, nil)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return fmt.Errorf("distribute_fees error: %v", res.Error)
	}

	return nil
}
