//go:build kwiltest

package tn_settlement

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/accounts"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"

	"github.com/trufnetwork/node/extensions/tn_settlement/internal"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// NO_OUTCOME_VALUE represents a NO outcome in settlement tests
const NO_OUTCOME_VALUE = "-1.000000000000000000"

// TestSettlementIntegration tests the automatic settlement functionality
func TestSettlementIntegration(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "TN_SETTLEMENT_Integration",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testFindUnsettledMarkets(t),
			testAttestationExists(t),
			testSettleMarketViaAction(t),
			testLoadSettlementConfig(t),
			testSkipMarketWithoutAttestation(t),
			testMultipleMarketsProcessing(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Test: FindUnsettledMarkets
// =============================================================================

func testFindUnsettledMarkets(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create stream and attestation
		streamID := "stfindmarket00000000000000000000"
		attestationHash := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market with settle_time in the past (relative to database time)
		// settleTime=100 is in the past (Jan 1970), BlockContext.Timestamp=50 for creation
		currentTime := int64(50)
		settleTime := int64(100) // Must be > currentTime for creation, but < database time for query
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = currentTime

		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, settleTime, int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_market failed: %v", createRes.Error)
		}
		require.Greater(t, queryID, 0)

		// Test FindUnsettledMarkets
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, accts, log.New())

		markets, err := ops.FindUnsettledMarkets(ctx, 10)
		require.NoError(t, err)
		require.Len(t, markets, 1, "should find 1 unsettled market")
		require.Equal(t, queryID, markets[0].ID)
		require.Equal(t, attestationHash, markets[0].Hash)

		t.Logf("✅ FindUnsettledMarkets found market queryID=%d with hash=%x", queryID, attestationHash)
		return nil
	}
}

// =============================================================================
// Test: AttestationExists
// =============================================================================

func testAttestationExists(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		streamID := "stattexists000000000000000000000"
		attestationHash := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Test AttestationExists
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, accts, log.New())

		exists, err := ops.AttestationExists(ctx, attestationHash)
		require.NoError(t, err)
		require.True(t, exists, "attestation should exist and be signed")

		// Test with non-existent hash
		fakeHash := make([]byte, 32)
		copy(fakeHash, []byte("nonexistent"))
		exists, err = ops.AttestationExists(ctx, fakeHash)
		require.NoError(t, err)
		require.False(t, exists, "fake attestation should not exist")

		t.Logf("✅ AttestationExists correctly validates signed attestations")
		return nil
	}
}

// =============================================================================
// Test: Settle Market Via Action
// =============================================================================

func testSettleMarketViaAction(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		streamID := "stsettleaction000000000000000000"
		attestationHash := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market (settleTime in future relative to BlockContext)
		creationTime := int64(50)
		settleTime := int64(100)
		settleCheckTime := int64(200) // After settleTime for settlement
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = creationTime

		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, settleTime, int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_market failed: %v", createRes.Error)
		}

		// Settle using settle_market action (after settleTime)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = settleCheckTime
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.Nil(t, settleRes.Error)

		// Verify settlement
		engineCtx = helper.NewEngineContext()
		var settled bool
		var winningOutcome *bool
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT settled, winning_outcome FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				settled = row.Values[0].(bool)
				if row.Values[1] != nil {
					outcome := row.Values[1].(bool)
					winningOutcome = &outcome
				}
				return nil
			})
		require.NoError(t, err)
		require.True(t, settled)
		require.NotNil(t, winningOutcome)
		require.True(t, *winningOutcome)

		t.Logf("✅ Market settled successfully via action, queryID=%d, outcome=YES", queryID)
		return nil
	}
}

// =============================================================================
// Test: LoadSettlementConfig
// =============================================================================

func testLoadSettlementConfig(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")
		platform.Deployer = deployer.Bytes()

		// Test LoadSettlementConfig with missing table (should return defaults)
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, accts, log.New())

		enabled, schedule, maxMarkets, retries, err := ops.LoadSettlementConfig(ctx)
		require.NoError(t, err)
		require.False(t, enabled, "should return false (default) when table doesn't exist")
		require.Equal(t, "", schedule)
		require.Equal(t, 10, maxMarkets)
		require.Equal(t, 3, retries)

		t.Logf("✅ LoadSettlementConfig with missing table returned defaults: enabled=%v, schedule=%s, max=%d, retries=%d",
			enabled, schedule, maxMarkets, retries)
		return nil
	}
}

// =============================================================================
// Test: Skip Market Without Attestation
// =============================================================================

func testSkipMarketWithoutAttestation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create stream and attestation WITHOUT signing (skip the SignAttestation step)
		streamID := "stskipnoatt000000000000000000000"
		attestationHash := createStreamWithoutSigningAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market (settleTime in future relative to BlockContext)
		creationTime := int64(50)
		settleTime := int64(100)
		settleCheckTime := int64(200)
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = creationTime

		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, settleTime, int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_market failed: %v", createRes.Error)
		}

		// Test AttestationExists should return false
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, accts, log.New())

		exists, err := ops.AttestationExists(ctx, attestationHash)
		require.NoError(t, err)
		require.False(t, exists, "attestation without signature should not be considered ready")

		// FindUnsettledMarkets should still find the market
		markets, err := ops.FindUnsettledMarkets(ctx, 10)
		require.NoError(t, err)
		require.Len(t, markets, 1)

		// But settlement should fail (even after settleTime)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = settleCheckTime
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.NotNil(t, settleRes.Error, "settle_market should fail without signed attestation")

		t.Logf("✅ Market without signed attestation correctly skipped: %v", settleRes.Error)
		return nil
	}
}

// =============================================================================
// Test: Multiple Markets Processing
// =============================================================================

func testMultipleMarketsProcessing(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create 3 markets (settleTime in future relative to BlockContext)
		creationTime := int64(50)
		settleTime := int64(100)
		settleCheckTime := int64(200)

		var queryIDs []int
		for i := 0; i < 3; i++ {
			// stmulti(7) + %02d(2) + 23 zeros = 32 total
			streamID := fmt.Sprintf("stmulti%02d00000000000000000000000", i)
			value := "1.000000000000000000"
			if i == 1 {
				value = NO_OUTCOME_VALUE // NO outcome for second market
			}

			attestationHash := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, value)

			engineCtx := helper.NewEngineContext()
			engineCtx.TxContext.BlockContext.Timestamp = creationTime

			var queryID int
			createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
				[]any{attestationHash, settleTime, int64(5), int64(1)},
				func(row *common.Row) error {
					queryID = int(row.Values[0].(int64))
					return nil
				})
			require.NoError(t, err)
			if createRes.Error != nil {
				t.Fatalf("create_market %d failed: %v", i, createRes.Error)
			}
			queryIDs = append(queryIDs, queryID)
		}

		// Test FindUnsettledMarkets with limit
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, accts, log.New())

		markets, err := ops.FindUnsettledMarkets(ctx, 2)
		require.NoError(t, err)
		require.Len(t, markets, 2, "should find 2 markets with limit=2")

		markets, err = ops.FindUnsettledMarkets(ctx, 10)
		require.NoError(t, err)
		require.Len(t, markets, 3, "should find all 3 markets with limit=10")

		// Settle all markets (after settleTime)
		for _, queryID := range queryIDs {
			engineCtx := helper.NewEngineContext()
			engineCtx.TxContext.BlockContext.Timestamp = settleCheckTime
			settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
				[]any{queryID}, nil)
			require.NoError(t, err)
			require.Nil(t, settleRes.Error)
		}

		// Verify all settled
		markets, err = ops.FindUnsettledMarkets(ctx, 10)
		require.NoError(t, err)
		require.Len(t, markets, 0, "all markets should be settled")

		t.Logf("✅ Multiple markets processing completed: settled %d markets", len(queryIDs))
		return nil
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

// createStreamWithoutSigningAttestation creates a stream and unsigned attestation (for testing skip logic)
func createStreamWithoutSigningAttestation(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	helper *attestationTests.AttestationTestHelper,
	deployer util.EthereumAddress,
	streamID string,
	valueStr string,
) []byte {
	dataProvider := deployer.Address()
	engineCtx := helper.NewEngineContext()

	// Create stream
	_, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
		[]any{streamID, "primitive"}, nil)
	require.NoError(t, err)

	// Insert data
	valueDecimal, err := kwilTypes.ParseDecimalExplicit(valueStr, 36, 18)
	require.NoError(t, err)

	_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
		[]any{
			[]string{dataProvider},
			[]string{streamID},
			[]int64{int64(1000)},
			[]*kwilTypes.Decimal{valueDecimal},
		}, nil)
	require.NoError(t, err)

	// Request attestation
	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		dataProvider, streamID, int64(500), int64(1500), nil, false,
	})
	require.NoError(t, err)

	var attestationHash []byte
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
		[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
		func(row *common.Row) error {
			attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
			return nil
		})
	require.NoError(t, err)
	if res.Error != nil {
		t.Fatalf("request_attestation failed: %v", res.Error)
	}

	// NOTE: Intentionally NOT calling helper.SignAttestation() to test unsigned attestation

	return attestationHash
}

// createStreamAndAttestation creates a stream, inserts data, and returns signed attestation hash
func createStreamAndAttestation(
	t *testing.T,
	ctx context.Context,
	platform *kwilTesting.Platform,
	helper *attestationTests.AttestationTestHelper,
	deployer util.EthereumAddress,
	streamID string,
	valueStr string,
) []byte {
	dataProvider := deployer.Address()

	// CRITICAL: Use the SAME engineCtx for all setup operations to ensure data visibility
	engineCtx := helper.NewEngineContext()

	// Create stream
	_, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
		[]any{streamID, "primitive"}, nil)
	require.NoError(t, err)

	// Insert data using the SAME engineCtx
	valueDecimal, err := kwilTypes.ParseDecimalExplicit(valueStr, 36, 18)
	require.NoError(t, err)

	_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
		[]any{
			[]string{dataProvider},
			[]string{streamID},
			[]int64{int64(1000)},
			[]*kwilTypes.Decimal{valueDecimal},
		}, nil)
	require.NoError(t, err)

	// Request attestation using the SAME engineCtx so it sees the inserted data
	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		dataProvider, streamID, int64(500), int64(1500), nil, false,
	})
	require.NoError(t, err)

	var requestTxID string
	var attestationHash []byte
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
		[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
		func(row *common.Row) error {
			requestTxID = row.Values[0].(string)
			attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
			return nil
		})
	require.NoError(t, err)
	if res.Error != nil {
		t.Logf("request_attestation error: %v", res.Error)
		t.Fatalf("request_attestation failed: %v", res.Error)
	}
	t.Logf("Created attestation: txID=%s, hash=%x", requestTxID, attestationHash)

	// Sign attestation
	helper.SignAttestation(requestTxID)

	return attestationHash
}
