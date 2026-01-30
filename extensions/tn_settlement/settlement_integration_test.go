//go:build kwiltest

package tn_settlement

import (
	"context"
	"fmt"
	"testing"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/accounts"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/extensions/tn_settlement/internal"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// NO_OUTCOME_VALUE represents a NO outcome in settlement tests
const NO_OUTCOME_VALUE = "-1.000000000000000000"

// Test constants - match existing erc20-bridge configuration
const (
	testExtensionName = "sepolia_bridge"

	// TRUF bridge constants for market creation fee
	testTRUFChain  = "hoodi"
	testTRUFEscrow = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testTRUFERC20  = "0x263ce78fef26600e4e428cebc91c2a52484b4fbf"
)

// Point counter for TRUF balance injection
var (
	trufPointCounter     int64 = 500
	lastTrufBalancePoint *int64
)

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
		// Reset point counter for this test
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give TRUF balance for market creation fee
		err = giveTrufBalance(ctx, platform, deployer.Address(), "10000000000000000000") // 10 TRUF
		require.NoError(t, err)

		// Create stream and attestation - returns query_components (ABI-encoded)
		streamID := "stfindmarket00000000000000000000"
		queryComponents := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market with settle_time in the past (relative to database time)
		// settleTime=100 is in the past (Jan 1970), BlockContext.Timestamp=50 for creation
		currentTime := int64(50)
		settleTime := int64(100) // Must be > currentTime for creation, but < database time for query
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = currentTime

		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_market failed: %v", createRes.Error)
		}
		require.Greater(t, queryID, 0)

		// Get the hash from the database (computed by create_market)
		var marketHash []byte
		engineCtx = helper.NewEngineContext()
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT hash FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				marketHash = row.Values[0].([]byte)
				return nil
			})
		require.NoError(t, err)

		// Test FindUnsettledMarkets
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

		markets, err := ops.FindUnsettledMarkets(ctx, 10)
		require.NoError(t, err)
		require.Len(t, markets, 1, "should find 1 unsettled market")
		require.Equal(t, queryID, markets[0].ID)
		require.Equal(t, marketHash, markets[0].Hash)

		t.Logf("✅ FindUnsettledMarkets found market queryID=%d with hash=%x", queryID, marketHash)
		return nil
	}
}

// =============================================================================
// Test: AttestationExists
// =============================================================================

func testAttestationExists(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point counter for this test
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give TRUF balance for market creation fee
		err = giveTrufBalance(ctx, platform, deployer.Address(), "10000000000000000000") // 10 TRUF
		require.NoError(t, err)

		streamID := "stattexists000000000000000000000"
		queryComponents := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market to get the computed hash
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_market failed: %v", createRes.Error)
		}

		// Get the hash from the database
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT hash FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				attestationHash = row.Values[0].([]byte)
				return nil
			})
		require.NoError(t, err)

		// Test AttestationExists
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

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
		// Reset point counter for this test
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give TRUF balance for market creation fee
		err = giveTrufBalance(ctx, platform, deployer.Address(), "10000000000000000000") // 10 TRUF
		require.NoError(t, err)

		streamID := "stsettleaction000000000000000000"
		queryComponents := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market (settleTime in future relative to BlockContext)
		creationTime := int64(50)
		settleTime := int64(100)
		settleCheckTime := int64(200) // After settleTime for settlement
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = creationTime

		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
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

		// Test LoadSettlementConfig (table exists from migration with seeded defaults)
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

		enabled, schedule, maxMarkets, retries, err := ops.LoadSettlementConfig(ctx)
		require.NoError(t, err)
		require.True(t, enabled, "should be true (enabled by migration 041)")
		require.Equal(t, "0,30 * * * *", schedule, "should be 30-minute schedule from migration 041")
		require.Equal(t, 10, maxMarkets)
		require.Equal(t, 3, retries)

		t.Logf("✅ LoadSettlementConfig loaded config from migration: enabled=%v, schedule=%s, max=%d, retries=%d",
			enabled, schedule, maxMarkets, retries)
		return nil
	}
}

// =============================================================================
// Test: Skip Market Without Attestation
// =============================================================================

func testSkipMarketWithoutAttestation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point counter for this test
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give TRUF balance for market creation fee
		err = giveTrufBalance(ctx, platform, deployer.Address(), "10000000000000000000") // 10 TRUF
		require.NoError(t, err)

		// Create stream and attestation WITHOUT signing (skip the SignAttestation step)
		streamID := "stskipnoatt000000000000000000000"
		queryComponents := createStreamWithoutSigningAttestation(t, ctx, platform, helper, deployer, streamID, "1.000000000000000000")

		// Create market (settleTime in future relative to BlockContext)
		creationTime := int64(50)
		settleTime := int64(100)
		settleCheckTime := int64(200)
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = creationTime

		var queryID int
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_market failed: %v", createRes.Error)
		}

		// Get the hash from the database
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT hash FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				attestationHash = row.Values[0].([]byte)
				return nil
			})
		require.NoError(t, err)

		// Test AttestationExists should return false
		accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
		require.NoError(t, err)
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

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
		// Reset point counter for this test
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Initialize ERC20 extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give TRUF balance for market creation fees (need 3 markets × 2 TRUF = 6 TRUF minimum)
		err = giveTrufBalance(ctx, platform, deployer.Address(), "20000000000000000000") // 20 TRUF
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

			queryComponents := createStreamAndAttestation(t, ctx, platform, helper, deployer, streamID, value)

			engineCtx := helper.NewEngineContext()
			engineCtx.TxContext.BlockContext.Timestamp = creationTime

			var queryID int
			createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
				[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
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
		ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

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

// giveTrufBalance gives TRUF balance to a user for market creation fee
func giveTrufBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	trufPointCounter++
	currentPoint := trufPointCounter

	err := testerc20.InjectERC20Transfer(
		ctx, platform,
		testTRUFChain, testTRUFEscrow, testTRUFERC20,
		wallet, wallet, amountStr,
		currentPoint, lastTrufBalancePoint,
	)
	if err != nil {
		return fmt.Errorf("failed to inject TRUF: %w", err)
	}

	p := currentPoint
	lastTrufBalancePoint = &p
	return nil
}

// encodeQueryComponents encodes query components as ABI (address,bytes32,string,bytes)
func encodeQueryComponents(dataProvider, streamID, actionID string, argsBytes []byte) ([]byte, error) {
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create address type: %w", err)
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes32 type: %w", err)
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create string type: %w", err)
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes type: %w", err)
	}

	args := gethAbi.Arguments{
		{Type: addressType},
		{Type: bytes32Type},
		{Type: stringType},
		{Type: bytesType},
	}

	// Convert data_provider string to address
	addr := gethCommon.HexToAddress(dataProvider)

	// Convert stream_id to bytes32 (pad to 32 bytes)
	var streamIDBytes32 [32]byte
	copy(streamIDBytes32[:], []byte(streamID))

	return args.Pack(addr, streamIDBytes32, actionID, argsBytes)
}

// createStreamWithoutSigningAttestation creates a stream and unsigned attestation (for testing skip logic)
// Returns query_components (ABI-encoded) for use with create_market
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

	// Encode action args for get_record
	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		dataProvider, streamID, int64(500), int64(1500), nil, false,
	})
	require.NoError(t, err)

	// Request attestation (but don't sign)
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
		[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
		func(row *common.Row) error {
			return nil
		})
	require.NoError(t, err)
	if res.Error != nil {
		t.Fatalf("request_attestation failed: %v", res.Error)
	}

	// NOTE: Intentionally NOT calling helper.SignAttestation() to test unsigned attestation

	// Return ABI-encoded query_components for create_market
	queryComponents, err := encodeQueryComponents(dataProvider, streamID, "get_record", argsBytes)
	require.NoError(t, err)

	return queryComponents
}

// createStreamAndAttestation creates a stream, inserts data, and returns ABI-encoded query_components
// for use with create_market (the hash is computed by create_market from query_components)
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

	// Encode action args for get_record - MUST match what we use in request_attestation
	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		dataProvider, streamID, int64(500), int64(1500), nil, false,
	})
	require.NoError(t, err)

	// Request attestation using the SAME engineCtx so it sees the inserted data
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

	// Return ABI-encoded query_components for create_market
	// The hash is computed by create_market from these query_components
	queryComponents, err := encodeQueryComponents(dataProvider, streamID, "get_record", argsBytes)
	require.NoError(t, err)

	return queryComponents
}
