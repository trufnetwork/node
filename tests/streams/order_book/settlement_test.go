//go:build kwiltest

package order_book

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// NO_OUTCOME_VALUE represents a NO outcome in settlement tests.
// We use -1.0 instead of 0.0 because insert_records filters WHERE value != 0,
// which prevents inserting exact 0.0 values. The parse_attestation_boolean
// precompile uses value.Sign() > 0 to determine outcome, so negative values
// result in FALSE (NO outcome) while positive values result in TRUE (YES outcome).
const NO_OUTCOME_VALUE = "-1.000000000000000000"

func TestSettlement(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_07_Settlement",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			// Happy path tests
			testSettleMarketHappyPath(t),
			testSettleMarketWithNoOutcome(t),
			testSettleMarketWithMultipleDatapoints(t),

			// Error tests
			testSettleMarketInvalidQueryID(t),
			testSettleMarketAlreadySettled(t),
			testSettleMarketTooEarly(t),
			testSettleMarketNoAttestation(t),
			testSettleMarketAttestationNotSigned(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testSettleMarketHappyPath(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a valid Ethereum address as deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		platform.Deployer = deployer.Bytes()

		// Setup attestation helper (handles ERC20 initialization)
		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		// Create data provider
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Use simple stream ID (exactly 32 characters)
		streamID := "stsettlementtest0000000000000000"
		dataProvider := deployer.Address()

		// CRITICAL: create_stream + insert_records + get_record must share the SAME
		// engine context to ensure inserted data is visible within the transaction.
		// Subsequent operations (request_attestation, create_market, settle_market)
		// intentionally use NEW contexts to simulate separate transactions.
		engineCtx := helper.NewEngineContext()

		// Create primitive stream
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"},
			nil)
		require.NoError(t, err)

		// Insert outcome data directly using the SAME engineCtx
		// This ensures the data is in the same transaction context
		eventTime := int64(1000)

		// Create decimal value (1.0 = YES outcome)
		valueStr := "1.000000000000000000" // 1.0 with 18 decimal places
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(valueStr, 36, 18)
		require.NoError(t, err)

		// Insert using the same engineCtx so data is visible to subsequent calls
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{eventTime},
				[]*kwilTypes.Decimal{valueDecimal},
			},
			nil)
		require.NoError(t, err)

		// Verify data was inserted by querying directly (reuse same context)
		var foundData bool
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_record",
			[]any{
				dataProvider,
				streamID,
				int64(500),
				int64(1500),
				nil,
				false,
			},
			func(row *common.Row) error {
				t.Logf("Found data: event_time=%v, value=%v", row.Values[0], row.Values[1])
				foundData = true
				return nil
			})
		require.NoError(t, err)
		require.True(t, foundData, "Data should be found in stream")

		// Request attestation for get_record
		// Args: data_provider, stream_id, from, to, frozen_at, use_cache
		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			dataProvider,
			streamID,
			int64(500),  // from (before our eventTime=1000)
			int64(1500), // to (after our eventTime=1000)
			nil,         // frozen_at (NULL = latest)
			false,       // use_cache
		})
		require.NoError(t, err)

		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{
				dataProvider,
				streamID,
				"get_record",
				argsBytes,
				false, // encrypt_sig
				nil,   // max_fee
			},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			t.Logf("request_attestation error: %v", res.Error)
			require.NoError(t, res.Error, "request_attestation failed")
		}
		require.NotEmpty(t, requestTxID)
		require.NotEmpty(t, attestationHash)
		t.Logf("Created attestation: txID=%s, hash=%x", requestTxID, attestationHash)

		// Sign the attestation
		helper.SignAttestation(requestTxID)

		// Create market using the attestation hash
		settleTime := int64(100) // Future timestamp
		maxSpread := int64(5)
		minOrderSize := int64(1)
		var queryID int

		// Use timestamp 50 for market creation (before settleTime)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, settleTime, maxSpread, minOrderSize},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Logf("create_market error: %v", createRes.Error)
			require.NoError(t, createRes.Error)
		}
		require.Greater(t, queryID, 0, "queryID should be positive")

		// Settle the market with timestamp 200 (after settleTime)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID},
			nil)
		require.NoError(t, err)
		if settleRes.Error != nil {
			t.Logf("settle_market error: %v", settleRes.Error)
		}
		require.Nil(t, settleRes.Error, "settle_market should succeed")

		// Verify market is settled
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
		require.True(t, settled, "market should be marked as settled")
		require.NotNil(t, winningOutcome, "winning_outcome should be set")
		require.True(t, *winningOutcome, "outcome should be TRUE (YES) since value was 1.0")

		return nil
	}
}

// =============================================================================
// Test: NO Outcome (value = 0.0)
// =============================================================================

func testSettleMarketWithNoOutcome(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		streamID := "stnooutcome000000000000000000000"
		dataProvider := deployer.Address()
		engineCtx := helper.NewEngineContext()

		// Create stream and insert NO outcome data (0.0)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)
		t.Logf("NO outcome - Created stream: %s", streamID)

		// NOTE: Using NO_OUTCOME_VALUE constant which represents -1.0
		// (parse_attestation_boolean uses: value.Sign() > 0, so -1.0 results in FALSE/NO)
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(NO_OUTCOME_VALUE, 36, 18)
		require.NoError(t, err)
		t.Logf("NO outcome - Parsed decimal value (using NO_OUTCOME_VALUE): %v", valueDecimal)

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{int64(1000)},
				[]*kwilTypes.Decimal{valueDecimal},
			}, nil)
		require.NoError(t, err)
		t.Logf("NO outcome - Inserted record with value=-1.0")

		// Verify data was inserted
		var foundData bool
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "get_record",
			[]any{dataProvider, streamID, int64(500), int64(1500), nil, false},
			func(row *common.Row) error {
				t.Logf("NO outcome - Found data: event_time=%v, value=%v", row.Values[0], row.Values[1])
				foundData = true
				return nil
			})
		require.NoError(t, err)
		require.True(t, foundData, "Data should be found in stream")

		// Request and sign attestation
		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			dataProvider, streamID, int64(500), int64(1500), nil, false,
		})
		require.NoError(t, err)

		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		helper.SignAttestation(requestTxID)

		// Create and settle market
		var queryID int
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)

		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.Nil(t, settleRes.Error)

		// Verify NO outcome (FALSE)
		engineCtx = helper.NewEngineContext()
		var winningOutcome *bool
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT winning_outcome FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				if row.Values[0] != nil {
					outcome := row.Values[0].(bool)
					winningOutcome = &outcome
				}
				return nil
			})
		require.NoError(t, err)
		require.NotNil(t, winningOutcome, "winning_outcome should be set")
		require.False(t, *winningOutcome, "outcome should be FALSE (NO) since value was -1.0 (negative)")

		return nil
	}
}

// =============================================================================
// Test: Multiple Datapoints (uses latest value)
// =============================================================================

func testSettleMarketWithMultipleDatapoints(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		streamID := "stmultiple0000000000000000000000"
		dataProvider := deployer.Address()
		engineCtx := helper.NewEngineContext()

		// Create stream
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		// Insert multiple datapoints: -1.0 (NO), 0.5 (uncertain), 1.0 (YES)
		// Test that parse_attestation_boolean uses the LATEST value (1.0)
		// NOTE: Using NO_OUTCOME_VALUE constant for the NO outcome
		valueNeg1, _ := kwilTypes.ParseDecimalExplicit(NO_OUTCOME_VALUE, 36, 18)
		value05, _ := kwilTypes.ParseDecimalExplicit("0.500000000000000000", 36, 18)
		value1, _ := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider, dataProvider, dataProvider},
				[]string{streamID, streamID, streamID},
				[]int64{int64(1000), int64(2000), int64(3000)},
				[]*kwilTypes.Decimal{valueNeg1, value05, value1},
			}, nil)
		require.NoError(t, err)
		t.Logf("Multiple datapoints - Inserted 3 values: -1.0 (NO), 0.5, 1.0 (YES)")

		// Request attestation for range containing all datapoints
		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			dataProvider, streamID, int64(500), int64(3500), nil, false,
		})
		require.NoError(t, err)

		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		helper.SignAttestation(requestTxID)

		// Create and settle market
		var queryID int
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)

		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.Nil(t, settleRes.Error)

		// Verify outcome is TRUE (uses latest value = 1.0)
		engineCtx = helper.NewEngineContext()
		var winningOutcome *bool
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT winning_outcome FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				if row.Values[0] != nil {
					outcome := row.Values[0].(bool)
					winningOutcome = &outcome
				}
				return nil
			})
		require.NoError(t, err)
		require.NotNil(t, winningOutcome)
		require.True(t, *winningOutcome, "outcome should be TRUE (YES) - uses latest value")

		return nil
	}
}

// =============================================================================
// Test: Invalid Query ID
// =============================================================================

func testSettleMarketInvalidQueryID(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		// Try to settle non-existent market
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{99999}, nil)
		require.NoError(t, err)
		require.NotNil(t, settleRes.Error, "should error on invalid query_id")
		require.Contains(t, settleRes.Error.Error(), "Market does not exist")

		return nil
	}
}

// =============================================================================
// Test: Already Settled
// =============================================================================

func testSettleMarketAlreadySettled(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		streamID := "stalreadysettled0000000000000000"
		dataProvider := deployer.Address()
		engineCtx := helper.NewEngineContext()

		// Create stream, insert data, request attestation
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		valueDecimal, _ := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{int64(1000)},
				[]*kwilTypes.Decimal{valueDecimal},
			}, nil)
		require.NoError(t, err)

		argsBytes, _ := tn_utils.EncodeActionArgs([]any{
			dataProvider, streamID, int64(500), int64(1500), nil, false,
		})

		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		helper.SignAttestation(requestTxID)

		// Create market
		var queryID int
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)

		// First settlement (should succeed)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.Nil(t, settleRes.Error)

		// Second settlement (should fail)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 300
		settleRes2, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.NotNil(t, settleRes2.Error, "should error when already settled")
		require.Contains(t, settleRes2.Error.Error(), "Market has already been settled")

		return nil
	}
}

// =============================================================================
// Test: Settlement Time Not Reached
// =============================================================================

func testSettleMarketTooEarly(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		streamID := "sttooearly0000000000000000000000"
		dataProvider := deployer.Address()
		engineCtx := helper.NewEngineContext()

		// Create stream, insert data, request attestation
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		valueDecimal, _ := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{int64(1000)},
				[]*kwilTypes.Decimal{valueDecimal},
			}, nil)
		require.NoError(t, err)

		argsBytes, _ := tn_utils.EncodeActionArgs([]any{
			dataProvider, streamID, int64(500), int64(1500), nil, false,
		})

		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		helper.SignAttestation(requestTxID)

		// Create market with settle_time = 1000
		var queryID int
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, int64(1000), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)

		// Try to settle too early (timestamp 500 < settle_time 1000)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 500
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.NotNil(t, settleRes.Error, "should error when settling too early")
		require.Contains(t, settleRes.Error.Error(), "Settlement time not yet reached")

		return nil
	}
}

// =============================================================================
// Test: Attestation Not Found
// =============================================================================

func testSettleMarketNoAttestation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		// Create market with fake attestation hash (no actual attestation exists)
		fakeHash := make([]byte, 32)
		for i := range fakeHash {
			fakeHash[i] = 0xAA
		}

		var queryID int
		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		_, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{fakeHash, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)

		// Try to settle (should fail - no attestation)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.NotNil(t, settleRes.Error, "should error when attestation not found")
		require.Contains(t, settleRes.Error.Error(), "Attestation not found")

		return nil
	}
}

// =============================================================================
// Test: Attestation Not Signed
// =============================================================================

func testSettleMarketAttestationNotSigned(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")
		platform.Deployer = deployer.Bytes()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		streamID := "stunsigned0000000000000000000000"
		dataProvider := deployer.Address()
		engineCtx := helper.NewEngineContext()

		// Create stream, insert data, request attestation (but don't sign it)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		valueDecimal, _ := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{int64(1000)},
				[]*kwilTypes.Decimal{valueDecimal},
			}, nil)
		require.NoError(t, err)

		argsBytes, _ := tn_utils.EncodeActionArgs([]any{
			dataProvider, streamID, int64(500), int64(1500), nil, false,
		})

		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
			func(row *common.Row) error {
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		// NOTE: Intentionally NOT signing the attestation

		// Create market
		var queryID int
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{attestationHash, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)

		// Try to settle (should fail - attestation not signed)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.NotNil(t, settleRes.Error, "should error when attestation not signed")
		require.Contains(t, settleRes.Error.Error(), "Attestation not yet signed")

		return nil
	}
}
