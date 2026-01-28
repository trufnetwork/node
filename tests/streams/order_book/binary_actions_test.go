//go:build kwiltest

package order_book

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// TestBinaryActions tests the new binary attestation actions
// that return TRUE/FALSE for prediction market settlement
func TestBinaryActions(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "BINARY_ACTIONS_01_AllActions",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			// price_above_threshold tests
			testPriceAboveThresholdTrue(t),
			testPriceAboveThresholdFalse(t),
			testPriceAboveThresholdExactlyAt(t),

			// price_below_threshold tests
			testPriceBelowThresholdTrue(t),
			testPriceBelowThresholdFalse(t),

			// value_in_range tests
			testValueInRangeTrue(t),
			testValueInRangeFalseBelow(t),
			testValueInRangeFalseAbove(t),
			testValueInRangeBoundary(t),

			// value_equals tests
			testValueEqualsExact(t),
			testValueEqualsWithTolerance(t),
			testValueEqualsOutsideTolerance(t),

			// Error cases
			testBinaryActionNoData(t),
			testValueInRangeInvalidRange(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupBinaryActionTest creates a data provider and stream with test data
// Returns the engineCtx that should be used for all subsequent operations
func setupBinaryActionTest(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, streamID string, eventTime int64, value string) (*common.EngineContext, string) {
	deployer := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
	platform.Deployer = deployer.Bytes()

	dataProvider := deployer.Address()

	t.Logf("Setup: Creating data provider %s", dataProvider)

	// Create data provider using setup helper (grants required roles)
	err := setup.CreateDataProvider(ctx, platform, dataProvider)
	require.NoError(t, err, "failed to create data provider")

	t.Logf("Setup: Data provider created, creating engine context")

	// Use attestation helper for engine context after data provider is created
	helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
	engineCtx := helper.NewEngineContext()

	t.Logf("Setup: Creating stream %s", streamID)

	// Create stream
	createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
		[]any{streamID, "primitive"}, nil)
	require.NoError(t, err, "failed to create stream")
	if createRes.Error != nil {
		t.Logf("Setup: create_stream action error: %v", createRes.Error)
		require.NoError(t, createRes.Error, "create_stream failed")
	}

	t.Logf("Setup: Stream created, inserting data")

	// Insert data
	valueDecimal, err := kwilTypes.ParseDecimalExplicit(value, 36, 18)
	require.NoError(t, err, "failed to parse value decimal")

	insertRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
		[]any{
			[]string{dataProvider},
			[]string{streamID},
			[]int64{eventTime},
			[]*kwilTypes.Decimal{valueDecimal},
		}, nil)
	require.NoError(t, err, "failed to insert records")
	if insertRes.Error != nil {
		t.Logf("Setup: insert_records action error: %v", insertRes.Error)
		require.NoError(t, insertRes.Error, "insert_records failed")
	}

	t.Logf("Setup: Data inserted successfully")

	return engineCtx, dataProvider
}

// =============================================================================
// price_above_threshold tests
// =============================================================================

func testPriceAboveThresholdTrue(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stpriceabovetest1000000000000000"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "55000.000000000000000000")

		// Debug: Verify the stream exists by calling get_record first
		t.Logf("Debug: Checking if stream %s exists for provider %s", streamID, dataProvider)
		var foundData bool
		debugRes, debugErr := platform.Engine.Call(engineCtx, platform.DB, "", "get_record",
			[]any{
				dataProvider,
				streamID,
				int64(500),
				int64(1500),
				nil,
				false,
			},
			func(row *common.Row) error {
				t.Logf("Debug: Found data - event_time=%v, value=%v", row.Values[0], row.Values[1])
				foundData = true
				return nil
			})
		if debugErr != nil {
			t.Logf("Debug: get_record error: %v", debugErr)
		}
		if debugRes != nil && debugRes.Error != nil {
			t.Logf("Debug: get_record action error: %v", debugRes.Error)
		}
		t.Logf("Debug: Found data = %v", foundData)

		// Call price_above_threshold with threshold = 50000
		thresholdDecimal, err := kwilTypes.ParseDecimalExplicit("50000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "price_above_threshold",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				thresholdDecimal,
				nil, // frozen_at
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			t.Logf("price_above_threshold error: %v", res.Error)
			require.NoError(t, res.Error, "price_above_threshold failed")
		}

		require.True(t, result, "55000 > 50000 should be TRUE")
		t.Log("price_above_threshold: 55000 > 50000 = TRUE (correct)")

		return nil
	}
}

func testPriceAboveThresholdFalse(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stpriceabovetest2000000000000000"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "45000.000000000000000000")

		thresholdDecimal, err := kwilTypes.ParseDecimalExplicit("50000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "price_above_threshold",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				thresholdDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "price_above_threshold failed")
		}

		require.False(t, result, "45000 > 50000 should be FALSE")
		t.Log("price_above_threshold: 45000 > 50000 = FALSE (correct)")

		return nil
	}
}

func testPriceAboveThresholdExactlyAt(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stpriceabovetest3000000000000000"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "50000.000000000000000000")

		thresholdDecimal, err := kwilTypes.ParseDecimalExplicit("50000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "price_above_threshold",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				thresholdDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "price_above_threshold failed")
		}

		// 50000 > 50000 is FALSE (not strictly greater)
		require.False(t, result, "50000 > 50000 should be FALSE (not strictly greater)")
		t.Log("price_above_threshold: 50000 > 50000 = FALSE (correct, not strictly greater)")

		return nil
	}
}

// =============================================================================
// price_below_threshold tests
// =============================================================================

func testPriceBelowThresholdTrue(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stpricebelowtest1000000000000000"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "3.500000000000000000")

		thresholdDecimal, err := kwilTypes.ParseDecimalExplicit("4.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "price_below_threshold",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				thresholdDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "price_below_threshold failed")
		}

		require.True(t, result, "3.5 < 4.0 should be TRUE")
		t.Log("price_below_threshold: 3.5 < 4.0 = TRUE (correct)")

		return nil
	}
}

func testPriceBelowThresholdFalse(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stpricebelowtest2000000000000000"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "5.000000000000000000")

		thresholdDecimal, err := kwilTypes.ParseDecimalExplicit("4.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "price_below_threshold",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				thresholdDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "price_below_threshold failed")
		}

		require.False(t, result, "5.0 < 4.0 should be FALSE")
		t.Log("price_below_threshold: 5.0 < 4.0 = FALSE (correct)")

		return nil
	}
}

// =============================================================================
// value_in_range tests
// =============================================================================

func testValueInRangeTrue(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueinrangetest00000000000001"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "100000.000000000000000000")

		minDecimal, err := kwilTypes.ParseDecimalExplicit("90000.000000000000000000", 36, 18)
		require.NoError(t, err)
		maxDecimal, err := kwilTypes.ParseDecimalExplicit("110000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_in_range",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				minDecimal,
				maxDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_in_range failed")
		}

		require.True(t, result, "100000 in [90000, 110000] should be TRUE")
		t.Log("value_in_range: 100000 in [90000, 110000] = TRUE (correct)")

		return nil
	}
}

func testValueInRangeFalseBelow(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueinrangetest00000000000002"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "80000.000000000000000000")

		minDecimal, err := kwilTypes.ParseDecimalExplicit("90000.000000000000000000", 36, 18)
		require.NoError(t, err)
		maxDecimal, err := kwilTypes.ParseDecimalExplicit("110000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_in_range",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				minDecimal,
				maxDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_in_range failed")
		}

		require.False(t, result, "80000 in [90000, 110000] should be FALSE")
		t.Log("value_in_range: 80000 in [90000, 110000] = FALSE (correct)")

		return nil
	}
}

func testValueInRangeFalseAbove(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueinrangetest00000000000003"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "120000.000000000000000000")

		minDecimal, err := kwilTypes.ParseDecimalExplicit("90000.000000000000000000", 36, 18)
		require.NoError(t, err)
		maxDecimal, err := kwilTypes.ParseDecimalExplicit("110000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_in_range",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				minDecimal,
				maxDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_in_range failed")
		}

		require.False(t, result, "120000 in [90000, 110000] should be FALSE")
		t.Log("value_in_range: 120000 in [90000, 110000] = FALSE (correct)")

		return nil
	}
}

func testValueInRangeBoundary(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueinrangetest00000000000004"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "90000.000000000000000000")

		minDecimal, err := kwilTypes.ParseDecimalExplicit("90000.000000000000000000", 36, 18)
		require.NoError(t, err)
		maxDecimal, err := kwilTypes.ParseDecimalExplicit("110000.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_in_range",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				minDecimal,
				maxDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_in_range failed")
		}

		// Boundary is inclusive: 90000 >= 90000 AND 90000 <= 110000
		require.True(t, result, "90000 in [90000, 110000] should be TRUE (inclusive boundary)")
		t.Log("value_in_range: 90000 in [90000, 110000] = TRUE (boundary is inclusive)")

		return nil
	}
}

// =============================================================================
// value_equals tests
// =============================================================================

func testValueEqualsExact(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueequalstest000000000000001"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "5.250000000000000000")

		targetDecimal, err := kwilTypes.ParseDecimalExplicit("5.250000000000000000", 36, 18)
		require.NoError(t, err)
		toleranceDecimal, err := kwilTypes.ParseDecimalExplicit("0.000000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_equals",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				targetDecimal,
				toleranceDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_equals failed")
		}

		require.True(t, result, "5.25 == 5.25 (tolerance 0) should be TRUE")
		t.Log("value_equals: 5.25 == 5.25 (tolerance 0) = TRUE (correct)")

		return nil
	}
}

func testValueEqualsWithTolerance(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueequalstest000000000000002"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "5.270000000000000000")

		targetDecimal, err := kwilTypes.ParseDecimalExplicit("5.250000000000000000", 36, 18)
		require.NoError(t, err)
		toleranceDecimal, err := kwilTypes.ParseDecimalExplicit("0.050000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_equals",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				targetDecimal,
				toleranceDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_equals failed")
		}

		require.True(t, result, "5.27 == 5.25 (tolerance 0.05) should be TRUE")
		t.Log("value_equals: 5.27 == 5.25 (tolerance 0.05) = TRUE (correct)")

		return nil
	}
}

func testValueEqualsOutsideTolerance(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "stvalueequalstest000000000000003"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "5.350000000000000000")

		targetDecimal, err := kwilTypes.ParseDecimalExplicit("5.250000000000000000", 36, 18)
		require.NoError(t, err)
		toleranceDecimal, err := kwilTypes.ParseDecimalExplicit("0.050000000000000000", 36, 18)
		require.NoError(t, err)

		var result bool
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_equals",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				targetDecimal,
				toleranceDecimal,
				nil,
			},
			func(row *common.Row) error {
				result = row.Values[0].(bool)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			require.NoError(t, res.Error, "value_equals failed")
		}

		require.False(t, result, "5.35 == 5.25 (tolerance 0.05) should be FALSE")
		t.Log("value_equals: 5.35 == 5.25 (tolerance 0.05) = FALSE (correct)")

		return nil
	}
}

// =============================================================================
// Error cases
// =============================================================================

func testBinaryActionNoData(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		platform.Deployer = deployer.Bytes()

		streamID := "stbinarynodata000000000000000000"
		dataProvider := deployer.Address()

		// Create data provider using setup helper
		err := setup.CreateDataProvider(ctx, platform, dataProvider)
		require.NoError(t, err)

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		engineCtx := helper.NewEngineContext()

		// Create stream but DON'T insert any data
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		thresholdDecimal, err := kwilTypes.ParseDecimalExplicit("50000.000000000000000000", 36, 18)
		require.NoError(t, err)

		// Should fail because no data exists
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "price_above_threshold",
			[]any{
				dataProvider,
				streamID,
				int64(1000),
				thresholdDecimal,
				nil,
			}, nil)
		require.NoError(t, err) // Engine call itself shouldn't fail

		// But the action should return an error
		require.NotNil(t, res.Error, "should error when no data exists")
		require.Contains(t, res.Error.Error(), "No data found")
		t.Log("price_above_threshold with no data: correctly returned error")

		return nil
	}
}

func testValueInRangeInvalidRange(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamID := "strangeinvalidtest00000000000001"
		eventTime := int64(1000)

		engineCtx, dataProvider := setupBinaryActionTest(t, ctx, platform, streamID, eventTime, "100.000000000000000000")

		// min > max is invalid
		minDecimal, err := kwilTypes.ParseDecimalExplicit("200.000000000000000000", 36, 18)
		require.NoError(t, err)
		maxDecimal, err := kwilTypes.ParseDecimalExplicit("100.000000000000000000", 36, 18)
		require.NoError(t, err)

		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "value_in_range",
			[]any{
				dataProvider,
				streamID,
				eventTime,
				minDecimal,
				maxDecimal,
				nil,
			}, nil)
		require.NoError(t, err)

		require.NotNil(t, res.Error, "should error when min > max")
		require.Contains(t, res.Error.Error(), "Invalid range")
		t.Log("value_in_range with min > max: correctly returned error")

		return nil
	}
}
