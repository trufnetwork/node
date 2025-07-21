package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"

	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	deployerTruflation = util.Unsafe_NewEthereumAddressFromString("0x4710a8d8f0d845da110086812a32de6d90d7ff5c")

	primitiveTruflation = util.GenerateStreamId("truflation_primitive")
	truflationPrefix    = "truflation_"
)

func TestTruflationFrozen(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "truflation_frozen_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTruflationFrozenTest(testAllBeforeFrozen(t)),
			setupTruflationFrozenTest(testAllAfterFrozen(t)),
			setupTruflationFrozenTest(testMixedBeforeAndAfter(t)),
			setupTruflationFrozenTest(testBoundaryConditions(t)),
			setupTruflationFrozenTest(testSingleRecords(t)),
			setupTruflationFrozenTest(testEdgeCases(t)),
			setupTruflationFrozenTest(testSameTimestamps(t)),
			setupTruflationFrozenTest(testRangeQuery(t)),
			setupTruflationFrozenTest(testWithoutFrozen(t)),
			setupTruflationFrozenTest(testFutureFrozen(t)),
		},
	}, testutils.GetTestOptions())
}

// Test Case 1 & 9: All records before frozen
func testAllBeforeFrozen(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		// Test Case 1: event_time 10
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 10")
		}

		expected10 := `
		| event_time | value  |
		| ---------- | ------ |
		| 10         | 102.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record10,
			Expected: expected10,
		})

		// Test Case 9: event_time 18
		eventTime18 := int64(18)
		record18, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime18,
			ToTime:        &eventTime18,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 18")
		}

		expected18 := `
		| event_time | value  |
		| ---------- | ------ |
		| 18         | 183.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record18,
			Expected: expected18,
		})

		return nil
	}
}

// Test Case 2 & 10: All records after frozen
func testAllAfterFrozen(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		// Test Case 2: event_time 11
		eventTime11 := int64(11)
		record11, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime11,
			ToTime:        &eventTime11,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 11")
		}

		expected11 := `
		| event_time | value  |
		| ---------- | ------ |
		| 11         | 110.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record11,
			Expected: expected11,
		})

		// Test Case 10: event_time 19
		eventTime19 := int64(19)
		record19, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime19,
			ToTime:        &eventTime19,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 19")
		}

		expected19 := `
		| event_time | value  |
		| ---------- | ------ |
		| 19         | 190.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record19,
			Expected: expected19,
		})

		return nil
	}
}

// Test Case 3: Mixed records before and after frozen
func testMixedBeforeAndAfter(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		eventTime12 := int64(12)
		record12, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime12,
			ToTime:        &eventTime12,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 12")
		}

		expected12 := `
		| event_time | value  |
		| ---------- | ------ |
		| 12         | 123.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record12,
			Expected: expected12,
		})

		return nil
	}
}

// Test Case 4: Boundary conditions
func testBoundaryConditions(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		eventTime13 := int64(13)
		record13, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime13,
			ToTime:        &eventTime13,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 13")
		}

		expected13 := `
		| event_time | value  |
		| ---------- | ------ |
		| 13         | 132.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record13,
			Expected: expected13,
		})

		return nil
	}
}

// Test Cases 5 & 6: Single records
func testSingleRecords(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		// Test Case 5: Single record before frozen
		eventTime14 := int64(14)
		record14, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime14,
			ToTime:        &eventTime14,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 14")
		}

		expected14 := `
		| event_time | value  |
		| ---------- | ------ |
		| 14         | 140.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record14,
			Expected: expected14,
		})

		// Test Case 6: Single record after frozen
		eventTime15 := int64(15)
		record15, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime15,
			ToTime:        &eventTime15,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 15")
		}

		expected15 := `
		| event_time | value  |
		| ---------- | ------ |
		| 15         | 150.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record15,
			Expected: expected15,
		})

		return nil
	}
}

// Test Cases 7, 8, 12, 13: Edge cases
func testEdgeCases(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		testCases := []struct {
			eventTime int64
			expected  string
		}{
			{
				eventTime: 16,
				expected: `
		| event_time | value  |
		| ---------- | ------ |
		| 16         | 162.000000000000000000 |
		`,
			},
			{
				eventTime: 17,
				expected: `
		| event_time | value  |
		| ---------- | ------ |
		| 17         | 172.000000000000000000 |
		`,
			},
			{
				eventTime: 22,
				expected: `
		| event_time | value  |
		| ---------- | ------ |
		| 22         | 221.000000000000000000 |
		`,
			},
			{
				eventTime: 23,
				expected: `
		| event_time | value  |
		| ---------- | ------ |
		| 23         | 232.000000000000000000 |
		`,
			},
		}

		for _, tc := range testCases {
			record, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
				Platform:      platform,
				StreamLocator: streamLocator,
				FromTime:      &tc.eventTime,
				ToTime:        &tc.eventTime,
				FrozenAt:      &frozenAt,
				Height:        0,
				Prefix:        &truflationPrefix,
			})
			if err != nil {
				return errors.Wrapf(err, "failed to get record for event_time %d", tc.eventTime)
			}

			table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
				Actual:   record,
				Expected: tc.expected,
			})
		}

		return nil
	}
}

// Test Cases 11 & 12: Same timestamps
func testSameTimestamps(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		// Test Case 11: Same timestamp before frozen
		eventTime20 := int64(20)
		record20, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime20,
			ToTime:        &eventTime20,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 20")
		}

		expected20 := `
		| event_time | value  |
		| ---------- | ------ |
		| 20         | 203.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record20,
			Expected: expected20,
		})

		// Test Case 12: Same timestamp after frozen
		eventTime21 := int64(21)
		record21, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime21,
			ToTime:        &eventTime21,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record for event_time 21")
		}

		expected21 := `
		| event_time | value  |
		| ---------- | ------ |
		| 21         | 213.000000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record21,
			Expected: expected21,
		})

		return nil
	}
}

// Test range query
func testRangeQuery(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		dateFrom := int64(10)
		dateTo := int64(23)

		rangeRecords, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get range records")
		}

		expectedRangeTable := `
		| event_time | value  |
		| ---------- | ------ |
		| 10         | 102.000000000000000000 |
		| 11         | 110.000000000000000000 |
		| 12         | 123.000000000000000000 |
		| 13         | 132.000000000000000000 |
		| 14         | 140.000000000000000000 |
		| 15         | 150.000000000000000000 |
		| 16         | 162.000000000000000000 |
		| 17         | 172.000000000000000000 |
		| 18         | 183.000000000000000000 |
		| 19         | 190.000000000000000000 |
		| 20         | 203.000000000000000000 |
		| 21         | 213.000000000000000000 |
		| 22         | 221.000000000000000000 |
		| 23         | 232.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   rangeRecords,
			Expected: expectedRangeTable,
		})

		return nil
	}
}

// Test without frozen mechanism
func testWithoutFrozen(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		// Test event_time 12 without frozen (should get latest)
		testEventTime := int64(12)

		recordWithoutFrozen, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &testEventTime,
			ToTime:        &testEventTime,
			FrozenAt:      nil, // No frozen mechanism
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record without frozen")
		}

		expectedWithoutFrozen := `
		| event_time | value  |
		| ---------- | ------ |
		| 12         | 125.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   recordWithoutFrozen,
			Expected: expectedWithoutFrozen,
		})

		return nil
	}
}

// Test with future frozen_at
func testFutureFrozen(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		futureFrozen := int64(1893456000) // Some date in 2030
		testEventTime := int64(12)

		recordFutureFrozen, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &testEventTime,
			ToTime:        &testEventTime,
			FrozenAt:      &futureFrozen,
			Height:        0,
			Prefix:        &truflationPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get record with future frozen")
		}

		expectedFutureFrozen := `
		| event_time | value  |
		| ---------- | ------ |
		| 12         | 125.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   recordFutureFrozen,
			Expected: expectedFutureFrozen,
		})

		return nil
	}
}

func setupTruflationFrozenTest(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, deployerTruflation.Bytes())

		err := setup.CreateDataProvider(ctx, platform, deployerTruflation.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// 1. Deploy primitive
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitiveTruflation, DataProvider: deployerTruflation},
				},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying Truflation primitive")
		}

		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		insertTestRecord := func(eventTime int64, value float64, truflationCreatedAt string, height int64) error {
			return setup.InsertTruflationDataBatch(ctx, setup.InsertTruflationDataInput{
				Platform: platform,
				PrimitiveStream: setup.TruflationStreamWithData{
					PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
						StreamLocator: streamLocator,
					},
					Data: []setup.InsertTruflationRecordInput{
						{EventTime: eventTime, Value: value, TruflationCreatedAt: truflationCreatedAt},
					},
				},
				Height: height,
			})
		}

		// frozen_at: 1714176000 = April 27, 2024 00:00:00 UTC

		// Test Case 1: Multiple records for same event_time with dates BEFORE frozen_at
		// Expected: Should select value 102 (newest before frozen)
		insertTestRecord(10, 100, "2024-04-01T10:00:00Z", 1)
		insertTestRecord(10, 101, "2024-04-15T15:30:00Z", 2)
		insertTestRecord(10, 102, "2024-04-26T23:59:59Z", 3) // Expected

		// Test Case 2: Multiple records for same event_time with dates AFTER frozen_at
		// Expected: Should select value 110 (oldest after frozen)
		insertTestRecord(11, 110, "2024-04-27T00:00:01Z", 4) // Expected
		insertTestRecord(11, 111, "2024-05-01T12:00:00Z", 5)
		insertTestRecord(11, 112, "2024-05-15T18:45:30Z", 6)

		// Test Case 3: Mixed - records both BEFORE and AFTER frozen_at
		// Expected: Should select value 123 (oldest after frozen, ignoring all before frozen)
		insertTestRecord(12, 120, "2024-04-01T08:00:00Z", 7)
		insertTestRecord(12, 121, "2024-04-20T14:30:00Z", 8)
		insertTestRecord(12, 122, "2024-04-26T22:00:00Z", 9)
		insertTestRecord(12, 123, "2024-04-28T06:00:00Z", 10) // Expected
		insertTestRecord(12, 124, "2024-05-10T09:15:00Z", 11)
		insertTestRecord(12, 125, "2024-06-01T00:00:00Z", 12)

		// Test Case 4: Records exactly at frozen_at boundary
		// Expected: Should select value 132 (oldest after frozen)
		insertTestRecord(13, 130, "2024-04-26T23:59:59Z", 13)
		insertTestRecord(13, 131, "2024-04-27T00:00:00Z", 14) // Exactly at frozen_at - counts as BEFORE/AT
		insertTestRecord(13, 132, "2024-04-27T00:00:01Z", 15) // Expected

		// Test Case 5: Only one record (before frozen)
		// Expected: Should select value 140
		insertTestRecord(14, 140, "2024-04-15T12:00:00Z", 16) // Expected

		// Test Case 6: Only one record (after frozen)
		// Expected: Should select value 150
		insertTestRecord(15, 150, "2024-05-15T12:00:00Z", 17) // Expected

		// Test Case 7: Edge case - very old and very new records
		// Expected: Should select value 162 (oldest after frozen)
		insertTestRecord(16, 160, "2023-01-01T00:00:00Z", 18)
		insertTestRecord(16, 161, "2024-04-26T23:59:58Z", 19)
		insertTestRecord(16, 162, "2024-04-27T00:00:02Z", 20) // Expected
		insertTestRecord(16, 163, "2025-01-01T00:00:00Z", 21)

		// Test Case 8: Milliseconds precision (if supported)
		// Expected: Should select value 172 (oldest after frozen)
		insertTestRecord(17, 170, "2024-04-26T23:59:59.999Z", 22)
		insertTestRecord(17, 171, "2024-04-27T00:00:00.000Z", 23)
		insertTestRecord(17, 172, "2024-04-27T00:00:00.001Z", 24) // Expected

		// Test Case 9: Multiple records all before frozen
		// Expected: Should select value 183 (newest before frozen)
		insertTestRecord(18, 180, "2024-01-01T00:00:00Z", 25)
		insertTestRecord(18, 181, "2024-02-15T12:30:45Z", 26)
		insertTestRecord(18, 182, "2024-03-20T18:45:30Z", 27)
		insertTestRecord(18, 183, "2024-04-26T23:59:30Z", 28) // Expected

		// Test Case 10: Multiple records all after frozen
		// Expected: Should select value 190 (oldest after frozen)
		insertTestRecord(19, 190, "2024-04-27T00:01:00Z", 29) // Expected
		insertTestRecord(19, 191, "2024-05-01T10:00:00Z", 30)
		insertTestRecord(19, 192, "2024-06-15T15:30:00Z", 31)
		insertTestRecord(19, 193, "2024-12-31T23:59:59Z", 32)

		// Test Case 11: Same truflation_created_at but different heights
		// Expected: For before frozen, pick highest height
		insertTestRecord(20, 200, "2024-04-26T20:00:00Z", 33)
		insertTestRecord(20, 201, "2024-04-26T20:00:00Z", 34)
		insertTestRecord(20, 202, "2024-04-26T20:00:00Z", 35)
		insertTestRecord(20, 203, "2024-04-26T20:00:00Z", 36) // Expected (same timestamp, highest height)

		insertTestRecord(21, 210, "2024-04-28T10:00:00Z", 37)
		insertTestRecord(21, 211, "2024-04-28T10:00:00Z", 38)
		insertTestRecord(21, 212, "2024-04-28T10:00:00Z", 39)
		insertTestRecord(21, 213, "2024-04-28T10:00:00Z", 40) // Expected (same timestamp, highest height)

		// Test Case 12: Sparse data with large gaps
		// Expected: Should select value 221 (oldest after frozen)
		insertTestRecord(22, 220, "2023-06-15T00:00:00Z", 41) // Very old
		insertTestRecord(22, 221, "2024-07-01T00:00:00Z", 42) // Expected (months after frozen)
		insertTestRecord(22, 222, "2024-12-25T00:00:00Z", 43) // End of year

		// Test Case 13: Rapid updates around frozen time
		// Expected: Should select value 232 (first one after frozen)
		insertTestRecord(23, 230, "2024-04-26T23:59:57Z", 44)
		insertTestRecord(23, 231, "2024-04-26T23:59:58Z", 45)
		insertTestRecord(23, 232, "2024-04-27T00:00:01Z", 46) // Expected
		insertTestRecord(23, 233, "2024-04-27T00:00:02Z", 47)
		insertTestRecord(23, 234, "2024-04-27T00:00:03Z", 48)

		// Run the actual test function
		return testFn(ctx, platform)
	}
}
