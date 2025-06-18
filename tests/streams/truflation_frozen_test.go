package tests

import (
	"context"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"

	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	deployerTruflation = util.Unsafe_NewEthereumAddressFromString("0x51111111111111ABCDEF0123456789ABCDEF0123")

	primitiveTruflation = util.GenerateStreamId("truflation_primitive")
)

func TestTruflationFrozen(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "truflation_frozen_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTruflationFrozenTest(testTruflationFrozen(t)),
		},
	}, testutils.GetTestOptions())
}

func testTruflationFrozen(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamLocator := types.StreamLocator{
			StreamId:     primitiveTruflation,
			DataProvider: deployerTruflation,
		}

		insertTestRecord := func(eventTime int64, value float64, truflationCreatedAt string, height int64) error {
			return setup.InsertTruflationDataBatch(ctx, setup.InsertTruflationDataInput{
					Platform:        platform,
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
		// Expected: For before frozen, pick highest height (value 203); for after frozen, pick lowest height (value 210)
		insertTestRecord(20, 200, "2024-04-26T20:00:00Z", 33)
		insertTestRecord(20, 201, "2024-04-26T20:00:00Z", 34)
		insertTestRecord(20, 202, "2024-04-26T20:00:00Z", 35)
		insertTestRecord(20, 203, "2024-04-26T20:00:00Z", 36) // Expected (same timestamp, highest height)

		insertTestRecord(21, 210, "2024-04-28T10:00:00Z", 37) // Expected (same timestamp, lowest height)
		insertTestRecord(21, 211, "2024-04-28T10:00:00Z", 38)
		insertTestRecord(21, 212, "2024-04-28T10:00:00Z", 39)
		insertTestRecord(21, 213, "2024-04-28T10:00:00Z", 40)

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

		return nil
	}
}

func setupTruflationFrozenTest(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, deployerTruflation.Bytes())

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

		// Run the actual test function
		return testFn(ctx, platform)
	}
}
