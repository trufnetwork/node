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
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	deployerTruflationComposed = util.Unsafe_NewEthereumAddressFromString("0x4710a8d8f0d845da110086812a32de6d90d7ff5c")

	truflationFrozenComposed1 = util.GenerateStreamId("truflation_composed1")
	truflationFrozenPrimitive1 = util.GenerateStreamId("truflation_primitive1")
	truflationFrozenPrimitive2 = util.GenerateStreamId("truflation_primitive2")
	truflationFrozenComposed2 = util.GenerateStreamId("truflation_composed2")
	truflationFrozenParentComposed = util.GenerateStreamId("truflation_parent_composed")
	truflationComposedPrefix = "truflation_"

	frozenAt = int64(1714176000) // 2024-04-27
)

func TestTruflationComposedFrozen(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "truflation_composed_frozen_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTruflationFrozenComposedTest(testTruflationComposed1(t)),
			setupTruflationFrozenComposedTest(testTruflationComposed2(t)),
			setupTruflationFrozenComposedTest(testTruflationParentComposed(t)),
		},
	}, testutils.GetTestOptions())
}

func setupTruflationFrozenComposedTest(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, deployerTruflationComposed.Bytes())

		// 1. Deploy primitives
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: truflationFrozenPrimitive1, DataProvider: deployerTruflationComposed},
				},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying truflationFrozenPrimitive1")
		}

		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: truflationFrozenPrimitive2, DataProvider: deployerTruflationComposed},
				},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying truflationFrozenPrimitive2")
		}

		// 2. Deploy composed streams
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform, 
			StreamId: truflationFrozenComposed1, 
			Height: 2,
		}); err != nil {
			return errors.Wrap(err, "deploying truflationFrozenComposed1")
		}

		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform, 
			StreamId: truflationFrozenComposed2, 
			Height: 2,
		}); err != nil {
			return errors.Wrap(err, "deploying truflationFrozenComposed2")
		}

		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform, 
			StreamId: truflationFrozenParentComposed, 
			Height: 2,
		}); err != nil {
			return errors.Wrap(err, "deploying parent composed")
		}

		// 3. Set taxonomies
		start := int64(1)
		
		// truflationFrozenComposed1 -> truflationFrozenPrimitive1 (weight 0.6), truflationFrozenPrimitive2 (weight 0.4)
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform, 
			StreamLocator: types.StreamLocator{StreamId: truflationFrozenComposed1, DataProvider: deployerTruflationComposed}, 
			DataProviders: []string{deployerTruflationComposed.Address(), deployerTruflationComposed.Address()}, 
			StreamIds: []string{truflationFrozenPrimitive1.String(), truflationFrozenPrimitive2.String()}, 
			Weights: []string{"0.6", "0.4"}, 
			StartTime: &start, 
			Height: 3,
		}); err != nil {
			return errors.Wrap(err, "tax for truflationFrozenComposed1")
		}

		// truflationFrozenComposed2 -> just truflationFrozenPrimitive1 (weight 1.0) for simpler testing
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform, 
			StreamLocator: types.StreamLocator{StreamId: truflationFrozenComposed2, DataProvider: deployerTruflationComposed}, 
			DataProviders: []string{deployerTruflationComposed.Address()}, 
			StreamIds: []string{truflationFrozenPrimitive1.String()}, 
			Weights: []string{"1.0"}, 
			StartTime: &start, 
			Height: 3,
		}); err != nil {
			return errors.Wrap(err, "tax for truflationFrozenComposed2")
		}

		// truflationFrozenParentComposed -> truflationFrozenComposed1 (weight 0.5), truflationFrozenComposed2 (weight 0.5)
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform, 
			StreamLocator: types.StreamLocator{StreamId: truflationFrozenParentComposed, DataProvider: deployerTruflationComposed}, 
			DataProviders: []string{deployerTruflationComposed.Address(), deployerTruflationComposed.Address()}, 
			StreamIds: []string{truflationFrozenComposed1.String(), truflationFrozenComposed2.String()}, 
			Weights: []string{"0.5", "0.5"}, 
			StartTime: &start, 
			Height: 3,
		}); err != nil {
			return errors.Wrap(err, "tax for parent composed")
		}

		// Helper function to insert test records
		insertTestRecord := func(primitiveId util.StreamId, eventTime int64, value float64, truflationCreatedAt string, height int64) error {
			return setup.InsertTruflationDataBatch(ctx, setup.InsertTruflationDataInput{
				Platform: platform,
				PrimitiveStream: setup.TruflationStreamWithData{
					PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
						StreamLocator: types.StreamLocator{
							StreamId:     primitiveId,
							DataProvider: deployerTruflationComposed,
						},
					},
					Data: []setup.InsertTruflationRecordInput{
						{EventTime: eventTime, Value: value, TruflationCreatedAt: truflationCreatedAt},
					},
				},
				Height: height,
			})
		}

		// Test Case 1: Multiple records before frozen for event_time 10
		insertTestRecord(truflationFrozenPrimitive1, 10, 100, "2024-04-01T10:00:00Z", 1)
		insertTestRecord(truflationFrozenPrimitive1, 10, 101, "2024-04-15T15:30:00Z", 2)
		insertTestRecord(truflationFrozenPrimitive1, 10, 102, "2024-04-26T23:59:59Z", 3)
		
		// Test Case 2: Mixed before and after frozen for event_time 11
		insertTestRecord(truflationFrozenPrimitive1, 11, 110, "2024-04-20T10:00:00Z", 4)
		insertTestRecord(truflationFrozenPrimitive1, 11, 111, "2024-04-28T10:00:00Z", 5)
		insertTestRecord(truflationFrozenPrimitive1, 11, 112, "2024-05-01T10:00:00Z", 6)

		// Different values to verify weighted averaging
		insertTestRecord(truflationFrozenPrimitive2, 10, 200, "2024-04-01T10:00:00Z", 7)
		insertTestRecord(truflationFrozenPrimitive2, 10, 202, "2024-04-15T15:30:00Z", 8)
		insertTestRecord(truflationFrozenPrimitive2, 10, 204, "2024-04-26T23:59:59Z", 9)

		insertTestRecord(truflationFrozenPrimitive2, 11, 220, "2024-04-20T10:00:00Z", 10)
		insertTestRecord(truflationFrozenPrimitive2, 11, 222, "2024-04-28T10:00:00Z", 11)
		insertTestRecord(truflationFrozenPrimitive2, 11, 224, "2024-05-01T10:00:00Z", 12)


		return testFn(ctx, platform)
	}
}

func testTruflationComposed1(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		streamLocator := types.StreamLocator{
			StreamId:     truflationFrozenComposed1,
			DataProvider: deployerTruflationComposed,
		}

		// Test event_time 10
		// truflationFrozenPrimitive1: value 102 (newest before frozen) * weight 0.6 = 61.2
		// truflationFrozenPrimitive2: value 204 (newest before frozen) * weight 0.4 = 81.6
		// Total: 142.8
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationComposedPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get truflationFrozenComposed1 record for event_time 10")
		}

		expected10 := `
		| event_time | value  |
		| ---------- | ------ |
		| 10         | 142.800000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record10,
			Expected: expected10,
		})

		// Test event_time 11
		// truflationFrozenPrimitive1: value 111 (oldest after frozen) * weight 0.6 = 66.6
		// truflationFrozenPrimitive2: value 222 (oldest after frozen) * weight 0.4 = 88.8
		// Total: 155.4
		eventTime11 := int64(11)
		record11, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime11,
			ToTime:        &eventTime11,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationComposedPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get truflationFrozenComposed1 record for event_time 11")
		}

		expected11 := `
		| event_time | value  |
		| ---------- | ------ |
		| 11         | 155.400000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record11,
			Expected: expected11,
		})

		return nil
	}
}

func testTruflationComposed2(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		truflationFrozenComposed2 := util.GenerateStreamId("truflation_composed2")
		
		streamLocator := types.StreamLocator{
			StreamId:     truflationFrozenComposed2,
			DataProvider: deployerTruflationComposed,
		}

		// Test event_time 10
		// Only truflationFrozenPrimitive1 with weight 1.0, so value should be 102
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationComposedPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get truflationFrozenComposed2 record for event_time 10")
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

		return nil
	}
}

func testTruflationParentComposed(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		truflationFrozenParentComposed := util.GenerateStreamId("truflation_parent_composed")
		
		streamLocator := types.StreamLocator{
			StreamId:     truflationFrozenParentComposed,
			DataProvider: deployerTruflationComposed,
		}

		// Test event_time 10
		// truflationFrozenComposed1: 142.8 * weight 0.5 = 71.4
		// truflationFrozenComposed2: 102.0 * weight 0.5 = 51.0
		// Total: 122.4
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
			Prefix:        &truflationComposedPrefix,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get truflationFrozenParentComposed record for event_time 10")
		}

		expected10 := `
		| event_time | value  |
		| ---------- | ------ |
		| 10         | 122.400000000000000000 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   record10,
			Expected: expected10,
		})

		return nil
	}
}