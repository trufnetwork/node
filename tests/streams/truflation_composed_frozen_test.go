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

func TestTruflationComposedFrozen(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "truflation_frozen_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTruflationFrozenComposedTest(testTruflationComposed1(t)),
			// setupTruflationFrozenComposedTest(testTruflationComposed2(t)),
			// setupTruflationFrozenComposedTest(testTruflationParentComposed(t)),
		},
	}, testutils.GetTestOptions())
}

func setupTruflationFrozenComposedTest(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, deployerTruflation.Bytes())

		// Define stream IDs
		primitive1 := util.GenerateStreamId("truflation_primitive1")
		primitive2 := util.GenerateStreamId("truflation_primitive2")
		composed1 := util.GenerateStreamId("truflation_composed1")
		composed2 := util.GenerateStreamId("truflation_composed2")
		parentComposed := util.GenerateStreamId("truflation_parent_composed")

		// 1. Deploy primitives
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitive1, DataProvider: deployerTruflation},
				},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying primitive1")
		}

		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitive2, DataProvider: deployerTruflation},
				},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying primitive2")
		}

		// 2. Deploy composed streams
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform, 
			StreamId: composed1, 
			Height: 2,
		}); err != nil {
			return errors.Wrap(err, "deploying composed1")
		}

		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform, 
			StreamId: composed2, 
			Height: 2,
		}); err != nil {
			return errors.Wrap(err, "deploying composed2")
		}

		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform, 
			StreamId: parentComposed, 
			Height: 2,
		}); err != nil {
			return errors.Wrap(err, "deploying parent composed")
		}

		// 3. Set taxonomies
		start := int64(1)
		
		// Composed1 -> primitive1 (weight 0.6), primitive2 (weight 0.4)
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform, 
			StreamLocator: types.StreamLocator{StreamId: composed1, DataProvider: deployerTruflation}, 
			DataProviders: []string{deployerTruflation.Address(), deployerTruflation.Address()}, 
			StreamIds: []string{primitive1.String(), primitive2.String()}, 
			Weights: []string{"0.6", "0.4"}, 
			StartTime: &start, 
			Height: 3,
		}); err != nil {
			return errors.Wrap(err, "tax for composed1")
		}

		// Composed2 -> just primitive1 (weight 1.0) for simpler testing
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform, 
			StreamLocator: types.StreamLocator{StreamId: composed2, DataProvider: deployerTruflation}, 
			DataProviders: []string{deployerTruflation.Address()}, 
			StreamIds: []string{primitive1.String()}, 
			Weights: []string{"1.0"}, 
			StartTime: &start, 
			Height: 3,
		}); err != nil {
			return errors.Wrap(err, "tax for composed2")
		}

		// ParentComposed -> composed1 (weight 0.5), composed2 (weight 0.5)
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform, 
			StreamLocator: types.StreamLocator{StreamId: parentComposed, DataProvider: deployerTruflation}, 
			DataProviders: []string{deployerTruflation.Address(), deployerTruflation.Address()}, 
			StreamIds: []string{composed1.String(), composed2.String()}, 
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
							DataProvider: deployerTruflation,
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
		insertTestRecord(primitive1, 10, 100, "2024-04-01T10:00:00Z", 1)
		insertTestRecord(primitive1, 10, 101, "2024-04-15T15:30:00Z", 2)
		insertTestRecord(primitive1, 10, 102, "2024-04-26T23:59:59Z", 3)
		
		// Test Case 2: Mixed before and after frozen for event_time 11
		insertTestRecord(primitive1, 11, 110, "2024-04-20T10:00:00Z", 4)
		insertTestRecord(primitive1, 11, 111, "2024-04-28T10:00:00Z", 5)
		insertTestRecord(primitive1, 11, 112, "2024-05-01T10:00:00Z", 6)

		// Different values to verify weighted averaging
		insertTestRecord(primitive2, 10, 200, "2024-04-01T10:00:00Z", 7)
		insertTestRecord(primitive2, 10, 202, "2024-04-15T15:30:00Z", 8)
		insertTestRecord(primitive2, 10, 204, "2024-04-26T23:59:59Z", 9)

		insertTestRecord(primitive2, 11, 220, "2024-04-20T10:00:00Z", 10)
		insertTestRecord(primitive2, 11, 222, "2024-04-28T10:00:00Z", 11)
		insertTestRecord(primitive2, 11, 224, "2024-05-01T10:00:00Z", 12)


		return testFn(ctx, platform)
	}
}

func testTruflationComposed1(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		frozenAt := int64(1714176000)
		composed1 := util.GenerateStreamId("truflation_composed1")
		
		streamLocator := types.StreamLocator{
			StreamId:     composed1,
			DataProvider: deployerTruflation,
		}

		// Test event_time 10
		// primitive1: value 102 (newest before frozen) * weight 0.6 = 61.2
		// primitive2: value 204 (newest before frozen) * weight 0.4 = 81.6
		// Total: 142.8
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get composed1 record for event_time 10")
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
		// primitive1: value 111 (oldest after frozen) * weight 0.6 = 66.6
		// primitive2: value 222 (oldest after frozen) * weight 0.4 = 88.8
		// Total: 155.4
		eventTime11 := int64(11)
		record11, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime11,
			ToTime:        &eventTime11,
			FrozenAt:      &frozenAt,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get composed1 record for event_time 11")
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
		frozenAt := int64(1714176000)
		composed2 := util.GenerateStreamId("truflation_composed2")
		
		streamLocator := types.StreamLocator{
			StreamId:     composed2,
			DataProvider: deployerTruflation,
		}

		// Test event_time 10
		// Only primitive1 with weight 1.0, so value should be 102
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get composed2 record for event_time 10")
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
		frozenAt := int64(1714176000)
		parentComposed := util.GenerateStreamId("truflation_parent_composed")
		
		streamLocator := types.StreamLocator{
			StreamId:     parentComposed,
			DataProvider: deployerTruflation,
		}

		// Test event_time 10
		// composed1: 142.8 * weight 0.5 = 71.4
		// composed2: 102.0 * weight 0.5 = 51.0
		// Total: 122.4
		eventTime10 := int64(10)
		record10, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: streamLocator,
			FromTime:      &eventTime10,
			ToTime:        &eventTime10,
			FrozenAt:      &frozenAt,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "failed to get parentComposed record for event_time 10")
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