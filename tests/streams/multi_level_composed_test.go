package tests

import (
	"context"
	"testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"

	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

var (
	deployerMultiLevel = util.Unsafe_NewEthereumAddressFromString("0x51111111111111ABCDEF0123456789ABCDEF0123")

	primitive1     = util.GenerateStreamId("s_p1")
	primitive2     = util.GenerateStreamId("s_p2")
	primitive3     = util.GenerateStreamId("s_p3")
	primitive4     = util.GenerateStreamId("s_p4")
	composed1      = util.GenerateStreamId("s_c1")
	composed2      = util.GenerateStreamId("s_c2")
	parentComposed = util.GenerateStreamId("s_pc")
)

// TestMultiLevelComposedStreams tests multi level composed streams
// which tests mainly about composed -> composed streams.
func TestMultiLevelComposedStreams(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "multi_level_composed_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupMultiLevelComposedStreams(testMultiLevelFunc(t)),
		},
	}, testutils.GetTestOptions())
}

func setupMultiLevelComposedStreams(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, deployerMultiLevel.Bytes())
		start := int64(0)

		err := setup.CreateDataProvider(ctx, platform, deployerMultiLevel.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// 1. Deploy primitives
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitive1, DataProvider: deployerMultiLevel},
				},
				Data: []setup.InsertRecordInput{{EventTime: 100, Value: 5.0}, {EventTime: 300, Value: 9.0}},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying primitive1")
		}
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitive2, DataProvider: deployerMultiLevel},
				},
				Data: []setup.InsertRecordInput{{EventTime: 100, Value: 10.0}, {EventTime: 300, Value: 18.0}},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying primitive2")
		}
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitive3, DataProvider: deployerMultiLevel},
				},
				Data: []setup.InsertRecordInput{{EventTime: 100, Value: 15.0}, {EventTime: 300, Value: 25.0}},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying primitive3")
		}
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: primitive4, DataProvider: deployerMultiLevel},
				},
				Data: []setup.InsertRecordInput{{EventTime: 100, Value: 20.0}, {EventTime: 300, Value: 45.0}},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying primitive4")
		}

		// 2. Deploy composed streams
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: composed1, Height: 2}); err != nil {
			return errors.Wrap(err, "deploying composed1")
		}
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: composed2, Height: 2}); err != nil {
			return errors.Wrap(err, "deploying composed2")
		}
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: parentComposed, Height: 2}); err != nil {
			return errors.Wrap(err, "deploying parent composed")
		}

		// 3. Set static taxonomies
		// Composed 1 -> primitive1, primitive2
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{Platform: platform, StreamLocator: types.StreamLocator{StreamId: composed1, DataProvider: deployerMultiLevel}, DataProviders: []string{deployerMultiLevel.Address(), deployerMultiLevel.Address()}, StreamIds: []string{primitive1.String(), primitive2.String()}, Weights: []string{"0.3", "0.1"}, StartTime: &start, Height: 3}); err != nil {
			return errors.Wrap(err, "tax for composed1")
		}
		// Composed 2 -> primitive3, primitive4
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{Platform: platform, StreamLocator: types.StreamLocator{StreamId: composed2, DataProvider: deployerMultiLevel}, DataProviders: []string{deployerMultiLevel.Address(), deployerMultiLevel.Address()}, StreamIds: []string{primitive3.String(), primitive4.String()}, Weights: []string{"0.2", "0.4"}, StartTime: &start, Height: 3}); err != nil {
			return errors.Wrap(err, "tax for composed2")
		}
		// Parent Composed -> composed 1, composed 2
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{Platform: platform, StreamLocator: types.StreamLocator{StreamId: parentComposed, DataProvider: deployerMultiLevel}, DataProviders: []string{deployerMultiLevel.Address(), deployerMultiLevel.Address()}, StreamIds: []string{composed1.String(), composed2.String()}, Weights: []string{"0.4", "0.6"}, StartTime: &start, Height: 3}); err != nil {
			return errors.Wrap(err, "tax for composed2")
		}

		return testFn(ctx, platform)
	}
}

func testMultiLevelFunc(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		multiLevelLocator := types.StreamLocator{StreamId: parentComposed, DataProvider: deployerSharedComplex}
		dateFrom := int64(100)
		dateTo := int64(500)

		// Composed 1 Index
		result1, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: types.StreamLocator{StreamId: composed1, DataProvider: deployerSharedComplex},
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndex")
		}
		expected1 := `
		| event_time | value  |
		| ---------- | ------ |
		| 100        | 100.000000000000000000 |
		| 300        | 180.000000000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result1,
			Expected: expected1,
		})

		// Composed 2 Index
		result2, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: types.StreamLocator{StreamId: composed2, DataProvider: deployerSharedComplex},
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndex")
		}
		expected2 := `
		| event_time | value  |
		| ---------- | ------ |
		| 100        | 100.000000000000000000 |
		| 300        | 205.555555555555555555 |
		`
		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result2,
			Expected: expected2,
		})

		// Parent Composed Index
		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: multiLevelLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error in testComplexComposedIndex")
		}

		expected := `
		| event_time | value  |
		| ---------- | ------ |
		| 100        | 100.000000000000000000 |
		| 300        | 195.333333333333333333 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}
