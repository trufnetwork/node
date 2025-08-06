package tests

import (
	"context"
	"fmt"
	"strings"
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

// Column index constants for taxonomy query results
const (
	colDataProvider      = 0
	colStreamId          = 1
	colChildDataProvider = 2
	colChildStreamId     = 3
	colWeight           = 4
	colCreatedAt        = 5
	colGroupSequence    = 6
	colStartTime        = 7
)

// TestTaxonomyQueryActions tests the new taxonomy query actions
func TestTaxonomyQueryActions(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "taxonomy_query_actions_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testListTaxonomiesByHeight(t),
			testListTaxonomiesByHeightWithLatestOnly(t),
			testListTaxonomiesByHeightPagination(t),
			testGetTaxonomiesForStreams(t),
			testGetTaxonomiesForStreamsLatestOnly(t),
			// Comprehensive column testing
			testTaxonomyQueryResultColumns(t),
			// Error condition tests
			testListTaxonomiesByHeightInvalidRange(t),
			testListTaxonomiesByHeightInvalidPagination(t),
			testGetTaxonomiesForStreamsMismatchedArrays(t),
		},
	}, testutils.GetTestOptionsWithCache().Options)
}

// testListTaxonomiesByHeight tests basic height-based taxonomy querying
func testListTaxonomiesByHeight(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create test streams
		composedStreamId := util.GenerateStreamId("composed_test_1")
		child1StreamId := util.GenerateStreamId("child_test_1")
		child2StreamId := util.GenerateStreamId("child_test_2")

		// Setup composed stream
		err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   100, // Create at height 100
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		// Setup child streams
		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   100,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     child1StreamId,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up first child stream")
		}

		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   100,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     child2StreamId,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up second child stream")
		}

		// Create taxonomies at different heights
		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// First taxonomy at height 150
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{child1StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
			Height:        150,
		})
		if err != nil {
			return errors.Wrap(err, "error setting first taxonomy")
		}

		// Second taxonomy at height 200
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{child2StreamId.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
			Height:        200,
		})
		if err != nil {
			return errors.Wrap(err, "error setting second taxonomy")
		}

		// Test querying taxonomies by height range
		fromHeight := int64(140)
		toHeight := int64(190)

		result, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: &fromHeight,
			ToHeight:   &toHeight,
			Limit:      nil,
			Offset:     nil,
			LatestOnly: nil,
			Height:     250,
		})
		if err != nil {
			return errors.Wrap(err, "error listing taxonomies by height")
		}

		// Should return only the first taxonomy (created at height 150)
		expected := `
		| data_provider | stream_id | child_data_provider | child_stream_id | weight | created_at | group_sequence | start_time |
		|---------------|-----------|---------------------|-----------------|--------|------------|----------------|------------|
		| 0x0000000000000000000000000000000000000123 | ` + composedStreamId.String() + ` | 0x0000000000000000000000000000000000000123 | ` + child1StreamId.String() + ` | 1.000000000000000000 | 150 | 1 | 0 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		return nil
	}
}

// testListTaxonomiesByHeightWithLatestOnly tests latest_only functionality with multiple child streams
func testListTaxonomiesByHeightWithLatestOnly(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000124")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create test streams
		composedStreamId := util.GenerateStreamId("composed_test_2")
		childStreamId1 := util.GenerateStreamId("child_test_3a")
		childStreamId2 := util.GenerateStreamId("child_test_3b")

		// Setup streams
		err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   300,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   300,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     childStreamId1,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up first child stream")
		}

		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   300,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     childStreamId2,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up second child stream")
		}

		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Create first taxonomy with single child
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{childStreamId1.String()},
			Weights:       []string{"1.0"},
			StartTime:     nil,
			Height:        350,
		})
		if err != nil {
			return errors.Wrap(err, "error setting first taxonomy")
		}

		// Create second taxonomy with multiple children (this should be returned by latest_only)
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address(), deployer.Address()},
			StreamIds:     []string{childStreamId1.String(), childStreamId2.String()},
			Weights:       []string{"0.6", "0.4"},
			StartTime:     nil,
			Height:        400,
		})
		if err != nil {
			return errors.Wrap(err, "error setting second taxonomy")
		}

		// Test with latest_only = true
		latestOnly := true
		result, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: nil, // Get all
			ToHeight:   nil, // Get all
			Limit:      nil,
			Offset:     nil,
			LatestOnly: &latestOnly,
			Height:     450,
		})
		if err != nil {
			return errors.Wrap(err, "error listing taxonomies with latest_only")
		}

		// Should return both children from the latest taxonomy (created at height 400, group_sequence 2)
		if len(result) != 2 {
			return errors.Errorf("expected 2 results for latest taxonomy with multiple children, got %d", len(result))
		}

		// Verify all results are from the latest taxonomy
		for _, row := range result {
			if row[colCreatedAt] != "400" {
				return errors.Errorf("expected created_at=400, got %s", row[colCreatedAt])
			}
			if row[colGroupSequence] != "2" {
				return errors.Errorf("expected group_sequence=2, got %s", row[colGroupSequence])
			}
		}

		return nil
	}
}

// testListTaxonomiesByHeightPagination tests pagination functionality
func testListTaxonomiesByHeightPagination(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000125")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create multiple taxonomies for pagination testing
		for i := 1; i <= 3; i++ {
			composedStreamId := util.GenerateStreamId(fmt.Sprintf("composed_paging_%d", i))
			childStreamId := util.GenerateStreamId(fmt.Sprintf("child_paging_%d", i))

			// Setup streams
			err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
				Platform: platform,
				StreamId: composedStreamId,
				Height:   500 + int64(i)*10,
			})
			if err != nil {
				return errors.Wrapf(err, "error setting up composed stream %d", i)
			}

			err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
				Platform: platform,
				Height:   500 + int64(i)*10,
				PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
					PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
						StreamLocator: types.StreamLocator{
							StreamId:     childStreamId,
							DataProvider: deployer,
						},
					},
					Data: []setup.InsertRecordInput{},
				},
			})
			if err != nil {
				return errors.Wrapf(err, "error setting up child stream %d", i)
			}

			// Create taxonomy
			composedStreamLocator := types.StreamLocator{
				StreamId:     composedStreamId,
				DataProvider: deployer,
			}

			err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
				Platform:      platform,
				StreamLocator: composedStreamLocator,
				DataProviders: []string{deployer.Address()},
				StreamIds:     []string{childStreamId.String()},
				Weights:       []string{"1.0"},
				StartTime:     nil,
				Height:        600 + int64(i)*10,
			})
			if err != nil {
				return errors.Wrapf(err, "error setting taxonomy %d", i)
			}
		}

		// Test pagination - get first 2 results
		limit := 2
		offset := 0
		result, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: nil,
			ToHeight:   nil,
			Limit:      &limit,
			Offset:     &offset,
			LatestOnly: nil,
			Height:     700,
		})
		if err != nil {
			return errors.Wrap(err, "error listing taxonomies with pagination")
		}

		// Should return first 2 taxonomies ordered by created_at ASC
		if len(result) != 2 {
			return errors.Errorf("expected 2 results, got %d", len(result))
		}

		// Test second page
		offset = 2
		result2, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: nil,
			ToHeight:   nil,
			Limit:      &limit,
			Offset:     &offset,
			LatestOnly: nil,
			Height:     700,
		})
		if err != nil {
			return errors.Wrap(err, "error listing taxonomies page 2")
		}

		// Should return remaining results
		if len(result2) == 0 {
			return errors.New("expected at least 1 result on page 2")
		}

		return nil
	}
}

// testGetTaxonomiesForStreams tests batch stream querying
func testGetTaxonomiesForStreams(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000126")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create test streams
		composedStreamId1 := util.GenerateStreamId("composed_batch_1")
		composedStreamId2 := util.GenerateStreamId("composed_batch_2")
		childStreamId := util.GenerateStreamId("child_batch")

		// Setup streams
		err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId1,
			Height:   800,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up first composed stream")
		}

		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId2,
			Height:   800,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up second composed stream")
		}

		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   800,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     childStreamId,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up child stream")
		}

		// Create taxonomies for both streams
		for i, streamId := range []util.StreamId{composedStreamId1, composedStreamId2} {
			composedStreamLocator := types.StreamLocator{
				StreamId:     streamId,
				DataProvider: deployer,
			}

			err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
				Platform:      platform,
				StreamLocator: composedStreamLocator,
				DataProviders: []string{deployer.Address()},
				StreamIds:     []string{childStreamId.String()},
				Weights:       []string{fmt.Sprintf("%.1f", float64(i+1)*0.5)},
				StartTime:     nil,
				Height:        850 + int64(i)*10,
			})
			if err != nil {
				return errors.Wrapf(err, "error setting taxonomy %d", i+1)
			}
		}

		// Test batch querying
		result, err := procedure.GetTaxonomiesForStreams(ctx, procedure.GetTaxonomiesForStreamsInput{
			Platform: platform,
			DataProviders: []string{
				deployer.Address(),
				deployer.Address(),
			},
			StreamIds: []string{
				composedStreamId1.String(),
				composedStreamId2.String(),
			},
			LatestOnly: nil,
			Height:     900,
		})
		if err != nil {
			return errors.Wrap(err, "error getting taxonomies for streams")
		}

		// Should return taxonomies for both streams
		if len(result) != 2 {
			return errors.Errorf("expected 2 results, got %d", len(result))
		}

		return nil
	}
}

// testGetTaxonomiesForStreamsLatestOnly tests batch querying with latest_only
func testGetTaxonomiesForStreamsLatestOnly(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000127")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create test stream
		composedStreamId := util.GenerateStreamId("composed_batch_latest")
		childStreamId := util.GenerateStreamId("child_batch_latest")

		// Setup streams
		err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   1000,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1000,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     childStreamId,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up child stream")
		}

		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Create multiple taxonomies for the same stream
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{childStreamId.String()},
			Weights:       []string{"0.3"},
			StartTime:     nil,
			Height:        1050,
		})
		if err != nil {
			return errors.Wrap(err, "error setting first taxonomy")
		}

		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{childStreamId.String()},
			Weights:       []string{"0.7"},
			StartTime:     nil,
			Height:        1100,
		})
		if err != nil {
			return errors.Wrap(err, "error setting second taxonomy")
		}

		// Test batch querying with latest_only = true
		latestOnly := true
		result, err := procedure.GetTaxonomiesForStreams(ctx, procedure.GetTaxonomiesForStreamsInput{
			Platform:      platform,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{composedStreamId.String()},
			LatestOnly:    &latestOnly,
			Height:        1150,
		})
		if err != nil {
			return errors.Wrap(err, "error getting taxonomies for streams with latest_only")
		}

		// Should return only the latest taxonomy
		if len(result) != 1 {
			return errors.Errorf("expected 1 result, got %d", len(result))
		}

		// Verify it's the latest one (weight 0.7)
		if result[0][colWeight] != "0.700000000000000000" {
			return errors.Errorf("expected weight 0.700000000000000000, got %s", result[0][colWeight])
		}

		return nil
	}
}

// testTaxonomyQueryResultColumns tests that all 8 columns are returned correctly with proper values
func testTaxonomyQueryResultColumns(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000199")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create test streams
		composedStreamId := util.GenerateStreamId("column_test_composed")
		childStreamId := util.GenerateStreamId("column_test_child")

		// Setup streams
		err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: composedStreamId,
			Height:   2000,
		})
		if err != nil {
			return errors.Wrap(err, "error setting up composed stream")
		}

		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   2000,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     childStreamId,
						DataProvider: deployer,
					},
				},
				Data: []setup.InsertRecordInput{},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up child stream")
		}

		composedStreamLocator := types.StreamLocator{
			StreamId:     composedStreamId,
			DataProvider: deployer,
		}

		// Create taxonomy with known values
		expectedWeight := "0.753000000000000000"
		expectedStartTime := int64(0)
		expectedHeight := int64(2100)

		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: composedStreamLocator,
			DataProviders: []string{deployer.Address()},
			StreamIds:     []string{childStreamId.String()},
			Weights:       []string{"0.753"},
			StartTime:     &expectedStartTime,
			Height:        expectedHeight,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy")
		}

		// Query the taxonomy
		result, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: nil,
			ToHeight:   nil,
			Limit:      nil,
			Offset:     nil,
			LatestOnly: nil,
			Height:     2200,
		})
		if err != nil {
			return errors.Wrap(err, "error listing taxonomies")
		}

		if len(result) == 0 {
			return errors.New("expected at least 1 taxonomy result")
		}

		// Test all 8 columns with expected values
		row := result[0]

		// Column 0: data_provider (TEXT)
		if row[colDataProvider] != deployer.Address() {
			return errors.Errorf("col %d (data_provider): expected %s, got %s", 
				colDataProvider, deployer.Address(), row[colDataProvider])
		}

		// Column 1: stream_id (TEXT)  
		if row[colStreamId] != composedStreamId.String() {
			return errors.Errorf("col %d (stream_id): expected %s, got %s", 
				colStreamId, composedStreamId.String(), row[colStreamId])
		}

		// Column 2: child_data_provider (TEXT)
		if row[colChildDataProvider] != deployer.Address() {
			return errors.Errorf("col %d (child_data_provider): expected %s, got %s", 
				colChildDataProvider, deployer.Address(), row[colChildDataProvider])
		}

		// Column 3: child_stream_id (TEXT)
		if row[colChildStreamId] != childStreamId.String() {
			return errors.Errorf("col %d (child_stream_id): expected %s, got %s", 
				colChildStreamId, childStreamId.String(), row[colChildStreamId])
		}

		// Column 4: weight (NUMERIC as string)
		if row[colWeight] != expectedWeight {
			return errors.Errorf("col %d (weight): expected %s, got %s", 
				colWeight, expectedWeight, row[colWeight])
		}

		// Column 5: created_at (INT8 as string)
		if row[colCreatedAt] != fmt.Sprintf("%d", expectedHeight) {
			return errors.Errorf("col %d (created_at): expected %d, got %s", 
				colCreatedAt, expectedHeight, row[colCreatedAt])
		}

		// Column 6: group_sequence (INT8 as string)
		expectedGroupSequence := "1" // First taxonomy for this stream
		if row[colGroupSequence] != expectedGroupSequence {
			return errors.Errorf("col %d (group_sequence): expected %s, got %s", 
				colGroupSequence, expectedGroupSequence, row[colGroupSequence])
		}

		// Column 7: start_time (INT8 as string)
		if row[colStartTime] != fmt.Sprintf("%d", expectedStartTime) {
			return errors.Errorf("col %d (start_time): expected %d, got %s", 
				colStartTime, expectedStartTime, row[colStartTime])
		}

		// Verify row has exactly 8 columns
		if len(row) != 8 {
			return errors.Errorf("expected exactly 8 columns, got %d", len(row))
		}

		return nil
	}
}

// testListTaxonomiesByHeightInvalidRange tests error handling for invalid height ranges
func testListTaxonomiesByHeightInvalidRange(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000200")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Test case 1: from_height > to_height should fail
		fromHeight := int64(1000)
		toHeight := int64(500)

		_, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: &fromHeight,
			ToHeight:   &toHeight,
			Limit:      nil,
			Offset:     nil,
			LatestOnly: nil,
			Height:     1100,
		})

		if err == nil {
			return errors.New("expected error for invalid height range (from_height > to_height), but got none")
		}

		// Verify the error message contains expected text
		expectedError := "Invalid height range"
		if !strings.Contains(err.Error(), expectedError) {
			return errors.Errorf("expected error message to contain '%s', got: %s", expectedError, err.Error())
		}

		return nil
	}
}

// testListTaxonomiesByHeightInvalidPagination tests error handling for invalid pagination parameters  
func testListTaxonomiesByHeightInvalidPagination(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000201")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Test case 1: negative limit should be handled gracefully
		negativeLimit := -10
		result, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: nil,
			ToHeight:   nil,
			Limit:      &negativeLimit,
			Offset:     nil,
			LatestOnly: nil,
			Height:     1200,
		})

		// Should not error since we now validate and set to 0 instead of erroring
		if err != nil {
			return errors.Wrap(err, "unexpected error with negative limit")
		}

		// With limit = 0 (converted from negative), should return empty results
		if len(result) != 0 {
			return errors.Errorf("expected empty results with negative limit (converted to 0), got %d results", len(result))
		}

		// Test case 2: negative offset should be handled gracefully
		negativeOffset := -5
		result2, err := procedure.ListTaxonomiesByHeight(ctx, procedure.ListTaxonomiesByHeightInput{
			Platform:   platform,
			FromHeight: nil,
			ToHeight:   nil,
			Limit:      nil,
			Offset:     &negativeOffset,
			LatestOnly: nil,
			Height:     1200,
		})

		// Should not error, but should behave like offset 0
		if err != nil {
			return errors.Wrap(err, "unexpected error with negative offset")
		}

		// Negative offset should be treated as 0, so results should be same as no offset
		_ = result2 // We just verify it doesn't crash

		return nil
	}
}

// testGetTaxonomiesForStreamsMismatchedArrays tests error handling for mismatched array lengths
func testGetTaxonomiesForStreamsMismatchedArrays(t *testing.T) kwilTesting.TestFunc {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Setup deployer
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000202")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Test case: mismatched array lengths should fail
		dataProviders := []string{
			deployer.Address(),
			deployer.Address(),
		}
		streamIds := []string{
			"stream1", // Only one stream ID, but two data providers
		}

		_, err := procedure.GetTaxonomiesForStreams(ctx, procedure.GetTaxonomiesForStreamsInput{
			Platform:      platform,
			DataProviders: dataProviders,
			StreamIds:     streamIds,
			LatestOnly:    nil,
			Height:        1300,
		})

		if err == nil {
			return errors.New("expected error for mismatched array lengths, but got none")
		}

		// Verify the error message contains expected text
		expectedError := "must have the same length"
		if !strings.Contains(err.Error(), expectedError) {
			return errors.Errorf("expected error message to contain '%s', got: %s", expectedError, err.Error())
		}

		return nil
	}
}
