/*
CATEGORY STREAMS TEST SUITE

This test file covers the category streams functionality:
- Testing the get_category_streams action which retrieves all substreams of a given stream
- Testing different time windows for stream hierarchies
*/

package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const (
	rootStreamName = "1c"
)

var rootStreamId = util.GenerateStreamId(rootStreamName)

// Helper function to get category streams and assert results against expected
func getCategoryAndAssert(t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	fromTime, toTime *int64, expectedTable string, timeDescription string) error {

	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address")
	}

	// Get substreams at specified time
	result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
		Platform:     platform,
		DataProvider: deployer.Address(),
		StreamId:     rootStreamId.String(),
		ActiveFrom:   fromTime,
		ActiveTo:     toTime,
	})

	if err != nil {
		return errors.Wrapf(err, "error getting substreams %s", timeDescription)
	}

	table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
		Actual:   result,
		Expected: expectedTable,
		ColumnTransformers: map[string]func(string) string{
			"stream_id": func(column string) string {
				s := util.GenerateStreamId(column)
				return s.String()
			},
		},
		SortColumns: []string{"stream_id"},
	})
	return nil
}

// Helper function to set taxonomy relationships between parent and children at a specific time
func setTaxonomyAtTime(ctx context.Context, platform *kwilTesting.Platform, deployer util.EthereumAddress,
	parent string, children []string, startTime int64) error {
	parentId := util.GenerateStreamId(parent)

	// Prepare arrays for SetTaxonomy
	dataProviders := make([]string, len(children))
	streamIds := make([]string, len(children))
	weights := make([]string, len(children))

	for i, child := range children {
		childId := util.GenerateStreamId(child)
		dataProviders[i] = deployer.Address()
		streamIds[i] = childId.String()
		weights[i] = "1"
	}

	err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform: platform,
		StreamLocator: types.StreamLocator{
			StreamId:     parentId,
			DataProvider: deployer,
		},
		DataProviders: dataProviders,
		StreamIds:     streamIds,
		Weights:       weights,
		StartTime:     &startTime,
	})
	if err != nil {
		return errors.Wrapf(err, "error creating taxonomy for %s at time %d", parent, startTime)
	}

	return nil
}

func TestCategoryStreams(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "category_streams_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithCategoryTestSetup(testGetAllSubstreams(t)),
			WithCategoryTestSetup(testGetSubstreamsAtTime0(t)),
			WithCategoryTestSetup(testGetSubstreamsAtTime5(t)),
			WithCategoryTestSetup(testGetSubstreamsAtTime6(t)),
			WithCategoryTestSetup(testGetSubstreamsAtTime10(t)),
			WithCategoryTestSetup(testGetSubstreamsTimeRange6To10(t)),
		},
	}, testutils.GetTestOptions())
}

// WithCategoryTestSetup is a helper function that sets up the test environment with streams and taxonomies
func WithCategoryTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create all streams
		streams := []string{
			"1c",
			"1.1c",
			"1.1.1p",
			"1.1.2p",
			"1.2c",
			"1.2.1p",
			"1.3p",
			"1.4c",
			"1.5p",
		}

		for _, streamName := range streams {
			streamId := util.GenerateStreamId(streamName)
			// Create a test stream
			err := setup.CreateStream(ctx, platform, setup.StreamInfo{
				Locator: types.StreamLocator{
					StreamId:     streamId,
					DataProvider: deployer,
				},
				Type: setup.ContractTypeComposed,
			})
			if err != nil {
				return errors.Wrapf(err, "error creating stream %s", streamName)
			}
		}

		// Setup taxonomies for time 0
		// Group taxonomies by parent stream
		taxonomiesByParent := map[string][]string{
			"1c":   {"1.1c", "1.2c", "1.3p", "1.4c"},
			"1.1c": {"1.1.1p", "1.1.2p"},
			"1.2c": {"1.2.1p"},
		}

		// Set taxonomies for each parent at time 0
		for parent, children := range taxonomiesByParent {
			if err := setTaxonomyAtTime(ctx, platform, deployer, parent, children, 0); err != nil {
				return err
			}
		}

		// Setup taxonomies for time 5
		taxonomiesByParentTime5 := map[string][]string{
			"1c":   {"1.1c"},
			"1.1c": {"1.1.1p"},
		}

		// Set taxonomies for each parent at time 5
		for parent, children := range taxonomiesByParentTime5 {
			if err := setTaxonomyAtTime(ctx, platform, deployer, parent, children, 5); err != nil {
				return err
			}
		}

		// Setup taxonomies for time 6 (to be disabled)
		// TODO: Uncomment and implement this when disabling taxonomies is supported
		/*
			taxonomiesByParentTime6 := map[string][]string{
				"1c": {"1.1c"},
			}

			// Set taxonomies for each parent at time 6
			for parent, children := range taxonomiesByParentTime6 {
				// First create the taxonomy
				if err := setTaxonomyAtTime(ctx, platform, deployer, parent, children, 6); err != nil {
					return err
				}

				// Get the taxonomy version - this is pseudo-code as we would need to retrieve the ID
				// taxonomyVersion := "some-id-for-the-taxonomy"

				// Then disable it
				// This is a pseudo-code example of how we would disable the taxonomy
				// when the functionality is supported
				err := procedure.DisableTaxonomy(ctx, procedure.DisableTaxonomyInput{
					Platform:   platform,
					TaxonomyId: taxonomyVersion,
				})
				if err != nil {
					return errors.Wrapf(err, "error disabling taxonomy for %s at time 6", parent)
				}
			}
		*/

		// Setup taxonomies for time 10
		taxonomiesByParentTime10 := map[string][]string{
			"1c": {"1.5p"},
		}

		// Set taxonomies for each parent at time 10
		for parent, children := range taxonomiesByParentTime10 {
			if err := setTaxonomyAtTime(ctx, platform, deployer, parent, children, 10); err != nil {
				return err
			}
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

// Test getting all substreams without time constraints
func testGetAllSubstreams(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		expected := `
		| data_provider                              | stream_id |
		|--------------------------------------------|-----------|
		| 0x0000000000000000000000000000000000000000 | 1c        |
		| 0x0000000000000000000000000000000000000000 | 1.1c      |
		| 0x0000000000000000000000000000000000000000 | 1.1.1p    |
		| 0x0000000000000000000000000000000000000000 | 1.1.2p    |
		| 0x0000000000000000000000000000000000000000 | 1.2c      |
		| 0x0000000000000000000000000000000000000000 | 1.2.1p    |
		| 0x0000000000000000000000000000000000000000 | 1.3p      |
		| 0x0000000000000000000000000000000000000000 | 1.4c      |
		| 0x0000000000000000000000000000000000000000 | 1.5p      |
		`

		return getCategoryAndAssert(t, ctx, platform, nil, nil, expected, "without time constraints")
	}
}

// Test getting substreams at time 0
func testGetSubstreamsAtTime0(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get substreams at time 0
		activeFrom := int64(0)
		activeTo := int64(0)

		expected := `
		| data_provider                              | stream_id |
		|--------------------------------------------|-----------|
		| 0x0000000000000000000000000000000000000000 | 1c        |
		| 0x0000000000000000000000000000000000000000 | 1.1c      |
		| 0x0000000000000000000000000000000000000000 | 1.1.1p    |
		| 0x0000000000000000000000000000000000000000 | 1.1.2p    |
		| 0x0000000000000000000000000000000000000000 | 1.2c      |
		| 0x0000000000000000000000000000000000000000 | 1.2.1p    |
		| 0x0000000000000000000000000000000000000000 | 1.3p      |
		| 0x0000000000000000000000000000000000000000 | 1.4c      |
		`

		return getCategoryAndAssert(t, ctx, platform, &activeFrom, &activeTo, expected, "at time 0")
	}
}

// Test getting substreams at time 5
func testGetSubstreamsAtTime5(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get substreams at time 5
		activeFrom := int64(5)
		activeTo := int64(5)

		expected := `
		| data_provider                              | stream_id |
		|--------------------------------------------|-----------|
		| 0x0000000000000000000000000000000000000000 | 1c        |
		| 0x0000000000000000000000000000000000000000 | 1.1c      |
		| 0x0000000000000000000000000000000000000000 | 1.1.1p    |
		`

		return getCategoryAndAssert(t, ctx, platform, &activeFrom, &activeTo, expected, "at time 5")
	}
}

// Test getting substreams at time 6
func testGetSubstreamsAtTime6(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get substreams at time 6
		activeFrom := int64(6)
		activeTo := int64(6)

		expected := `
		| data_provider                              | stream_id |
		|--------------------------------------------|-----------|
		| 0x0000000000000000000000000000000000000000 | 1c        |
		| 0x0000000000000000000000000000000000000000 | 1.1c      |
		| 0x0000000000000000000000000000000000000000 | 1.1.1p    |
		`

		return getCategoryAndAssert(t, ctx, platform, &activeFrom, &activeTo, expected, "at time 6")
	}
}

// Test getting substreams at time 10
func testGetSubstreamsAtTime10(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get substreams at time 10
		activeFrom := int64(10)
		activeTo := int64(10)

		expected := `
		| data_provider                              | stream_id |
		|--------------------------------------------|-----------|
		| 0x0000000000000000000000000000000000000000 | 1c        |
		| 0x0000000000000000000000000000000000000000 | 1.5p      |
		`

		return getCategoryAndAssert(t, ctx, platform, &activeFrom, &activeTo, expected, "at time 10")
	}
}

// Test getting substreams in time range 6 to 10
func testGetSubstreamsTimeRange6To10(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get substreams in time range 6 to 10
		activeFrom := int64(6)
		activeTo := int64(10)

		expected := `
		| data_provider                              | stream_id |
		|--------------------------------------------|-----------|
		| 0x0000000000000000000000000000000000000000 | 1c        |
		| 0x0000000000000000000000000000000000000000 | 1.1c      |
		| 0x0000000000000000000000000000000000000000 | 1.1.1p    |
		| 0x0000000000000000000000000000000000000000 | 1.5p      |
		`

		return getCategoryAndAssert(t, ctx, platform, &activeFrom, &activeTo, expected, "in time range 6 to 10")
	}
}
