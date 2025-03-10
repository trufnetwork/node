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
	dataProvider   = "provider1"
)

var rootStreamId = util.GenerateStreamId(rootStreamName)

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
			_, err := setup.CreateStream(ctx, platform, setup.StreamInfo{
				Locator: types.StreamLocator{
					StreamId:     streamId,
					DataProvider: deployer,
				},
				Type: setup.ContractTypePrimitive,
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
				StartTime:     0, // Time 0
			})
			if err != nil {
				return errors.Wrapf(err, "error creating taxonomy for %s at time 0", parent)
			}
		}

		// Setup taxonomies for time 5
		taxonomiesByParentTime5 := map[string][]string{
			"1c":   {"1.1c"},
			"1.1c": {"1.1.1p"},
		}

		// Set taxonomies for each parent at time 5
		for parent, children := range taxonomiesByParentTime5 {
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
				StartTime:     5, // Time 5
			})
			if err != nil {
				return errors.Wrapf(err, "error creating taxonomy for %s at time 5", parent)
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

				// First create the taxonomy
				result, err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
					Platform: platform,
					StreamLocator: types.StreamLocator{
						StreamId:     parentId,
						DataProvider: deployer,
					},
					DataProviders: dataProviders,
					StreamIds:     streamIds,
					Weights:       weights,
					StartTime:     6, // Time 6
				})
				if err != nil {
					return errors.Wrapf(err, "error creating taxonomy for %s at time 6", parent)
				}

				// Then disable it
				// This is a pseudo-code example of how we would disable the taxonomy
				// when the functionality is supported
				err = procedure.DisableTaxonomy(ctx, procedure.DisableTaxonomyInput{
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
				StartTime:     10, // Time 10
			})
			if err != nil {
				return errors.Wrapf(err, "error creating taxonomy for %s at time 10", parent)
			}
		}

		// Run the actual test function
		return testFn(ctx, platform)
	}
}

// Test getting all substreams without time constraints
func testGetAllSubstreams(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get all substreams
		result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     rootStreamName,
			ActiveFrom:   nil,
			ActiveTo:     nil,
		})

		if err != nil {
			return errors.Wrap(err, "error getting all substreams")
		}

		expected := `
		| data_provider | stream_id |
		|---------------|-----------|
		| provider1     | 1c        |
		| provider1     | 1.1c      |
		| provider1     | 1.1.1p    |
		| provider1     | 1.1.2p    |
		| provider1     | 1.2c      |
		| provider1     | 1.2.1p    |
		| provider1     | 1.3p      |
		| provider1     | 1.4c      |
		| provider1     | 1.5p      |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

// Test getting substreams at time 0
func testGetSubstreamsAtTime0(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get substreams at time 0
		activeFrom := int64(0)
		activeTo := int64(0)
		result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     rootStreamName,
			ActiveFrom:   &activeFrom,
			ActiveTo:     &activeTo,
		})

		if err != nil {
			return errors.Wrap(err, "error getting substreams at time 0")
		}

		expected := `
		| data_provider | stream_id |
		|---------------|-----------|
		| provider1     | 1c        |
		| provider1     | 1.1c      |
		| provider1     | 1.1.1p    |
		| provider1     | 1.1.2p    |
		| provider1     | 1.2c      |
		| provider1     | 1.2.1p    |
		| provider1     | 1.3p      |
		| provider1     | 1.4c      |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

// Test getting substreams at time 5
func testGetSubstreamsAtTime5(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get substreams at time 5
		activeFrom := int64(5)
		activeTo := int64(5)
		result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     rootStreamName,
			ActiveFrom:   &activeFrom,
			ActiveTo:     &activeTo,
		})

		if err != nil {
			return errors.Wrap(err, "error getting substreams at time 5")
		}

		expected := `
		| data_provider | stream_id |
		|---------------|-----------|
		| provider1     | 1c        |
		| provider1     | 1.1c      |
		| provider1     | 1.1.1p    |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

// Test getting substreams at time 6
func testGetSubstreamsAtTime6(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get substreams at time 6
		activeFrom := int64(6)
		activeTo := int64(6)
		result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     rootStreamName,
			ActiveFrom:   &activeFrom,
			ActiveTo:     &activeTo,
		})

		if err != nil {
			return errors.Wrap(err, "error getting substreams at time 6")
		}

		expected := `
		| data_provider | stream_id |
		|---------------|-----------|
		| provider1     | 1c        |
		| provider1     | 1.1c      |
		| provider1     | 1.1.1p    |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

// Test getting substreams at time 10
func testGetSubstreamsAtTime10(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get substreams at time 10
		activeFrom := int64(10)
		activeTo := int64(10)
		result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     rootStreamName,
			ActiveFrom:   &activeFrom,
			ActiveTo:     &activeTo,
		})

		if err != nil {
			return errors.Wrap(err, "error getting substreams at time 10")
		}

		expected := `
		| data_provider | stream_id |
		|---------------|-----------|
		| provider1     | 1c        |
		| provider1     | 1.5p      |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}

// Test getting substreams in time range 6 to 10
func testGetSubstreamsTimeRange6To10(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}

		// Get substreams in time range 6 to 10
		activeFrom := int64(6)
		activeTo := int64(10)
		result, err := procedure.GetCategoryStreams(ctx, procedure.GetCategoryStreamsInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     rootStreamName,
			ActiveFrom:   &activeFrom,
			ActiveTo:     &activeTo,
		})

		if err != nil {
			return errors.Wrap(err, "error getting substreams in time range 6 to 10")
		}

		expected := `
		| data_provider | stream_id |
		|---------------|-----------|
		| provider1     | 1c        |
		| provider1     | 1.1c      |
		| provider1     | 1.1.1p    |
		| provider1     | 1.5p      |
		`

		table.AssertResultRowsEqualMarkdownTable(t, result, expected)

		return nil
	}
}
