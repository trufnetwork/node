package tests

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/kwilteam/kwil-db/common"
	kwilTypes "github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
	 Varying Taxonomy Weights Test

	This test verifies the correct calculation of weights in a multi-level taxonomy hierarchy
	with weights changing over time.

	Test scenario:
	Stream A (composed)
	├─ Stream B (composed) - Initially 60% weight, changes to 80% on time 100, removed on time 300
	│  ├─ Stream D (primitive) - Initially 70% weight, changes to 50% on time 200
	│  └─ Stream E (primitive) - Initially 30% weight, changes to 50% on time 200
	└─ Stream C (primitive) - Initially 40% weight, changes to 20% on time 100, then 100% on time 300

	Key time points:
	- Time 0: Initial taxonomy setup
	- Time 100: First change for A (B: 60% → 80%, C: 40% → 20%)
	- Time 200: Change for B (D: 70% → 50%, E: 30% → 50%)
	- Time 300: Second change for A (B is removed, C gets 100%)
*/

// Define stream names for better readability
const (
	streamAName = "A_composed"
	streamBName = "B_composed"
	streamCName = "C_primitive"
	streamDName = "D_primitive"
	streamEName = "E_primitive"
)

// Define stream IDs based on names
var (
	streamAId = util.GenerateStreamId(streamAName)
	streamBId = util.GenerateStreamId(streamBName)
	streamCId = util.GenerateStreamId(streamCName)
	streamDId = util.GenerateStreamId(streamDName)
	streamEId = util.GenerateStreamId(streamEName)
)

// Time points for taxonomy changes
const (
	initialSetupTime = int64(0)
	firstChangeTime  = int64(100)
	secondChangeTime = int64(200)
	thirdChangeTime  = int64(300)
)

// Input structure for the GetAllWeightsForQuery function
type GetAllWeightsForQueryInput struct {
	Platform     *kwilTesting.Platform
	DataProvider string
	StreamId     string
	FromTime     int64
	ToTime       int64
}

// Result structure for the GetAllWeightsForQuery function
type WeightResult struct {
	DataProvider string
	StreamId     string
	StartTime    *int64
	EndTime      *int64
	Weight       kwilTypes.Decimal
}

// TaxonomyEntry represents a row from the taxonomy table
type TaxonomyEntry struct {
	ParentDataProvider string
	ParentStreamId     string
	ChildDataProvider  string
	ChildStreamId      string
	Weight             kwilTypes.Decimal
	StartTime          int64
}

// GetAllWeightsForQuery calls the get_all_weights_for_query procedure
func GetAllWeightsForQuery(ctx context.Context, input GetAllWeightsForQueryInput) ([]WeightResult, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in GetAllWeightsForQuery")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 0,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_all_weights_for_query", []any{
		input.DataProvider,
		input.StreamId,
		input.FromTime,
		input.ToTime,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error calling get_all_weights_for_query")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in get_all_weights_for_query result")
	}

	// Process results
	results := make([]WeightResult, 0, len(resultRows))
	for _, row := range resultRows {
		if len(row) != 5 {
			return nil, errors.Errorf("unexpected row format, expected 5 columns, got %d", len(row))
		}

		// Convert to appropriate types
		dataProvider, ok := row[0].(string)
		if !ok {
			return nil, errors.New("data_provider is not a string: " + fmt.Sprintf("real type: %T, value: %v", row[0], row[0]))
		}
		streamId, ok := row[1].(string)
		if !ok {
			return nil, errors.New("stream_id is not a string: " + fmt.Sprintf("real type: %T, value: %v", row[1], row[1]))
		}
		startTime, ok := row[2].(int64)
		if !ok {
			if row[2] != nil {
				return nil, errors.New("start_time is not an int64: " + fmt.Sprintf("real type: %T, value: %v", row[2], row[2]))
			}
		}
		endTime, ok := row[3].(int64)
		if !ok {
			if row[3] != nil {
				return nil, errors.New("end_time is not an int64: " + fmt.Sprintf("real type: %T, value: %v", row[3], row[3]))
			}
		}
		weight, ok := row[4].(*kwilTypes.Decimal)
		if !ok {
			return nil, errors.New("weight is not a *kwilTypes.Decimal: " + fmt.Sprintf("real type: %T, value: %v", row[4], row[4]))
		}

		results = append(results, WeightResult{
			DataProvider: dataProvider,
			StreamId:     streamId,
			StartTime:    &startTime,
			EndTime:      &endTime,
			Weight:       *weight,
		})
	}

	return results, nil
}

// TestAGGR07_VaryingTaxonomyWeights tests the correct calculation of weights in a multi-level taxonomy hierarchy
func TestAGGR07_VaryingTaxonomyWeights(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "aggr07_varying_taxonomy_weights_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAGGR07_VaryingTaxonomyWeights(t),
		},
	}, testutils.GetTestOptions())
}

// Helper function to set a taxonomy with specific weights
func setTaxonomyWithWeights(
	ctx context.Context,
	platform *kwilTesting.Platform,
	parentStreamName string,
	childrenInfo map[string]string, // map[streamName]weight
	startTime int64,
) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address")
	}

	// Get parent stream ID
	var parentId util.StreamId
	switch parentStreamName {
	case streamAName:
		parentId = streamAId
	case streamBName:
		parentId = streamBId
	default:
		return errors.Errorf("unknown parent stream name: %s", parentStreamName)
	}

	// Convert map to arrays for SetTaxonomy
	childDataProviders := make([]string, 0, len(childrenInfo))
	childStreamIds := make([]string, 0, len(childrenInfo))
	weights := make([]string, 0, len(childrenInfo))

	for childStreamName, weight := range childrenInfo {
		// Get child stream ID
		var childId util.StreamId
		switch childStreamName {
		case streamBName:
			childId = streamBId
		case streamCName:
			childId = streamCId
		case streamDName:
			childId = streamDId
		case streamEName:
			childId = streamEId
		default:
			return errors.Errorf("unknown child stream name: %s", childStreamName)
		}

		childDataProviders = append(childDataProviders, deployer.Address())
		childStreamIds = append(childStreamIds, childId.String())
		weights = append(weights, weight)
	}

	// Set the taxonomy
	err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform: platform,
		StreamLocator: types.StreamLocator{
			StreamId:     parentId,
			DataProvider: deployer,
		},
		DataProviders: childDataProviders,
		StreamIds:     childStreamIds,
		Weights:       weights,
		StartTime:     &startTime,
		Height:        0,
	})
	if err != nil {
		return errors.Wrapf(err, "error setting taxonomy for %s at time %d", parentStreamName, startTime)
	}

	return nil
}

// Helper function to visualize the taxonomy structure with weights
func visualizeTaxonomy(activeTime int64, results []WeightResult) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\nTaxonomy Structure at Time %d:\n", activeTime))
	sb.WriteString("Stream A (composed)\n")

	// Map to store stream weights by ID for easier lookup
	weightMap := make(map[string]float64)
	for _, r := range results {
		weight, _ := r.Weight.Float64()
		weightMap[r.StreamId] = weight
	}

	// Check if B is present in the taxonomy (it's removed at time 300)
	hasBStream := activeTime < thirdChangeTime

	// Determine weights for B and C based on the time
	var bWeight, cWeight float64
	if activeTime < firstChangeTime {
		// Initial setup
		bWeight, cWeight = 0.6, 0.4
	} else if activeTime < thirdChangeTime {
		// After first change
		bWeight, cWeight = 0.8, 0.2
	} else {
		// After third change
		bWeight, cWeight = 0.0, 1.0
	}

	// Determine weights for D and E based on the time
	var dWeight, eWeight float64
	if activeTime < secondChangeTime {
		// Initial setup for B's children
		dWeight, eWeight = 0.7, 0.3
	} else {
		// After second change
		dWeight, eWeight = 0.5, 0.5
	}

	// Calculate expected final weights
	var expectedDWeight, expectedEWeight float64
	if hasBStream {
		expectedDWeight = bWeight * dWeight
		expectedEWeight = bWeight * eWeight
	}

	// Format the visualization
	if hasBStream {
		sb.WriteString(fmt.Sprintf("├─ Stream B (%.2f)\n", bWeight))
		sb.WriteString(fmt.Sprintf("│  ├─ Stream D (%.2f * %.2f = %.4f) - Actual: %.6f\n",
			bWeight, dWeight, expectedDWeight, weightMap[streamDId.String()]))
		sb.WriteString(fmt.Sprintf("│  └─ Stream E (%.2f * %.2f = %.4f) - Actual: %.6f\n",
			bWeight, eWeight, expectedEWeight, weightMap[streamEId.String()]))
	}
	sb.WriteString(fmt.Sprintf("└─ Stream C (%.2f) - Actual: %.6f\n",
		cWeight, weightMap[streamCId.String()]))

	return sb.String()
}

// DumpTaxonomyData retrieves all taxonomy entries from the database for debugging
func DumpTaxonomyData(ctx context.Context, platform *kwilTesting.Platform) ([]TaxonomyEntry, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in DumpTaxonomyData")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 0,
		},
		TxID:   platform.Txid(),
		Signer: platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Direct SQL query to get taxonomy data
	var entries []TaxonomyEntry
	err = platform.Engine.Execute(engineContext, platform.DB,
		"SELECT data_provider, stream_id, child_data_provider, child_stream_id, weight, start_time FROM taxonomies ORDER BY stream_id, child_stream_id, start_time",
		nil,
		func(row *common.Row) error {
			if len(row.Values) != 6 {
				return errors.Errorf("unexpected row format, expected 6 columns, got %d", len(row.Values))
			}

			// Convert to appropriate types
			parentDataProvider, ok := row.Values[0].(string)
			if !ok {
				return errors.New("parent_data_provider is not a string")
			}
			parentStreamId, ok := row.Values[1].(string)
			if !ok {
				return errors.New("parent_stream_id is not a string")
			}
			childDataProvider, ok := row.Values[2].(string)
			if !ok {
				return errors.New("child_data_provider is not a string")
			}
			childStreamId, ok := row.Values[3].(string)
			if !ok {
				return errors.New("child_stream_id is not a string")
			}
			weight, ok := row.Values[4].(*kwilTypes.Decimal)
			if !ok {
				return errors.New("weight is not a *kwilTypes.Decimal")
			}
			startTime, ok := row.Values[5].(int64)
			if !ok {
				return errors.New("start_time is not an int64")
			}

			entries = append(entries, TaxonomyEntry{
				ParentDataProvider: parentDataProvider,
				ParentStreamId:     parentStreamId,
				ChildDataProvider:  childDataProvider,
				ChildStreamId:      childStreamId,
				Weight:             *weight,
				StartTime:          startTime,
			})
			return nil
		})

	if err != nil {
		return nil, errors.Wrap(err, "error querying taxonomy table")
	}

	return entries, nil
}

// FormatTaxonomyDump formats the taxonomy entries for debugging output
func FormatTaxonomyDump(entries []TaxonomyEntry) string {
	var sb strings.Builder
	sb.WriteString("\nRaw Taxonomy Data from Database:\n")
	sb.WriteString("+-----------------+----------------+----------------+---------------+--------+------------+\n")
	sb.WriteString("| Parent Provider | Parent Stream  | Child Provider | Child Stream | Weight | Start Time |\n")
	sb.WriteString("+-----------------+----------------+----------------+---------------+--------+------------+\n")

	for _, entry := range entries {
		// Format parent stream ID for readability
		parentStreamName := "Unknown"
		if entry.ParentStreamId == streamAId.String() {
			parentStreamName = streamAName
		} else if entry.ParentStreamId == streamBId.String() {
			parentStreamName = streamBName
		}

		// Format child stream ID for readability
		childStreamName := "Unknown"
		if entry.ChildStreamId == streamBId.String() {
			childStreamName = streamBName
		} else if entry.ChildStreamId == streamCId.String() {
			childStreamName = streamCName
		} else if entry.ChildStreamId == streamDId.String() {
			childStreamName = streamDName
		} else if entry.ChildStreamId == streamEId.String() {
			childStreamName = streamEName
		}

		// Format weight
		weightStr, _ := entry.Weight.Float64()

		sb.WriteString(fmt.Sprintf("| %-15s | %-14s | %-14s | %-13s | %.4f | %-10d |\n",
			entry.ParentDataProvider[:10]+"...",
			parentStreamName,
			entry.ChildDataProvider[:10]+"...",
			childStreamName,
			weightStr,
			entry.StartTime,
		))
	}

	sb.WriteString("+-----------------+----------------+----------------+---------------+--------+------------+----------+\n")
	return sb.String()
}

// testAGGR07_VaryingTaxonomyWeights implements the main test for varying taxonomy weights
func testAGGR07_VaryingTaxonomyWeights(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// 1. Create all streams
		streams := []string{
			streamAName,
			streamBName,
			streamCName,
			streamDName,
			streamEName,
		}

		for _, streamName := range streams {
			var streamId util.StreamId
			// Use the pre-defined stream IDs
			switch streamName {
			case streamAName:
				streamId = streamAId
			case streamBName:
				streamId = streamBId
			case streamCName:
				streamId = streamCId
			case streamDName:
				streamId = streamDId
			case streamEName:
				streamId = streamEId
			default:
				streamId = util.GenerateStreamId(streamName)
			}

			// Create a stream
			err := setup.CreateStream(ctx, platform, setup.StreamInfo{
				Locator: types.StreamLocator{
					StreamId:     streamId,
					DataProvider: deployer,
				},
				Type: setup.ContractTypeComposed, // We can use composed for all since we're not testing actual values
			})
			if err != nil {
				return errors.Wrapf(err, "error creating stream %s", streamName)
			}
		}

		// 2. Set up initial taxonomies (Time 0)
		// Stream A → B (60%), C (40%)
		err := setTaxonomyWithWeights(ctx, platform, streamAName, map[string]string{
			streamBName: "0.6",
			streamCName: "0.4",
		}, initialSetupTime)
		if err != nil {
			return err
		}

		// Stream B → D (70%), E (30%)
		err = setTaxonomyWithWeights(ctx, platform, streamBName, map[string]string{
			streamDName: "0.7",
			streamEName: "0.3",
		}, initialSetupTime)
		if err != nil {
			return err
		}

		// 3. Set up taxonomies at Time 100
		// Stream A → B (80%), C (20%)
		err = setTaxonomyWithWeights(ctx, platform, streamAName, map[string]string{
			streamBName: "0.8",
			streamCName: "0.2",
		}, firstChangeTime)
		if err != nil {
			return err
		}

		// 4. Set up taxonomies at Time 200
		// Stream B → D (50%), E (50%)
		err = setTaxonomyWithWeights(ctx, platform, streamBName, map[string]string{
			streamDName: "0.5",
			streamEName: "0.5",
		}, secondChangeTime)
		if err != nil {
			return err
		}

		// 5. Set up taxonomies at Time 300
		// Stream A → C (100%)
		err = setTaxonomyWithWeights(ctx, platform, streamAName, map[string]string{
			streamCName: "1.0",
		}, thirdChangeTime)
		if err != nil {
			return err
		}

		// After setting up all taxonomies, dump the raw taxonomy data for debugging
		taxonomyEntries, err := DumpTaxonomyData(ctx, platform)
		if err != nil {
			// Don't fail the test if we can't dump taxonomy data, just log a warning
			t.Logf("Warning: Failed to dump taxonomy data: %v", err)
		} else {
			t.Logf("%s", FormatTaxonomyDump(taxonomyEntries))
		}

		// 6. Test get_all_weights_for_query at different time points
		testCases := []struct {
			description     string
			activeFrom      int64
			expectedResults map[string]float64 // Map of streamName -> expected weight
		}{
			{
				description: "Initial setup (Time 50)",
				activeFrom:  50,
				expectedResults: map[string]float64{
					streamCName: 0.4,
					streamDName: 0.42, // 0.6 * 0.7
					streamEName: 0.18, // 0.6 * 0.3
				},
			},
			{
				description: "After first change (Time 150)",
				activeFrom:  150,
				expectedResults: map[string]float64{
					streamCName: 0.2,
					streamDName: 0.56, // 0.8 * 0.7
					streamEName: 0.24, // 0.8 * 0.3
				},
			},
			{
				description: "After second change (Time 250)",
				activeFrom:  250,
				expectedResults: map[string]float64{
					streamCName: 0.2,
					streamDName: 0.4, // 0.8 * 0.5
					streamEName: 0.4, // 0.8 * 0.5
				},
			},
			{
				description: "After third change (Time 350)",
				activeFrom:  350,
				expectedResults: map[string]float64{
					streamCName: 1.0,
					// D and E should not be present as B is no longer in the hierarchy
				},
			},
		}

		// Run tests
		for _, tc := range testCases {
			// Get weights for query at the specific time
			results, err := GetAllWeightsForQuery(ctx, GetAllWeightsForQueryInput{
				Platform:     platform,
				DataProvider: deployer.Address(),
				StreamId:     streamAId.String(),
				FromTime:     tc.activeFrom,
				ToTime:       tc.activeFrom,
			})
			if err != nil {
				return errors.Wrapf(err, "error getting weights for test case: %s", tc.description)
			}

			// Map result streams to their weights for easier comparison
			resultWeights := make(map[string]float64)
			for _, r := range results {
				// Convert the streamId back to a human-readable name for comparison
				streamName := ""
				// Compare with generated stream IDs to identify which primitive stream this is
				if r.StreamId == streamCId.String() {
					streamName = streamCName
				} else if r.StreamId == streamDId.String() {
					streamName = streamDName
				} else if r.StreamId == streamEId.String() {
					streamName = streamEName
				}

				if streamName != "" {
					weight, err := r.Weight.Float64()
					if err != nil {
						return errors.Wrapf(err, "error converting weight to float64 for stream %s", streamName)
					}
					resultWeights[streamName] = weight
				}
			}

			// Create a detailed comparison for debugging
			var mismatchFound bool
			var debugInfo strings.Builder
			debugInfo.WriteString(fmt.Sprintf("\n=== Test Case: %s (Time %d) ===\n", tc.description, tc.activeFrom))

			// Add taxonomy visualization
			debugInfo.WriteString(visualizeTaxonomy(tc.activeFrom, results))

			debugInfo.WriteString("\nExpected weights:\n")

			// Sort stream names for consistent output
			expectedStreamNames := make([]string, 0, len(tc.expectedResults))
			for streamName := range tc.expectedResults {
				expectedStreamNames = append(expectedStreamNames, streamName)
			}
			sort.Strings(expectedStreamNames)

			for _, streamName := range expectedStreamNames {
				expectedWeight := tc.expectedResults[streamName]
				debugInfo.WriteString(fmt.Sprintf("  %s: %.6f\n", streamName, expectedWeight))
			}

			debugInfo.WriteString("Actual weights:\n")

			// Sort stream names for consistent output
			actualStreamNames := make([]string, 0, len(resultWeights))
			for streamName := range resultWeights {
				actualStreamNames = append(actualStreamNames, streamName)
			}
			sort.Strings(actualStreamNames)

			for _, streamName := range actualStreamNames {
				actualWeight := resultWeights[streamName]
				expectedWeight, exists := tc.expectedResults[streamName]

				if !exists {
					mismatchFound = true
					debugInfo.WriteString(fmt.Sprintf("  %s: %.6f (UNEXPECTED)\n", streamName, actualWeight))
				} else if !assert.InDelta(t, expectedWeight, actualWeight, 0.001) {
					mismatchFound = true
					debugInfo.WriteString(fmt.Sprintf("  %s: %.6f (EXPECTED: %.6f, DIFF: %.6f)\n",
						streamName, actualWeight, expectedWeight, math.Abs(actualWeight-expectedWeight)))
				} else {
					debugInfo.WriteString(fmt.Sprintf("  %s: %.6f (OK)\n", streamName, actualWeight))
				}
			}

			// Check for missing streams
			for streamName, expectedWeight := range tc.expectedResults {
				if _, exists := resultWeights[streamName]; !exists {
					mismatchFound = true
					debugInfo.WriteString(fmt.Sprintf("  %s: MISSING (EXPECTED: %.6f)\n", streamName, expectedWeight))
				}
			}

			// Check expected number of results
			expectedCount := len(tc.expectedResults)
			actualCount := len(results)
			if expectedCount != actualCount {
				mismatchFound = true
				debugInfo.WriteString(fmt.Sprintf("Expected %d results, got %d\n", expectedCount, actualCount))
			}

			// Log the debug info regardless of test success/failure
			t.Logf("%s", debugInfo.String())

			// If there's a mismatch, fail the test with detailed information
			if mismatchFound {
				return errors.New(fmt.Sprintf("Weight mismatch detected in test case '%s':\n%s",
					tc.description, debugInfo.String()))
			}

			// The original verification logic is now handled by the detailed comparison above
		}

		return nil
	}
}
