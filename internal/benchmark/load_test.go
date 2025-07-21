package benchmark

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Cache verification configuration
const enableCacheCheck = true // Set to false to disable cache verification

// -----------------------------------------------------------------------------
// Main benchmark test function
// -----------------------------------------------------------------------------

// Main benchmark test function
func TestBench(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	LogPhaseEnter(t, "TestBench", "Starting main benchmark test function")
	defer LogPhaseExit(t, time.Now(), "TestBench", "Finished main benchmark test function")

	// notify on interrupt. Otherwise, tests will not stop
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			fmt.Println("interrupt signal received")
			cleanupDocker()
			cancel()
		}
	}()
	defer cleanupDocker()

	// set default LOG_RESULTS to true
	if os.Getenv("LOG_RESULTS") == "" {
		os.Setenv("LOG_RESULTS", "true")
	}

	// try get resultPath from env
	resultPath := os.Getenv("RESULTS_PATH")
	if resultPath == "" {
		resultPath = "./benchmark_results.csv"
	}

	// Delete the file if it exists
	if err := deleteFileIfExists(resultPath); err != nil {
		err = errors.Wrap(err, "failed to delete file if exists")
		t.Fatal(err)
	}

	// -- Setup Test Parameters --

	// Common parameters
	// number of samples to run for each test
	samples := 1
	// visibilities to test
	visibilities := []util.VisibilityEnum{
		util.PublicVisibility,
		// util.PrivateVisibility,
	}

	// Specific Parameters

	type SpecificParams struct {
		ShapePairs [][]int
		DataPoints []int
	}

	getRecordsInAMonthWithInterval := func(interval time.Duration) int {
		return int(time.Hour * 24 * 30 / interval)
	}
	// prevent lint error
	_ = getRecordsInAMonthWithInterval

	// number of data points to query
	dataPoints := []int{
		// 1,
		// 7,
		// 30,
		365,
		// sanity check
		// 1,
		// data points on one month:
		// 1 record per 5 seconds
		// getRecordsInAMonthWithInterval(time.Second * 5),
		// 1 record per 1 minute
		// getRecordsInAMonthWithInterval(time.Minute),
		// 1 record per 5 minutes
		// getRecordsInAMonthWithInterval(time.Minute * 5),
		// 1 record per 1 hour
		// getRecordsInAMonthWithInterval(time.Hour),
	}

	// shapePairs is a list of tuples, where each tuple represents a pair of qtyStreams and branchingFactor
	// qtyStreams is the number of streams in the tree
	// branchingFactor is the branching factor of the tree
	// if branchingFactor is math.MaxInt, it means the tree is flat

	dateShapePairs := [][]int{
		// qtyStreams, branchingFactor
		// testing 1 stream only
		// {1, 1},

		//flat trees = cost of adding a new stream to our composed
		// {50, math.MaxInt},
		// {100, math.MaxInt},
		// {200, math.MaxInt},
		{400, math.MaxInt},
		// // 800 streams kills t3.small instances for memory starvation. But probably because it stores the whole tree in memory
		// //{800, math.MaxInt},
		// //{1500, math.MaxInt}, // this gives error: Out of shared memory

		// // deep trees = cost of adding depth
		// {50, 1},
		// {100, 1},
		// //{200, 1}, // we can't go deeper than 180, for call stack size issues

		// // to get difference for stream qty on a real world situation
		// {50, 8},
		// {100, 8},
		// {200, 8},
		// {400, 8},
		// //{800, 8},

		// // to get difference for branching factor
		// {200, 2},
		// {200, 4},
		// // {200, 8}, // already tested above
		// {200, 16},
		// {200, 32},
	}

	// -----

	allParams := []SpecificParams{
		{
			ShapePairs: dateShapePairs,
			DataPoints: dataPoints,
		},
	}

	var functionTests []kwilTesting.TestFunc
	// a channel to receive results from the tests
	var resultsCh chan []Result

	// Set up cache configuration for verification tests
	var cacheConfig *testutils.CacheOptions
	if enableCacheCheck {
		// Fixed deployer address from constants.go: 0x0000000000000000000000000000000200000000
		deployerAddr := "0x0000000000000000000000000000000200000000"
		// Only cache the root stream (index 0) that benchmarks actually query
		rootStreamID := util.GenerateStreamId("test_stream_0")
		cacheConfig = testutils.NewCacheOptions().
			WithEnabled().
			WithMaxBlockAge(-1*time.Second).
			// Use 5-field cron as unified across validation and scheduler; this never runs (Feb 31)
			WithResolutionSchedule("0 0 31 2 *").
			WithStream(deployerAddr, rootStreamID.String(), "0 0 31 2 *")
	}

	// create combinations of shapePairs and visibilities
	for _, specificParams := range allParams {
		for _, shapePair := range specificParams.ShapePairs {
			for _, visibility := range visibilities {
				// Original non-cache case
				functionTests = append(functionTests, getBenchmarkFn(BenchmarkCase{
					Visibility:      visibility,
					QtyStreams:      shapePair[0],
					BranchingFactor: shapePair[1],
					Samples:         samples,
					DataPointsSet:   specificParams.DataPoints,
					Procedures: []ProcedureEnum{
						ProcedureGetRecord,
						ProcedureGetIndex,
						ProcedureGetChangeIndex,
						ProcedureGetFirstRecord,
						ProcedureGetLastRecord,
					},
					CacheEnabled: false,
				},
					// use pointer, so we can reassign the results channel
					&resultsCh))

				// Cache verification case (only if enabled)
				if enableCacheCheck {
					functionTests = append(functionTests, getBenchmarkFn(BenchmarkCase{
						Visibility:      visibility,
						QtyStreams:      shapePair[0],
						BranchingFactor: shapePair[1],
						Samples:         samples,
						DataPointsSet:   specificParams.DataPoints,
						Procedures: []ProcedureEnum{
							ProcedureGetRecord,
							ProcedureGetIndex,
							ProcedureGetChangeIndex,
							ProcedureGetFirstRecord,
							ProcedureGetLastRecord,
						},
						CacheEnabled: true,
						CacheConfig:  cacheConfig,
					},
						// use pointer, so we can reassign the results channel
						&resultsCh))
				}
			}
		}
	}

	// let's chunk tests into groups, becuase these tests are very long
	// and postgres may fail during the test
	groupsOfTests := chunk(functionTests, 2)

	var successResults []Result

	for i, groupOfTests := range groupsOfTests {
		groupName := "benchmark_test_" + strconv.Itoa(i)
		LogPhaseEnter(t, "TestGroup", "Starting test group %d/%d: %s", i+1, len(groupsOfTests), groupName)
		groupStartTime := time.Now()

		schemaTest := kwilTesting.SchemaTest{
			Name:          groupName,
			FunctionTests: groupOfTests,
			SeedScripts:   migrations.GetSeedScriptPaths(),
		}

		t.Run(schemaTest.Name, func(t *testing.T) {
			const maxRetries = 1
			var err error
		RetryFor:
			for attempt := 1; attempt <= maxRetries; attempt++ {
				select {
				case <-ctx.Done():
					t.Fatalf("context cancelled")
				default:
					LogInfo(t, "Attempt %d/%d for test group %s", attempt, maxRetries, schemaTest.Name)
					// wrap in a function so we can defer close the results channel
					func() {
						resultsCh = make(chan []Result, len(groupOfTests))
						defer close(resultsCh)

						// Use testutils runner to enable tn_cache setup
						testutils.RunSchemaTest(t, schemaTest, testutils.GetTestOptionsWithCache(cacheConfig))
					}()

					if err == nil {
						LogInfo(t, "Test group %s completed successfully on attempt %d.", schemaTest.Name, attempt)
						for result := range resultsCh {
							successResults = append(successResults, result...)
						}
						// break the retries loop
						break RetryFor
					}

					t.Logf("Attempt %d failed: %s", attempt, err)
					if attempt < maxRetries {
						time.Sleep(time.Second * time.Duration(attempt)) // Exponential backoff
					}
				}
			}
			if err != nil {
				t.Fatalf("Test failed after %d attempts: %s", maxRetries, err)
			}
		})
		LogPhaseExit(t, groupStartTime, "TestGroup", "Finished test group %d/%d: %s", i+1, len(groupsOfTests), groupName)
	}

	// save results to file
	if err := saveResults(successResults, resultPath); err != nil {
		t.Fatalf("failed to save results: %s", err)
	}
}
