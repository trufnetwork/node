package benchmark

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"testing"
	"time"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/truflation/tsn-sdk/core/util"
)

// should execute docker", "rm", "-f", "kwil-testing-postgres
func cleanupDocker() {
	// Execute the cleanup command
	cmd := exec.Command("docker", "rm", "-f", "kwil-testing-postgres")
	err := cmd.Run()
	if err != nil {
		fmt.Printf("Error during cleanup: %v\n", err)
	}
}

// Main benchmark test function
func TestBench(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

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

	// shapePairs is a list of tuples, where each tuple represents a pair of qtyStreams and branchingFactor
	// qtyStreams is the number of streams in the tree
	// branchingFactor is the branching factor of the tree
	// if branchingFactor is math.MaxInt, it means the tree is flat

	shapePairs := [][]int{
		// qtyStreams, branchingFactor
		// testing 1 stream only
		{1, 1},

		//flat trees = cost of adding a new stream to our composed
		{50, math.MaxInt},
		{100, math.MaxInt},
		{200, math.MaxInt},
		{400, math.MaxInt},
		// 800 streams kills t3.small instances for memory starvation. But probably because it stores the whole tree in memory
		//{800, math.MaxInt},
		//{1500, math.MaxInt}, // this gives error: Out of shared memory

		// deep trees = cost of adding depth
		{50, 1},
		{100, 1},
		//{200, 1}, // we can't go deeper than 180, for call stack size issues

		// to get difference for stream qty on a real world situation
		{50, 8},
		{100, 8},
		{200, 8},
		{400, 8},
		//{800, 8},

		// to get difference for branching factor
		{200, 2},
		{200, 4},
		// {200, 8}, // already tested above
		{200, 16},
		{200, 32},
	}

	samples := 3

	days := []int{1, 7, 30, 365}

	visibilities := []util.VisibilityEnum{util.PublicVisibility, util.PrivateVisibility}

	var functionTests []kwilTesting.TestFunc
	// a channel to receive results from the tests
	var resultsCh chan []Result

	// create combinations of shapePairs and visibilities
	for _, qtyStreams := range shapePairs {
		for _, visibility := range visibilities {
			functionTests = append(functionTests, getBenchmarFn(BenchmarkCase{
				Visibility:      visibility,
				QtyStreams:      qtyStreams[0],
				BranchingFactor: qtyStreams[1],
				Samples:         samples,
				Days:            days,
				Procedures:      []ProcedureEnum{ProcedureGetRecord, ProcedureGetIndex, ProcedureGetChangeIndex},
			},
				// use pointer, so we can reassign the results channel
				&resultsCh))
		}
	}

	// let's chunk tests into groups, becuase these tests are very long
	// and postgres may fail during the test
	groupsOfTests := chunk(functionTests, 2)

	var successResults []Result

	for i, groupOfTests := range groupsOfTests {
		schemaTest := kwilTesting.SchemaTest{
			Name:          "benchmark_test_" + strconv.Itoa(i),
			SchemaFiles:   []string{},
			FunctionTests: groupOfTests,
		}

		t.Run(schemaTest.Name, func(t *testing.T) {
			const maxRetries = 3
			var err error
		RetryFor:
			for attempt := 1; attempt <= maxRetries; attempt++ {
				select {
				case <-ctx.Done():
					t.Fatalf("context cancelled")
				default:
					// wrap in a function so we can defer close the results channel
					func() {
						resultsCh = make(chan []Result, len(groupOfTests))
						defer close(resultsCh)

						err = schemaTest.Run(ctx, &kwilTesting.Options{
							UseTestContainer: true,
							Logger:           t,
						})
					}()

					if err == nil {
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
	}

	// save results to file
	if err := saveResults(successResults, resultPath); err != nil {
		t.Fatalf("failed to save results: %s", err)
	}
}

func chunk[T any](arr []T, chunkSize int) [][]T {
	var result [][]T

	for i := 0; i < len(arr); i += chunkSize {
		end := i + chunkSize
		if end > len(arr) {
			end = len(arr)
		}

		result = append(result, arr[i:end])
	}

	return result
}
