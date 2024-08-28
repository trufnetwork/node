package benchmark

import (
	"os"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/util"
)

// Main benchmark test function
func TestBench(t *testing.T) {
	// try get resultPath from env
	resultPath := os.Getenv("RESULTS_PATH")
	if resultPath == "" {
		resultPath = "./benchmark_results.csv"
	}

	// Delete the file if it exists
	if err := deleteFileIfExists(resultPath); err != nil {
		t.Fatal(err)
	}

	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "benchmark_test",
		SchemaFiles: []string{},
		FunctionTests: []kwilTesting.TestFunc{
			runBenchmark(RunBenchmarkInput{
				Visibility: util.PublicVisibility,
				Depths:     depths,
				Days:       days,
				ResultPath: resultPath,
			}),
			runBenchmark(RunBenchmarkInput{
				Visibility: util.PrivateVisibility,
				Depths:     depths,
				Days:       days,
				ResultPath: resultPath,
			}),
		},
	})
}
