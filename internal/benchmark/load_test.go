package benchmark

import (
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/util"
	"testing"
)

// Main benchmark test function
func TestBench(t *testing.T) {
	depths := []int{0, 1, 10, 50, 100}
	days := []int{1, 7, 30, 365}

	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "benchmark_test",
		SchemaFiles: []string{},
		FunctionTests: []kwilTesting.TestFunc{
			runBenchmark(RunBenchmarkInput{
				Visibility: util.PublicVisibility,
				Depths:     depths,
				Days:       days,
			}),
			runBenchmark(RunBenchmarkInput{
				Visibility: util.PrivateVisibility,
				Depths:     depths,
				Days:       days,
			}),
		},
	})
}
