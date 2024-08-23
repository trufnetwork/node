package benchmark

import (
	"context"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/util"
	"slices"
	"testing"
)

// Main benchmark test function
func TestBench(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "benchmark_test",
		SchemaFiles: []string{},
		FunctionTests: []kwilTesting.TestFunc{
			runBenchmark(RunBenchmarkInput{
				Visibility: util.PublicVisibility,
			}),
			runBenchmark(RunBenchmarkInput{
				Visibility: util.PrivateVisibility,
			}),
		},
	})
}

type RunBenchmarkInput struct {
	Visibility util.VisibilityEnum
}

func runBenchmark(input RunBenchmarkInput) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := MustNewEthereumAddressFromString("0x0000000000000000000000000000000200000000")
		platform.Deployer = deployer.Bytes()
		benchCases := generateBenchmarkCases(input.Visibility)
		// get max depth based on the cases
		maxDepth := slices.MaxFunc(benchCases, func(a, b BenchmarkCase) int {
			return a.Depth - b.Depth
		})
		// get schemas based on the max depth
		schemas := getSchemas(maxDepth.Depth)

		if err := setupSchemas(ctx, platform, schemas, input.Visibility); err != nil {
			return err
		}

		results, err := runBenchmarkCases(ctx, platform, benchCases)
		if err != nil {
			return err
		}

		printResults(results)
		return nil
	}
}
