package benchmark

import (
	"context"
	"slices"
	"time"

	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/testing"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/util"
)

// Benchmark case generation and execution
func generateBenchmarkCases(input RunBenchmarkInput) []BenchmarkCase {
	var cases []BenchmarkCase
	procedures := []ProcedureEnum{ProcedureGetRecord, ProcedureGetIndex, ProcedureGetChangeIndex}

	for _, depth := range input.Depths {
		for _, day := range input.Days {
			for _, procedure := range procedures {
				cases = append(cases, BenchmarkCase{
					Depth: depth, Days: day, Visibility: input.Visibility,
					Procedure: procedure, Samples: samplesPerCase,
				})
			}
		}
	}
	return cases
}

func runBenchmarkCases(ctx context.Context, platform *testing.Platform, cases []BenchmarkCase) ([]Result, error) {
	results := make([]Result, len(cases))
	for i, c := range cases {
		result, err := runBenchmarkCase(ctx, platform, c)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}
	return results, nil
}

func runBenchmarkCase(ctx context.Context, platform *testing.Platform, c BenchmarkCase) (Result, error) {
	nthDbId := utils.GenerateDBID(getStreamId(c.Depth).String(), platform.Deployer)
	fromDate := fixedDate.AddDate(0, 0, -c.Days).Format("2006-01-02")
	toDate := fixedDate.Format("2006-01-02")

	result := Result{Case: c, CaseDurations: make([]time.Duration, c.Samples)}

	for i := 0; i < c.Samples; i++ {
		start := time.Now()
		args := []any{fromDate, toDate, nil}
		if c.Procedure == ProcedureGetChangeIndex {
			args = append(args, 1) // change index accept an additional arg: $days_interval
		}
		if err := executeStreamProcedure(ctx, platform, nthDbId, string(c.Procedure), args); err != nil {
			return Result{}, err
		}
		result.CaseDurations[i] = time.Since(start)
	}

	return result, nil
}

type RunBenchmarkInput struct {
	ResultPath string
	Visibility util.VisibilityEnum
	Depths     []int
	Days       []int
}

func runBenchmark(input RunBenchmarkInput) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		benchCases := generateBenchmarkCases(input)

		deployer := MustNewEthereumAddressFromString("0x0000000000000000000000000000000200000000")
		platform.Deployer = deployer.Bytes()
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

		return saveResults(results, input.ResultPath)
	}
}
