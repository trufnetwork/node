package benchmark

import (
	"context"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/util"
	"time"
)

// Benchmark case generation and execution
func generateBenchmarkCases(visibility util.VisibilityEnum) []BenchmarkCase {
	var cases []BenchmarkCase
	depths := []int{0, 1, 10, 50, 100}
	days := []int{1, 7, 30, 365}
	procedures := []ProcedureEnum{ProcedureGetRecord, ProcedureGetIndex, ProcedureGetChangeIndex}

	for _, depth := range depths {
		for _, day := range days {
			for _, procedure := range procedures {
				cases = append(cases, BenchmarkCase{
					Depth: depth, Days: day, Visibility: visibility,
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

	result.MeanDuration = calculateMeanDuration(result.CaseDurations)
	return result, nil
}
