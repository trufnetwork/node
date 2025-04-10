package benchmark

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	benchutil "github.com/trufnetwork/node/internal/benchmark/util"

	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/trufnetwork/node/internal/benchmark/trees"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/sdk-go/core/util"
)

func runBenchmark(ctx context.Context, platform *kwilTesting.Platform, c BenchmarkCase, tree trees.Tree) ([]Result, error) {
	var results []Result

	err := setupSchemas(ctx, platform, SetupSchemasInput{
		BenchmarkCase: c,
		Tree:          tree,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup schemas")
	}

	// Triggering the analyze command for the given tables makes the query planner
	// more accurate and the query execution time more consistent. It makes sure to match a production environment.
	err = updateQueryPlanner(ctx, platform, []string{"taxonomies", "streams", "primitive_events", "metadata"})
	if err != nil {
		return nil, errors.Wrap(err, "failed to update query planner")
	}

	for _, dataPoints := range c.DataPointsSet {
		for _, procedure := range c.Procedures {
			result, err := runSingleTest(ctx, RunSingleTestInput{
				Platform:   platform,
				Case:       c,
				DataPoints: dataPoints,
				Procedure:  procedure,
				Tree:       tree,
			})
			if err != nil {
				return nil, errors.Wrap(err, "failed to run single test")
			}
			results = append(results, result)
		}
	}

	return results, nil
}

func updateQueryPlanner(ctx context.Context, platform *kwilTesting.Platform, tables []string) error {
	full_qualified_tables := make([]string, len(tables))
	for i, table := range tables {
		// on main schema by default
		full_qualified_tables[i] = fmt.Sprintf("main.%s", table)
	}
	// we just run the analyze command for the given tables
	query := fmt.Sprintf("ANALYZE %s;", strings.Join(full_qualified_tables, ", "))
	_, err := platform.DB.Execute(ctx, query)
	return err
}

type RunSingleTestInput struct {
	Platform   *kwilTesting.Platform
	Case       BenchmarkCase
	DataPoints int
	Procedure  ProcedureEnum
	Tree       trees.Tree
}

// runSingleTest runs a single test for the given input and returns the result.
func runSingleTest(ctx context.Context, input RunSingleTestInput) (Result, error) {
	// we're querying the index-0 stream because this is the root stream
	rangeParams := getRangeParameters(input.DataPoints)
	fromDate := rangeParams.FromDate.Unix()
	toDate := rangeParams.ToDate.Unix()

	nthLocator := types.StreamLocator{
		DataProvider: *MustEthereumAddressFromBytes(input.Platform.Deployer),
		StreamId:     *getStreamId(0),
	}
	result := Result{
		Case:          input.Case,
		Procedure:     input.Procedure,
		DataPoints:    input.DataPoints,
		MaxDepth:      input.Tree.MaxDepth,
		CaseDurations: make([]time.Duration, input.Case.Samples),
	}

	for i := 0; i < input.Case.Samples; i++ {
		// args for:
		// get_record: dataProvider, streamId, fromDate, toDate, frozenAt
		// get_index: dataProvider, streamId, fromDate, toDate, frozenAt, baseDate
		// get_index_change: dataProvider, streamId, fromDate, toDate, frozenAt, baseDate, daysInterval
		locator_args := []any{nthLocator.DataProvider.Address(), nthLocator.StreamId.String()}
		args := append(locator_args, []any{fromDate, toDate, nil}...)
		switch input.Procedure {
		case ProcedureGetIndex:
			args = append(args, nil) // baseDate
		case ProcedureGetChangeIndex:
			args = append(args, nil) // baseDate
			args = append(args, 1)   // daysInterval
		case ProcedureGetFirstRecord:
			// we reset as is not the same structure as the other procedures
			// get_first_record: dataProvider, streamId, afterDate, frozenAt
			args = append(locator_args, nil, nil) // afterDate, frozenAt
		case ProcedureGetLastRecord:
			// we reset as is not the same structure as the other procedures
			// get_last_record: dataProvider, streamId, beforeDate, frozenAt
			args = append(locator_args, nil, nil) // beforeDate, frozenAt
		}

		// FYI: we already tested sleeping for 10 seconds before running to see if
		// the  memory is affected by previous operations, but it's not.
		// time.Sleep(10 * time.Second)

		collector, err := benchutil.StartDockerMemoryCollector("kwil-testing-postgres")
		if err != nil {
			return Result{}, err
		}

		// Wait for the collector to receive at least one stats sample
		if err := collector.WaitForFirstSample(); err != nil {
			collector.Stop()
			return Result{}, err
		}

		start := time.Now()
		// we read using the reader address to be sure visibility is tested
		rows, err := executeStreamProcedure(ctx, input.Platform, string(input.Procedure), args, readerAddress.Bytes())
		if err != nil {
			collector.Stop()
			return Result{}, err
		}
		if len(rows) == 0 {
			// if the procedure returns no rows, we consider it as an error
			collector.Stop()
			return Result{}, errors.New("procedure returned no rows")
		}
		result.CaseDurations[i] = time.Since(start)

		collector.Stop()
		result.MemoryUsage, err = collector.GetMaxMemoryUsage()
		if err != nil {
			return Result{}, err
		}
	}

	return result, nil
}

type RunBenchmarkInput struct {
	ResultPath string
	Visibility util.VisibilityEnum
	QtyStreams int
	DataPoints []int
	Samples    int
}

// it returns a result channel to be accumulated by the caller
func getBenchmarkFn(benchmarkCase BenchmarkCase, resultCh *chan []Result) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {

		log.Println("running benchmark", benchmarkCase)
		platform = procedure.WithSigner(platform, deployer.Bytes())

		tree := trees.NewTree(trees.NewTreeInput{
			QtyStreams:      benchmarkCase.QtyStreams,
			BranchingFactor: benchmarkCase.BranchingFactor,
		})

		// we can't run the benchmark if the tree is too deep, due to postgreSQL limitations
		if tree.MaxDepth > maxDepth {
			return fmt.Errorf("tree max depth (%d) is greater than max depth (%d)", tree.MaxDepth, maxDepth)
		}

		results, err := runBenchmark(ctx, platform, benchmarkCase, tree)
		if err != nil {
			return errors.Wrap(err, "failed to run benchmark")
		}

		// if LOG_RESULTS is set, we print the results to the console
		if os.Getenv("LOG_RESULTS") == "true" {
			printResults(results)
		}

		*resultCh <- results
		return nil
	}
}
