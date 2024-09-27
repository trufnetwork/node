package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/truflation/tsn-db/internal/benchmark/benchexport"
	"github.com/truflation/tsn-sdk/core/util"
	"golang.org/x/exp/constraints"
)

// getStreamId generates a unique StreamId for a stream at a given index.
func getStreamId(index int) *util.StreamId {
	id := util.GenerateStreamId("test_stream_" + strconv.Itoa(index))
	return &id
}

// generateRecords creates a slice of records with random values for each day
// between the given fromDate and toDate, inclusive.
func generateRecords(fromDate, toDate time.Time) [][]any {
	var records [][]any
	for d := fromDate; !d.After(toDate); d = d.AddDate(0, 0, 1) {
		value, _ := apd.New(rand.Int63n(100000000000000), 0).Float64()
		records = append(records, []any{d.Format("2006-01-02"), fmt.Sprintf("%.2f", value)})
	}
	return records
}

// executeStreamProcedure executes a procedure on the given platform and database.
// It handles the common setup for procedure execution, including transaction data.
func executeStreamProcedure(ctx context.Context, platform *kwilTesting.Platform, dbid, procedure string, args []any, signer []byte) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: procedure,
		Dataset:   dbid,
		Args:      args,
		TransactionData: common.TransactionData{
			Signer: signer,
			Caller: MustEthereumAddressFromBytes(signer).Address(),
			TxID:   platform.Txid(),
			Height: 0,
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to execute stream procedure")
	}
	return nil
}

// printResults outputs the benchmark results in a human-readable format.
func printResults(results []Result) {
	fmt.Println("Benchmark Results:")
	for _, r := range results {
		fmt.Printf(
			"Qty Streams: %d, Branching Factor: %d, Days Queried: %d, Visibility: %s, Procedure: %s, Samples: %d, Memory Usage: %s\n",
			r.Case.QtyStreams,
			r.Case.BranchingFactor,
			r.DaysQueried,
			visibilityToString(r.Case.Visibility),
			string(r.Procedure),
			r.Case.Samples,
			formatMemoryUsage(r.MemoryUsage),
		)
		fmt.Printf("  Mean Duration: %v\n", Average(r.CaseDurations))
		fmt.Printf("  Min Duration: %v\n", slices.Min(r.CaseDurations))
		fmt.Printf("  Max Duration: %v\n", slices.Max(r.CaseDurations))
		fmt.Println()
	}
}

func Average[T constraints.Integer | constraints.Float](values []T) T {
	sum := T(0)
	for _, v := range values {
		sum += v
	}
	return sum / T(len(values))
}

func saveResults(results []Result, filePath string) error {
	savedResults := make([]benchexport.SavedResults, len(results))
	for i, r := range results {
		savedResults[i] = benchexport.SavedResults{
			Procedure:       string(r.Procedure), // procedure
			Samples:         r.Case.Samples,
			BranchingFactor: r.Case.BranchingFactor,                  // depth
			QtyStreams:      r.Case.QtyStreams,                       // n_of_streams
			Days:            r.DaysQueried,                           // n_of_dates
			DurationMs:      Average(r.CaseDurations).Milliseconds(), // duration_ms
			Visibility:      visibilityToString(r.Case.Visibility),   // visibility
		}
	}
	// Save as CSV
	if err := benchexport.SaveOrAppendToCSV(savedResults, filePath); err != nil {
		return errors.Wrap(err, "failed to save results")
	}

	return nil
}

func deleteFileIfExists(filePath string) error {
	// Delete the CSV file if it exists
	if _, err := os.Stat(filePath); err == nil {
		if err = os.Remove(filePath); err != nil {
			return errors.Wrap(err, "failed to delete file")
		}
	}

	// Delete the Markdown file if it exists
	mdFilePath := strings.Replace(filePath, ".csv", ".md", 1)
	if _, err := os.Stat(mdFilePath); err == nil {
		if err = os.Remove(mdFilePath); err != nil {
			return errors.Wrap(err, "failed to delete file")
		}
	}

	return nil
}

func visibilityToString(visibility util.VisibilityEnum) string {
	switch visibility {
	case util.PublicVisibility:
		return "Public"
	case util.PrivateVisibility:
		return "Private"
	default:
		return "Unknown"
	}
}

func formatMemoryUsage(memoryUsage uint64) string {
	return fmt.Sprintf("%d MB", memoryUsage/1024/1024)
}

// MustNewEthereumAddressFromString creates an EthereumAddress from a string,
// panicking if the conversion fails. Use with caution and only in contexts
// where a failure to create the address is unrecoverable.
func MustNewEthereumAddressFromString(s string) util.EthereumAddress {
	addr, err := util.NewEthereumAddressFromString(s)
	if err != nil {
		panic(errors.Wrap(err, "failed to create EthereumAddress"))
	}
	return addr
}

// MustNewEthereumAddressFromBytes creates an EthereumAddress from a byte slice,
// panicking if the conversion fails. Use with caution and only in contexts
// where a failure to create the address is unrecoverable.
func MustNewEthereumAddressFromBytes(b []byte) util.EthereumAddress {
	addr, err := util.NewEthereumAddressFromBytes(b)
	if err != nil {
		panic(errors.Wrap(err, "failed to create EthereumAddress"))
	}
	return addr
}

func MustEthereumAddressFromBytes(b []byte) *util.EthereumAddress {
	addr, err := util.NewEthereumAddressFromBytes(b)
	if err != nil {
		panic(errors.Wrap(err, "failed to create EthereumAddress"))
	}
	return &addr
}
