package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/benchmark/benchexport"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
	"golang.org/x/exp/constraints"
)

// getStreamId generates a unique StreamId for a stream at a given index.
func getStreamId(index int) *util.StreamId {
	id := util.GenerateStreamId("test_stream_" + strconv.Itoa(index))
	return &id
}

// generateRecords creates a slice of records with random values for each day
// between the given fromDate and toDate, inclusive.
func generateRecords(rangeParams RangeParameters) []setup.InsertRecordInput {
	var records []setup.InsertRecordInput
	for d := rangeParams.FromDate; !d.After(rangeParams.ToDate); d = d.Add(secondInterval) {
		value, _ := apd.New(rand.Int63n(100000000000000), 0).Float64()
		records = append(records, setup.InsertRecordInput{
			EventTime: d.Unix(),
			Value:     value,
		})
	}

	return records
}

// executeStreamProcedure executes a procedure on the given platform and database.
// It handles the common setup for procedure execution, including transaction data.
func executeStreamProcedure(ctx context.Context, platform *kwilTesting.Platform, logger *testing.T, procedure string, args []any, signer []byte) ([]common.Row, error) {
	LogPhaseEnter(logger, "executeStreamProcedure", "Procedure: %s, Signer: %s, Args: %v", procedure, BytesToHex(signer), args)
	defer LogPhaseExit(logger, time.Now(), "executeStreamProcedure", "Procedure: %s", procedure)
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         platform.Txid(),
		Signer:       signer,
		Caller:       MustEthereumAddressFromBytes(signer).Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var rows []common.Row

	call, err := platform.Engine.Call(engineContext, platform.DB, "", procedure, args, func(row *common.Row) error {
		rows = append(rows, *row)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute stream procedure")
	}
	if call.Error != nil {
		// Log the specific call error, even if rows are also empty
		LogInfo(logger, "[ERROR] executeStreamProcedure - Call Error: %v (Procedure: %s, Args: %v)", call.Error, procedure, args)
		return nil, errors.Wrap(call.Error, "failed to execute stream procedure")
	}

	// Log if no rows are returned, this is not necessarily an error from Engine.Call but benchmark treats it as such
	if len(rows) == 0 {
		LogInfo(logger, "executeStreamProcedure - No rows returned (Procedure: %s, Args: %v, CallErrorFromEngine: %v, ErrFromEngineCallWrapper: %v)", procedure, args, call.Error, err)
	}
	return rows, nil
}

// BytesToHex is a simple utility to convert byte slice to hex string for logging.
// Added here as it's a small helper often useful in logging byte arrays like signers/addresses.
func BytesToHex(b []byte) string {
	return fmt.Sprintf("0x%x", b)
}

// printResults outputs the benchmark results in a human-readable format.
func printResults(results []Result) {
	fmt.Println("Benchmark Results:")
	for _, r := range results {
		cacheInfo := "No Cache"
		if r.Case.CacheEnabled {
			cacheInfo = fmt.Sprintf("Cache Enabled (Verified: %t, Delta: %v)", r.CacheVerified, r.CachePerformanceDelta)
		}
		
		fmt.Printf(
			"Qty Streams: %d, Branching Factor: %d, Data Points: %d, Visibility: %s, Procedure: %s, Samples: %d, Memory Usage: %s, Cache: %s\n",
			r.Case.QtyStreams,
			r.Case.BranchingFactor,
			r.DataPoints,
			visibilityToString(r.Case.Visibility),
			string(r.Procedure),
			r.Case.Samples,
			formatMemoryUsage(r.MemoryUsage),
			cacheInfo,
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
			DataPoints:      r.DataPoints,                            // n_of_dates
			DurationMs:      Average(r.CaseDurations).Milliseconds(), // duration_ms
			Visibility:      visibilityToString(r.Case.Visibility),   // visibility
			UnixOnly:        false,                                   // legacy field, always false
			CacheEnabled:    r.Case.CacheEnabled,                     // whether cache was enabled
			CacheVerified:   r.CacheVerified,                         // whether cache verification passed
			CacheDeltaMs:    r.CachePerformanceDelta.Milliseconds(),  // performance difference
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

// should execute docker", "rm", "-f", "kwil-testing-postgres
func cleanupDocker() {
	// Execute the cleanup command
	cmd := exec.Command("docker", "rm", "-f", "kwil-testing-postgres")
	err := cmd.Run()
	if err != nil {
		fmt.Printf("Error during cleanup: %v\n", err)
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

// getRangeParameters generates the range parameters for the given data points and unixOnly flag.
// - it generates the fromDate and toDate based on the data points and unixOnly flag
// - it returns the range parameters
func getRangeParameters(dataPoints int) RangeParameters {
	toDate := fixedDate
	delta := int(secondInterval)

	// Subtract (dataPoints - 1) because we want to include the interval at toDate
	fromDate := toDate.Add(-time.Duration(delta * (dataPoints - 1)))
	return RangeParameters{
		DataPoints: dataPoints,
		FromDate:   fromDate,
		ToDate:     toDate,
	}
}

// getMaxRangeParams returns the maximum range parameters for the given data points and unixOnly flag.
// - it returns the maximum data points and the range parameters
func getMaxRangeParams(dataPoints []int) RangeParameters {
	maxDataPoints := slices.Max(dataPoints)
	return getRangeParameters(maxDataPoints)
}
