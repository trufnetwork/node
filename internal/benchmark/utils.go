package benchmark

import (
	"context"
	"fmt"
	"github.com/cockroachdb/apd/v3"
	"github.com/kwilteam/kwil-db/common"
	kwiltypes "github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-db/internal/contracts"
	"github.com/truflation/tsn-sdk/core/util"
	"golang.org/x/exp/constraints"
	"log"
	"math/rand"
	"slices"
	"strconv"
	"time"
)

// getStreamId generates a StreamId based on the given depth.
// For depth 0, it returns the RootStreamId, as its a primitive stream
// For other depths, it generates a new StreamId with a composed prefix.
func getStreamId(depth int) *util.StreamId {
	if depth == 0 {
		return &RootStreamId
	}
	id := util.GenerateStreamId(ComposedStreamPrefix + "_" + strconv.Itoa(depth))
	return &id
}

// generateRecords creates a slice of records with random values for each day
// between the given fromDate and toDate, inclusive.
func generateRecords(fromDate, toDate time.Time) [][]any {
	var records [][]any
	for d := fromDate; !d.After(toDate); d = d.AddDate(0, 0, 1) {
		value, _ := apd.New(rand.Int63n(10000), 0).Float64()
		records = append(records, []any{d.Format("2006-01-02"), fmt.Sprintf("%.2f", value)})
	}
	return records
}

// executeStreamProcedure executes a procedure on the given platform and database.
// It handles the common setup for procedure execution, including transaction data.
func executeStreamProcedure(ctx context.Context, platform *kwilTesting.Platform, dbid, procedure string, args []any) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: procedure,
		Dataset:   dbid,
		Args:      args,
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 0,
		},
	})
	return err
}

// printResults outputs the benchmark results in a human-readable format.
func printResults(results []Result) {
	fmt.Println("Benchmark Results:")
	for _, r := range results {
		fmt.Printf("Depth: %d, Days: %d, Visibility: %s, Procedure: %s\n",
			r.Case.Depth, r.Case.Days, visibilityToString(r.Case.Visibility), r.Case.Procedure)
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
	// TODO: Implement saving results to a file
	log.Println("Missing implementation for saving results to file")
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

// getSchemas generates a slice of Schema pointers based on the given depth.
// It includes the primary stream schema and composed stream schemas up to the specified depth.
func getSchemas(depth int) []*kwiltypes.Schema {
	var schemas []*kwiltypes.Schema

	primaryStreamSchema, err := parse.Parse(contracts.PrimitiveStreamContent)
	if err != nil {
		panic(err) // panic is ok, this is a test
	}
	primaryStreamSchema.Name = RootStreamId.String()
	schemas = append(schemas, primaryStreamSchema)

	for i := 1; i <= depth; i++ {
		composedStreamSchema, err := parse.Parse(contracts.ComposedStreamContent)
		if err != nil {
			panic(err) // panic is ok, this is a test
		}
		composedStreamSchema.Name = getStreamId(i).String()
		schemas = append(schemas, composedStreamSchema)
	}

	return schemas
}

// MustNewEthereumAddressFromString creates an EthereumAddress from a string,
// panicking if the conversion fails. Use with caution and only in contexts
// where a failure to create the address is unrecoverable.
func MustNewEthereumAddressFromString(s string) util.EthereumAddress {
	addr, err := util.NewEthereumAddressFromString(s)
	if err != nil {
		panic(err)
	}
	return addr
}

// MustNewEthereumAddressFromBytes creates an EthereumAddress from a byte slice,
// panicking if the conversion fails. Use with caution and only in contexts
// where a failure to create the address is unrecoverable.
func MustNewEthereumAddressFromBytes(b []byte) util.EthereumAddress {
	addr, err := util.NewEthereumAddressFromBytes(b)
	if err != nil {
		panic(err)
	}
	return addr
}
