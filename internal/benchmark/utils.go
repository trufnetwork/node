package benchmark

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/cockroachdb/apd/v3"
	"github.com/kwilteam/kwil-db/common"
	kwiltypes "github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-db/internal/contracts"
	"github.com/truflation/tsn-sdk/core/util"
	"golang.org/x/exp/constraints"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
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

func saveAsCSV(results []Result, filePath string) error {
	// Open the file in append mode, or create it if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Check if the file is empty to determine whether to write the header
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// Create a new CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header row only if the file is empty
	if stat.Size() == 0 {
		header := []string{"procedure", "depth", "n_of_dates", "duration_ms", "compose_visibility", "read_visibility"}
		if err = writer.Write(header); err != nil {
			return err
		}
	}

	// Write each result
	for _, result := range results {
		row := []string{
			string(result.Case.Procedure),                                       // procedure
			strconv.Itoa(result.Case.Depth),                                     // depth
			strconv.Itoa(result.Case.Days),                                      // n_of_dates
			strconv.FormatInt(Average(result.CaseDurations).Milliseconds(), 10), // duration_ms
			visibilityToString(result.Case.Visibility),                          // visibility
		}
		if err = writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func saveAsMarkdown(results []Result, filePath string) error {
	// Open the file in append mode, or create it if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Check if the file is empty to determine whether to write the header
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// Write the header row only if the file is empty
	if stat.Size() == 0 {
		// Write the current date
		date := time.Now().Format("2006-01-02")
		_, err = file.WriteString(fmt.Sprintf("Date: %s\n\n## Dates x Depth\n\n", date))
		if err != nil {
			return err
		}
	}

	// Group results by [procedure][visibility]
	groupedResults := make(map[string]map[string]map[int]map[int]time.Duration)
	for _, result := range results {
		procedure := string(result.Case.Procedure)
		visibility := visibilityToString(result.Case.Visibility)
		if _, ok := groupedResults[procedure]; !ok {
			groupedResults[procedure] = make(map[string]map[int]map[int]time.Duration)
		}
		if _, ok := groupedResults[procedure][visibility]; !ok {
			groupedResults[procedure][visibility] = make(map[int]map[int]time.Duration)
		}
		if _, ok := groupedResults[procedure][visibility][result.Case.Days]; !ok {
			groupedResults[procedure][visibility][result.Case.Days] = make(map[int]time.Duration)
		}
		groupedResults[procedure][visibility][result.Case.Days][result.Case.Depth] = Average(result.CaseDurations)
	}

	// Write markdown for each procedure and visibility combination
	for procedure, visibilities := range groupedResults {
		for visibility, daysMap := range visibilities {
			//TODO: replace with instance type once we can get it from the platform
			instanceType := runtime.GOOS + "_" + runtime.GOARCH
			if _, err = file.WriteString(fmt.Sprintf("%s - %s - %s\n\n", instanceType, procedure, visibility)); err != nil {
				return err
			}

			// Write table header
			_, err = file.WriteString("| queried days / depth |")
			for _, depth := range depths {
				if _, err = file.WriteString(fmt.Sprintf(" %d |", depth)); err != nil {
					return err
				}
			}
			_, err = file.WriteString("\n|----------------------|")
			for range depths {
				if _, err = file.WriteString("---|"); err != nil {
					return err
				}
			}
			_, err = file.WriteString("\n")

			// Write table rows
			for _, day := range days {
				row := fmt.Sprintf("| %d ", day)
				for _, depth := range depths {
					if duration, ok := daysMap[day][depth]; ok {
						row += fmt.Sprintf("| %d ", duration.Milliseconds())
					} else {
						row += "|    "
					}
				}
				row += "|\n"
				if _, err = file.WriteString(row); err != nil {
					return err
				}
			}

			if _, err = file.WriteString("\n"); err != nil {
				return err
			}
		}
	}

	return nil
}

func saveResults(results []Result, filePath string) error {
	// Save as CSV
	if err := saveAsCSV(results, filePath); err != nil {
		return err
	}

	// Save as Markdown
	if err := saveAsMarkdown(results, strings.Replace(filePath, ".csv", ".md", 1)); err != nil {
		return err
	}

	return nil
}

func deleteFileIfExists() error {
	// Delete the CSV file if it exists
	if _, err := os.Stat(filePath); err == nil {
		if err = os.Remove(filePath); err != nil {
			return err
		}
	}

	// Delete the Markdown file if it exists
	mdFilePath := strings.Replace(filePath, ".csv", ".md", 1)
	if _, err := os.Stat(mdFilePath); err == nil {
		if err = os.Remove(mdFilePath); err != nil {
			return err
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
