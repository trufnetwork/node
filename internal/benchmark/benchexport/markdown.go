package benchexport

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/fbiville/markdown-table-formatter/pkg/markdown"
	"golang.org/x/exp/slices"
)

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

type SaveAsMarkdownInput struct {
	Results      []SavedResults
	CurrentDate  time.Time
	InstanceType string
	FilePath     string
}

// groupKey is our single structure for grouping, replacing nested maps.
type groupKey struct {
	BranchingFactor int
	Procedure       string
	Visibility      string
	DataPoints      int
	QtyStreams      int
	UnixOnly        bool
}

// -----------------------------------------------------------------------------
// Main Entry Point
// -----------------------------------------------------------------------------

// SaveAsMarkdown is the main entry point.
func SaveAsMarkdown(input SaveAsMarkdownInput) error {
	if err := validateSampleCounts(input.Results); err != nil {
		return err
	}

	// Gather distinct values for data points, qty streams, and branching factors.
	dataPoints := distinctDataPoints(input.Results)
	qtyStreams := distinctQtyStreams(input.Results)
	branchingFactors := distinctBranchingFactors(input.Results)

	// Group your results by the key (branchingFactor + procedure + visibility + dataPoints + qtyStreams + unixOnly).
	grouped := groupResults(input.Results)

	// Open the target file and handle header writing if empty.
	file, err := os.OpenFile(input.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", input.FilePath, err)
	}
	defer file.Close()

	// Check if file is empty => if so, write some basic header info.
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	if stat.Size() == 0 {
		if err := writeHeader(file, input); err != nil {
			return err
		}
	}

	// Create the entire markdown content in-memory and then write it once.
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("### %s\n\n", input.InstanceType))

	for _, bf := range branchingFactors {
		sb.WriteString(fmt.Sprintf("#### Branching Factor: %d\n\n", bf))

		// For each branching factor, gather all unique procedures and sort them.
		procs := distinctProceduresForBF(grouped, bf)
		sort.Slice(procs, func(i, j int) bool {
			return procs[i] < procs[j]
		})

		for _, proc := range procs {
			visList := distinctVisibilities(grouped, bf, proc)
			sort.Slice(visList, func(i, j int) bool {
				return visList[i] < visList[j]
			})

			for _, vis := range visList {
				sb.WriteString(fmt.Sprintf("%s - %s - %s\n\n", input.InstanceType, proc, vis))

				// Distinguish by UnixOnly => gather which UnixOnly modes exist
				unixModes := distinctUnixOnlyValues(grouped, bf, proc, vis)
				sort.Slice(unixModes, func(i, j int) bool {
					// false should come before true, purely by taste
					return !unixModes[i] && unixModes[j]
				})

				for _, uMode := range unixModes {
					if err := writeTableForUnixMode(&sb, grouped, input, bf, proc, vis, uMode, dataPoints, qtyStreams); err != nil {
						return fmt.Errorf("failed to write table for UnixOnly=%v: %w", uMode, err)
					}
				}
				sb.WriteString("\n")
			}
			sb.WriteString("\n")
		}
	}

	// Finally, write everything we collected to the file at once.
	if _, err = file.WriteString(sb.String()); err != nil {
		return fmt.Errorf("failed to write markdown to file: %w", err)
	}

	log.Printf("Saving to %s complete!", input.FilePath)
	return nil
}

// -----------------------------------------------------------------------------
// Table Generation
// -----------------------------------------------------------------------------

// writeTableForUnixMode writes a single table for a specific UnixOnly mode.
func writeTableForUnixMode(
	sb *strings.Builder,
	grouped map[groupKey]int64,
	input SaveAsMarkdownInput,
	bf int,
	proc string,
	vis string,
	uMode bool,
	dataPoints []int,
	qtyStreams []int,
) error {
	sb.WriteString(fmt.Sprintf("**UnixOnly = %v**\n\n", uMode))

	// Collect the columns (QtyStreams) that actually have data
	existingQty := existingQtyStreams(grouped, bf, proc, vis, uMode, dataPoints, qtyStreams)

	// Make a table with a top row = [ "Days / Qty", q1, q2, ... ]
	headers := make([]string, 0, len(existingQty)+1)
	headers = append(headers, "Data points / Qty streams")
	for _, q := range existingQty {
		headers = append(headers, fmt.Sprintf("%d", q))
	}

	// Build table
	tableFormatter := markdown.NewTableFormatterBuilder().
		WithPrettyPrint().
		Build(headers...)

	var rows [][]string
	for _, d := range dataPoints {
		var row []string
		row = append(row, fmt.Sprintf("%d", d))
		var rowHasData bool

		for _, q := range existingQty {
			key := groupKey{
				BranchingFactor: bf,
				Procedure:       proc,
				Visibility:      vis,
				DataPoints:      d,
				QtyStreams:      q,
				UnixOnly:        uMode,
			}
			duration, ok := grouped[key]
			if ok {
				row = append(row, fmt.Sprintf("%d", duration))
				rowHasData = true
			} else {
				row = append(row, "-")
			}
		}

		// If no data for the entire row, skip it.
		if rowHasData {
			rows = append(rows, row)
		}
	}

	formattedTable, err := tableFormatter.Format(rows)
	if err != nil {
		return fmt.Errorf("failed to format table: %w", err)
	}
	sb.WriteString(formattedTable + "\n\n")
	return nil
}

// -----------------------------------------------------------------------------
// Validation and Header Writing
// -----------------------------------------------------------------------------

// validateSampleCounts ensures all results have the same sample count.
func validateSampleCounts(results []SavedResults) error {
	counts := make(map[int]int)
	for _, r := range results {
		counts[r.Samples]++
	}
	if len(counts) > 1 {
		return fmt.Errorf("results have different amount of samples")
	}
	return nil
}

// writeHeader writes the initial lines (date, sample info, etc.) to an empty file.
func writeHeader(file *os.File, input SaveAsMarkdownInput) error {
	dateStr := input.CurrentDate.Format("2006-01-02 15:04:05")
	if _, err := file.WriteString(fmt.Sprintf("Date: %s\n\n## Data points / Qty streams\n\n", dateStr)); err != nil {
		return err
	}
	samples := input.Results[0].Samples
	if _, err := file.WriteString(fmt.Sprintf("Samples per query: %d\n", samples)); err != nil {
		return err
	}
	if _, err := file.WriteString("Results in milliseconds\n\n"); err != nil {
		return err
	}
	return nil
}

// -----------------------------------------------------------------------------
// Data Grouping and Transformation
// -----------------------------------------------------------------------------

// groupResults returns a map of groupKey -> durationMs, so we can do simple lookups.
func groupResults(results []SavedResults) map[groupKey]int64 {
	out := make(map[groupKey]int64)
	for _, r := range results {
		key := groupKey{
			BranchingFactor: r.BranchingFactor,
			Procedure:       r.Procedure,
			Visibility:      r.Visibility,
			DataPoints:      r.DataPoints,
			QtyStreams:      r.QtyStreams,
			UnixOnly:        r.UnixOnly,
		}
		out[key] = r.DurationMs
	}
	return out
}

// -----------------------------------------------------------------------------
// Distinct Value Helpers (Raw Results)
// -----------------------------------------------------------------------------

// distinctDataPoints gathers distinct data points (days) from results, sorted ascending.
func distinctDataPoints(results []SavedResults) []int {
	set := make(map[int]struct{})
	for _, r := range results {
		set[r.DataPoints] = struct{}{}
	}
	out := make([]int, 0, len(set))
	for d := range set {
		out = append(out, d)
	}
	slices.Sort(out)
	return out
}

// distinctQtyStreams gathers distinct quantity of streams from results, sorted ascending.
func distinctQtyStreams(results []SavedResults) []int {
	set := make(map[int]struct{})
	for _, r := range results {
		set[r.QtyStreams] = struct{}{}
	}
	out := make([]int, 0, len(set))
	for q := range set {
		out = append(out, q)
	}
	slices.Sort(out)
	return out
}

// distinctBranchingFactors gathers distinct branching factors, sorted ascending.
func distinctBranchingFactors(results []SavedResults) []int {
	set := make(map[int]struct{})
	for _, r := range results {
		set[r.BranchingFactor] = struct{}{}
	}
	out := make([]int, 0, len(set))
	for bf := range set {
		out = append(out, bf)
	}
	slices.Sort(out)
	return out
}

// -----------------------------------------------------------------------------
// Distinct Value Helpers (Using Grouped Map)
// -----------------------------------------------------------------------------

// distinctProceduresForBF returns all distinct procedures associated with a particular BF.
func distinctProceduresForBF(grouped map[groupKey]int64, bf int) []string {
	procs := make(map[string]struct{})
	for k := range grouped {
		if k.BranchingFactor == bf {
			procs[k.Procedure] = struct{}{}
		}
	}
	out := make([]string, 0, len(procs))
	for p := range procs {
		out = append(out, p)
	}
	return out
}

// distinctVisibilities returns all distinct visibilities for a given BF + procedure.
func distinctVisibilities(grouped map[groupKey]int64, bf int, proc string) []string {
	visSet := make(map[string]struct{})
	for k := range grouped {
		if k.BranchingFactor == bf && k.Procedure == proc {
			visSet[k.Visibility] = struct{}{}
		}
	}
	out := make([]string, 0, len(visSet))
	for v := range visSet {
		out = append(out, v)
	}
	return out
}

// distinctUnixOnlyValues returns all UnixOnly modes (true/false) used for BF+proc+vis.
func distinctUnixOnlyValues(grouped map[groupKey]int64, bf int, proc string, vis string) []bool {
	modeSet := make(map[bool]struct{})
	for k := range grouped {
		if k.BranchingFactor == bf && k.Procedure == proc && k.Visibility == vis {
			modeSet[k.UnixOnly] = struct{}{}
		}
	}
	out := make([]bool, 0, len(modeSet))
	for m := range modeSet {
		out = append(out, m)
	}
	return out
}

// existingQtyStreams picks only those QtyStreams that actually have data for a given BF/proc/vis/unixOnly combination.
func existingQtyStreams(
	grouped map[groupKey]int64,
	bf int,
	proc string,
	vis string,
	unixMode bool,
	dataPoints []int,
	qtyStreams []int,
) []int {
	var result []int
	for _, q := range qtyStreams {
		hasData := false
		for _, d := range dataPoints {
			key := groupKey{
				BranchingFactor: bf,
				Procedure:       proc,
				Visibility:      vis,
				DataPoints:      d,
				QtyStreams:      q,
				UnixOnly:        unixMode,
			}
			if _, ok := grouped[key]; ok {
				hasData = true
				break
			}
		}
		if hasData {
			result = append(result, q)
		}
	}
	return result
}
