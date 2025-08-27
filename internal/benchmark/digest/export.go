package digest

import (
	"fmt"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/montanaflynn/stats"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

// SavedDigestResult represents a single row in the CSV export.
// This structure uses JSON tags for CSV header generation and follows the existing benchexport pattern.
type SavedDigestResult struct {
	Streams       int `json:"streams"`
	DaysPerStream int `json:"days_per_stream"`
	RecordsPerDay int `json:"records_per_day"`

	Pattern           string  `json:"pattern"`
	Samples           int     `json:"samples"`
	Candidates        int     `json:"candidates"`
	ProcessedDays     int     `json:"processed_days"`
	TotalDeleted      int     `json:"total_deleted"`
	TotalPreserved    int     `json:"total_preserved"`
	DurationMs        int64   `json:"duration_ms"`
	DaysPerSec        float64 `json:"days_per_sec"`
	RowsDeletedPerSec float64 `json:"rows_deleted_per_sec"`
	MemoryMB          uint64  `json:"memory_mb"`
	WalBytes          *int64  `json:"wal_bytes,omitempty"` // Optional WAL tracking
	Timestamp         string  `json:"timestamp"`           // When the result was recorded
}

// convertSavedResultToRunResult converts a SavedDigestResult back to DigestRunResult.
// This handles the reverse transformation from export format to internal types.
func convertSavedResultToRunResult(saved SavedDigestResult) DigestRunResult {
	return DigestRunResult{
		Case: DigestBenchmarkCase{
			Streams:       saved.Streams,
			DaysPerStream: saved.DaysPerStream,
			RecordsPerDay: saved.RecordsPerDay,

			Pattern: saved.Pattern,
			Samples: saved.Samples,
		},
		Candidates:           saved.Candidates,
		ProcessedDays:        saved.ProcessedDays,
		TotalDeletedRows:     saved.TotalDeleted,
		TotalPreservedRows:   saved.TotalPreserved,
		Duration:             time.Duration(saved.DurationMs) * time.Millisecond,
		MemoryMaxBytes:       saved.MemoryMB * 1024 * 1024, // Convert MB back to bytes
		DaysPerSecond:        saved.DaysPerSec,
		RowsDeletedPerSecond: saved.RowsDeletedPerSec,
		WALBytes:             saved.WalBytes,
	}
}

// ConvertToSavedResult converts a DigestRunResult to a SavedDigestResult for CSV export.
// This handles the transformation from internal types to export format.
func ConvertToSavedResult(result DigestRunResult) SavedDigestResult {
	saved := SavedDigestResult{
		Streams:       result.Case.Streams,
		DaysPerStream: result.Case.DaysPerStream,
		RecordsPerDay: result.Case.RecordsPerDay,

		Pattern:           result.Case.Pattern,
		Samples:           result.Case.Samples,
		Candidates:        result.Candidates,
		ProcessedDays:     result.ProcessedDays,
		TotalDeleted:      result.TotalDeletedRows,
		TotalPreserved:    result.TotalPreservedRows,
		DurationMs:        result.Duration.Milliseconds(),
		DaysPerSec:        result.DaysPerSecond,
		RowsDeletedPerSec: result.RowsDeletedPerSecond,
		MemoryMB:          result.MemoryMaxBytes / 1024 / 1024, // Convert bytes to MB
		WalBytes:          result.WALBytes,
		Timestamp:         time.Now().Format(time.RFC3339),
	}

	return saved
}

// SaveDigestResultsCSV exports multiple DigestRunResult instances to a CSV file.
// This reuses the existing benchexport pattern for consistency.
func SaveDigestResultsCSV(results []DigestRunResult, filePath string) error {
	if len(results) == 0 {
		return errors.New("no results to save")
	}

	// Convert all results to saved format
	var savedResults []SavedDigestResult
	for _, result := range results {
		savedResults = append(savedResults, ConvertToSavedResult(result))
	}

	// Reuse existing benchexport functionality
	return saveOrAppendToCSVUsingBenchexport(savedResults, filePath)
}

// saveOrAppendToCSVUsingBenchexport saves data using gocsv for robust CSV handling.
// This provides better error handling and automatic type conversion.
func saveOrAppendToCSVUsingBenchexport(data []SavedDigestResult, filePath string) error {
	// Open file in create/truncate mode for now (simplified approach)
	// TODO: Implement proper append functionality when needed
	file, err := os.Create(filePath)
	if err != nil {
		return errors.Wrapf(err, "error creating file %s", filePath)
	}
	defer file.Close()

	// Write data with headers
	if err := gocsv.MarshalFile(&data, file); err != nil {
		return errors.Wrap(err, "error writing CSV data")
	}

	return nil
}

// LoadDigestResultsCSV loads SavedDigestResult instances from a CSV file.
// This uses gocsv for robust CSV parsing with automatic type conversion.
func LoadDigestResultsCSV(filePath string) ([]SavedDigestResult, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening CSV file %s", filePath)
	}
	defer file.Close()

	var results []SavedDigestResult
	if err := gocsv.UnmarshalFile(file, &results); err != nil {
		return nil, errors.Wrapf(err, "error loading CSV file %s", filePath)
	}

	return results, nil
}

// ExportResultsSummary creates a summary report of benchmark results.
// This generates a human-readable summary using tablewriter for better formatting.
func ExportResultsSummary(results []DigestRunResult, filePath string) error {
	if len(results) == 0 {
		return errors.New("no results to summarize")
	}

	file, err := os.Create(filePath)
	if err != nil {
		return errors.Wrapf(err, "error creating summary file %s", filePath)
	}
	defer file.Close()

	// Write summary header
	fmt.Fprintf(file, "# Digest Benchmark Results Summary\n\n")
	fmt.Fprintf(file, "Generated at: %s\n\n", time.Now().Format(time.RFC3339))

	// Write summary statistics using tablewriter
	fmt.Fprintf(file, "## Summary Statistics\n\n")

	summaryTable := tablewriter.NewWriter(file)
	summaryTable.SetHeader([]string{"Metric", "Value"})
	summaryTable.SetBorder(false)
	summaryTable.Append([]string{"Total Results", fmt.Sprintf("%d", len(results))})
	summaryTable.Append([]string{"Unique Cases", fmt.Sprintf("%d", countUniqueCases(results))})
	summaryTable.Append([]string{"Total Candidates", fmt.Sprintf("%d", sumCandidates(results))})
	summaryTable.Append([]string{"Total Processed Days", fmt.Sprintf("%d", sumProcessedDays(results))})
	summaryTable.Append([]string{"Total Deleted Rows", fmt.Sprintf("%d", sumDeletedRows(results))})
	summaryTable.Append([]string{"Average Duration", averageDuration(results).String()})
	summaryTable.Append([]string{"Peak Memory Usage", fmt.Sprintf("%d MB", peakMemoryMB(results))})
	summaryTable.Render()

	// Write per-case breakdown using tablewriter
	fmt.Fprintf(file, "\n## Per-Case Breakdown\n\n")

	caseTable := tablewriter.NewWriter(file)
	caseTable.SetHeader([]string{
		"Streams", "Days/Stream", "Records/Day", "Delete Cap",
		"Pattern", "Samples", "Candidates", "Duration", "Days/sec", "Memory MB",
	})
	caseTable.SetBorder(true)
	caseTable.SetRowSeparator("-")
	caseTable.SetColumnSeparator("|")
	caseTable.SetCenterSeparator("+")

	for _, result := range results {
		caseTable.Append([]string{
			fmt.Sprintf("%d", result.Case.Streams),
			fmt.Sprintf("%d", result.Case.DaysPerStream),
			fmt.Sprintf("%d", result.Case.RecordsPerDay),
			fmt.Sprintf("%d", result.Case.DeleteCap),

			result.Case.Pattern,
			fmt.Sprintf("%d", result.Case.Samples),
			fmt.Sprintf("%d", result.Candidates),
			result.Duration.String(),
			fmt.Sprintf("%.2f", result.DaysPerSecond),
			fmt.Sprintf("%d", result.MemoryMaxBytes/1024/1024),
		})
	}

	caseTable.Render()

	// Write performance analysis section
	fmt.Fprintf(file, "\n## Performance Analysis\n\n")

	// Calculate and display performance insights using lo functions
	if len(results) > 0 {
		durations := lo.Map(results, func(r DigestRunResult, _ int) time.Duration { return r.Duration })
		minDuration := lo.Min(durations)
		maxDuration := lo.Max(durations)
		avgDuration := lo.Mean(lo.Map(durations, func(d time.Duration, _ int) float64 { return float64(d.Nanoseconds()) }))

		throughputs := lo.Map(results, func(r DigestRunResult, _ int) float64 { return r.DaysPerSecond })
		maxThroughput := lo.Max(throughputs)

		fmt.Fprintf(file, "### Duration Statistics\n")
		fmt.Fprintf(file, "- Min Duration: %v\n", minDuration)
		fmt.Fprintf(file, "- Max Duration: %v\n", maxDuration)
		fmt.Fprintf(file, "- Avg Duration: %v\n", time.Duration(avgDuration))
		fmt.Fprintf(file, "- Max Throughput: %.2f days/sec\n", maxThroughput)

		// Pattern performance comparison
		patternGroups := lo.GroupBy(results, func(r DigestRunResult) string { return r.Case.Pattern })
		if len(patternGroups) > 1 {
			fmt.Fprintf(file, "\n### Pattern Performance Comparison\n")
			for pattern, patternResults := range patternGroups {
				avgThroughput := lo.Mean(lo.Map(patternResults, func(r DigestRunResult, _ int) float64 { return r.DaysPerSecond }))
				fmt.Fprintf(file, "- %s: %.2f days/sec (avg)\n", pattern, avgThroughput)
			}
		}
	}

	return nil
}

// Helper functions for summary statistics
func countUniqueCases(results []DigestRunResult) int {
	caseMap := make(map[string]bool)
	for _, result := range results {
		key := fmt.Sprintf("%d_%d_%d_%s_%d",
			result.Case.Streams,
			result.Case.DaysPerStream,
			result.Case.RecordsPerDay,
			result.Case.Pattern,
			result.Case.Samples,
		)
		caseMap[key] = true
	}
	return len(caseMap)
}

func sumCandidates(results []DigestRunResult) int {
	total := 0
	for _, result := range results {
		total += result.Candidates
	}
	return total
}

func sumProcessedDays(results []DigestRunResult) int {
	total := 0
	for _, result := range results {
		total += result.ProcessedDays
	}
	return total
}

func sumDeletedRows(results []DigestRunResult) int {
	total := 0
	for _, result := range results {
		total += result.TotalDeletedRows
	}
	return total
}

func averageDuration(results []DigestRunResult) time.Duration {
	if len(results) == 0 {
		return 0
	}
	var total time.Duration
	for _, result := range results {
		total += result.Duration
	}
	return total / time.Duration(len(results))
}

func peakMemoryMB(results []DigestRunResult) uint64 {
	var peak uint64
	for _, result := range results {
		if result.MemoryMaxBytes > peak {
			peak = result.MemoryMaxBytes
		}
	}
	return peak / 1024 / 1024
}

// MergeResultsFiles merges multiple CSV result files into a single file.
// This is useful for combining results from multiple benchmark runs.
func MergeResultsFiles(inputFiles []string, outputFile string) error {
	if len(inputFiles) == 0 {
		return errors.New("no input files provided")
	}

	var allResults []SavedDigestResult

	// Load all results from input files
	for _, file := range inputFiles {
		results, err := LoadDigestResultsCSV(file)
		if err != nil {
			return errors.Wrapf(err, "error loading results from %s", file)
		}
		allResults = append(allResults, results...)
	}

	// Convert SavedDigestResult back to DigestRunResult for saving
	var runResults []DigestRunResult
	for _, saved := range allResults {
		runResults = append(runResults, convertSavedResultToRunResult(saved))
	}

	// Save merged results
	return SaveDigestResultsCSV(runResults, outputFile)
}

// ValidateCSVFile validates that a CSV file contains valid benchmark results.
// This checks for required columns and data integrity.
func ValidateCSVFile(filePath string) error {
	results, err := LoadDigestResultsCSV(filePath)
	if err != nil {
		return errors.Wrap(err, "error loading CSV file")
	}

	if len(results) == 0 {
		return errors.New("CSV file contains no results")
	}

	// Validate each result
	for i, result := range results {
		if err := validateSavedResult(result); err != nil {
			return errors.Wrapf(err, "result %d validation failed", i)
		}
	}

	return nil
}

// validateSavedResult validates a single SavedDigestResult for data integrity.
func validateSavedResult(result SavedDigestResult) error {
	if result.Streams <= 0 {
		return errors.New("streams must be positive")
	}
	if result.DaysPerStream <= 0 {
		return errors.New("days_per_stream must be positive")
	}
	if result.RecordsPerDay <= 0 {
		return errors.New("records_per_day must be positive")
	}

	if result.Samples <= 0 {
		return errors.New("samples must be positive")
	}
	if result.Candidates < 0 {
		return errors.New("candidates cannot be negative")
	}
	if result.ProcessedDays < 0 {
		return errors.New("processed_days cannot be negative")
	}
	if result.DurationMs < 0 {
		return errors.New("duration_ms cannot be negative")
	}
	if result.MemoryMB == 0 {
		return errors.New("memory_mb should be measured")
	}

	return nil
}

// GenerateBenchmarkReport creates a comprehensive benchmark report using all new libraries.
// This demonstrates the power of combining gocsv, tablewriter, and montanaflynn/stats.
func GenerateBenchmarkReport(results []DigestRunResult, outputDir string) error {
	if len(results) == 0 {
		return errors.New("no results to report")
	}

	// Save raw results using gocsv
	csvPath := fmt.Sprintf("%s/benchmark_results.csv", outputDir)
	if err := SaveDigestResultsCSV(results, csvPath); err != nil {
		return errors.Wrap(err, "failed to save CSV results")
	}

	// Generate summary report using tablewriter
	summaryPath := fmt.Sprintf("%s/benchmark_summary.md", outputDir)
	if err := ExportResultsSummary(results, summaryPath); err != nil {
		return errors.Wrap(err, "failed to generate summary report")
	}

	// Generate statistical analysis report
	statsPath := fmt.Sprintf("%s/statistical_analysis.md", outputDir)
	if err := GenerateStatisticalAnalysisReport(results, statsPath); err != nil {
		return errors.Wrap(err, "failed to generate statistical analysis")
	}

	return nil
}

// GenerateStatisticalAnalysisReport creates detailed statistical analysis using montanaflynn/stats.
func GenerateStatisticalAnalysisReport(results []DigestRunResult, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return errors.Wrapf(err, "error creating analysis file %s", filePath)
	}
	defer file.Close()

	fmt.Fprintf(file, "# Statistical Analysis Report\n\n")
	fmt.Fprintf(file, "Generated at: %s\n\n", time.Now().Format(time.RFC3339))

	// Extract metrics for statistical analysis
	durations := lo.Map(results, func(r DigestRunResult, _ int) float64 { return float64(r.Duration.Milliseconds()) })
	throughputs := lo.Map(results, func(r DigestRunResult, _ int) float64 { return r.DaysPerSecond })
	memoryUsages := lo.Map(results, func(r DigestRunResult, _ int) float64 { return float64(r.MemoryMaxBytes) / 1024 / 1024 })

	// Duration statistics
	fmt.Fprintf(file, "## Duration Analysis (milliseconds)\n\n")
	if mean, err := stats.Mean(durations); err == nil {
		fmt.Fprintf(file, "- Mean: %.2f ms\n", mean)
	}
	if median, err := stats.Median(durations); err == nil {
		fmt.Fprintf(file, "- Median: %.2f ms\n", median)
	}
	if stdDev, err := stats.StandardDeviation(durations); err == nil {
		fmt.Fprintf(file, "- Standard Deviation: %.2f ms\n", stdDev)
	}
	if p95, err := stats.Percentile(durations, 95); err == nil {
		fmt.Fprintf(file, "- 95th Percentile: %.2f ms\n", p95)
	}
	if p99, err := stats.Percentile(durations, 99); err == nil {
		fmt.Fprintf(file, "- 99th Percentile: %.2f ms\n", p99)
	}

	// Throughput statistics
	fmt.Fprintf(file, "\n## Throughput Analysis (days/second)\n\n")
	if mean, err := stats.Mean(throughputs); err == nil {
		fmt.Fprintf(file, "- Mean: %.2f days/sec\n", mean)
	}
	if max, err := stats.Max(throughputs); err == nil {
		fmt.Fprintf(file, "- Maximum: %.2f days/sec\n", max)
	}
	if min, err := stats.Min(throughputs); err == nil {
		fmt.Fprintf(file, "- Minimum: %.2f days/sec\n", min)
	}

	// Memory usage statistics
	fmt.Fprintf(file, "\n## Memory Usage Analysis (MB)\n\n")
	if mean, err := stats.Mean(memoryUsages); err == nil {
		fmt.Fprintf(file, "- Mean: %.2f MB\n", mean)
	}
	if max, err := stats.Max(memoryUsages); err == nil {
		fmt.Fprintf(file, "- Peak: %.2f MB\n", max)
	}

	// Pattern comparison using lo.GroupBy
	fmt.Fprintf(file, "\n## Pattern Performance Comparison\n\n")
	patternGroups := lo.GroupBy(results, func(r DigestRunResult) string { return r.Case.Pattern })

	patternTable := tablewriter.NewWriter(file)
	patternTable.SetHeader([]string{"Pattern", "Count", "Avg Throughput", "Avg Duration", "Peak Memory"})
	patternTable.SetBorder(true)

	for pattern, patternResults := range patternGroups {
		patternThroughputs := lo.Map(patternResults, func(r DigestRunResult, _ int) float64 { return r.DaysPerSecond })
		patternDurations := lo.Map(patternResults, func(r DigestRunResult, _ int) float64 { return float64(r.Duration.Milliseconds()) })
		patternMemories := lo.Map(patternResults, func(r DigestRunResult, _ int) float64 { return float64(r.MemoryMaxBytes) / 1024 / 1024 })

		avgThroughput, _ := stats.Mean(patternThroughputs)
		avgDuration, _ := stats.Mean(patternDurations)
		peakMemory, _ := stats.Max(patternMemories)

		patternTable.Append([]string{
			pattern,
			fmt.Sprintf("%d", len(patternResults)),
			fmt.Sprintf("%.2f", avgThroughput),
			fmt.Sprintf("%.2f ms", avgDuration),
			fmt.Sprintf("%.2f MB", peakMemory),
		})
	}

	patternTable.Render()

	// Performance insights
	fmt.Fprintf(file, "\n## Performance Insights\n\n")

	// Identify best performing pattern
	bestPattern := ""
	bestThroughput := 0.0
	for pattern, patternResults := range patternGroups {
		avgThroughput, _ := stats.Mean(lo.Map(patternResults, func(r DigestRunResult, _ int) float64 { return r.DaysPerSecond }))
		if avgThroughput > bestThroughput {
			bestThroughput = avgThroughput
			bestPattern = pattern
		}
	}

	fmt.Fprintf(file, "- **Best Pattern**: %s (%.2f days/sec average)\n", bestPattern, bestThroughput)

	// Duration variability analysis
	if stdDev, err := stats.StandardDeviation(durations); err == nil {
		if mean, err := stats.Mean(durations); err == nil {
			coefficientOfVariation := stdDev / mean
			if coefficientOfVariation > 0.5 {
				fmt.Fprintf(file, "- **Duration Variability**: High (%.2f%%) - Consider investigating sources of variance\n", coefficientOfVariation*100)
			} else if coefficientOfVariation > 0.2 {
				fmt.Fprintf(file, "- **Duration Variability**: Moderate (%.2f%%) - Performance is reasonably consistent\n", coefficientOfVariation*100)
			} else {
				fmt.Fprintf(file, "- **Duration Variability**: Low (%.2f%%) - Excellent performance consistency\n", coefficientOfVariation*100)
			}
		}
	}

	return nil
}
