package digest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	utils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/sdk-go/core/util"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/trufnetwork/node/internal/migrations"
)

// DigestBenchmarkScale defines the configuration for different benchmark scales.
// - exact_fit: Measures optimal performance when data volume matches delete cap exactly (10K/100K caps)
// - with_excess: Tests real-world scenarios with excess data, detecting indexing/algorithmic issues (1K/10K/100K caps)
//
// Uses TestCases[0].RecordsPerDay for setup data generation (eliminates SetupRecords redundancy)
type DigestBenchmarkScale struct {
	Name         string
	Description  string
	SetupStreams int
	SetupDays    int
	TestCases    []DigestBenchmarkCase // RecordsPerDay from first test case used for setup
}

// Benchmark scale configurations - kept DRY in a single location for easy maintenance
var benchmarkScales = map[string]DigestBenchmarkScale{
	"smoke": {
		Name:         "smoke",
		Description:  "fastest test suite for basic validation",
		SetupStreams: 5,
		SetupDays:    1,
		TestCases: []DigestBenchmarkCase{
			{Streams: 2, DaysPerStream: 1, RecordsPerDay: 2, DeleteCap: 1000, Pattern: PatternRandom, Samples: 1},
			{Streams: 3, DaysPerStream: 1, RecordsPerDay: 2, DeleteCap: 1000, Pattern: PatternRandom, Samples: 1},
		},
	},
	"exact_fit": {
		Name:         "exact_fit",
		Description:  "exact fit testing - data volume matches delete cap exactly",
		SetupStreams: 347,
		SetupDays:    12,
		TestCases: []DigestBenchmarkCase{
			{Streams: 347, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: 10000, Pattern: PatternRandom, Samples: DefaultSamples, IdempotencyCheck: true},
			{Streams: 347, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: 100000, Pattern: PatternRandom, Samples: DefaultSamples, IdempotencyCheck: true},
		},
	},
	"with_excess": {
		Name:         "with_excess",
		Description:  "excess data testing - measures performance with more data than processed",
		SetupStreams: 25000,
		SetupDays:    12,
		TestCases: []DigestBenchmarkCase{
			{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: 1000, Pattern: PatternRandom, Samples: DefaultSamples},   // 1K cap - tests small batch performance (~600k records/day)
			{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: 10000, Pattern: PatternRandom, Samples: DefaultSamples},  // 10K cap - tests medium batch performance (~600k records/day)
			{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: 100000, Pattern: PatternRandom, Samples: DefaultSamples}, // 100K cap - tests large batch performance (~600k records/day)
		},
	},
}

// TestBenchDigest_Smoke runs the smoke test suite for basic digest functionality validation.
// This is the fastest test suite, focusing on core functionality rather than performance.
// IDE will show individual run gutter for this test.
func TestBenchDigest_Smoke(t *testing.T) {
	runDigestBenchmarkScale(t, benchmarkScales["smoke"])
}

// TestBenchDigest_ExactFit runs the exact fit test suite.
// Measures performance when data volume matches delete cap exactly (optimal scenarios).
// IDE will show individual run gutter for this test.
func TestBenchDigest_ExactFit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}
	runDigestBenchmarkScale(t, benchmarkScales["exact_fit"])
}

// TestBenchDigest_WithExcess runs the with excess test suite.
// Tests performance with excess data beyond processing capacity (real-world scenarios).
// IDE will show individual run gutter for this test.
func TestBenchDigest_WithExcess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}
	runDigestBenchmarkScale(t, benchmarkScales["with_excess"])
}

// createBenchmarkSuiteFunc creates a benchmark suite function for the given scale.
// All scales now use consistent delete cap behavior with runCaseWithDeleteCap.
func createBenchmarkSuiteFunc(t *testing.T, scale DigestBenchmarkScale) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		if len(scale.TestCases) == 0 {
			t.Fatalf("Scale %s has no test cases defined", scale.Name)
		}

		testCases := scale.TestCases
		// Ensure setup covers the largest case and all cases agree on RecordsPerDay
		maxStreams, maxDays := 0, 0
		setupRecords := testCases[0].RecordsPerDay
		for _, tc := range testCases {
			if tc.Streams > maxStreams {
				maxStreams = tc.Streams
			}
			if tc.DaysPerStream > maxDays {
				maxDays = tc.DaysPerStream
			}
			if tc.RecordsPerDay != setupRecords {
				t.Fatalf("inconsistent RecordsPerDay across cases: expected %d, got %d", setupRecords, tc.RecordsPerDay)
			}
		}
		if scale.SetupStreams < maxStreams || scale.SetupDays < maxDays {
			t.Fatalf("insufficient setup: have %d streams/%d days; need at least %d/%d", scale.SetupStreams, scale.SetupDays, maxStreams, maxDays)
		}

		// Provide scale-specific context in logging
		switch scale.Name {
		case "smoke":
			t.Logf("Running smoke test suite with delete cap variations (%d cases)", len(testCases))
			t.Logf("Focus: Fast validation of basic digest functionality")
		case "exact_fit":
			t.Logf("Running exact_fit test suite with delete cap variations (%d cases)", len(testCases))
			t.Logf("Focus: Delete cap behavior when data volume matches processing capacity exactly")
		case "with_excess":
			t.Logf("Running with_excess test suite with delete cap variations (%d cases)", len(testCases))
			t.Logf("Focus: Delete cap behavior (1K/10K/100K) with excess data to detect indexing/algorithmic issues")
		}

		// Setup data generation for all test cases
		setupCase := DigestBenchmarkCase{
			Streams:       scale.SetupStreams,
			DaysPerStream: scale.SetupDays,
			RecordsPerDay: setupRecords,
			Pattern:       PatternRandom,
			Samples:       1,
		}

		setupInput := DigestSetupInput{Platform: platform, Case: setupCase}
		if err := SetupBenchmarkData(ctx, setupInput); err != nil {
			return fmt.Errorf("global setup failed: %w", err)
		}

		// Run each test case in a transaction using consistent delete cap behavior
		results := make(map[string][]DigestRunResult)
		for i, testCase := range testCases {
			t.Logf("Running %s case %d/%d with delete_cap=%d", scale.Name, i+1, len(testCases), testCase.DeleteCap)

			if err := runCaseWithDeleteCap(ctx, platform, testCase, func(txPlatform *kwilTesting.Platform, caseResults []DigestRunResult) {
				caseKey := fmt.Sprintf("streams_%d_days_%d_records_%d_deletecap_%d_pattern_%s",
					testCase.Streams, testCase.DaysPerStream, testCase.RecordsPerDay, testCase.DeleteCap, testCase.Pattern)
				results[caseKey] = caseResults
			}); err != nil {
				return fmt.Errorf("failed to run %s case %d (delete_cap=%d): %w", scale.Name, i, testCase.DeleteCap, err)
			}
		}

		// Collect and export results
		var allResults []DigestRunResult
		for _, caseResults := range results {
			allResults = append(allResults, caseResults...)
		}

		outputFile := fmt.Sprintf("bench_results/digest_%s_results.csv", scale.Name)
		if err := os.MkdirAll(filepath.Dir(outputFile), 0o755); err != nil {
			t.Logf("Warning: failed to ensure results dir: %v", err)
		}
		if err := SaveDigestResultsCSV(allResults, outputFile); err != nil {
			t.Logf("Warning: failed to save results to %s: %v", outputFile, err)
		} else {
			t.Logf("%s test results saved to %s", cases.Title(language.English).String(scale.Name), outputFile)
		}

		// Create summary report
		summaryFile := fmt.Sprintf("bench_results/digest_%s_results_summary.md", scale.Name)
		if err := os.MkdirAll(filepath.Dir(summaryFile), 0o755); err != nil {
			t.Logf("Warning: failed to ensure summary dir: %v", err)
		}
		if err := ExportResultsSummary(allResults, summaryFile); err != nil {
			t.Logf("Warning: failed to create summary report: %v", err)
		} else {
			t.Logf("Summary report created at %s", summaryFile)
		}

		t.Logf("%s test completed successfully - processed %d total results", cases.Title(language.English).String(scale.Name), len(allResults))
		return nil
	}
}

// runDigestBenchmarkScale runs a specific benchmark scale.
// This is the core function that handles all benchmark execution logic.
func runDigestBenchmarkScale(t *testing.T, scale DigestBenchmarkScale) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        fmt.Sprintf("digest_benchmark_%s", scale.Name),
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestBenchmarkSetup(createBenchmarkSuiteFunc(t, scale)),
		},
	}, utils.GetTestOptions())
}

// WithDigestBenchmarkSetup sets up the testing environment for digest benchmarks.
// This wrapper function handles common setup tasks required for all digest tests.
func WithDigestBenchmarkSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a proper deployer address and set it on the platform
		deployer, err := util.NewEthereumAddressFromString(DefaultDataProviderAddress)
		if err != nil {
			return fmt.Errorf("failed to create deployer address: %w", err)
		}
		platform.Deployer = deployer.Bytes()
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Note: Stream creation and data setup is handled in the individual test functions
		// This wrapper only sets up the platform environment

		return testFn(ctx, platform)
	}
}

// runCaseWithDeleteCap runs a benchmark case with a specific delete cap inside a transaction.
// It uses existing streams and data, only varying the delete cap parameter.
//
// KEY BEHAVIOR: Even with large datasets (e.g., 10k streams × 30 days), the delete cap
// parameter controls how many records actually get processed. For example:
//   - With delete_cap=1000, only ~1k records will be deleted regardless of dataset size
//   - With delete_cap=10000, only ~10k records will be deleted regardless of dataset size
//   - With delete_cap=1000000, only ~1M records will be deleted regardless of dataset size
//   - The large dataset ensures realistic indexing/query patterns and exposes performance
//     issues that smaller datasets might miss
func runCaseWithDeleteCap(ctx context.Context, platform *kwilTesting.Platform, testCase DigestBenchmarkCase, resultHandler func(*kwilTesting.Platform, []DigestRunResult)) error {
	// Create a nested transaction
	tx, err := platform.DB.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create a tx-scoped platform
	txPlatform := &kwilTesting.Platform{
		Engine:   platform.Engine,
		DB:       tx,
		Deployer: platform.Deployer,
		Logger:   platform.Logger,
	}

	// Build candidates for this case using existing streams
	streamRefs, err := getStreamRefsUpTo(ctx, txPlatform, testCase.Streams)
	if err != nil {
		return fmt.Errorf("failed to get stream refs: %w", err)
	}
	if len(streamRefs) < testCase.Streams {
		return fmt.Errorf("insufficient streams available: need %d, got %d", testCase.Streams, len(streamRefs))
	}

	// Prepare pending_prune_days for this case only
	totalCandidates := testCase.Streams * testCase.DaysPerStream
	streamRefCandidates := make([]int, totalCandidates)
	dayIdxCandidates := make([]int, totalCandidates)
	idx := 0
	for _, streamRef := range streamRefs[:testCase.Streams] {
		for day := 0; day < testCase.DaysPerStream; day++ {
			streamRefCandidates[idx] = streamRef
			dayIdxCandidates[idx] = day
			idx++
		}
	}
	if err := InsertPendingPruneDays(ctx, txPlatform, streamRefCandidates, dayIdxCandidates); err != nil {
		return fmt.Errorf("failed to insert pending prune days: %w", err)
	}

	// Run the benchmark for this case using the tx platform with the specific delete cap
	runner := NewBenchmarkRunner(txPlatform, nil)
	caseResults, err := runner.RunBenchmarkCase(ctx, testCase)
	if err != nil {
		return fmt.Errorf("failed to run benchmark case: %w", err)
	}

	resultHandler(txPlatform, caseResults)
	return nil
}

// Environment variable parsing is now handled by koanf in LoadCustomConfig()

// Test utilities for validation and debugging

// TestDigestTypes validates that all digest types are properly defined and consistent.
func TestDigestTypes(t *testing.T) {
	// Test that all required patterns are supported
	patterns := []string{"random", "dups50", "monotonic", "equal", "time_dup"}

	for _, pattern := range patterns {
		benchmarkCase := DigestBenchmarkCase{
			Streams:       1,
			DaysPerStream: 1,
			RecordsPerDay: 10,

			DeleteCap: DefaultDeleteCap,
			Pattern:   pattern,
			Samples:   1,
		}

		if err := ValidateBenchmarkCase(benchmarkCase); err != nil {
			t.Errorf("Pattern %s validation failed: %v", pattern, err)
		}
	}

	t.Logf("All %d digest patterns validated successfully", len(patterns))
}

// TestDigestResultValidation validates result validation logic.
func TestDigestResultValidation(t *testing.T) {
	// Test valid result
	validResult := DigestRunResult{
		Case: DigestBenchmarkCase{
			Streams:       100,
			DaysPerStream: 30,
			RecordsPerDay: 50,

			DeleteCap: DefaultDeleteCap,
			Pattern:   "random",
			Samples:   3,
		},
		Candidates:       3000,
		ProcessedDays:    3000,
		TotalDeletedRows: 147000,

		Duration:       1000000000, // 1 second
		MemoryMaxBytes: 104857600,  // 100 MB
	}

	if err := ValidateResult(validResult); err != nil {
		t.Errorf("Valid result failed validation: %v", err)
	}

	// Test invalid result (negative values)
	invalidResult := validResult
	invalidResult.Candidates = -1

	if err := ValidateResult(invalidResult); err == nil {
		t.Error("Invalid result should have failed validation")
	}

	t.Log("Result validation logic works correctly")
}

// TestDigestCSVExport tests the CSV export functionality.
func TestDigestCSVExport(t *testing.T) {
	// Create test results
	results := []DigestRunResult{
		{
			Case: DigestBenchmarkCase{
				Streams:       100,
				DaysPerStream: 1,
				RecordsPerDay: 50,

				DeleteCap: DefaultDeleteCap,
				Pattern:   "random",
				Samples:   1,
			},
			Candidates:       100,
			ProcessedDays:    100,
			TotalDeletedRows: 4900,

			Duration:       234000000, // 234ms
			MemoryMaxBytes: 12582912,  // 12 MB
		},
	}

	// Test CSV export
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_digest_results.csv")

	if err := SaveDigestResultsCSV(results, tempFile); err != nil {
		t.Fatalf("Failed to save CSV: %v", err)
	}

	// Test CSV loading
	loaded, err := LoadDigestResultsCSV(tempFile)
	if err != nil {
		t.Fatalf("Failed to load CSV: %v", err)
	}

	if len(loaded) != len(results) {
		t.Errorf("Expected %d results, got %d", len(results), len(loaded))
	}

	// Test CSV validation
	if err := ValidateCSVFile(tempFile); err != nil {
		t.Errorf("CSV validation failed: %v", err)
	}

	t.Log("CSV export/import functionality works correctly")
}

// TestDigestMarkdownExport tests the improved markdown table export functionality.
func TestDigestMarkdownExport(t *testing.T) {
	// Create test results with realistic data
	results := []DigestRunResult{
		{
			Case: DigestBenchmarkCase{
				Streams:       25000,
				DaysPerStream: 12,
				RecordsPerDay: 24,
				DeleteCap:     1000,
				Pattern:       "random",
				Samples:       3,
			},
			Candidates:           300000,
			ProcessedDays:        300000,
			TotalDeletedRows:     1000,
			Duration:             946942286,          // ~947ms
			MemoryMaxBytes:       4650 * 1024 * 1024, // 4650 MB
			StreamDaysPerSecond:  64.42,
			RowsDeletedPerSecond: 1000 / (946942286 / 1e9), // Calculate from duration
		},
		{
			Case: DigestBenchmarkCase{
				Streams:       25000,
				DaysPerStream: 12,
				RecordsPerDay: 24,
				DeleteCap:     10000,
				Pattern:       "random",
				Samples:       3,
			},
			Candidates:           300000,
			ProcessedDays:        300000,
			TotalDeletedRows:     10000,
			Duration:             979084756,          // ~979ms
			MemoryMaxBytes:       4657 * 1024 * 1024, // 4657 MB
			StreamDaysPerSecond:  634.27,
			RowsDeletedPerSecond: 10000 / (979084756 / 1e9), // Calculate from duration
		},
	}

	// Test markdown export to temp directory
	tempDir := t.TempDir()
	summaryFile := filepath.Join(tempDir, "test_markdown_summary.md")
	if err := ExportResultsSummary(results, summaryFile); err != nil {
		t.Fatalf("Failed to export markdown summary: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(summaryFile); os.IsNotExist(err) {
		t.Fatalf("Summary file was not created: %s", summaryFile)
	}

	t.Logf("Improved markdown summary exported to %s", summaryFile)
}

// BenchmarkDigestAggregation benchmarks result aggregation performance.
func BenchmarkDigestAggregation(b *testing.B) {
	// Setup test data
	results := make([]DigestRunResult, 100)
	for i := range results {
		results[i] = DigestRunResult{
			Candidates:       100,
			ProcessedDays:    100,
			TotalDeletedRows: 5000,

			Duration:       100000000, // 100ms
			MemoryMaxBytes: 10485760,  // 10 MB
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		AggregateResults(results)
	}
}
