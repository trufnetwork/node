package digest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	utils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/sdk-go/core/util"

	"github.com/trufnetwork/node/internal/migrations"
)

// TestBenchDigest_Smoke runs the smoke test suite for basic digest functionality validation.
// This is the fastest test suite, focusing on core functionality rather than performance.
func TestBenchDigest_Smoke(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_benchmark_smoke",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestBenchmarkSetup(testDigestSmokeSuite(t)),
		},
	}, utils.GetTestOptions())
}

// TestBenchDigest_Medium runs the medium test suite for scaling analysis.
// This tests performance characteristics across different scales.
// Uses delete cap focused approach with realistic dataset to expose indexing issues.
func TestBenchDigest_Medium(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_benchmark_medium",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestBenchmarkSetup(testDigestMediumSuite(t)),
		},
	}, utils.GetTestOptions())
}

// TestBenchDigest_Big runs the big test suite for stress testing.
// This tests maximum scale performance with large data sets.
// Uses delete cap focused approach with big dataset to expose scaling limits.
func TestBenchDigest_Big(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_benchmark_big",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestBenchmarkSetup(testDigestBigSuite(t)),
		},
	}, utils.GetTestOptions())
}

// TestBenchDigest_Custom runs a custom test suite based on environment variables.
// This allows for flexible test configuration without code changes.
func TestBenchDigest_Custom(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_benchmark_custom",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestBenchmarkSetup(testDigestCustomSuite(t)),
		},
	}, utils.GetTestOptions())
}

// TestBenchDigest_DeleteCap runs the delete cap focused test suite.
// This creates maximum streams once and tests different delete cap values under transactions.
//
// PURPOSE: The large dataset (25k streams × 12 days × 24 records/day) creates a realistic
// production-like environment that exposes indexing and performance issues. The delete
// cap parameter (1K, 10K, 100K, 1M) is what actually controls the processing volume, allowing
// us to measure how the system performs with different batch sizes while maintaining
// realistic data distribution patterns that match production usage (~600k records/day).
func TestBenchDigest_DeleteCap(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "digest_benchmark_delete_cap",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithDigestBenchmarkSetup(testDigestDeleteCapSuite(t)),
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

// runTestCaseInTransaction runs a single test case in its own nested transaction.
// Inspired by the WithTx pattern, this provides perfect test isolation.
func runTestCaseInTransaction(ctx context.Context, platform *kwilTesting.Platform, testCase DigestBenchmarkCase, resultHandler func(*kwilTesting.Platform, []DigestRunResult)) error {
	// Create a nested transaction (inspired by WithTx pattern)
	tx, err := platform.DB.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Automatically cleans up all changes

	// Create a new platform scoped to this transaction
	txPlatform := &kwilTesting.Platform{
		Engine:   platform.Engine,
		DB:       tx, // Use the transaction-specific DB connection
		Deployer: platform.Deployer,
		Logger:   platform.Logger,
	}

	// Set up the deployer on the transaction platform
	deployer, err := util.NewEthereumAddressFromString(DefaultDataProviderAddress)
	if err != nil {
		return fmt.Errorf("failed to create deployer address: %w", err)
	}
	txPlatform.Deployer = deployer.Bytes()
	txPlatform = procedure.WithSigner(txPlatform, deployer.Bytes())

	// Setup benchmark data for this specific test case
	setupInput := DigestSetupInput{
		Platform: txPlatform,
		Case:     testCase,
	}

	if err := SetupBenchmarkData(ctx, setupInput); err != nil {
		return fmt.Errorf("failed to setup benchmark data: %w", err)
	}

	// Run this specific test case
	runner := NewBenchmarkRunner(txPlatform, nil) // No logger needed for transaction
	caseResults, err := runner.RunBenchmarkCase(ctx, testCase)
	if err != nil {
		return fmt.Errorf("failed to run benchmark case: %w", err)
	}

	// Pass results back via callback (before transaction rollback)
	resultHandler(txPlatform, caseResults)

	return nil
}

// runCaseUsingExistingData runs a benchmark case inside a transaction, reusing globally prepared data.
// It only prepares the pending_prune_days for the specific case within the transaction and rolls back after.
func runCaseUsingExistingData(ctx context.Context, platform *kwilTesting.Platform, testCase DigestBenchmarkCase, resultHandler func(*kwilTesting.Platform, []DigestRunResult)) error {
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

	// Run the benchmark for this case using the tx platform
	runner := NewBenchmarkRunner(txPlatform, nil)
	caseResults, err := runner.RunBenchmarkCase(ctx, testCase)
	if err != nil {
		return fmt.Errorf("failed to run benchmark case: %w", err)
	}

	resultHandler(txPlatform, caseResults)
	return nil
}

// testDigestSmokeSuite runs the smoke test cases for basic validation.
func testDigestSmokeSuite(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		runner := NewBenchmarkRunner(platform, t)

		// Use very small test cases for fast smoke test
		smokeCases := []DigestBenchmarkCase{
			{Streams: 2, DaysPerStream: 1, RecordsPerDay: 2, BatchSize: 10, DeleteCap: DefaultDeleteCap, Pattern: PatternRandom, Samples: 1},
			{Streams: 3, DaysPerStream: 1, RecordsPerDay: 2, BatchSize: 10, DeleteCap: DefaultDeleteCap, Pattern: PatternRandom, Samples: 1},
		}

		t.Logf("Running smoke test suite with %d cases", len(smokeCases))

		// Setup minimal benchmark data for smoke tests
		t.Logf("Setting up benchmark data...")
		setupStart := time.Now()

		setupCase := DigestBenchmarkCase{
			Streams:       5, // Very small for fast smoke test
			DaysPerStream: 1, // Single day
			RecordsPerDay: 5, // Very few records
			BatchSize:     DefaultBatchSize,
			DeleteCap:     DefaultDeleteCap,
			Pattern:       PatternRandom,
			Samples:       1,
		}
		setupInput := DigestSetupInput{
			Platform: platform,
			Case:     setupCase,
		}

		if err := SetupBenchmarkData(ctx, setupInput); err != nil {
			return fmt.Errorf("failed to setup benchmark data: %w", err)
		}

		setupDuration := time.Since(setupStart)
		t.Logf("Benchmark data setup completed in %v", setupDuration)

		// Run benchmark cases
		t.Logf("Running benchmark cases...")
		benchmarkStart := time.Now()

		results, err := runner.RunMultipleCases(ctx, smokeCases)
		if err != nil {
			return fmt.Errorf("smoke test suite failed: %w", err)
		}

		benchmarkDuration := time.Since(benchmarkStart)
		t.Logf("Benchmark execution completed in %v", benchmarkDuration)

		// Export results
		t.Logf("Exporting results...")
		var allResults []DigestRunResult
		for _, caseResults := range results {
			allResults = append(allResults, caseResults...)
		}

		outputFile := ResultsFileSmoke
		if err := SaveDigestResultsCSV(allResults, outputFile); err != nil {
			t.Logf("Warning: failed to save results to %s: %v", outputFile, err)
		} else {
			t.Logf("Smoke test results saved to %s", outputFile)
		}

		// Log summary
		totalDuration := time.Since(setupStart)
		t.Logf("Smoke test completed successfully - processed %d total results in %v", len(allResults), totalDuration)

		return nil
	}
}

// testDigestMediumSuite runs the medium test cases for scaling analysis.
func testDigestMediumSuite(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use medium-scale test cases
		mediumCases := MediumTestCases

		t.Logf("Running medium test suite with delete cap variations (%d cases)", len(mediumCases))
		t.Logf("This suite creates maximum streams once and tests delete cap variations under transactions")
		t.Logf("Focus: Delete cap behavior (1K/10K/100K/1M) with realistic data volume for indexing performance testing")

		// 1) Global setup ONCE with maximum streams (superset for all cases)
		// Use the case with the most streams as our global setup
		maxStreamsCase := DigestBenchmarkCase{
			Streams:       10000,                   // Maximum streams needed for delete cap tests
			DaysPerStream: 30,                      // Maximum days needed
			RecordsPerDay: ProductionRecordsPerDay, // Production records per day
			BatchSize:     DefaultBatchSize,
			DeleteCap:     DefaultDeleteCap,
			Pattern:       PatternRandom,
			Samples:       1,
		}
		t.Logf("Performing global setup with superset: %+v", maxStreamsCase)
		if err := SetupBenchmarkData(ctx, DigestSetupInput{Platform: platform, Case: maxStreamsCase}); err != nil {
			return fmt.Errorf("global setup failed: %w", err)
		}

		// 2) Per-case execution inside a transaction with different delete caps
		results := make(map[string][]DigestRunResult)
		for i, testCase := range mediumCases {
			t.Logf("Running medium case %d/%d with delete_cap=%d: %+v", i+1, len(mediumCases), testCase.DeleteCap, testCase)

			if err := runCaseWithDeleteCap(ctx, platform, testCase, func(txPlatform *kwilTesting.Platform, caseResults []DigestRunResult) {
				caseKey := fmt.Sprintf("streams_%d_days_%d_records_%d_batch_%d_deletecap_%d_pattern_%s",
					testCase.Streams, testCase.DaysPerStream, testCase.RecordsPerDay, testCase.BatchSize, testCase.DeleteCap, testCase.Pattern)
				results[caseKey] = caseResults
			}); err != nil {
				return fmt.Errorf("failed to run medium case %d: %w", i, err)
			}
		}

		// Export results
		var allResults []DigestRunResult
		for _, caseResults := range results {
			allResults = append(allResults, caseResults...)
		}

		outputFile := ResultsFileMedium
		if err := SaveDigestResultsCSV(allResults, outputFile); err != nil {
			t.Logf("Warning: failed to save results to %s: %v", outputFile, err)
		} else {
			t.Logf("Medium test results saved to %s", outputFile)
		}

		// Create summary report
		summaryFile := strings.Replace(outputFile, ".csv", "_summary.md", 1)
		if err := ExportResultsSummary(allResults, summaryFile); err != nil {
			t.Logf("Warning: failed to create summary report: %v", err)
		} else {
			t.Logf("Summary report created at %s", summaryFile)
		}

		t.Logf("Medium test completed successfully - processed %d total results", len(allResults))

		return nil
	}
}

// testDigestBigSuite runs the big test cases for stress testing.
func testDigestBigSuite(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use big-scale test cases
		bigCases := BigTestCases

		t.Logf("Running big test suite with delete cap variations (%d cases)", len(bigCases))
		t.Logf("Warning: Big tests may take significant time and resources")
		t.Logf("This suite creates maximum streams once and tests delete cap variations under transactions")
		t.Logf("Strategy: Large dataset exposes scaling issues, delete cap (1K/10K/100K/1M) controls actual processing volume")

		// 1) Global setup ONCE with maximum streams (superset for all cases)
		// This big dataset creates the most challenging environment to expose
		// performance and scaling issues that only appear at production scale.
		// The delete cap (not the total data size) controls actual deletion volume.
		maxStreamsCase := DigestBenchmarkCase{
			Streams:       50000,                   // Scaled down for manageability while maintaining big conditions
			DaysPerStream: 60,                      // Long history to test temporal query patterns
			RecordsPerDay: ProductionRecordsPerDay, // Production-like data density (24 records/day)
			BatchSize:     DefaultBatchSize,
			DeleteCap:     DefaultDeleteCap, // This will be overridden per test case
			Pattern:       PatternRandom,
			Samples:       1,
		}
		t.Logf("Performing global setup with superset: %+v", maxStreamsCase)
		t.Logf("Note: Big dataset tests system limits, but delete cap controls actual processing volume")
		if err := SetupBenchmarkData(ctx, DigestSetupInput{Platform: platform, Case: maxStreamsCase}); err != nil {
			return fmt.Errorf("global setup failed: %w", err)
		}

		// 2) Per-case execution inside a transaction with different delete caps
		results := make(map[string][]DigestRunResult)
		for i, testCase := range bigCases {
			t.Logf("Running big case %d/%d with delete_cap=%d: %+v", i+1, len(bigCases), testCase.DeleteCap, testCase)

			if err := runCaseWithDeleteCap(ctx, platform, testCase, func(txPlatform *kwilTesting.Platform, caseResults []DigestRunResult) {
				caseKey := fmt.Sprintf("streams_%d_days_%d_records_%d_batch_%d_deletecap_%d_pattern_%s",
					testCase.Streams, testCase.DaysPerStream, testCase.RecordsPerDay, testCase.BatchSize, testCase.DeleteCap, testCase.Pattern)
				results[caseKey] = caseResults
			}); err != nil {
				return fmt.Errorf("failed to run big case %d: %w", i, err)
			}
		}

		// Export results
		var allResults []DigestRunResult
		for _, caseResults := range results {
			allResults = append(allResults, caseResults...)
		}

		outputFile := "digest_big_results.csv"
		if err := SaveDigestResultsCSV(allResults, outputFile); err != nil {
			t.Logf("Warning: failed to save results to %s: %v", outputFile, err)
		} else {
			t.Logf("Big test results saved to %s", outputFile)
		}

		// Create summary report
		summaryFile := strings.Replace(outputFile, ".csv", "_summary.md", 1)
		if err := ExportResultsSummary(allResults, summaryFile); err != nil {
			t.Logf("Warning: failed to create summary report: %v", err)
		} else {
			t.Logf("Summary report created at %s", summaryFile)
		}

		t.Logf("Big test completed successfully - processed %d total results", len(allResults))

		return nil
	}
}

// CustomBenchmarkConfig holds configuration for custom benchmark runs.
type CustomBenchmarkConfig struct {
	Streams     []int    `koanf:"streams"`
	Days        []int    `koanf:"days"`
	Records     []int    `koanf:"records"`
	BatchSizes  []int    `koanf:"batch_sizes"`
	Patterns    []string `koanf:"patterns"`
	Samples     int      `koanf:"samples"`
	ResultsPath string   `koanf:"results_path"`
}

// DefaultCustomConfig returns the default configuration for custom benchmarks.
func DefaultCustomConfig() CustomBenchmarkConfig {
	return CustomBenchmarkConfig{
		Streams:     []int{100, 1000, 10000},
		Days:        []int{1, 30},
		Records:     []int{2, 50},
		BatchSizes:  []int{10, 50, 200},
		Patterns:    []string{"random", "dups50", "monotonic"},
		Samples:     3,
		ResultsPath: ResultsFileCustom,
	}
}

// LoadCustomConfig loads benchmark configuration from environment variables using koanf.
func LoadCustomConfig() (CustomBenchmarkConfig, error) {
	k := koanf.New(".")

	// Load default configuration
	config := DefaultCustomConfig()

	// Override with environment variables
	if err := k.Load(env.Provider(EnvPrefixDigest, ".", func(s string) string {
		// Convert DIGEST_STREAMS -> streams, etc.
		return strings.ToLower(strings.TrimPrefix(s, EnvPrefixDigest))
	}), nil); err != nil {
		return config, fmt.Errorf("error loading env config: %w", err)
	}

	// Parse comma-separated integer lists
	if streamsStr := k.String(EnvKeyStreams); streamsStr != "" {
		if streams, err := parseIntList(streamsStr); err != nil {
			return config, fmt.Errorf("error parsing streams: %w", err)
		} else {
			config.Streams = streams
		}
	}

	if daysStr := k.String(EnvKeyDays); daysStr != "" {
		if days, err := parseIntList(daysStr); err != nil {
			return config, fmt.Errorf("error parsing days: %w", err)
		} else {
			config.Days = days
		}
	}

	if recordsStr := k.String(EnvKeyRecords); recordsStr != "" {
		if records, err := parseIntList(recordsStr); err != nil {
			return config, fmt.Errorf("error parsing records: %w", err)
		} else {
			config.Records = records
		}
	}

	if batchSizesStr := k.String(EnvKeyBatchSizes); batchSizesStr != "" {
		if batchSizes, err := parseIntList(batchSizesStr); err != nil {
			return config, fmt.Errorf("error parsing batch_sizes: %w", err)
		} else {
			config.BatchSizes = batchSizes
		}
	}

	// Parse simple values
	if samples := k.Int(EnvKeySamples); samples > 0 {
		config.Samples = samples
	}

	if resultsPath := k.String(EnvKeyResults); resultsPath != "" {
		config.ResultsPath = resultsPath
	}

	return config, nil
}

// parseIntList parses a comma-separated string into a slice of integers.
func parseIntList(s string) ([]int, error) {
	if s == "" {
		return []int{}, nil
	}

	parts := strings.Split(s, ",")
	result := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		val, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid integer '%s': %w", part, err)
		}
		result = append(result, val)
	}

	return result, nil
}

// testDigestDeleteCapSuite runs delete cap focused test cases.
// This creates maximum streams once and tests different delete cap values under transactions.
//
// KEY INSIGHT: The multiple streams and days create a realistic dataset that exposes
// indexing and performance issues that smaller datasets might miss. The delete cap
// parameter (1K, 10K, 1M) is what actually controls how many records get processed/deleted, not
// the total data volume. The dataset uses production-like patterns (24 records/day) to ensure
// we're testing in conditions similar to production while focusing the performance measurement
// on the delete cap behavior.
func testDigestDeleteCapSuite(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use medium-scale test cases (same as medium suite for consistency)
		deleteCapCases := MediumTestCases

		t.Logf("Running delete cap test suite with %d cases", len(deleteCapCases))
		t.Logf("This suite creates maximum streams once and tests delete cap variations under transactions")
		t.Logf("Note: Large dataset ensures realistic indexing/query patterns, but delete cap (1K/10K/100K/1M) controls actual deletion volume")

		// 1) Global setup ONCE with maximum streams (superset for all cases)
		// This creates a realistic dataset that exceeds what we'll actually delete,
		// ensuring we test with production-like data volumes and indexing patterns.
		// The delete cap (not the total data size) controls actual deletion volume.
		maxStreamsCase := DigestBenchmarkCase{
			Streams:       10000,                   // Creates realistic stream diversity for indexing tests
			DaysPerStream: 30,                      // Sufficient history to test temporal patterns
			RecordsPerDay: ProductionRecordsPerDay, // Production-like data density (24 records/day)
			BatchSize:     DefaultBatchSize,
			DeleteCap:     DefaultDeleteCap, // This will be overridden per test case
			Pattern:       PatternRandom,
			Samples:       1,
		}
		t.Logf("Performing global setup with superset: %+v", maxStreamsCase)
		t.Logf("Note: This large dataset ensures realistic indexing/query patterns for performance testing")
		if err := SetupBenchmarkData(ctx, DigestSetupInput{Platform: platform, Case: maxStreamsCase}); err != nil {
			return fmt.Errorf("global setup failed: %w", err)
		}

		// 2) Per-case execution inside a transaction with different delete caps
		results := make(map[string][]DigestRunResult)
		for i, testCase := range deleteCapCases {
			t.Logf("Running delete cap case %d/%d with delete_cap=%d: %+v", i+1, len(deleteCapCases), testCase.DeleteCap, testCase)

			if err := runCaseWithDeleteCap(ctx, platform, testCase, func(txPlatform *kwilTesting.Platform, caseResults []DigestRunResult) {
				caseKey := fmt.Sprintf("streams_%d_days_%d_records_%d_batch_%d_deletecap_%d_pattern_%s",
					testCase.Streams, testCase.DaysPerStream, testCase.RecordsPerDay, testCase.BatchSize, testCase.DeleteCap, testCase.Pattern)
				results[caseKey] = caseResults
			}); err != nil {
				return fmt.Errorf("failed to run delete cap case %d: %w", i, err)
			}
		}

		// Export results
		var allResults []DigestRunResult
		for _, caseResults := range results {
			allResults = append(allResults, caseResults...)
		}

		outputFile := "digest_delete_cap_results.csv"
		if err := SaveDigestResultsCSV(allResults, outputFile); err != nil {
			t.Logf("Warning: failed to save results to %s: %v", outputFile, err)
		} else {
			t.Logf("Delete cap test results saved to %s", outputFile)
		}

		// Create summary report
		summaryFile := strings.Replace(outputFile, ".csv", "_summary.md", 1)
		if err := ExportResultsSummary(allResults, summaryFile); err != nil {
			t.Logf("Warning: failed to create summary report: %v", err)
		} else {
			t.Logf("Summary report created at %s", summaryFile)
		}

		t.Logf("Delete cap test completed successfully - processed %d total results", len(allResults))

		return nil
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

// testDigestCustomSuite runs custom test cases based on environment variables.
func testDigestCustomSuite(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		runner := NewBenchmarkRunner(platform, t)

		// Load configuration using koanf
		config, err := LoadCustomConfig()
		if err != nil {
			t.Logf("Warning: Failed to load custom config, using defaults: %v", err)
			config = DefaultCustomConfig()
		}

		// Generate all combinations using lo functions for cleaner code
		var customCases []DigestBenchmarkCase

		// Use nested loops to generate all combinations
		for _, streams := range config.Streams {
			for _, days := range config.Days {
				for _, records := range config.Records {
					for _, batchSize := range config.BatchSizes {
						for _, pattern := range config.Patterns {
							customCases = append(customCases, DigestBenchmarkCase{
								Streams:          streams,
								DaysPerStream:    days,
								RecordsPerDay:    records,
								BatchSize:        batchSize,
								DeleteCap:        DefaultDeleteCap,
								Pattern:          pattern,
								Samples:          config.Samples,
								IdempotencyCheck: false, // Disable for performance in large runs
							})
						}
					}
				}
			}
		}

		t.Logf("Running custom test suite with %d cases", len(customCases))
		t.Logf("Configuration: streams=%v, days=%v, records=%v, batch_sizes=%v, patterns=%v, samples=%d",
			config.Streams, config.Days, config.Records, config.BatchSizes, config.Patterns, config.Samples)

		results, err := runner.RunMultipleCases(ctx, customCases)
		if err != nil {
			return fmt.Errorf("custom test suite failed: %w", err)
		}

		// Export results
		var allResults []DigestRunResult
		for _, caseResults := range results {
			allResults = append(allResults, caseResults...)
		}

		if err := SaveDigestResultsCSV(allResults, config.ResultsPath); err != nil {
			t.Logf("Warning: failed to save results to %s: %v", config.ResultsPath, err)
		} else {
			t.Logf("Custom test results saved to %s", config.ResultsPath)
		}

		// Create summary report
		summaryFile := strings.Replace(config.ResultsPath, ".csv", "_summary.md", 1)
		if err := ExportResultsSummary(allResults, summaryFile); err != nil {
			t.Logf("Warning: failed to create summary report: %v", err)
		} else {
			t.Logf("Summary report created at %s", summaryFile)
		}

		t.Logf("Custom test completed successfully - processed %d total results", len(allResults))

		return nil
	}
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
			BatchSize:     5,
			DeleteCap:     DefaultDeleteCap,
			Pattern:       pattern,
			Samples:       1,
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
			BatchSize:     10,
			DeleteCap:     DefaultDeleteCap,
			Pattern:       "random",
			Samples:       3,
		},
		Candidates:         3000,
		ProcessedDays:      3000,
		TotalDeletedRows:   147000,
		TotalPreservedRows: 3000,
		Duration:           1000000000, // 1 second
		MemoryMaxBytes:     104857600,  // 100 MB
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
				BatchSize:     50,
				DeleteCap:     DefaultDeleteCap,
				Pattern:       "random",
				Samples:       1,
			},
			Candidates:         100,
			ProcessedDays:      100,
			TotalDeletedRows:   4900,
			TotalPreservedRows: 200,
			Duration:           234000000, // 234ms
			MemoryMaxBytes:     12582912,  // 12 MB
		},
	}

	// Test CSV export
	tempFile := "/tmp/test_digest_results.csv"
	defer os.Remove(tempFile)

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

// BenchmarkDigestSliceCandidates benchmarks the candidate slicing performance.
func BenchmarkDigestSliceCandidates(b *testing.B) {
	// Setup test data
	streamRefs := make([]int, 10000)
	dayIdxs := make([]int, 10000)
	for i := range streamRefs {
		streamRefs[i] = i % 1000
		dayIdxs[i] = i % 365
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SliceCandidates(streamRefs, dayIdxs, 50)
	}
}

// BenchmarkDigestAggregation benchmarks result aggregation performance.
func BenchmarkDigestAggregation(b *testing.B) {
	// Setup test data
	results := make([]DigestRunResult, 100)
	for i := range results {
		results[i] = DigestRunResult{
			Candidates:         100,
			ProcessedDays:      100,
			TotalDeletedRows:   5000,
			TotalPreservedRows: 200,
			Duration:           100000000, // 100ms
			MemoryMaxBytes:     10485760,  // 10 MB
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		AggregateResults(results)
	}
}
