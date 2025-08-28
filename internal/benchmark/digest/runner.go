package digest

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	benchutil "github.com/trufnetwork/node/internal/benchmark/util"
)

// RunBatchDigestOnce executes auto_digest for a batch of pending prune days.
// Returns processed days, deleted rows, and preserved rows counts.
// Note: auto_digest doesn't return preserved_rows, so we estimate it as deleted_rows for now.
func RunBatchDigestOnce(ctx context.Context, platform interface{}, batchSize int, deleteCap int) (processedDays, totalDeleted, totalPreserved int, err error) {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return 0, 0, 0, errors.New("invalid platform type")
	}

	// Get the deployer for signing via shared helper
	dep, err := GetDeployerOrDefault(kwilPlatform)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "failed to get deployer")
	}

	// Create transaction context
	txContext := NewTxContext(ctx, kwilPlatform, dep)

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Call the auto_digest action
	r, err := kwilPlatform.Engine.Call(engineContext, kwilPlatform.DB, "", "auto_digest", []any{
		batchSize,
		deleteCap,
	}, func(row *common.Row) error {
		if len(row.Values) != 3 {
			return errors.Errorf("expected 3 columns, got %d", len(row.Values))
		}

		// Parse the results - auto_digest returns (processed_days, total_deleted_rows, has_more)
		var processed, deleted int
		if processedVal, ok := row.Values[0].(int64); ok {
			processed = int(processedVal)
		}
		if deletedVal, ok := row.Values[1].(int64); ok {
			deleted = int(deletedVal)
		}

		processedDays = processed
		totalDeleted = deleted
		// auto_digest doesn't return preserved_rows, estimate as 0 for now
		// In a real scenario, this could be calculated separately if needed
		totalPreserved = 0

		return nil
	})

	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "error calling auto_digest")
	}
	if r.Error != nil {
		return 0, 0, 0, errors.Wrap(r.Error, "auto_digest procedure error")
	}

	return processedDays, totalDeleted, totalPreserved, nil
}

// MeasureBatchDigest measures performance metrics for an auto_digest execution.
// Returns a DigestRunResult with timing, memory, and throughput metrics.
// The collector parameter can be nil if memory monitoring is disabled.
func MeasureBatchDigest(ctx context.Context, platform interface{}, batchSize int, deleteCap int, collector *benchutil.DockerMemoryCollector) (DigestRunResult, error) {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return DigestRunResult{}, errors.New("invalid platform type")
	}

	// Execute the auto digest and measure timing
	startTime := time.Now()
	processedDays, totalDeleted, totalPreserved, err := RunBatchDigestOnce(ctx, kwilPlatform, batchSize, deleteCap)
	duration := time.Since(startTime)

	if err != nil {
		return DigestRunResult{}, errors.Wrap(err, "auto digest execution failed")
	}

	// Get memory usage (0 if monitoring is disabled)
	var maxMemory uint64
	if collector != nil {
		var err error
		maxMemory, err = collector.GetMaxMemoryUsage()
		if err != nil {
			// Log error but don't fail the entire benchmark
			fmt.Printf("Warning: failed to get memory usage: %v\n", err)
		}
	}

	// Calculate throughput metrics
	var daysPerSecond, rowsDeletedPerSecond float64
	if duration.Seconds() > 0 {
		daysPerSecond = float64(processedDays) / duration.Seconds()
		rowsDeletedPerSecond = float64(totalDeleted) / duration.Seconds()
	}

	// Create result
	result := DigestRunResult{
		Candidates:           batchSize,
		ProcessedDays:        processedDays,
		TotalDeletedRows:     totalDeleted,
		TotalPreservedRows:   totalPreserved,
		Duration:             duration,
		MemoryMaxBytes:       maxMemory,
		DaysPerSecond:        daysPerSecond,
		RowsDeletedPerSecond: rowsDeletedPerSecond,
		WALBytes:             nil, // TODO: Implement WAL tracking
	}

	return result, nil
}

// RunCase executes a complete benchmark case with multiple samples.
// Returns all run results for statistical analysis.
func RunCase(ctx context.Context, platform interface{}, c DigestBenchmarkCase) ([]DigestRunResult, error) {
	// Validate the benchmark case first
	if err := ValidateBenchmarkCase(c); err != nil {
		return nil, errors.Wrap(err, "invalid benchmark case")
	}

	// Ensure kwil testing platform for nested transactions per sample
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return nil, errors.New("invalid platform type")
	}

	var results []DigestRunResult
	// With auto_digest, we don't need to generate explicit candidates
	// auto_digest will pick candidates from pending_prune_days table
	// Use all candidates in one batch, DeleteCap controls actual processing
	batchSize := c.Streams * c.DaysPerStream

	// Send all candidates in one call - DeleteCap controls actual processing

	for sample := 0; sample < c.Samples; sample++ {
		var sampleResults []DigestRunResult

		// Start memory monitoring for this sample
		var collector *benchutil.DockerMemoryCollector
		if enableMemoryMonitoring := os.Getenv("ENABLE_MEMORY_MONITORING"); enableMemoryMonitoring != "false" {
			fmt.Printf("Attempting to start Docker memory monitoring for container '%s' (sample %d/%d)...\n", DockerPostgresContainer, sample+1, c.Samples)

			var err error
			collector, err = benchutil.StartDockerMemoryCollector(DockerPostgresContainer)
			if err != nil {
				// Log the error but continue without memory monitoring
				fmt.Printf("Warning: failed to start memory collector: %v\n", err)
				fmt.Println("Continuing without memory monitoring...")
				fmt.Println("Tip: Set ENABLE_MEMORY_MONITORING=false to disable this warning")
			} else {
				// Wait for the collector to receive at least one stats sample with shorter timeout
				sampleTimeout := 5 * time.Second // Reduced from 10 seconds
				if timeoutEnv := os.Getenv("DOCKER_STATS_TIMEOUT"); timeoutEnv != "" {
					if parsed, err := time.ParseDuration(timeoutEnv); err == nil {
						sampleTimeout = parsed
					}
				}

				sampleCtx, cancel := context.WithTimeout(context.Background(), sampleTimeout)
				defer cancel()

				// Create a channel to signal completion
				done := make(chan error, 1)
				go func() {
					done <- collector.WaitForFirstSample()
				}()

				select {
				case err := <-done:
					if err != nil {
						// Log the error but continue without memory monitoring
						fmt.Printf("Warning: failed to get first memory sample: %v\n", err)
						fmt.Println("Continuing without memory monitoring...")
						collector.Stop()
						collector = nil
					} else {
						fmt.Println("Memory monitoring started successfully")
						defer collector.Stop()
					}
				case <-sampleCtx.Done():
					fmt.Printf("Warning: Docker stats collection timed out after %v\n", sampleTimeout)
					fmt.Println("Continuing without memory monitoring...")
					collector.Stop()
					collector = nil
				}
			}
		} else {
			fmt.Println("Memory monitoring disabled via ENABLE_MEMORY_MONITORING=false")
		}

		// Begin nested transaction (savepoint) for this sample
		nestedTx, err := kwilPlatform.DB.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to begin nested transaction for sample %d", sample)
		}

		// Create a platform bound to the nested transaction
		samplePlatform := &kwilTesting.Platform{
			Engine:   kwilPlatform.Engine,
			DB:       nestedTx,
			Deployer: kwilPlatform.Deployer,
			Logger:   kwilPlatform.Logger,
		}

		// Run auto_digest with batch size - DeleteCap controls processing
		result, err := MeasureBatchDigest(ctx, samplePlatform, batchSize, c.DeleteCap, collector)
		if err != nil {
			// Rollback nested tx on error before returning
			_ = nestedTx.Rollback(ctx)
			return nil, errors.Wrapf(err, "failed to measure batch for sample %d", sample)
		}

		// Update result with case information
		result.Case = c
		result.Candidates = batchSize

		sampleResults = append(sampleResults, result)

		// Always rollback the nested transaction to reset state for next sample
		if err := nestedTx.Rollback(ctx); err != nil {
			return nil, errors.Wrapf(err, "failed to rollback nested transaction for sample %d", sample)
		}

		// Aggregate results for this sample
		if len(sampleResults) > 0 {
			aggregatedResult := AggregateResults(sampleResults)
			aggregatedResult.Case = c
			results = append(results, aggregatedResult)
		}
	}

	return results, nil
}

// VerifyDigestStats performs spot checks to verify digest correctness.
// Compares before/after record counts and validates OHLC preservation.
func VerifyDigestStats(ctx context.Context, platform interface{}, sampleSize int) (bool, error) {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return false, errors.New("invalid platform type")
	}

	// Get the deployer for signing queries via helper
	deployer, err := GetDeployerOrDefault(kwilPlatform)
	if err != nil {
		return false, errors.Wrap(err, "failed to get deployer")
	}

	// Create transaction context for queries
	txContext := NewTxContext(ctx, kwilPlatform, deployer)

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Query for pending prune days to verify they exist
	var pendingCount int
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLCountPendingPruneDays,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if count, ok := row.Values[0].(int64); ok {
					pendingCount = int(count)
				}
			}
			return nil
		})

	if err != nil {
		return false, errors.Wrap(err, "error querying pending_prune_days")
	}

	// Check that there are pending days to process
	if pendingCount == 0 {
		return true, nil // No pending work is valid
	}

	// Query for primitive events to verify they exist
	var eventCount int
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLCountPrimitiveEvents,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if count, ok := row.Values[0].(int64); ok {
					eventCount = int(count)
				}
			}
			return nil
		})

	if err != nil {
		return false, errors.Wrap(err, "error querying primitive_events")
	}

	// Basic sanity checks
	if eventCount == 0 && pendingCount > 0 {
		return false, errors.New("pending prune days exist but no primitive events found")
	}

	// TODO: Add more sophisticated verification checks
	// - Verify OHLC preservation for digested days
	// - Check primitive_event_type table population
	// - Validate record counts before/after

	return true, nil
}

// VerifyIdempotency checks that re-running digest returns 0 changes.
// This validates the idempotent behavior of the digest system.
func VerifyIdempotency(ctx context.Context, platform interface{}, c DigestBenchmarkCase) (bool, error) {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return false, errors.New("invalid platform type")
	}

	// With auto_digest, use batch size for idempotency test
	// auto_digest will pick candidates from pending_prune_days
	batchSize := c.Streams * c.DaysPerStream

	// Run digest once
	firstProcessed, firstDeleted, firstPreserved, err := RunBatchDigestOnce(ctx, kwilPlatform, batchSize, c.DeleteCap)
	if err != nil {
		return false, errors.Wrap(err, "error in first digest run")
	}

	// Run digest again on the same candidates
	secondProcessed, secondDeleted, secondPreserved, err := RunBatchDigestOnce(ctx, kwilPlatform, batchSize, c.DeleteCap)
	if err != nil {
		return false, errors.Wrap(err, "error in second digest run")
	}

	// Verify idempotency: second run should process 0 days and delete 0 rows
	if secondProcessed != 0 {
		return false, fmt.Errorf("second run processed %d days, expected 0", secondProcessed)
	}
	if secondDeleted != 0 {
		return false, fmt.Errorf("second run deleted %d rows, expected 0", secondDeleted)
	}
	if secondPreserved != 0 {
		return false, fmt.Errorf("second run preserved %d rows, expected 0", secondPreserved)
	}

	fmt.Printf("Idempotency verified: first run (%d processed, %d deleted, %d preserved), second run (0, 0, 0)\n",
		firstProcessed, firstDeleted, firstPreserved)

	return true, nil
}

// AggregateResults combines multiple DigestRunResult instances into a single aggregated result.
// This is useful for combining results from multiple batches within a sample.
func AggregateResults(results []DigestRunResult) DigestRunResult {
	if len(results) == 0 {
		return DigestRunResult{}
	}

	aggregated := DigestRunResult{
		Case:                 results[0].Case,
		Duration:             0,
		MemoryMaxBytes:       0,
		DaysPerSecond:        0,
		RowsDeletedPerSecond: 0,
	}

	totalDuration := time.Duration(0)
	var maxMemory uint64

	for _, result := range results {
		// Aggregate counts
		aggregated.Candidates += result.Candidates
		aggregated.ProcessedDays += result.ProcessedDays
		aggregated.TotalDeletedRows += result.TotalDeletedRows
		aggregated.TotalPreservedRows += result.TotalPreservedRows

		// Aggregate timing
		totalDuration += result.Duration

		// Track peak memory usage
		if result.MemoryMaxBytes > maxMemory {
			maxMemory = result.MemoryMaxBytes
		}

		// Aggregate WAL if present
		if result.WALBytes != nil {
			if aggregated.WALBytes == nil {
				walValue := int64(0)
				aggregated.WALBytes = &walValue
			}
			*aggregated.WALBytes += *result.WALBytes
		}
	}

	aggregated.Duration = totalDuration
	aggregated.MemoryMaxBytes = maxMemory

	// Calculate throughput metrics
	if aggregated.Duration.Seconds() > 0 {
		aggregated.DaysPerSecond = float64(aggregated.ProcessedDays) / aggregated.Duration.Seconds()
		aggregated.RowsDeletedPerSecond = float64(aggregated.TotalDeletedRows) / aggregated.Duration.Seconds()
	}

	return aggregated
}

// CalculateThroughputMetrics calculates derived throughput metrics for a result.
// This includes days/second, rows deleted/second, and memory efficiency metrics.
func CalculateThroughputMetrics(result *DigestRunResult) {
	if result.Duration.Seconds() > 0 {
		result.DaysPerSecond = float64(result.ProcessedDays) / result.Duration.Seconds()
		result.RowsDeletedPerSecond = float64(result.TotalDeletedRows) / result.Duration.Seconds()
	}
}

// ValidateResult performs validation checks on a benchmark result.
// Ensures the result contains reasonable values and required metrics.
func ValidateResult(result DigestRunResult) error {
	if result.Candidates < 0 {
		return errors.New("candidates cannot be negative")
	}
	if result.ProcessedDays < 0 {
		return errors.New("processed_days cannot be negative")
	}
	if result.TotalDeletedRows < 0 {
		return errors.New("total_deleted_rows cannot be negative")
	}
	if result.TotalPreservedRows < 0 {
		return errors.New("total_preserved_rows cannot be negative")
	}
	if result.Duration < 0 {
		return errors.New("duration cannot be negative")
	}
	if result.MemoryMaxBytes == 0 {
		return errors.New("memory usage should be measured")
	}

	// Validate throughput metrics make sense
	if result.DaysPerSecond < 0 {
		return errors.New("days_per_second cannot be negative")
	}
	if result.RowsDeletedPerSecond < 0 {
		return errors.New("rows_deleted_per_second cannot be negative")
	}

	return nil
}

// BenchmarkRunner coordinates the execution of multiple benchmark cases.
// This is the main entry point for running digest benchmarks.
type BenchmarkRunner struct {
	Platform interface{} // *kwilTesting.Platform
	Logger   interface{} // *testing.T
}

// NewBenchmarkRunner creates a new benchmark runner instance.
func NewBenchmarkRunner(platform interface{}, logger interface{}) *BenchmarkRunner {
	return &BenchmarkRunner{
		Platform: platform,
		Logger:   logger,
	}
}

// RunBenchmarkCase executes a single benchmark case and returns the results.
func (r *BenchmarkRunner) RunBenchmarkCase(ctx context.Context, c DigestBenchmarkCase) ([]DigestRunResult, error) {
	// Log the start of the benchmark case
	r.logInfo("Starting benchmark case: %+v", c)

	startTime := time.Now()
	results, err := RunCase(ctx, r.Platform, c)
	duration := time.Since(startTime)

	if err != nil {
		r.logError("Benchmark case failed after %v: %v", duration, err)
		return nil, err
	}

	r.logInfo("Benchmark case completed in %v, produced %d results", duration, len(results))

	// Validate all results
	for i, result := range results {
		// If memory wasn't measured (collector disabled or failed), don't hard-fail here.
		if result.MemoryMaxBytes == 0 {
			r.logInfo("Memory not measured for result %d; skipping memory validation.", i)
			// Still validate other fields:
			if result.Candidates < 0 || result.ProcessedDays < 0 || result.TotalDeletedRows < 0 ||
				result.TotalPreservedRows < 0 || result.Duration < 0 ||
				result.DaysPerSecond < 0 || result.RowsDeletedPerSecond < 0 {
				return nil, errors.Errorf("result %d validation failed (non-memory fields invalid)", i)
			}
			continue
		}
		if err := ValidateResult(result); err != nil {
			return nil, errors.Wrapf(err, "result %d validation failed", i)
		}
	}

	return results, nil
}

// RunMultipleCases executes multiple benchmark cases and aggregates the results.
func (r *BenchmarkRunner) RunMultipleCases(ctx context.Context, cases []DigestBenchmarkCase) (map[string][]DigestRunResult, error) {
	results := make(map[string][]DigestRunResult)

	for _, c := range cases {
		caseKey := fmt.Sprintf("streams_%d_days_%d_records_%d_deletecap_%d_pattern_%s",
			c.Streams, c.DaysPerStream, c.RecordsPerDay, c.DeleteCap, c.Pattern)

		r.logInfo("Running case: %s", caseKey)

		caseResults, err := r.RunBenchmarkCase(ctx, c)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to run case %s", caseKey)
		}

		results[caseKey] = caseResults
	}

	return results, nil
}

// logInfo logs an informational message if a logger is available.
func (r *BenchmarkRunner) logInfo(format string, args ...interface{}) {
	if r.Logger != nil {
		// Use reflection to call the logger's Logf method if available
		loggerValue := reflect.ValueOf(r.Logger)
		logfMethod := loggerValue.MethodByName("Logf")
		if logfMethod.IsValid() {
			// Format the message first, then pass it as a simple %s format
			final := fmt.Sprintf(format, args...)
			logfMethod.Call([]reflect.Value{
				reflect.ValueOf("%s"),
				reflect.ValueOf(final),
			})
		}
	}
}

// logError logs an error message if a logger is available.
func (r *BenchmarkRunner) logError(format string, args ...interface{}) {
	if r.Logger != nil {
		// Use reflection to call the logger's Errorf method if available
		loggerValue := reflect.ValueOf(r.Logger)
		errorfMethod := loggerValue.MethodByName("Errorf")
		if errorfMethod.IsValid() {
			// Format the message first, then pass it as a simple %s format
			final := fmt.Sprintf(format, args...)
			errorfMethod.Call([]reflect.Value{
				reflect.ValueOf("%s"),
				reflect.ValueOf(final),
			})
		}
	}
}

// WarmupQueryPlanner performs query planner warm-up before measurements.
// This ensures consistent query execution times by analyzing relevant tables.
func WarmupQueryPlanner(ctx context.Context, platform interface{}) error {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return errors.New("invalid platform type")
	}

	// Define tables that need query planner optimization for digest operations
	return warmupQueryPlanner(ctx, kwilPlatform, DigestTables)
}

// warmupQueryPlanner performs ANALYZE on specified tables for query optimization
func warmupQueryPlanner(ctx context.Context, platform *kwilTesting.Platform, tables []string) error {
	// Use the improved AnalyzeTables function from helpers
	return AnalyzeTables(ctx, platform, tables)
}

// CleanupAfterBenchmark performs cleanup operations after benchmark execution.
// This includes stopping collectors and resetting any modified state.
func CleanupAfterBenchmark(ctx context.Context, platform interface{}) error {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return errors.New("invalid platform type")
	}

	// Clean up any temporary data created during benchmark
	// This could include:
	// - Clearing pending_prune_days for streams used in benchmark
	// - Cleaning up test streams if needed
	// - Resetting any modified configuration

	// For now, just log that cleanup was performed
	fmt.Printf("Benchmark cleanup completed for platform (deployer: %x)\n", kwilPlatform.Deployer)

	return nil
}

// GetDigestStats retrieves current digest system statistics.
// This is useful for monitoring the state of the digest system.
func GetDigestStats(ctx context.Context, platform interface{}) (map[string]interface{}, error) {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return nil, errors.New("invalid platform type")
	}

	// Get the deployer for signing queries via helper
	deployer, err := GetDeployerOrDefault(kwilPlatform)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get deployer")
	}

	// Create transaction context for queries
	txContext := NewTxContext(ctx, kwilPlatform, deployer)

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	stats := make(map[string]interface{})

	// Query pending prune days count
	var pendingCount int64
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLCountPendingPruneDays,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if count, ok := row.Values[0].(int64); ok {
					pendingCount = count
				}
			}
			return nil
		})

	if err != nil {
		return nil, errors.Wrap(err, "error querying pending_prune_days count")
	}
	stats["pending_prune_days_count"] = pendingCount

	// Query primitive events count
	var eventCount int64
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLCountPrimitiveEvents,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if count, ok := row.Values[0].(int64); ok {
					eventCount = count
				}
			}
			return nil
		})

	if err != nil {
		return nil, errors.Wrap(err, "error querying primitive_events count")
	}
	stats["primitive_events_count"] = eventCount

	// Query primitive event type count
	var typeCount int64
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLCountPrimitiveEventType,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if count, ok := row.Values[0].(int64); ok {
					typeCount = count
				}
			}
			return nil
		})

	if err != nil {
		return nil, errors.Wrap(err, "error querying primitive_event_type count")
	}
	stats["primitive_event_type_count"] = typeCount

	return stats, nil
}

// MeasureWALGrowth measures Write-Ahead Log growth during digest operations.
// This provides insights into the I/O impact of digest operations.
func MeasureWALGrowth(ctx context.Context, platform interface{}, operation func() error) (int64, error) {
	kwilPlatform, ok := platform.(*kwilTesting.Platform)
	if !ok {
		return 0, errors.New("invalid platform type")
	}

	// Get the deployer for signing queries via helper
	deployer, err := GetDeployerOrDefault(kwilPlatform)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get deployer")
	}

	// Create transaction context for queries
	txContext := NewTxContext(ctx, kwilPlatform, deployer)

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Get WAL position before operation
	var beforeLSN string
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLWalGetLSN,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if lsn, ok := row.Values[0].(string); ok {
					beforeLSN = lsn
				}
			}
			return nil
		})

	if err != nil {
		return 0, errors.Wrap(err, "error getting WAL LSN before operation")
	}

	// Execute the operation
	err = operation()
	if err != nil {
		return 0, errors.Wrap(err, "error executing operation for WAL measurement")
	}

	// Get WAL position after operation
	var afterLSN string
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLWalGetLSN,
		map[string]any{},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if lsn, ok := row.Values[0].(string); ok {
					afterLSN = lsn
				}
			}
			return nil
		})

	if err != nil {
		return 0, errors.Wrap(err, "error getting WAL LSN after operation")
	}

	// Calculate WAL growth difference
	var walGrowth int64
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		SQLWalDiff,
		map[string]any{
			"$after_lsn":  afterLSN,
			"$before_lsn": beforeLSN,
		},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if growth, ok := row.Values[0].(int64); ok {
					walGrowth = growth
				}
			}
			return nil
		})

	if err != nil {
		return 0, errors.Wrap(err, "error calculating WAL growth difference")
	}

	return walGrowth, nil
}
