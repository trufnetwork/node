package digest

import (
	"time"
)

// DigestBenchmarkCase defines the configuration for a single digest benchmark test case.
// It encapsulates all parameters needed to generate test data and execute the benchmark.
type DigestBenchmarkCase struct {
	// Streams specifies the number of primitive streams to create for the benchmark
	Streams int

	// DaysPerStream specifies the number of days of data to generate per stream
	DaysPerStream int

	// RecordsPerDay specifies the number of records to generate per stream per day
	RecordsPerDay int

	// DeleteCap specifies the maximum number of rows that can be deleted in a single auto_digest call
	DeleteCap int

	// Pattern specifies the data pattern to generate ("random", "dups50", "monotonic", "equal", "time_dup")
	Pattern string

	// Samples specifies the number of times to repeat the measurement for statistical accuracy
	Samples int

	// IdempotencyCheck specifies whether to verify that re-running digest returns 0 changes
	IdempotencyCheck bool
}

// DigestRunResult contains the results and metrics from a single benchmark execution.
// It captures both input parameters and measured performance characteristics.
type DigestRunResult struct {
	// Case contains the original benchmark case configuration
	Case DigestBenchmarkCase

	// Candidates contains the total number of candidates submitted for processing
	Candidates int

	// ProcessedDays contains the actual number of days that were processed
	ProcessedDays int

	// TotalDeletedRows contains the total number of rows deleted during digest processing
	TotalDeletedRows int

	// Duration contains the total execution time for the auto_digest operation
	Duration time.Duration

	// MemoryMaxBytes contains the peak memory usage in bytes during execution
	MemoryMaxBytes uint64

	// StreamDaysPerSecond contains the throughput metric: processed stream-days per second
	StreamDaysPerSecond float64

	// RowsDeletedPerSecond contains the throughput metric: deleted_rows per second
	RowsDeletedPerSecond float64

	// WALBytes contains the WAL (Write-Ahead Log) growth in bytes (optional metric)
	WALBytes *int64
}

// DigestBenchmarkPattern defines the interface for different data generation patterns.
// This follows the Strategy pattern to allow for different data generation strategies.
type DigestBenchmarkPattern interface {
	// GenerateRecords generates a slice of records based on the pattern's logic
	GenerateRecords(dayStart int64, records int) []InsertRecordInput

	// GetPatternName returns the string identifier for this pattern
	GetPatternName() string
}

// InsertRecordInput represents a single primitive event record to be inserted.
// This structure matches the expected input format for primitive data insertion.
type InsertRecordInput struct {
	// EventTime specifies the timestamp for this record
	EventTime int64

	// Value specifies the numeric value for this record
	Value float64

	// CreatedAt specifies the creation timestamp (for tie-breaking in OHLC calculations)
	CreatedAt int64
}

// DigestSetupInput encapsulates all parameters needed for setting up benchmark data.
// This structure centralizes setup configuration to avoid parameter sprawl.
type DigestSetupInput struct {
	// Platform provides access to the testing platform and database
	Platform interface{} // *kwilTesting.Platform

	// Case contains the benchmark case configuration
	Case DigestBenchmarkCase

	// DataProvider contains the Ethereum address of the data provider
	DataProvider interface{} // util.EthereumAddress
}

// DigestVerificationInput contains parameters for verifying digest correctness.
// This structure groups verification parameters for consistency checking.
type DigestVerificationInput struct {
	// Platform provides access to the testing platform and database
	Platform interface{} // *kwilTesting.Platform

	// StreamRef specifies which stream to verify
	StreamRef int

	// DayIndex specifies which day to verify
	DayIndex int64

	// ExpectedRecords contains the expected record count before digest
	ExpectedRecords int

	// SampleSize specifies how many records to sample for correctness verification
	SampleSize int
}

// DigestMeasurementInput contains parameters for measuring digest performance.
// This structure encapsulates timing and resource measurement configuration.
type DigestMeasurementInput struct {
	// Platform provides access to the testing platform and database
	Platform interface{} // *kwilTesting.Platform

	// StreamRefs contains the stream references to process
	StreamRefs []int

	// DayIdxs contains the day indices to process
	DayIdxs []int

	// Collector provides memory monitoring functionality
	Collector interface{} // *benchutil.DockerMemoryCollector
}
