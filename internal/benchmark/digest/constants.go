package digest

// Database and system constants
const (
	// DockerPostgresContainer is the name of the PostgreSQL container for memory monitoring
	DockerPostgresContainer = "kwil-testing-postgres"

	// DefaultSchema is the database schema used for fully qualifying tables
	DefaultSchema = "main"

	// DefaultDataProviderAddress is the fallback address used in tests and benchmarks
	DefaultDataProviderAddress = "0x1234567890123456789012345678901234567890"

	// DaySeconds is the number of seconds in a day
	DaySeconds = 86400

	// HourSeconds is the number of seconds in an hour
	HourSeconds = 3600

	// DefaultDigestConfig holds default configuration values for the digest system
	DefaultDigestConfig = `
	{
		"max_batch_size": 1000,
		"auto_digest_enabled": true,
		"prune_threshold_days": 30,
		"batch_timeout_seconds": 300
	}`
)

// Table names for digest operations
var (
	// DigestTables contains all table names relevant to digest operations
	DigestTables = []string{
		"primitive_events",
		"primitive_event_type",
		"pending_prune_days",
		"streams",
	}

	// CoreDigestTables contains the essential tables for digest functionality
	CoreDigestTables = []string{
		"primitive_events",
		"pending_prune_days",
	}
)

// Benchmark configuration constants
const (
	InsertBatchSize = 20000

	// DefaultDeleteCap is the default maximum number of rows that can be deleted in a single auto_digest call
	// This matches the default value in the auto_digest SQL function
	DefaultDeleteCap = 10000

	// DeleteCapSmall is 1K - for testing small batch performance
	DeleteCapSmall = 1000

	// DeleteCapMedium is 10K - for testing medium batch performance
	DeleteCapMedium = 10000

	// DeleteCapLarge is 100K - for testing large batch performance
	DeleteCapLarge = 100000

	// DeleteCapXL is 1M - for testing extra large batch performance
	DeleteCapXL = 1000000

	// ProductionRecordsPerDay is the realistic number of records per day in production
	ProductionRecordsPerDay = 24

	// DefaultSamples is the default number of samples per benchmark case
	DefaultSamples = 3

	// DefaultTimeoutSeconds is the default timeout for benchmark operations
	DefaultTimeoutSeconds = 300

	// MaxWorkers is the maximum number of concurrent workers for data insertion
	MaxWorkers = 50

	// MaxConcurrency is the maximum concurrency level for sized wait group
	MaxConcurrency = 10

	// MemoryCheckInterval is the interval for memory usage checks
	MemoryCheckInterval = 100 * 1024 * 1024 // 100MB
)

// Data generation patterns
const (
	// Pattern names for different data generation strategies
	PatternRandom    = "random"
	PatternDups50    = "dups50"
	PatternMonotonic = "monotonic"
	PatternEqual     = "equal"
	PatternTimeDup   = "time_dup"
)

// File paths and extensions
const (
	// DefaultResultsPath is the default path for benchmark results
	DefaultResultsPath = "benchmark_results.csv"

	// DefaultSummaryPath is the default path for summary reports
	DefaultSummaryPath = "benchmark_summary.md"

	// DefaultStatsPath is the default path for statistical analysis reports
	DefaultStatsPath = "statistical_analysis.md"

	// CSVExtension is the file extension for CSV files
	CSVExtension = ".csv"

	// MarkdownExtension is the file extension for Markdown files
	MarkdownExtension = ".md"

	// ResultsFileSmoke is the default output file for smoke suite
	ResultsFileSmoke = "./digest_smoke_results.csv"
	// ResultsFileMedium is the default output file for medium suite
	ResultsFileMedium = "./digest_medium_results.csv"
	// ResultsFileExtreme is the default output file for extreme suite
	ResultsFileExtreme = "./digest_extreme_results.csv"
	// ResultsFileCustom is the default output file for custom suite
	ResultsFileCustom = "./digest_custom_results.csv"
)

// Memory and resource limits
const (
	// MaxMemoryThreshold is the maximum memory usage threshold before warning
	MaxMemoryThreshold = 1024 * 1024 * 1024 // 1GB

	// WALGrowthThreshold is the maximum WAL growth threshold before warning
	WALGrowthThreshold = 100 * 1024 * 1024 // 100MB
)

// Test configuration
const (
	// SmokeTestStreams is the number of streams for smoke tests
	SmokeTestStreams = 2

	// SmokeTestDays is the number of days per stream for smoke tests
	SmokeTestDays = 1

	// SmokeTestRecords is the number of records per day for smoke tests
	SmokeTestRecords = 100

	// MediumTestStreams is the number of streams for medium tests
	MediumTestStreams = 5

	// MediumTestDays is the number of days per stream for medium tests
	MediumTestDays = 10

	// MediumTestRecords is the number of records per day for medium tests
	MediumTestRecords = 1000

	// ExtremeTestStreams is the number of streams for extreme tests
	ExtremeTestStreams = 20

	// ExtremeTestDays is the number of days per stream for extreme tests
	ExtremeTestDays = 50

	// ExtremeTestRecords is the number of records per day for extreme tests
	ExtremeTestRecords = 10000
)

// Pre-defined benchmark cases for different test suites
var (
	// SmokeTestCases defines the benchmark cases for smoke testing
	// Quick tests with small data to verify basic functionality
	SmokeTestCases = []DigestBenchmarkCase{
		{Streams: 10, DaysPerStream: 1, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapSmall, Pattern: PatternRandom, Samples: 1}, // 1K cap - fast smoke test
	}

	// MediumTestCases defines test cases for medium-scale testing
	// Tests delete cap variations (1K, 10K, 100K, 1M) with realistic production data
	//
	// IMPORTANT: The 25k streams × 12 days are NOT intended to test deleting all that data.
	// They create a realistic environment with substantial data volume (~7.2M total records)
	// to expose indexing and performance issues. The delete cap controls actual processing,
	// giving us ~600k records/day for realistic medium-scale processing scenarios.
	MediumTestCases = []DigestBenchmarkCase{
		{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapSmall, Pattern: PatternRandom, Samples: DefaultSamples},  // 1K cap - tests small batch performance (~600k records/day)
		{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapMedium, Pattern: PatternRandom, Samples: DefaultSamples}, // 10K cap - tests medium batch performance (~600k records/day)
		{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapLarge, Pattern: PatternRandom, Samples: DefaultSamples},  // 100K cap - tests large batch performance (~600k records/day)
		{Streams: 25000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapXL, Pattern: PatternRandom, Samples: DefaultSamples},     // 1M cap - tests extra large batch performance (~600k records/day)
	}

	// BigTestCases defines test cases for big-scale testing
	// Tests delete cap behavior at big scales with large datasets
	//
	// IMPORTANT: The 50k streams × 12 days are NOT intended to test deleting all that data.
	// They create a big environment to stress-test the system and expose performance
	// issues that only appear at scale. The delete cap controls actual processing,
	// giving us ~1.2M records/day for realistic large-scale processing scenarios.
	BigTestCases = []DigestBenchmarkCase{
		{Streams: 50000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapSmall, Pattern: PatternRandom, Samples: 2},  // 1K cap - tests small batch with big data (~1.2M records/day)
		{Streams: 50000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapMedium, Pattern: PatternRandom, Samples: 2}, // 10K cap - tests medium batch with big data (~1.2M records/day)
		{Streams: 50000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapLarge, Pattern: PatternRandom, Samples: 2},  // 100K cap - tests large batch with big data (~1.2M records/day)
		{Streams: 50000, DaysPerStream: 12, RecordsPerDay: ProductionRecordsPerDay, DeleteCap: DeleteCapXL, Pattern: PatternRandom, Samples: 2},     // 1M cap - tests extra large batch with big data (~1.2M records/day)
	}
)

// Env & configuration keys
const (
	// EnvPrefixDigest is the environment variables prefix for custom config
	EnvPrefixDigest = "DIGEST_"

	EnvKeyStreams = "streams"
	EnvKeyDays    = "days"
	EnvKeyRecords = "records"

	EnvKeyPatterns = "patterns"
	EnvKeySamples  = "samples"
	EnvKeyResults  = "results_path"
)

// Common SQL queries
const (
	SQLCountPendingPruneDays   = "SELECT COUNT(*) FROM pending_prune_days"
	SQLCountPrimitiveEvents    = "SELECT COUNT(*) FROM primitive_events"
	SQLCountPrimitiveEventType = "SELECT COUNT(*) FROM primitive_event_type"

	SQLWalGetLSN = "SELECT pg_current_wal_insert_lsn()::text"
	SQLWalDiff   = "SELECT pg_wal_lsn_diff($after_lsn, $before_lsn)"
)
