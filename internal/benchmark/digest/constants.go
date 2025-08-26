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
	// DefaultBatchSize is the default batch size for digest operations
	// Increased from 1000 to 5000 to reduce GC pressure from array copies
	DefaultBatchSize = 5000

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
	SmokeTestCases = []DigestBenchmarkCase{
		{Streams: 100, DaysPerStream: 1, RecordsPerDay: 50, BatchSize: 50, Pattern: PatternRandom, Samples: 1},
		{Streams: 1000, DaysPerStream: 1, RecordsPerDay: 50, BatchSize: 50, Pattern: PatternRandom, Samples: 1},
		{Streams: 10000, DaysPerStream: 1, RecordsPerDay: 50, BatchSize: 50, Pattern: PatternRandom, Samples: 1},
	}

	// MediumTestCases defines the benchmark cases for medium-scale testing
	MediumTestCases = []DigestBenchmarkCase{
		{Streams: 10000, DaysPerStream: 1, RecordsPerDay: 2, BatchSize: 100, Pattern: PatternRandom, Samples: DefaultSamples},
		{Streams: 10000, DaysPerStream: 1, RecordsPerDay: 2, BatchSize: 500, Pattern: PatternRandom, Samples: DefaultSamples},
		{Streams: 10000, DaysPerStream: 1, RecordsPerDay: 2, BatchSize: 1000, Pattern: PatternRandom, Samples: DefaultSamples},
		{Streams: 10000, DaysPerStream: 30, RecordsPerDay: 2, BatchSize: 500, Pattern: PatternRandom, Samples: DefaultSamples},
		{Streams: 10000, DaysPerStream: 1, RecordsPerDay: 50, BatchSize: 1000, Pattern: PatternRandom, Samples: DefaultSamples},
	}

	// ExtremeTestCases defines the benchmark cases for extreme-scale testing
	ExtremeTestCases = []DigestBenchmarkCase{
		{Streams: 100000, DaysPerStream: 1, RecordsPerDay: 4, BatchSize: 200, Pattern: PatternRandom, Samples: 2},
		{Streams: 100000, DaysPerStream: 30, RecordsPerDay: 4, BatchSize: 200, Pattern: PatternRandom, Samples: 2},
		{Streams: 100000, DaysPerStream: 90, RecordsPerDay: 4, BatchSize: 200, Pattern: PatternRandom, Samples: 2},
	}
)

// Env & configuration keys
const (
	// EnvPrefixDigest is the environment variables prefix for custom config
	EnvPrefixDigest = "DIGEST_"

	EnvKeyStreams    = "streams"
	EnvKeyDays       = "days"
	EnvKeyRecords    = "records"
	EnvKeyBatchSizes = "batch_sizes"
	EnvKeyPatterns   = "patterns"
	EnvKeySamples    = "samples"
	EnvKeyResults    = "results_path"
)

// Common SQL queries
const (
	SQLCountPendingPruneDays   = "SELECT COUNT(*) FROM pending_prune_days"
	SQLCountPrimitiveEvents    = "SELECT COUNT(*) FROM primitive_events"
	SQLCountPrimitiveEventType = "SELECT COUNT(*) FROM primitive_event_type"

	SQLWalGetLSN = "SELECT pg_current_wal_insert_lsn()::text"
	SQLWalDiff   = "SELECT pg_wal_lsn_diff($after_lsn, $before_lsn)"
)
