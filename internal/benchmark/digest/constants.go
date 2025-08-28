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
)

// Benchmark configuration constants
const (
	InsertBatchSize = 20000

	// DefaultDeleteCap is the default maximum number of rows that can be deleted in a single auto_digest call
	// This matches the default value in the auto_digest SQL function
	DefaultDeleteCap = 10000

	// ProductionRecordsPerDay is the realistic number of records per day in production
	ProductionRecordsPerDay = 24

	// DefaultSamples is the default number of samples per benchmark case
	DefaultSamples = 3
)

// Data generation patterns
const (
	// PatternRandom is the primary data generation pattern used in benchmarks
	PatternRandom = "random"
)

// Common SQL queries
const (
	SQLCountPendingPruneDays   = "SELECT COUNT(*) FROM pending_prune_days"
	SQLCountPrimitiveEvents    = "SELECT COUNT(*) FROM primitive_events"
	SQLCountPrimitiveEventType = "SELECT COUNT(*) FROM primitive_event_type"

	SQLWalGetLSN = "SELECT pg_current_wal_insert_lsn()::text"
	SQLWalDiff   = "SELECT pg_wal_lsn_diff($after_lsn, $before_lsn)"
)
