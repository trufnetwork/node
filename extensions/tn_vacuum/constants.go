package tn_vacuum

const (
	// ExtensionName is used for hook registration and config namespace.
	ExtensionName = "tn_vacuum"
)

const (
	defaultBlockInterval = int64(50000)
	minBlockInterval     = int64(1)
)

// Configuration keys
const (
	ConfigKeyEnabled          = "enabled"
	ConfigKeyBlockInterval    = "block_interval"
	ConfigKeyPgRepackJobs     = "pg_repack_jobs"
	ConfigKeyPgRepackUser     = "pg_repack_user"
	ConfigKeyPgRepackPassword = "pg_repack_password"
)

// DefaultPgRepackUser is the role pg_repack runs as when kwild's own database
// user is not a superuser and pg_repack_user is not set. The node's
// deployments/postgres-roles.sql creates it.
const DefaultPgRepackUser = "tn_repack"

// Database connection defaults
const (
	DefaultPostgresHost = "127.0.0.1"
	DefaultPostgresPort = "5432"
	DefaultSSLMode      = "sslmode=disable"
)

// Report status values
const (
	StatusOK     = "ok"
	StatusFailed = "failed"
)
