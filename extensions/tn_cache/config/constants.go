package config

// ExtensionName is the name used to identify this extension in configuration
const ExtensionName = "tn_cache"

// Validation constants
const (
	// EthereumAddressPrefix is the expected prefix for Ethereum addresses
	EthereumAddressPrefix = "0x"
	// EthereumAddressLength is the expected length of Ethereum addresses including prefix
	EthereumAddressLength = 42
	// StreamIDPrefix is the required prefix for stream identifiers
	StreamIDPrefix = "st"
	// WildcardStreamID represents a wildcard pattern for all streams
	WildcardStreamID = "*"
)

// Priority constants (currently unused - will be implemented with cache refresh logic)
// const (
//	// Priority-based conflict resolution will be added when cache refresh logic is implemented
// )

// Configuration field names
const (
	ConfigFieldEnabled             = "enabled"
	ConfigFieldResolutionSchedule  = "resolution_schedule"
	ConfigFieldStreamsInline       = "streams_inline"
	ConfigFieldStreamsCSVFile      = "streams_csv_file"
)

// Error messages
const (
	ErrMsgInvalidEthereumAddress = "invalid ethereum address format"
	ErrMsgInvalidStreamID        = "invalid stream ID format"
	ErrMsgInvalidCronSchedule    = "invalid cron schedule"
	ErrMsgEmptyRequired          = "required field cannot be empty"
)

// Default values
const (
	// DefaultResolutionSchedule is daily at midnight UTC (6 fields: second minute hour day month weekday)
	DefaultResolutionSchedule = "0 0 0 * * *"
)