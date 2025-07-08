package constants

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
	// CacheSchemaName is the PostgreSQL schema name for cache tables. Can't be the same as the extension name, because kwil creates a namespace of that name.
	CacheSchemaName = "ext_tn_cache"
	// PrecompileName is the name of the precompile function
	PrecompileName = "tn_cache"
)

// Error messages
const (
	ErrMsgInvalidEthereumAddress = "invalid ethereum address format"
	ErrMsgInvalidStreamID        = "invalid stream ID format"
	ErrMsgInvalidCronSchedule    = "invalid cron schedule"
	ErrMsgEmptyRequired          = "required field cannot be empty"
)
