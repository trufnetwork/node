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
	// BaseTimeNoneSentinel represents "no base_time" when persisted
	BaseTimeNoneSentinel = int64(-1)
)

// Error messages
const (
	ErrMsgInvalidEthereumAddress = "invalid ethereum address format"
	ErrMsgInvalidStreamID        = "invalid stream ID format"
	ErrMsgInvalidCronSchedule    = "invalid cron schedule"
	ErrMsgEmptyRequired          = "required field cannot be empty"
	ErrMsgExtensionNotEnabled    = "tn_cache extension is not enabled"
	ErrMsgCacheDBNotInitialized  = "cache database not initialized"
	ErrMsgValueNotDecimal        = "value is not a decimal. received %T"
)

// SQL constants
const (
	// MaxInt8 is used as default for NULL upper bounds in queries
	MaxInt8 = int64(9223372036854775000)
	// DefaultPrecision for NUMERIC type
	NumericPrecision = 36
	// DefaultScale for NUMERIC type
	NumericScale = 18
)
