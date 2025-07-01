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
)

// Error messages
const (
	ErrMsgInvalidEthereumAddress = "invalid ethereum address format"
	ErrMsgInvalidStreamID        = "invalid stream ID format"
	ErrMsgInvalidCronSchedule    = "invalid cron schedule"
	ErrMsgEmptyRequired          = "required field cannot be empty"
)