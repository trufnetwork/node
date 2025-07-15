package validation

import "regexp"

// Compiled regex patterns for validation - compiled once for performance
var (
	// hexRegex matches hexadecimal strings
	hexRegex = regexp.MustCompile(`^[0-9a-fA-F]+$`)

	// alphanumericRegex matches alphanumeric strings (letters and numbers only)
	alphanumericRegex = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

	// ethereumAddressRegex matches Ethereum addresses with 0x prefix
	ethereumAddressRegex = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

	// streamIDRegex matches valid stream IDs (st prefix + alphanumeric)
	streamIDRegex = regexp.MustCompile(`^st[a-z0-9]+$`)
)

// IsValidHex checks if a string contains only hexadecimal characters
func IsValidHex(s string) bool {
	return hexRegex.MatchString(s)
}

// IsValidAlphanumeric checks if a string contains only alphanumeric characters
func IsValidAlphanumeric(s string) bool {
	return alphanumericRegex.MatchString(s)
}

// IsValidEthereumAddressFormat checks if a string matches Ethereum address format
func IsValidEthereumAddressFormat(addr string) bool {
	return ethereumAddressRegex.MatchString(addr)
}

// IsValidStreamIDFormat checks if a string matches stream ID format
func IsValidStreamIDFormat(streamID string) bool {
	return streamIDRegex.MatchString(streamID)
}
