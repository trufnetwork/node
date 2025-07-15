package validation

import (
	"errors"
	"strings"

	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
)

// EthereumAddressRule validates ethereum addresses in the data_provider field
type EthereumAddressRule struct{}

// Name returns the name of this validation rule
func (r *EthereumAddressRule) Name() string {
	return "ethereum_address"
}

// Validate checks if the data_provider field contains a valid ethereum address
func (r *EthereumAddressRule) Validate(spec sources.StreamSpec) error {
	return ValidateEthereumAddress(spec.DataProvider)
}

// ValidateEthereumAddress validates an ethereum address format
func ValidateEthereumAddress(addr string) error {
	if addr == "" {
		return errors.New(constants.ErrMsgEmptyRequired)
	}

	// Check if it starts with 0x
	if !strings.HasPrefix(addr, constants.EthereumAddressPrefix) {
		return errors.New("ethereum address must start with '" + constants.EthereumAddressPrefix + "'")
	}

	// Check length (0x + 40 hex characters = 42 total)
	if len(addr) != constants.EthereumAddressLength {
		return errors.New("ethereum address must be exactly 42 characters (0x + 40 hex)")
	}

	// Check if it's lowercase (normalized form)
	if strings.ToLower(addr) != addr {
		return errors.New("ethereum address must be lowercase")
	}

	// Use pre-compiled regex for validation
	if !IsValidEthereumAddressFormat(addr) {
		return errors.New(constants.ErrMsgInvalidEthereumAddress)
	}

	return nil
}

// NormalizeEthereumAddress converts an ethereum address to its canonical form (lowercase)
func NormalizeEthereumAddress(addr string) string {
	return strings.ToLower(addr)
}

