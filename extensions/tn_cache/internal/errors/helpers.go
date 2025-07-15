package errors

import (
	"strings"
)

// IsNotFoundError checks if an error indicates a resource was not found.
// This is used to determine if we should return empty results instead of failing.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := strings.ToLower(err.Error())
	for _, pattern := range NotFoundErrorPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}

// IsNonRetryableError determines if an error should not be retried.
// Non-retryable errors include:
// - Permission/auth issues: Won't be resolved by retrying
// - Schema/structure issues: Require manual intervention  
// - Invalid configurations: Need config changes
// - Syntax errors: Code/query issues that won't fix themselves
func IsNonRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := strings.ToLower(err.Error())
	for _, pattern := range NonRetryableErrorPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	return false
}