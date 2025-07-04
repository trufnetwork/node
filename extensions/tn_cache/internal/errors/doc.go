// Package errors provides error handling utilities for the TN Cache extension.
//
// Error Handling Strategy:
//
// The TN Cache extension follows a deterministic error handling approach that
// distinguishes between different error categories to ensure system reliability
// while gracefully handling missing or deleted resources.
//
// Error Categories:
//
// 1. Non-Retryable Errors (Fatal):
//   - Permission/authentication issues: Won't be resolved by retrying
//   - Schema/structure problems: Require manual intervention
//   - Invalid configurations: Need configuration changes
//   - Syntax errors: Code/query issues that won't fix themselves
//     These errors stop execution immediately and are logged as errors.
//
// 2. Retryable Errors (Transient):
//   - Network timeouts
//   - Temporary connection issues
//   - Resource temporarily unavailable
//     These errors are retried with exponential backoff and circuit breaker protection.
//
// 3. Not-Found Errors (Graceful Degradation):
//   - Stream not found
//   - Provider not found
//   - No data available
//     These are logged as warnings and return empty results to allow the system
//     to continue operating with available resources.
//
// Key Principles:
//
// - All database operations are atomic to prevent partial states
// - Missing resources don't fail the entire operation
// - Clear distinction between configuration errors (fail fast) and runtime errors
// - Comprehensive logging at appropriate levels for debugging
//
// Usage:
//
//	if err != nil {
//	    if errors.IsNotFoundError(err) {
//	        logger.Warn("resource not found", "error", err)
//	        return emptyResult, nil
//	    }
//	    if errors.IsNonRetryableError(err) {
//	        logger.Error("non-retryable error", "error", err)
//	        return nil, err
//	    }
//	    // Retryable error - will be handled by retry logic
//	    return nil, err
//	}
package errors
