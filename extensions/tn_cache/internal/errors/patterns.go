package errors

// Error pattern constants for maintainability
const (
	// Non-retryable error patterns - these indicate permanent failures
	ErrPatternSchemaNotExist      = "schema does not exist"
	ErrPatternPermissionDenied    = "permission denied"
	ErrPatternInvalidConfig       = "invalid configuration"
	ErrPatternContextCanceled     = "context canceled"
	ErrPatternContextDeadline     = "context deadline exceeded"
	ErrPatternUnauthorized        = "unauthorized"
	ErrPatternForbidden           = "forbidden"
	ErrPatternActionNotExist      = "action does not exist"
	ErrPatternInvalidStreamType   = "invalid stream type"
	ErrPatternMalformed           = "malformed"
	ErrPatternSyntaxError         = "syntax error"
)

// Not found error patterns - these indicate missing resources
const (
	ErrPatternNotFound   = "not found"
	ErrPatternDoesNotExist = "does not exist"
	ErrPatternNoRows     = "no rows"
)

// NonRetryableErrorPatterns contains all error patterns that should not be retried
var NonRetryableErrorPatterns = []string{
	ErrPatternSchemaNotExist,
	ErrPatternPermissionDenied,
	ErrPatternInvalidConfig,
	ErrPatternContextCanceled,
	ErrPatternContextDeadline,
	ErrPatternUnauthorized,
	ErrPatternForbidden,
	ErrPatternActionNotExist,
	ErrPatternInvalidStreamType,
	ErrPatternMalformed,
	ErrPatternSyntaxError,
}

// NotFoundErrorPatterns contains all error patterns that indicate a resource was not found
var NotFoundErrorPatterns = []string{
	ErrPatternNotFound,
	ErrPatternDoesNotExist,
	ErrPatternNoRows,
}