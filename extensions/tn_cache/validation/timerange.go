package validation

import (
	"errors"
	"time"

	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
)

// TimeRangeRule validates time range specifications in stream configurations
type TimeRangeRule struct{}

// Name returns the name of this validation rule
func (r *TimeRangeRule) Name() string {
	return "time_range"
}

// Validate checks if the time range fields are valid
func (r *TimeRangeRule) Validate(spec sources.StreamSpec) error {
	return ValidateTimeRange(spec.From)
}

// ValidateTimeRange validates the 'from' timestamp value for cache configuration
// Only validates the 'from' field since 'to' has been removed from TimeRange
func ValidateTimeRange(from *int64) error {
	if from == nil {
		// No from timestamp specified, which is valid (cache from beginning)
		return nil
	}

	// For cache configuration, we're more lenient with timestamps
	// We allow historical data (negative timestamps) and very old data
	
	// Check if timestamp is not too far in the future (allow some buffer for clock skew)
	now := time.Now().Unix()
	maxFuture := now + (24 * 60 * 60) // 24 hours in the future
	if *from > maxFuture {
		return errors.New("from timestamp cannot be more than 24 hours in the future")
	}

	// For cache use cases, we allow:
	// - Zero timestamps (epoch start)
	// - Negative timestamps (historical/test data)
	// - Very old timestamps (no minimum age restriction)

	return nil
}

