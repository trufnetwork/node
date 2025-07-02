package validation

import (
	"errors"
	"strings"

	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
)

// StreamIDRule validates stream IDs in the stream_id field
type StreamIDRule struct{}

// Name returns the name of this validation rule
func (r *StreamIDRule) Name() string {
	return "stream_id"
}

// Validate checks if the stream_id field contains a valid stream ID or wildcard
func (r *StreamIDRule) Validate(spec sources.StreamSpec) error {
	return ValidateStreamID(spec.StreamID)
}

// ValidateStreamID validates a stream ID format, allowing "*" as a wildcard
func ValidateStreamID(streamID string) error {
	if streamID == "" {
		return errors.New(constants.ErrMsgEmptyRequired)
	}

	// Allow "*" as a wildcard for provider-wide caching
	if streamID == constants.WildcardStreamID {
		return nil
	}

	// Check if it starts with "st"
	if !strings.HasPrefix(streamID, constants.StreamIDPrefix) {
		return errors.New("stream ID must start with '" + constants.StreamIDPrefix + "' or be '" + constants.WildcardStreamID + "' for wildcard")
	}

	// Use pre-compiled regex for validation
	if !IsValidStreamIDFormat(streamID) {
		return errors.New(constants.ErrMsgInvalidStreamID)
	}

	return nil
}

// IsWildcardStreamID returns true if the stream ID is a wildcard
func IsWildcardStreamID(streamID string) bool {
	return streamID == constants.WildcardStreamID
}