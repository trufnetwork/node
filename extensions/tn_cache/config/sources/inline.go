package sources

import (
	"context"
	"encoding/json"
	"fmt"
)

// InlineSource handles stream configurations specified as JSON within the TOML file
type InlineSource struct {
	streamsJSON string
}

// NewInlineSource creates a new inline configuration source
func NewInlineSource(streamsJSON string) *InlineSource {
	return &InlineSource{
		streamsJSON: streamsJSON,
	}
}

// Name returns the name of this configuration source
func (s *InlineSource) Name() string {
	return "inline"
}

// Load parses the JSON stream configuration and returns StreamSpec objects
func (s *InlineSource) Load(ctx context.Context, rawConfig map[string]string) ([]StreamSpec, error) {
	if s.streamsJSON == "" {
		return []StreamSpec{}, nil
	}

	var specs []StreamSpec
	if err := json.Unmarshal([]byte(s.streamsJSON), &specs); err != nil {
		return nil, fmt.Errorf("failed to parse streams JSON: %w", err)
	}

	// Mark the source for each spec for debugging and conflict resolution
	for i := range specs {
		specs[i].Source = s.Name()
	}

	return specs, nil
}

// Validate checks if the JSON is well-formed without fully parsing it
func (s *InlineSource) Validate(rawConfig map[string]string) error {
	if s.streamsJSON == "" {
		// Empty configuration is valid
		return nil
	}

	// Try to parse as JSON to ensure it's well-formed
	var temp interface{}
	if err := json.Unmarshal([]byte(s.streamsJSON), &temp); err != nil {
		return fmt.Errorf("streams configuration is not valid JSON: %w", err)
	}

	// Additional validation: ensure it's an array
	if _, ok := temp.([]interface{}); !ok {
		return fmt.Errorf("streams configuration must be a JSON array")
	}

	return nil
}