package config

import (
	"fmt"
	"strings"

	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// transformToInstructions converts validated StreamSpecs into executable instructions
func (l *Loader) transformToInstructions(specs []sources.StreamSpec) ([]InstructionDirective, error) {
	var directives []InstructionDirective

	for i, spec := range specs {
		directive, err := l.specToDirective(spec, i)
		if err != nil {
			return nil, fmt.Errorf("failed to transform spec %d from source %s: %w", i, spec.Source, err)
		}
		directives = append(directives, directive)
	}

	return directives, nil
}

// specToDirective converts a single StreamSpec to an InstructionDirective
func (l *Loader) specToDirective(spec sources.StreamSpec, index int) (InstructionDirective, error) {
	// Validate the cron schedule (parsing will be done later by executor)
	if err := validation.ValidateCronSchedule(spec.CronSchedule); err != nil {
		return InstructionDirective{}, fmt.Errorf("invalid cron schedule: %w", err)
	}

	// Determine directive type based on stream ID
	directiveType := DirectiveSpecific
	if validation.IsWildcardStreamID(spec.StreamID) {
		directiveType = DirectiveProviderWildcard
	}

	// Normalize the data provider address (lowercase)
	normalizedProvider := validation.NormalizeEthereumAddress(spec.DataProvider)

	// Generate a unique ID for this directive
	id := generateDirectiveID(normalizedProvider, spec.StreamID, spec.Source, index)

	return InstructionDirective{
		ID:           id,
		Type:         directiveType,
		DataProvider: normalizedProvider,
		StreamID:     spec.StreamID,
		Schedule: Schedule{
			CronExpr: spec.CronSchedule,
			// Note: We're not storing the parsed schedule in the config types
			// to avoid import cycles. It will be parsed when needed by the executor.
		},
		TimeRange: TimeRange{
			From: spec.From,
		},
		IncludeChildren: spec.IncludeChildren,
		Metadata: Metadata{
			Source: spec.Source,
			// Priority calculation removed - will be implemented when cache refresh logic is built
			Priority: 0,
		},
	}, nil
}

// generateDirectiveID creates a unique identifier for a cache directive
func generateDirectiveID(dataProvider, streamID, source string, index int) string {
	// Create a deterministic ID based on the directive content
	parts := []string{
		strings.ToLower(dataProvider),
		streamID,
		source,
		fmt.Sprintf("%d", index),
	}
	return strings.Join(parts, "_")
}

// Note: Complex priority calculation removed - will be implemented when cache refresh logic is built
// For now, we only need basic deduplication to prevent exact duplicates (same data_provider + stream_id)

// deduplicateInstructions removes exact duplicate instructions (same data_provider + stream_id)
func (l *Loader) deduplicateInstructions(instructions []InstructionDirective) []InstructionDirective {
	// Use a map to track unique cache keys and remove exact duplicates
	instructionMap := make(map[string]InstructionDirective)

	for _, instruction := range instructions {
		cacheKey := instruction.GetCacheKey()
		
		// Check if we already have an instruction for this cache key
		if _, exists := instructionMap[cacheKey]; exists {
			// Keep the first one (deterministic behavior) - ignore duplicates
			// TODO: Implement proper conflict resolution when cache refresh logic is built
			continue
		}
		instructionMap[cacheKey] = instruction
	}

	// Convert map back to slice
	var result []InstructionDirective
	for _, instruction := range instructionMap {
		result = append(result, instruction)
	}

	// Ensure we always return a non-nil slice
	if result == nil {
		result = []InstructionDirective{}
	}

	return result
}

// GetCacheKey returns a unique key for a cache directive (for deduplication)
func (directive *InstructionDirective) GetCacheKey() string {
	return directive.DataProvider + ":" + directive.StreamID
}