package config

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// Loader orchestrates the loading, validation, and transformation of cache configuration
type Loader struct {
	validator     *validation.RuleSet
	sourceFactory *sources.SourceFactory
}

// NewLoader creates a new configuration loader with default validation rules
func NewLoader() *Loader {
	return &Loader{
		validator:     validation.DefaultRules(),
		sourceFactory: sources.NewSourceFactory(),
	}
}

// NewLoaderWithValidation creates a new configuration loader with custom validation rules
func NewLoaderWithValidation(validator *validation.RuleSet) *Loader {
	return &Loader{
		validator:     validator,
		sourceFactory: sources.NewSourceFactory(),
	}
}

// LoadAndProcessFromMap loads configuration directly from a map (for extension configs)
func (l *Loader) LoadAndProcessFromMap(ctx context.Context, configMap map[string]string) (*ProcessedConfig, error) {
	return l.loadAndProcessInternal(ctx, configMap)
}

// LoadAndProcess loads configuration from all sources, validates it, and transforms it into executable instructions
func (l *Loader) LoadAndProcess(ctx context.Context, rawConfig RawConfig) (*ProcessedConfig, error) {
	// Convert to map format expected by sources
	configMap := map[string]string{
		"enabled": rawConfig.Enabled,
	}
	
	// Add streams_inline if provided
	if rawConfig.StreamsInline != "" {
		configMap["streams_inline"] = rawConfig.StreamsInline
	}
	
	// Add streams_csv_file if provided
	if rawConfig.StreamsCSVFile != "" {
		configMap["streams_csv_file"] = rawConfig.StreamsCSVFile
	}
	
	// Add resolution_schedule if provided
	if rawConfig.ResolutionSchedule != "" {
		configMap["resolution_schedule"] = rawConfig.ResolutionSchedule
	}
	
	// Add max_block_age if provided
	if rawConfig.MaxBlockAge != 0 {
		configMap["max_block_age"] = time.Duration(rawConfig.MaxBlockAge).String()
	}
	
	return l.loadAndProcessInternal(ctx, configMap)
}

// loadAndProcessInternal is the internal implementation used by both public methods
func (l *Loader) loadAndProcessInternal(ctx context.Context, configMap map[string]string) (*ProcessedConfig, error) {

	// Create configuration sources from raw config
	configSources, err := l.sourceFactory.CreateSources(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create configuration sources: %w", err)
	}

	// Validate sources before loading
	for _, source := range configSources {
		if err := source.Validate(configMap); err != nil {
			return nil, fmt.Errorf("source %s validation failed: %w", source.Name(), err)
		}
	}

	// Load all stream specifications from all sources
	var allSpecs []sources.StreamSpec
	var sourceNames []string

	for _, source := range configSources {
		specs, err := source.Load(ctx, configMap)
		if err != nil {
			return nil, fmt.Errorf("failed to load from source %s: %w", source.Name(), err)
		}
		allSpecs = append(allSpecs, specs...)
		sourceNames = append(sourceNames, source.Name())
	}

	// Validate all loaded specifications
	if err := l.validator.ValidateAll(allSpecs); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	// Transform to cache directives
	directives, err := l.transformToDirectives(allSpecs)
	if err != nil {
		return nil, fmt.Errorf("failed to transform configuration to directives: %w", err)
	}

	// Deduplicate and merge directives
	finalDirectives := l.deduplicateDirectives(directives)

	// Ensure sources is never nil
	if sourceNames == nil {
		sourceNames = []string{}
	}

	// Get resolution schedule or use default
	resolutionSchedule := configMap["resolution_schedule"]
	if resolutionSchedule == "" {
		resolutionSchedule = DefaultResolutionSchedule
	}
	
	// Validate resolution schedule if provided
	if err := validation.ValidateCronSchedule(resolutionSchedule); err != nil {
		return nil, fmt.Errorf("invalid resolution schedule: %w", err)
	}

	// Parse max_block_age (default to 1 hour)
	var maxBlockAge int64 = 3600
	if configMap["max_block_age"] != "" {
		// Parse duration string using Go's time.ParseDuration (same as types.Duration)
		dur, err := time.ParseDuration(configMap["max_block_age"])
		if err != nil {
			return nil, fmt.Errorf("invalid max_block_age: %w", err)
		}
		maxBlockAge = int64(dur.Seconds())
	}

	return &ProcessedConfig{
		Enabled:            configMap["enabled"] == "true",
		ResolutionSchedule: resolutionSchedule,
		Directives:         finalDirectives,
		Sources:            sourceNames,
		MaxBlockAge:        maxBlockAge,
	}, nil
}

// GetValidationRules returns the current validation rule set
func (l *Loader) GetValidationRules() *validation.RuleSet {
	return l.validator
}

// SetValidationRules updates the validation rule set
func (l *Loader) SetValidationRules(validator *validation.RuleSet) {
	l.validator = validator
}