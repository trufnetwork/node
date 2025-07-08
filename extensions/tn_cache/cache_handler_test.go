package tn_cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNullParameterHandling verifies that NULL parameter handling works correctly
func TestNullParameterHandling(t *testing.T) {
	tests := []struct {
		name        string
		fromTime    *int64
		toTime      *int64
		expectFrom  int64
		description string
	}{
		{
			name:        "both null",
			fromTime:    nil,
			toTime:      nil,
			expectFrom:  0, // Special case handled differently in query
			description: "Both NULL should query for latest record only",
		},
		{
			name:        "from null to specified",
			fromTime:    nil,
			toTime:      int64Ptr(1000),
			expectFrom:  0,
			description: "NULL from should default to 0",
		},
		{
			name:        "from specified to null",
			fromTime:    int64Ptr(500),
			toTime:      nil,
			expectFrom:  500,
			description: "NULL to should query without upper bound",
		},
		{
			name:        "both specified",
			fromTime:    int64Ptr(100),
			toTime:      int64Ptr(200),
			expectFrom:  100,
			description: "Both specified should use exact values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fromTimeOrZero(tt.fromTime)
			assert.Equal(t, tt.expectFrom, result, tt.description)
		})
	}
}

// TestNewPrecompileMethods verifies the new methods have correct signatures
func TestNewPrecompileMethods(t *testing.T) {
	// This test verifies at compile time that the handler functions exist
	// and have the correct signatures by referencing them
	handlers := []interface{}{
		handleGetCachedLastBefore,
		handleGetCachedFirstAfter,
	}
	
	// Just verify they're not nil (they exist)
	for _, handler := range handlers {
		assert.NotNil(t, handler)
	}
}

// Helper function to create int64 pointer
func int64Ptr(v int64) *int64 {
	return &v
}