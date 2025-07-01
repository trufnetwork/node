package tn_cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
)

func TestParseConfigIntegration(t *testing.T) {
	tests := []struct {
		name        string
		extConfig   map[string]map[string]string
		expectError bool
		description string
	}{
		{
			name:        "no config - graceful handling",
			extConfig:   map[string]map[string]string{},
			expectError: false,
			description: "Missing configuration should return disabled state",
		},
		{
			name: "disabled extension",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "false",
				},
			},
			expectError: false,
			description: "Disabled extension should parse successfully",
		},
		{
			name: "valid configuration with wildcard",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[
						{
							"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
							"stream_id": "*",
							"cron_schedule": "0 * * * *"
						}
					]`,
				},
			},
			expectError: false,
			description: "Valid wildcard configuration should work",
		},
		{
			name: "valid configuration with specific stream",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[
						{
							"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
							"stream_id": "st123456789012345678901234567890",
							"cron_schedule": "0 0 * * *",
							"from": 1719849600
						}
					]`,
				},
			},
			expectError: false,
			description: "Valid specific stream configuration should work",
		},
		{
			name: "invalid cron schedule",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[
						{
							"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
							"stream_id": "st123456789012345678901234567890",
							"cron_schedule": "invalid cron"
						}
					]`,
				},
			},
			expectError: true,
			description: "Invalid cron schedule should fail",
		},
		{
			name: "invalid ethereum address",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[
						{
							"data_provider": "not_an_address",
							"stream_id": "st123456789012345678901234567890",
							"cron_schedule": "0 * * * *"
						}
					]`,
				},
			},
			expectError: true,
			description: "Invalid ethereum address should fail",
		},
		{
			name: "invalid json",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{invalid json}]`,
				},
			},
			expectError: true,
			description: "Invalid JSON should fail",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock service
			service := &common.Service{
				Logger:      log.DiscardLogger,
				LocalConfig: &config.Config{Extensions: tt.extConfig},
			}

			// Call the function
			result, err := ParseConfig(service)

			// Check error expectation
			if tt.expectError {
				require.Error(t, err, "Expected error for: %s", tt.description)
				return
			}

			require.NoError(t, err, "Unexpected error for: %s", tt.description)
			require.NotNil(t, result, "Result should not be nil")

			// Basic validation for successful cases
			assert.NotNil(t, result.Instructions, "Instructions should not be nil")
			assert.NotNil(t, result.Sources, "Sources should not be nil")

			// For enabled configurations, verify we have instructions
			if result.Enabled {
				assert.Greater(t, len(result.Instructions), 0, "Enabled config should have instructions")
			}
		})
	}
}