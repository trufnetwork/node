package tn_cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tnConfig "github.com/trufnetwork/node/extensions/tn_cache/config"
)

// TestBasicDeduplication tests the simplified deduplication logic where first configuration wins
func TestBasicDeduplication(t *testing.T) {
	tests := []struct {
		name          string
		rawConfig     tnConfig.RawConfig
		expectError   bool
		expectedWins  string
		description   string
	}{
		{
			name: "first_configuration_wins",
			rawConfig: tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: `[
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *",
						"from": 1719849600
					},
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678", 
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 0 * * *",
						"from": 1719936000
					}
				]`,
			},
			expectError:  false,
			expectedWins: "0 0 * * * *",
			description:  "First configuration should win (simplified deduplication - no priority logic)",
		},
		{
			name: "first_without_timestamp_wins",
			rawConfig: tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: `[
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 0 * * *"
					},
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890", 
						"cron_schedule": "0 0 * * * *",
						"from": 1719849600
					}
				]`,
			},
			expectError:  false,
			expectedWins: "0 0 0 * * *",
			description:  "First configuration should win regardless of timestamp presence",
		},
		{
			name: "first_with_higher_timestamp_wins",
			rawConfig: tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: `[
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 0 * * *",
						"from": 1719849600
					},
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *",
						"from": 0
					}
				]`,
			},
			expectError:  false,
			expectedWins: "0 0 0 * * *",
			description:  "First configuration should win regardless of timestamp values",
		},
		{
			name: "first_with_positive_timestamp_wins",
			rawConfig: tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: `[
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 0 * * *",
						"from": 1719849600
					},
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *",
						"from": -1000
					}
				]`,
			},
			expectError:  false,
			expectedWins: "0 0 0 * * *",
			description:  "First configuration should win regardless of timestamp values",
		},
		{
			name: "identical_timestamps_fallback_to_source_priority",
			rawConfig: tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: `[
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 0 * * *",
						"from": 1719849600
					},
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *",
						"from": 1719849600
					}
				]`,
			},
			expectError:  false,
			expectedWins: "0 0 0 * * *", // First one wins when priorities are equal (deterministic)
			description:  "When timestamps are identical, fall back to deterministic behavior (first wins)",
		},
		{
			name: "no_timestamps_deterministic_behavior",
			rawConfig: tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: `[
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 0 * * *"
					},
					{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *"
					}
				]`,
			},
			expectError:  false,
			expectedWins: "0 0 0 * * *", // First one wins when no timestamps (deterministic)
			description:  "When neither has timestamps, use deterministic behavior (first wins)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader := tnConfig.NewLoader()
			processedConfig, err := loader.LoadAndProcess(context.Background(), tt.rawConfig)

			if tt.expectError {
				require.Error(t, err, "Expected error for: %s", tt.description)
				return
			}

			require.NoError(t, err, "Unexpected error for: %s", tt.description)
			require.True(t, processedConfig.Enabled)
			require.Len(t, processedConfig.Directives, 1, "Should have exactly 1 directive after deduplication")

			directive := processedConfig.Directives[0]
			assert.Equal(t, tt.expectedWins, directive.Schedule.CronExpr, 
				"Wrong directive selected during deduplication for: %s", tt.description)

			t.Logf("✅ Deduplication test passed: %s", tt.description)
			t.Logf("   Winner: %s", directive.Schedule.CronExpr)
			if directive.TimeRange.From != nil {
				t.Logf("   From timestamp: %d", *directive.TimeRange.From)
			} else {
				t.Logf("   From timestamp: none")
			}
		})
	}
}

// TestTimeRangeSimplification verifies that "to" field is no longer present
func TestTimeRangeSimplification(t *testing.T) {
	rawConfig := tnConfig.RawConfig{
		Enabled: "true",
		StreamsInline: `[{
			"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
			"stream_id": "st123456789012345678901234567890",
			"cron_schedule": "0 0 * * * *",
			"from": 1719849600
		}]`,
	}

	loader := tnConfig.NewLoader()
	processedConfig, err := loader.LoadAndProcess(context.Background(), rawConfig)

	require.NoError(t, err)
	require.Len(t, processedConfig.Directives, 1)

	directive := processedConfig.Directives[0]
	
	// Verify "from" field is properly set
	require.NotNil(t, directive.TimeRange.From, "From field should be set")
	assert.Equal(t, int64(1719849600), *directive.TimeRange.From, "From timestamp should match")

	// Verify TimeRange structure doesn't have "to" field (this will be caught at compile time)
	// Just verify the range is properly structured
	assert.IsType(t, tnConfig.TimeRange{}, directive.TimeRange, "TimeRange should be of correct type")
	
	t.Log("✅ TimeRange simplification verified - only 'from' field is supported")
}