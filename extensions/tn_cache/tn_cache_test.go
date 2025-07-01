package tn_cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name           string
		extConfig      map[string]map[string]string
		expectedConfig *Config
		expectError    bool
	}{
		{
			name:           "no config",
			extConfig:      map[string]map[string]string{},
			expectedConfig: &Config{Enabled: false, Streams: []StreamConfig{}},
			expectError:    false,
		},
		{
			name: "disabled extension",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "false",
				},
			},
			expectedConfig: &Config{Enabled: false, Streams: []StreamConfig{}},
			expectError:    false,
		},
		{
			name: "enabled extension with no streams",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
				},
			},
			expectedConfig: &Config{Enabled: true, Streams: []StreamConfig{}},
			expectError:    false,
		},
		{
			name: "enabled extension with streams",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams": `[
						{
							"data_provider": "0x1234567890",
							"stream_id": "st1234",
							"cron_schedule": "0 * * * *"
						},
						{
							"data_provider": "0xabcdef",
							"stream_id": "st5678",
							"cron_schedule": "0 0 * * *",
							"from": 1719849600
						}
					]`,
				},
			},
			expectedConfig: &Config{
				Enabled: true,
				Streams: []StreamConfig{
					{
						DataProvider: "0x1234567890",
						StreamID:     "st1234",
						CronSchedule: "0 * * * *",
					},
					{
						DataProvider: "0xabcdef",
						StreamID:     "st5678",
						CronSchedule: "0 0 * * *",
						From:         1719849600,
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid streams json",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams": `[invalid json]`,
				},
			},
			expectedConfig: nil,
			expectError:    true,
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

			// Check error
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Check configuration
			assert.Equal(t, tt.expectedConfig.Enabled, result.Enabled)
			assert.Len(t, result.Streams, len(tt.expectedConfig.Streams))

			// If there are streams, check them in detail
			if len(tt.expectedConfig.Streams) > 0 {
				for i, expected := range tt.expectedConfig.Streams {
					assert.Equal(t, expected.DataProvider, result.Streams[i].DataProvider)
					assert.Equal(t, expected.StreamID, result.Streams[i].StreamID)
					assert.Equal(t, expected.CronSchedule, result.Streams[i].CronSchedule)
					assert.Equal(t, expected.From, result.Streams[i].From)
				}
			}
		})
	}
} 