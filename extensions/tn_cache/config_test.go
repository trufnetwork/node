package tn_cache

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"

	tnConfig "github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
)

// TestIncludeChildrenFunctionality tests include_children field handling across JSON and CSV formats
// This consolidates the original TestIncludeChildrenTransformation and TestCSVSource_IncludeChildrenFunctionality
func TestIncludeChildrenFunctionality(t *testing.T) {
	t.Run("JSON format", func(t *testing.T) {
		testCases := []struct {
			name            string
			includeChildren interface{} // can be bool or omitted
			expectedValue   bool
		}{
			{"explicit_true", true, true},
			{"explicit_false", false, false},
			{"omitted_defaults_false", nil, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var jsonConfig string
				if tc.includeChildren == nil {
					jsonConfig = `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "stcomposedstream123",
						"cron_schedule": "0 0 * * * *"
					}]`
				} else {
					jsonConfig = fmt.Sprintf(`[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "stcomposedstream123",
						"cron_schedule": "0 0 * * * *",
						"include_children": %v
					}]`, tc.includeChildren)
				}

				rawConfig := tnConfig.RawConfig{
					Enabled:       "true",
					StreamsInline: jsonConfig,
				}

				loader := tnConfig.NewLoader(log.NewStdoutLogger())
				processedConfig, err := loader.LoadAndProcess(context.Background(), rawConfig)

				require.NoError(t, err)
				require.Len(t, processedConfig.Directives, 1)
				assert.Equal(t, tc.expectedValue, processedConfig.Directives[0].IncludeChildren)
			})
		}
	})

	t.Run("CSV format", func(t *testing.T) {
		testCases := []struct {
			name           string
			csvContent     string
			expectedValues []bool
			expectError    bool
			errorContains  string
		}{
			{
				name:           "explicit_true",
				csvContent:     `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * * *,1719849600,true`,
				expectedValues: []bool{true},
			},
			{
				name:           "explicit_false",
				csvContent:     `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * * *,1719849600,false`,
				expectedValues: []bool{false},
			},
			{
				name:           "omitted_defaults_false",
				csvContent:     `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * * *,1719849600`,
				expectedValues: []bool{false},
			},
			{
				name:           "empty_field_defaults_false",
				csvContent:     `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * * *,1719849600,`,
				expectedValues: []bool{false},
			},
			{
				name:           "without_timestamp_but_with_include_children",
				csvContent:     `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * * *,,true`,
				expectedValues: []bool{true},
			},
			{
				name: "mixed_values",
				csvContent: `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * *,1719849600,true
0x9876543210fedcba9876543210fedcba98765432,ststream2,0 0 0 * * *,1719936000,false
0xabcdefabcdefabcdefabcdefabcdefabcdefabcd,ststream3,0 */15 * * * *,,true`,
				expectedValues: []bool{true, false, true},
			},
			{
				name:          "invalid_value",
				csvContent:    `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 0 0 * * * *,1719849600,maybe`,
				expectError:   true,
				errorContains: "invalid include_children value",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				csvFile := createTempCSV(t, tc.csvContent)
				defer cleanup(t, csvFile)

				source := sources.NewCSVSource(csvFile, "")
				specs, err := source.Load(context.Background(), map[string]string{})

				if tc.expectError {
					require.Error(t, err)
					if tc.errorContains != "" {
						assert.Contains(t, err.Error(), tc.errorContains)
					}
					return
				}

				require.NoError(t, err)
				require.Len(t, specs, len(tc.expectedValues))

				for i, expected := range tc.expectedValues {
					assert.Equal(t, expected, specs[i].IncludeChildren,
						"Spec %d: expected include_children=%v, got %v", i, expected, specs[i].IncludeChildren)
				}
			})
		}
	})
}

// TestConfigValidation tests high-ROI validation scenarios that prevent node failures
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name          string
		extConfig     map[string]map[string]string
		expectError   bool
		errorContains string
		description   string
	}{
		{
			name:        "no config - graceful handling",
			extConfig:   map[string]map[string]string{},
			expectError: false,
			description: "Missing config should be handled gracefully",
		},
		{
			name: "disabled extension - graceful handling",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "false",
				},
			},
			expectError: false,
			description: "Disabled extension should not cause errors",
		},
		{
			name: "invalid cron schedule - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "invalid cron"
					}]`,
				},
			},
			expectError:   true,
			errorContains: "cron_schedule validation failed",
			description:   "Invalid cron schedules should fail with clear error",
		},
		{
			name: "invalid ethereum address - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "not_an_address",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *"
					}]`,
				},
			},
			expectError:   true,
			errorContains: "ethereum_address validation failed",
			description:   "Invalid ethereum addresses should fail with clear error",
		},
		{
			name: "invalid stream ID - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "invalid_stream_id",
						"cron_schedule": "0 0 * * * *"
					}]`,
				},
			},
			expectError:   true,
			errorContains: "stream_id validation failed",
			description:   "Invalid stream IDs should fail with clear error",
		},
		{
			name: "corrupted JSON - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled":        "true",
					"streams_inline": `[{invalid json}]`,
				},
			},
			expectError:   true,
			errorContains: "streams configuration is not valid JSON",
			description:   "Corrupted JSON should fail with clear error",
		},
		{
			name: "valid wildcard configuration",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "*",
						"cron_schedule": "0 0 * * * *"
					}]`,
				},
			},
			expectError: false,
			description: "Wildcard stream ID should be accepted",
		},
		{
			name: "valid configuration with include_children",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "stcomposedstream123",
						"cron_schedule": "0 0 * * * *",
						"from": 1719849600,
						"include_children": true
					}]`,
				},
			},
			expectError: false,
			description: "Configuration with include_children should be processed successfully",
		},
		{
			name: "valid configuration with from timestamp",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * *",
						"from": 1719849600
					}]`,
				},
			},
			expectError: false,
			description: "Valid configuration with from timestamp should work",
		},
		{
			name: "valid configuration with max_block_age",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled":       "true",
					"max_block_age": "30m",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *"
					}]`,
				},
			},
			expectError: false,
			description: "Configuration with duration string max_block_age should work",
		},
		{
			name: "far future timestamp - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *",
						"from": 9999999999
					}]`,
				},
			},
			expectError:   true,
			errorContains: "time_range validation failed",
			description:   "Far future timestamps should fail",
		},
		{
			name: "both JSON and CSV provided - should error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 0 * * * *"
					}]`,
					"streams_csv_file": "streams.csv",
				},
			},
			expectError:   true,
			errorContains: "cannot specify both 'streams_inline' and 'streams_csv_file'",
			description:   "Providing both JSON and CSV should fail with clear error",
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
				require.Error(t, err, "Expected error for test: %s", tt.description)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains,
						"Error should contain expected text for test: %s", tt.description)
				}
				return
			}

			require.NoError(t, err, "Unexpected error for test: %s", tt.description)
			require.NotNil(t, result, "Result should not be nil for test: %s", tt.description)

			// For non-error cases, verify basic structure
			if !tt.expectError {
				assert.NotNil(t, result.Directives, "Directives should not be nil")
				assert.NotNil(t, result.Sources, "Sources should not be nil")
			}
		})
	}
}

// TestWildcardStreamResolution tests wildcard stream configuration handling
func TestWildcardStreamResolution(t *testing.T) {
	rawConfig := tnConfig.RawConfig{
		Enabled: "true",
		StreamsInline: `[{
			"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
			"stream_id": "*",
			"cron_schedule": "0 0 * * * *"
		}]`,
	}

	loader := tnConfig.NewLoader(log.NewStdoutLogger())
	processedConfig, err := loader.LoadAndProcess(context.Background(), rawConfig)

	require.NoError(t, err)
	require.True(t, processedConfig.Enabled)
	require.Len(t, processedConfig.Directives, 1)

	directive := processedConfig.Directives[0]
	assert.Equal(t, tnConfig.DirectiveProviderWildcard, directive.Type)
	assert.Equal(t, "*", directive.StreamID)
	assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", directive.DataProvider)
}

// TestConfigurationMergeAndDeduplication removed - redundant with TestBasicDeduplication in priority_test.go
// Both tests verify the same "first configuration wins" deduplication logic

// TestCronScheduleValidation tests cron schedule validation across both JSON and CSV formats
// This consolidates the original TestCronScheduleValidation and TestCSVSource_CronScheduleVariations
func TestCronScheduleValidation(t *testing.T) {
	testCases := []struct {
		name        string
		schedule    string
		shouldError bool
		description string
	}{
		// Valid schedules (consolidated from both tests)
		{"hourly", "0 * * * *", false, "Every hour"},
		{"daily", "0 0 * * *", false, "Daily at midnight"},
		{"weekly", "0 0 * * 0", false, "Weekly on Sunday"},
		{"monthly", "0 0 1 * *", false, "Monthly on 1st"},
		{"every_15_min", "*/15 * * * *", false, "Every 15 minutes"},
		{"complex_weekdays", "30 2 * * 1-5", false, "Weekdays at 2:30 AM"},

		// Invalid schedules
		{"invalid_text", "invalid", true, "Non-cron text"},
		{"invalid_minute", "60 * * * *", true, "Invalid minute (60)"},
		{"too_many_fields", "* * * * * *", true, "Too many fields"},
		{"too_few_fields", "0", true, "Too few fields"},
		{"empty", "", true, "Empty schedule"},
	}

	for _, tc := range testCases {
		t.Run("json_"+tc.name, func(t *testing.T) {
			// Test through JSON configuration (full pipeline validation)
			rawConfig := tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: fmt.Sprintf(`[{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
					"stream_id": "st123456789012345678901234567890",
					"cron_schedule": "%s"
				}]`, tc.schedule),
			}

			loader := tnConfig.NewLoader(log.NewStdoutLogger())
			_, err := loader.LoadAndProcess(context.Background(), rawConfig)

			if tc.shouldError {
				require.Error(t, err, "Expected error for %s: %s", tc.description, tc.schedule)
				assert.Contains(t, err.Error(), "cron_schedule validation failed")
			} else {
				require.NoError(t, err, "Expected success for %s: %s", tc.description, tc.schedule)
			}
		})

		// Only test valid schedules through CSV (CSV source doesn't validate cron syntax)
		if !tc.shouldError {
			t.Run("csv_"+tc.name, func(t *testing.T) {
				csvContent := fmt.Sprintf("0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,%s", tc.schedule)
				csvFile := createTempCSV(t, csvContent)
				defer cleanup(t, csvFile)

				source := sources.NewCSVSource(csvFile, "")
				specs, err := source.Load(context.Background(), map[string]string{})

				require.NoError(t, err, "CSV loading should succeed for valid cron: %s", tc.schedule)
				require.Len(t, specs, 1)
				assert.Equal(t, tc.schedule, specs[0].CronSchedule)
			})
		}
	}
}

// CSV Test Helper Functions
func createTempCSV(t *testing.T, content string) string {
	tmpFile, err := ioutil.TempFile("", "test_streams_*.csv")
	require.NoError(t, err)
	defer tmpFile.Close()

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)

	return tmpFile.Name()
}

func createTempDir(t *testing.T) string {
	tmpDir, err := ioutil.TempDir("", "test_csv_config_*")
	require.NoError(t, err)
	return tmpDir
}

func cleanup(t *testing.T, paths ...string) {
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			t.Logf("Warning: failed to cleanup %s: %v", path, err)
		}
	}
}

// TestCSVSource_BasicFunctionality tests basic CSV parsing functionality
func TestCSVSource_BasicFunctionality(t *testing.T) {
	tests := []struct {
		name          string
		csvContent    string
		expectedSpecs int
		expectError   bool
		errorContains string
	}{
		{
			name: "valid CSV with three columns",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 0 0 * * *`,
			expectedSpecs: 2,
			expectError:   false,
		},
		{
			name: "valid CSV with four columns (with from timestamp)",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 0 0 * * * *,1719849600
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 0 0 * * *,1719936000`,
			expectedSpecs: 2,
			expectError:   false,
		},
		{
			name: "valid CSV with comments and empty lines",
			csvContent: `# This is a comment
0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,*/15 * * * *

# Another comment
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 0 0 * * *,1719849600`,
			expectedSpecs: 2,
			expectError:   false,
		},
		{
			name: "invalid CSV - insufficient columns",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890
incomplete_row,missing_cron`,
			expectedSpecs: 0,
			expectError:   true,
			errorContains: "insufficient columns",
		},
		{
			name: "invalid CSV - empty required fields",
			csvContent: `,st123456789012345678901234567890,0 * * * *
0x1234567890abcdef1234567890abcdef12345678,,0 * * * *
0x1111111111111111111111111111111111111111,st123456789012345678901234567890,`,
			expectedSpecs: 0,
			expectError:   true,
			errorContains: "cannot be empty",
		},
		{
			name:          "invalid CSV - bad timestamp",
			csvContent:    `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 0 0 * * * *,invalid_timestamp`,
			expectedSpecs: 0,
			expectError:   true,
			errorContains: "invalid from timestamp",
		},
		{
			name: "valid CSV with wildcard stream ID",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,*,0 0 0 * * *
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 * * * *`,
			expectedSpecs: 2,
			expectError:   false,
		},
		{
			name:          "empty CSV file",
			csvContent:    "",
			expectedSpecs: 0,
			expectError:   false,
		},
		{
			name: "CSV with only comments and empty lines",
			csvContent: `# Comment 1

# Comment 2

`,
			expectedSpecs: 0,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary CSV file
			csvFile := createTempCSV(t, tt.csvContent)
			defer cleanup(t, csvFile)

			// Create CSV source
			source := sources.NewCSVSource(csvFile, "")

			// Test Load method
			rawConfig := map[string]string{}
			specs, err := source.Load(context.Background(), rawConfig)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Len(t, specs, tt.expectedSpecs)

			// Validate each spec has expected properties
			for _, spec := range specs {
				assert.NotEmpty(t, spec.DataProvider)
				assert.NotEmpty(t, spec.StreamID)
				assert.NotEmpty(t, spec.CronSchedule)
				assert.Contains(t, spec.Source, csvFile)
			}
		})
	}
}

// TestCSVSource_FileOperations removed - tested basic file system operations rather than extension-specific logic

// TestMutualExclusivity tests that JSON and CSV configuration are mutually exclusive
func TestMutualExclusivity(t *testing.T) {
	t.Run("both JSON and CSV provided to SourceFactory", func(t *testing.T) {
		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"streams_inline": `[{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "st123456789012345678901234567890",
				"cron_schedule": "0 0 * * * *"
			}]`,
			"streams_csv_file": "nonexistent.csv", // File doesn't need to exist for this test
		}

		_, err := factory.CreateSources(rawConfig)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot specify both 'streams_inline' and 'streams_csv_file'")
		assert.Contains(t, err.Error(), "use either inline JSON or CSV file, not both")
	})

	t.Run("only JSON provided - should work", func(t *testing.T) {
		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"streams_inline": `[{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "st123456789012345678901234567890",
				"cron_schedule": "0 0 * * * *"
			}]`,
		}

		configSources, err := factory.CreateSources(rawConfig)
		require.NoError(t, err)
		assert.Len(t, configSources, 1)
	})

	t.Run("only CSV provided - should work", func(t *testing.T) {
		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"streams_csv_file": csvFile,
		}

		configSources, err := factory.CreateSources(rawConfig)
		require.NoError(t, err)
		assert.Len(t, configSources, 1)
	})

	t.Run("neither JSON nor CSV provided - should work (empty config)", func(t *testing.T) {
		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"enabled": "true",
		}

		configSources, err := factory.CreateSources(rawConfig)
		require.NoError(t, err)
		assert.Len(t, configSources, 0)
	})
}

// TestCSVSource_IncludeChildrenFunctionality removed - consolidated into TestIncludeChildrenFunctionality
// This test was redundant with the comprehensive include_children test above

// TestCSVSource_CronScheduleVariations removed - consolidated into TestCronScheduleValidation
// This test was redundant with the comprehensive cron validation test above

// TestCSVSource_TimestampHandling tests core timestamp parsing scenarios
func TestCSVSource_TimestampHandling(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
		expected  *int64
		hasError  bool
	}{
		{
			name:      "valid_timestamp",
			timestamp: "1719849600",
			expected:  func() *int64 { v := int64(1719849600); return &v }(),
			hasError:  false,
		},
		{
			name:      "empty_timestamp",
			timestamp: "",
			expected:  nil,
			hasError:  false,
		},
		{
			name:      "invalid_timestamp",
			timestamp: "not_a_number",
			expected:  nil,
			hasError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			csvContent := fmt.Sprintf("0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 0 0 * * * *,%s", tt.timestamp)
			csvFile := createTempCSV(t, csvContent)
			defer cleanup(t, csvFile)

			source := sources.NewCSVSource(csvFile, "")
			specs, err := source.Load(context.Background(), map[string]string{})

			if tt.hasError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, specs, 1)

			if tt.expected == nil {
				assert.Nil(t, specs[0].From)
			} else {
				require.NotNil(t, specs[0].From)
				assert.Equal(t, *tt.expected, *specs[0].From)
			}
		})
	}
}

// TestCSVSource_Integration removed - redundant with TestCSVSource_BasicFunctionality and TestMutualExclusivity
// This test duplicated CSV parsing scenarios already covered in more focused unit tests

// TestCSVSource_LoaderIntegration removed - redundant with TestCSVSource_BasicFunctionality
// This test duplicated CSV parsing and validation scenarios already covered in focused unit tests
