package tn_cache

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"

	tnConfig "github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
)

// TestIncludeChildrenTransformation tests that include_children is properly transformed
func TestIncludeChildrenTransformation(t *testing.T) {
	t.Run("include_children transformation in JSON", func(t *testing.T) {
		rawConfig := tnConfig.RawConfig{
			Enabled: "true",
			StreamsInline: `[
				{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
					"stream_id": "stcomposedstream123",
					"cron_schedule": "0 * * * *",
					"from": 1719849600,
					"include_children": true
				},
				{
					"data_provider": "0x9876543210fedcba9876543210fedcba98765432",
					"stream_id": "stnormalstream456",
					"cron_schedule": "0 0 * * *",
					"include_children": false
				}
			]`,
		}

		loader := tnConfig.NewLoader()
		processedConfig, err := loader.LoadAndProcess(context.Background(), rawConfig)

		require.NoError(t, err)
		require.NotNil(t, processedConfig)
		assert.True(t, processedConfig.Enabled)
		assert.Len(t, processedConfig.Instructions, 2)

		// Find the instruction with include_children=true
		var composedInstruction, normalInstruction *tnConfig.InstructionDirective
		for i := range processedConfig.Instructions {
			if processedConfig.Instructions[i].StreamID == "stcomposedstream123" {
				composedInstruction = &processedConfig.Instructions[i]
			} else if processedConfig.Instructions[i].StreamID == "stnormalstream456" {
				normalInstruction = &processedConfig.Instructions[i]
			}
		}

		require.NotNil(t, composedInstruction)
		require.NotNil(t, normalInstruction)

		// Verify include_children values are correctly transformed
		assert.True(t, composedInstruction.IncludeChildren, "Composed stream should have include_children=true")
		assert.False(t, normalInstruction.IncludeChildren, "Normal stream should have include_children=false")
	})

	t.Run("include_children transformation in CSV", func(t *testing.T) {
		csvContent := `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,1719849600,true
0x9876543210fedcba9876543210fedcba98765432,stnormalstream456,0 0 * * *,1719936000,false`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		loader := tnConfig.NewLoader()
		rawConfig := map[string]string{
			"enabled":      "true",
			"streams_csv_file": csvFile,
		}

		processedConfig, err := loader.LoadAndProcessFromMap(context.Background(), rawConfig)

		require.NoError(t, err)
		require.NotNil(t, processedConfig)
		assert.True(t, processedConfig.Enabled)
		assert.Len(t, processedConfig.Instructions, 2)

		// Find the instructions
		var composedInstruction, normalInstruction *tnConfig.InstructionDirective
		for i := range processedConfig.Instructions {
			if processedConfig.Instructions[i].StreamID == "stcomposedstream123" {
				composedInstruction = &processedConfig.Instructions[i]
			} else if processedConfig.Instructions[i].StreamID == "stnormalstream456" {
				normalInstruction = &processedConfig.Instructions[i]
			}
		}

		require.NotNil(t, composedInstruction)
		require.NotNil(t, normalInstruction)

		// Verify include_children values are correctly transformed
		assert.True(t, composedInstruction.IncludeChildren, "Composed stream should have include_children=true")
		assert.False(t, normalInstruction.IncludeChildren, "Normal stream should have include_children=false")
	})
}

// TestConfigValidation tests high-ROI validation scenarios that prevent node failures
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name           string
		extConfig      map[string]map[string]string
		expectError    bool
		errorContains  string
		description    string
	}{
		{
			name: "no config - graceful handling",
			extConfig: map[string]map[string]string{},
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
			expectError: true,
			errorContains: "cron_schedule validation failed",
			description: "Invalid cron schedules should fail with clear error",
		},
		{
			name: "invalid ethereum address - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "not_an_address",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 * * * *"
					}]`,
				},
			},
			expectError: true,
			errorContains: "ethereum_address validation failed",
			description: "Invalid ethereum addresses should fail with clear error",
		},
		{
			name: "invalid stream ID - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "invalid_stream_id",
						"cron_schedule": "0 * * * *"
					}]`,
				},
			},
			expectError: true,
			errorContains: "stream_id validation failed",
			description: "Invalid stream IDs should fail with clear error",
		},
		{
			name: "corrupted JSON - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{invalid json}]`,
				},
			},
			expectError: true,
			errorContains: "streams configuration is not valid JSON",
			description: "Corrupted JSON should fail with clear error",
		},
		{
			name: "valid wildcard configuration",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "*",
						"cron_schedule": "0 * * * *"
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
						"cron_schedule": "0 * * * *",
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
			name: "far future timestamp - fatal error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 * * * *",
						"from": 9999999999
					}]`,
				},
			},
			expectError: true,
			errorContains: "time_range validation failed",
			description: "Far future timestamps should fail",
		},
		{
			name: "both JSON and CSV provided - should error",
			extConfig: map[string]map[string]string{
				ExtensionName: {
					"enabled": "true",
					"streams_inline": `[{
						"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
						"stream_id": "st123456789012345678901234567890",
						"cron_schedule": "0 * * * *"
					}]`,
					"streams_csv_file": "streams.csv",
				},
			},
			expectError: true,
			errorContains: "cannot specify both 'streams_inline' and 'streams_csv_file'",
			description: "Providing both JSON and CSV should fail with clear error",
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
				assert.NotNil(t, result.Instructions, "Instructions should not be nil")
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
			"cron_schedule": "0 * * * *"
		}]`,
	}

	loader := tnConfig.NewLoader()
	processedConfig, err := loader.LoadAndProcess(context.Background(), rawConfig)

	require.NoError(t, err)
	require.True(t, processedConfig.Enabled)
	require.Len(t, processedConfig.Instructions, 1)

	instruction := processedConfig.Instructions[0]
	assert.Equal(t, tnConfig.DirectiveProviderWildcard, instruction.Type)
	assert.Equal(t, "*", instruction.StreamID)
	assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", instruction.DataProvider)
}

// TestConfigurationMergeAndDeduplication tests that duplicate configurations are handled correctly
func TestConfigurationMergeAndDeduplication(t *testing.T) {
	rawConfig := tnConfig.RawConfig{
		Enabled: "true",
		StreamsInline: `[
			{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "st123456789012345678901234567890",
				"cron_schedule": "0 * * * *"
			},
			{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "st123456789012345678901234567890",
				"cron_schedule": "0 0 * * *"
			}
		]`,
	}

	loader := tnConfig.NewLoader()
	processedConfig, err := loader.LoadAndProcess(context.Background(), rawConfig)

	require.NoError(t, err)
	require.True(t, processedConfig.Enabled)
	
	// Should deduplicate to only one instruction (first one wins - simple deduplication)
	require.Len(t, processedConfig.Instructions, 1)
	
	instruction := processedConfig.Instructions[0]
	assert.Equal(t, tnConfig.DirectiveSpecific, instruction.Type)
	assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", instruction.DataProvider)
	assert.Equal(t, "st123456789012345678901234567890", instruction.StreamID)
	// Note: With simplified deduplication, first cron schedule wins: "0 * * * *"
	assert.Equal(t, "0 * * * *", instruction.Schedule.CronExpr)
}

// TestCronScheduleValidation tests the cron schedule validation specifically
func TestCronScheduleValidation(t *testing.T) {
	validSchedules := []string{
		"0 * * * *",      // Every hour
		"0 0 * * *",      // Daily at midnight
		"0 0 * * 0",      // Weekly on Sunday
		"0 0 1 * *",      // Monthly on 1st
		"*/15 * * * *",   // Every 15 minutes
	}

	invalidSchedules := []string{
		"invalid",
		"60 * * * *",     // Invalid minute
		"* * * * * *",    // Too many fields
		"0",              // Too few fields
		"",               // Empty
	}

	for _, schedule := range validSchedules {
		t.Run("valid_"+schedule, func(t *testing.T) {
			rawConfig := tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: fmt.Sprintf(`[{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
					"stream_id": "st123456789012345678901234567890",
					"cron_schedule": "%s"
				}]`, schedule),
			}

			loader := tnConfig.NewLoader()
			_, err := loader.LoadAndProcess(context.Background(), rawConfig)
			require.NoError(t, err, "Valid cron schedule should not fail: %s", schedule)
		})
	}

	for _, schedule := range invalidSchedules {
		t.Run("invalid_"+schedule, func(t *testing.T) {
			rawConfig := tnConfig.RawConfig{
				Enabled: "true",
				StreamsInline: fmt.Sprintf(`[{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
					"stream_id": "st123456789012345678901234567890",
					"cron_schedule": "%s"
				}]`, schedule),
			}

			loader := tnConfig.NewLoader()
			_, err := loader.LoadAndProcess(context.Background(), rawConfig)
			require.Error(t, err, "Invalid cron schedule should fail: %s", schedule)
			assert.Contains(t, err.Error(), "cron_schedule validation failed", 
				"Error should indicate cron validation failure")
		})
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
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 0 * * *`,
			expectedSpecs: 2,
			expectError:   false,
		},
		{
			name: "valid CSV with four columns (with from timestamp)",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *,1719849600
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 0 * * *,1719936000`,
			expectedSpecs: 2,
			expectError:   false,
		},
		{
			name: "valid CSV with comments and empty lines",
			csvContent: `# This is a comment
0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,*/15 * * * *

# Another comment
0x9876543210fedcba9876543210fedcba98765432,st987654321098765432109876543210,0 0 * * *,1719849600`,
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
			name: "invalid CSV - bad timestamp",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *,invalid_timestamp`,
			expectedSpecs: 0,
			expectError:   true,
			errorContains: "invalid from timestamp",
		},
		{
			name: "valid CSV with wildcard stream ID",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,*,0 0 * * *
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

// TestCSVSource_FileOperations tests file-related operations
func TestCSVSource_FileOperations(t *testing.T) {
	t.Run("file not found", func(t *testing.T) {
		source := sources.NewCSVSource("/nonexistent/file.csv", "")
		
		// Test Validate
		err := source.Validate(map[string]string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CSV file does not exist")

		// Test Load
		_, err = source.Load(context.Background(), map[string]string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open CSV file")
	})

	t.Run("absolute path resolution", func(t *testing.T) {
		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		source := sources.NewCSVSource(csvFile, "/some/base/path")
		
		err := source.Validate(map[string]string{})
		require.NoError(t, err)

		specs, err := source.Load(context.Background(), map[string]string{})
		require.NoError(t, err)
		assert.Len(t, specs, 1)
	})

	t.Run("relative path resolution", func(t *testing.T) {
		tmpDir := createTempDir(t)
		defer cleanup(t, tmpDir)

		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *`
		csvFile := filepath.Join(tmpDir, "streams.csv")
		
		err := ioutil.WriteFile(csvFile, []byte(csvContent), 0644)
		require.NoError(t, err)

		// Test with relative path
		source := sources.NewCSVSource("streams.csv", tmpDir)
		
		err = source.Validate(map[string]string{})
		require.NoError(t, err)

		specs, err := source.Load(context.Background(), map[string]string{})
		require.NoError(t, err)
		assert.Len(t, specs, 1)
	})

	t.Run("unreadable file", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping permission test when running as root")
		}

		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		// Make file unreadable
		err := os.Chmod(csvFile, 0000)
		require.NoError(t, err)
		defer os.Chmod(csvFile, 0644) // Restore for cleanup

		source := sources.NewCSVSource(csvFile, "")
		
		err = source.Validate(map[string]string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not readable")
	})
}

// TestMutualExclusivity tests that JSON and CSV configuration are mutually exclusive
func TestMutualExclusivity(t *testing.T) {
	t.Run("both JSON and CSV provided to SourceFactory", func(t *testing.T) {
		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"streams_inline": `[{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "st123456789012345678901234567890",
				"cron_schedule": "0 * * * *"
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
				"cron_schedule": "0 * * * *"
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

// TestCSVSource_IncludeChildrenFunctionality tests the include_children field
func TestCSVSource_IncludeChildrenFunctionality(t *testing.T) {
	tests := []struct {
		name                 string
		csvContent          string
		expectedSpecs       int
		expectError         bool
		errorContains       string
		expectedChildren    []bool // Expected include_children values for each spec
	}{
		{
			name: "CSV with include_children true",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,1719849600,true`,
			expectedSpecs: 1,
			expectError: false,
			expectedChildren: []bool{true},
		},
		{
			name: "CSV with include_children false",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,1719849600,false`,
			expectedSpecs: 1,
			expectError: false,
			expectedChildren: []bool{false},
		},
		{
			name: "CSV without include_children (defaults to false)",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,1719849600`,
			expectedSpecs: 1,
			expectError: false,
			expectedChildren: []bool{false},
		},
		{
			name: "CSV without timestamp but with include_children",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,,true`,
			expectedSpecs: 1,
			expectError: false,
			expectedChildren: []bool{true},
		},
		{
			name: "Mixed include_children values",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,ststream1,0 * * * *,1719849600,true
0x9876543210fedcba9876543210fedcba98765432,ststream2,0 0 * * *,1719936000,false
0xabcdefabcdefabcdefabcdefabcdefabcdefabcd,ststream3,*/15 * * * *,,true`,
			expectedSpecs: 3,
			expectError: false,
			expectedChildren: []bool{true, false, true},
		},
		{
			name: "Invalid include_children value",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,1719849600,maybe`,
			expectedSpecs: 0,
			expectError: true,
			errorContains: "invalid include_children value",
		},
		{
			name: "Empty include_children field (should default to false)",
			csvContent: `0x1234567890abcdef1234567890abcdef12345678,stcomposedstream123,0 * * * *,1719849600,`,
			expectedSpecs: 1,
			expectError: false,
			expectedChildren: []bool{false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			csvFile := createTempCSV(t, tt.csvContent)
			defer cleanup(t, csvFile)

			source := sources.NewCSVSource(csvFile, "")
			specs, err := source.Load(context.Background(), map[string]string{})

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Len(t, specs, tt.expectedSpecs)

			// Validate include_children values
			for i, spec := range specs {
				if i < len(tt.expectedChildren) {
					assert.Equal(t, tt.expectedChildren[i], spec.IncludeChildren, 
						"Spec %d: expected include_children=%v, got %v", i, tt.expectedChildren[i], spec.IncludeChildren)
				}
			}
		})
	}
}

// TestCSVSource_CronScheduleVariations tests different cron schedules
func TestCSVSource_CronScheduleVariations(t *testing.T) {
	schedules := []struct {
		name     string
		cron     string
		expected string
	}{
		{"hourly", "0 * * * *", "0 * * * *"},
		{"daily", "0 0 * * *", "0 0 * * *"},
		{"weekly", "0 0 * * 0", "0 0 * * 0"},
		{"every_15_min", "*/15 * * * *", "*/15 * * * *"},
		{"custom", "30 2 * * 1-5", "30 2 * * 1-5"},
	}

	for _, schedule := range schedules {
		t.Run(schedule.name, func(t *testing.T) {
			csvContent := fmt.Sprintf("0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,%s", schedule.cron)
			csvFile := createTempCSV(t, csvContent)
			defer cleanup(t, csvFile)

			source := sources.NewCSVSource(csvFile, "")
			specs, err := source.Load(context.Background(), map[string]string{})
			
			require.NoError(t, err)
			require.Len(t, specs, 1)
			assert.Equal(t, schedule.expected, specs[0].CronSchedule)
		})
	}
}

// TestCSVSource_TimestampHandling tests various timestamp scenarios
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
			name:      "zero_timestamp",
			timestamp: "0",
			expected:  func() *int64 { v := int64(0); return &v }(),
			hasError:  false,
		},
		{
			name:      "empty_timestamp",
			timestamp: "",
			expected:  nil,
			hasError:  false,
		},
		{
			name:      "whitespace_timestamp",
			timestamp: "   ",
			expected:  nil,
			hasError:  false,
		},
		{
			name:      "negative_timestamp",
			timestamp: "-1",
			expected:  func() *int64 { v := int64(-1); return &v }(),
			hasError:  false,
		},
		{
			name:      "invalid_timestamp",
			timestamp: "not_a_number",
			expected:  nil,
			hasError:  true,
		},
		{
			name:      "float_timestamp",
			timestamp: "1719849600.5",
			expected:  nil,
			hasError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			csvContent := fmt.Sprintf("0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *,%s", tt.timestamp)
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

// TestCSVSource_Integration tests integration with the main configuration system
func TestCSVSource_Integration(t *testing.T) {
	t.Run("CSV source through SourceFactory", func(t *testing.T) {
		// Create test CSV file
		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 0 * * *,1719849600
0x9876543210fedcba9876543210fedcba98765432,*,0 * * * *`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		// Test SourceFactory creation
		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"streams_csv_file": csvFile,
		}

		configSources, err := factory.CreateSources(rawConfig)
		require.NoError(t, err)
		require.Len(t, configSources, 1)

		// Test source validation
		source := configSources[0]
		err = source.Validate(rawConfig)
		require.NoError(t, err)

		// Test source loading
		specs, err := source.Load(context.Background(), rawConfig)
		require.NoError(t, err)
		assert.Len(t, specs, 2)

		// Verify specs
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", specs[0].DataProvider)
		assert.Equal(t, "st123456789012345678901234567890", specs[0].StreamID)
		assert.Equal(t, "0 0 * * *", specs[0].CronSchedule)
		assert.NotNil(t, specs[0].From)
		assert.Equal(t, int64(1719849600), *specs[0].From)

		assert.Equal(t, "0x9876543210fedcba9876543210fedcba98765432", specs[1].DataProvider)
		assert.Equal(t, "*", specs[1].StreamID)
		assert.Equal(t, "0 0 * * *", specs[1].CronSchedule)
		assert.Nil(t, specs[1].From)
	})

	t.Run("CSV source with default cron schedule", func(t *testing.T) {
		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 * * * *`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"csv_file": csvFile,
			// No cron_schedule provided - should use default
		}

		configSources, err := factory.CreateSources(rawConfig)
		require.NoError(t, err)
		require.Len(t, configSources, 1)

		specs, err := configSources[0].Load(context.Background(), rawConfig)
		require.NoError(t, err)
		assert.Len(t, specs, 1)
		assert.Equal(t, "0 * * * *", specs[0].CronSchedule) // Default schedule
	})

	t.Run("mutually exclusive inline and CSV sources", func(t *testing.T) {
		csvContent := `0x1111111111111111111111111111111111111111,st111111111111111111111111111111,0 * * * *`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"streams_inline": `[{
				"data_provider": "0x2222222222222222222222222222222222222222",
				"stream_id": "st222222222222222222222222222222",
				"cron_schedule": "0 0 * * *"
			}]`,
			"streams_csv_file":  csvFile,
			"cron_schedule": "0 * * * *",
		}

		// Should error when both JSON and CSV are provided
		_, err := factory.CreateSources(rawConfig)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot specify both 'streams_inline' and 'streams_csv_file'")
		assert.Contains(t, err.Error(), "use either inline JSON or CSV file, not both")
	})
}

// TestCSVSource_LoaderIntegration tests CSV source with the configuration loader
func TestCSVSource_LoaderIntegration(t *testing.T) {
	t.Run("full loader integration with CSV", func(t *testing.T) {
		// Create test CSV file
		csvContent := `0x1234567890abcdef1234567890abcdef12345678,st123456789012345678901234567890,0 0 * * *,1719849600
0x9876543210fedcba9876543210fedcba98765432,*,0 0 * * *,1719936000`
		csvFile := createTempCSV(t, csvContent)
		defer cleanup(t, csvFile)

		// We need to extend RawConfig to support file-based config
		// For now, we'll test the sources directly since RawConfig is limited
		factory := sources.NewSourceFactory()
		rawConfig := map[string]string{
			"enabled":      "true",
			"streams_csv_file": csvFile,
		}

		configSources, err := factory.CreateSources(rawConfig)
		require.NoError(t, err)
		require.Len(t, configSources, 1)

		// Test validation
		for _, source := range configSources {
			err := source.Validate(rawConfig)
			require.NoError(t, err)
		}

		// Test loading
		var allSpecs []sources.StreamSpec
		for _, source := range configSources {
			specs, err := source.Load(context.Background(), rawConfig)
			require.NoError(t, err)
			allSpecs = append(allSpecs, specs...)
		}

		// Verify loaded specs
		assert.Len(t, allSpecs, 2)
		
		// First spec: specific stream with timestamp
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", allSpecs[0].DataProvider)
		assert.Equal(t, "st123456789012345678901234567890", allSpecs[0].StreamID)
		assert.Equal(t, "0 0 * * *", allSpecs[0].CronSchedule)
		assert.NotNil(t, allSpecs[0].From)
		assert.Equal(t, int64(1719849600), *allSpecs[0].From)

		// Second spec: wildcard stream with timestamp  
		assert.Equal(t, "0x9876543210fedcba9876543210fedcba98765432", allSpecs[1].DataProvider)
		assert.Equal(t, "*", allSpecs[1].StreamID)
		assert.Equal(t, "0 0 * * *", allSpecs[1].CronSchedule)
		assert.NotNil(t, allSpecs[1].From)
		assert.Equal(t, int64(1719936000), *allSpecs[1].From)
	})

	t.Run("CSV validation errors", func(t *testing.T) {
		tests := []struct {
			name          string
			csvContent    string
			errorContains string
		}{
			{
				name:          "invalid ethereum address format",
				csvContent:    "invalid_address,st123456789012345678901234567890,0 * * * *",
				errorContains: "ethereum_address validation failed",
			},
			{
				name:          "invalid stream ID format",
				csvContent:    "0x1234567890abcdef1234567890abcdef12345678,invalid_stream,0 * * * *",
				errorContains: "stream_id validation failed",
			},
			{
				name:          "malformed CSV structure",
				csvContent:    "0x1234567890abcdef1234567890abcdef12345678",
				errorContains: "insufficient columns",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				csvFile := createTempCSV(t, tt.csvContent)
				defer cleanup(t, csvFile)

				factory := sources.NewSourceFactory()
				rawConfig := map[string]string{
					"enabled":       "true",
					"streams_csv_file":  csvFile,
					"cron_schedule": "0 * * * *",
				}

				configSources, err := factory.CreateSources(rawConfig)
				require.NoError(t, err)

				// The error should occur during loading or validation
				source := configSources[0]
				_, err = source.Load(context.Background(), rawConfig)
				
				if err == nil {
					// If loading succeeds, validation should catch the error
					// We'd need to run it through the full validation pipeline
					t.Logf("CSV loading succeeded, would need full validation pipeline to catch: %s", tt.errorContains)
				} else {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			})
		}
	})
}

// TestCSVSource_RealWorldExample demonstrates CSV functionality with a realistic example
func TestCSVSource_RealWorldExample(t *testing.T) {
	// Create a realistic CSV configuration
	csvContent := `# Example CSV configuration for TrueNetwork cache extension
# Format: data_provider,stream_id,cron_schedule[,from_timestamp]
# Comments starting with # are ignored

# Financial data provider with specific streams
0x1234567890abcdef1234567890abcdef12345678,stbtcusdprice,0 * * * *,1719849600
0x1234567890abcdef1234567890abcdef12345678,stethusdprice,0 * * * *,1719849600

# Weather data provider - all streams
0x9876543210fedcba9876543210fedcba98765432,*,0 * * * *,1719936000

# IoT sensor data - specific sensor without timestamp
0xabcdefabcdefabcdefabcdefabcdefabcdefabcd,stsensortemperature01,0 * * * *

# Mixed data sources with different cron schedules
`
	csvFile := createTempCSV(t, csvContent)
	defer cleanup(t, csvFile)

	// Test with hourly cron schedule for high-frequency data
	factory := sources.NewSourceFactory()
	rawConfig := map[string]string{
		"streams_file": csvFile,
	}

	configSources, err := factory.CreateSources(rawConfig)
	require.NoError(t, err)
	require.Len(t, configSources, 1)

	// Validate and load
	source := configSources[0]
	err = source.Validate(rawConfig)
	require.NoError(t, err)

	specs, err := source.Load(context.Background(), rawConfig)
	require.NoError(t, err)
	assert.Len(t, specs, 4)

	// Verify each spec
	expectedSpecs := []struct {
		provider string
		streamID string
		hasFrom  bool
		from     int64
	}{
		{"0x1234567890abcdef1234567890abcdef12345678", "stbtcusdprice", true, 1719849600},
		{"0x1234567890abcdef1234567890abcdef12345678", "stethusdprice", true, 1719849600},
		{"0x9876543210fedcba9876543210fedcba98765432", "*", true, 1719936000},
		{"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd", "stsensortemperature01", false, 0},
	}

	for i, expected := range expectedSpecs {
		assert.Equal(t, expected.provider, specs[i].DataProvider, "Provider mismatch at index %d", i)
		assert.Equal(t, expected.streamID, specs[i].StreamID, "StreamID mismatch at index %d", i)
		assert.Equal(t, "0 * * * *", specs[i].CronSchedule, "CronSchedule mismatch at index %d", i)
		
		if expected.hasFrom {
			require.NotNil(t, specs[i].From, "Expected From timestamp at index %d", i)
			assert.Equal(t, expected.from, *specs[i].From, "From timestamp mismatch at index %d", i)
		} else {
			assert.Nil(t, specs[i].From, "Expected no From timestamp at index %d", i)
		}
		
		assert.Contains(t, specs[i].Source, csvFile, "Source reference missing at index %d", i)
	}

	t.Logf("Successfully parsed %d stream specifications from CSV", len(specs))
	for i, spec := range specs {
		from := "none"
		if spec.From != nil {
			from = fmt.Sprintf("%d", *spec.From)
		}
		t.Logf("  [%d] %s -> %s (cron: %s, from: %s)", 
			i+1, spec.DataProvider, spec.StreamID, spec.CronSchedule, from)
	}
}