package digest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestAggregateResults tests the result aggregation functionality used in benchmarks.
func TestAggregateResults(t *testing.T) {
	tests := []struct {
		name     string
		results  []DigestRunResult
		expected DigestRunResult
	}{
		{
			name:     "empty_input",
			results:  []DigestRunResult{},
			expected: DigestRunResult{},
		},
		{
			name: "single_result",
			results: []DigestRunResult{
				{
					Case: DigestBenchmarkCase{
						Streams:       10,
						DaysPerStream: 5,
						RecordsPerDay: 100,
						DeleteCap:     1000,
						Pattern:       PatternRandom,
						Samples:       3,
					},
					Candidates:       50,
					ProcessedDays:    25,
					TotalDeletedRows: 1000,

					Duration:             5 * time.Second,
					MemoryMaxBytes:       1024 * 1024 * 100, // 100MB
					StreamDaysPerSecond:  5.0,
					RowsDeletedPerSecond: 200.0,
				},
			},
			expected: DigestRunResult{
				Case: DigestBenchmarkCase{
					Streams:       10,
					DaysPerStream: 5,
					RecordsPerDay: 100,
					DeleteCap:     1000,
					Pattern:       PatternRandom,
					Samples:       3,
				},
				Candidates:       50,
				ProcessedDays:    25,
				TotalDeletedRows: 1000,

				Duration:             5 * time.Second,
				MemoryMaxBytes:       1024 * 1024 * 100, // 100MB
				StreamDaysPerSecond:  5.0,
				RowsDeletedPerSecond: 200.0,
			},
		},
		{
			name: "multiple_results_aggregation",
			results: []DigestRunResult{
				{
					Candidates:       30,
					ProcessedDays:    15,
					TotalDeletedRows: 500,

					Duration:             3 * time.Second,
					MemoryMaxBytes:       50 * 1024 * 1024, // 50MB
					StreamDaysPerSecond:  5.0,
					RowsDeletedPerSecond: 166.67,
				},
				{
					Candidates:       20,
					ProcessedDays:    10,
					TotalDeletedRows: 300,

					Duration:             2 * time.Second,
					MemoryMaxBytes:       75 * 1024 * 1024, // 75MB
					StreamDaysPerSecond:  5.0,
					RowsDeletedPerSecond: 150.0,
				},
			},
			expected: DigestRunResult{
				Candidates:       50,  // 30 + 20
				ProcessedDays:    25,  // 15 + 10
				TotalDeletedRows: 800, // 500 + 300

				Duration:             5 * time.Second,  // 3 + 2
				MemoryMaxBytes:       75 * 1024 * 1024, // max(50MB, 75MB)
				StreamDaysPerSecond:  5.0,              // 25 / 5
				RowsDeletedPerSecond: 160.0,            // 800 / 5
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AggregateResults(tt.results)

			if len(tt.results) == 0 {
				assert.Equal(t, DigestRunResult{}, result)
				return
			}

			assert.Equal(t, tt.expected.Candidates, result.Candidates)
			assert.Equal(t, tt.expected.ProcessedDays, result.ProcessedDays)
			assert.Equal(t, tt.expected.TotalDeletedRows, result.TotalDeletedRows)

			assert.Equal(t, tt.expected.Duration, result.Duration)
			assert.Equal(t, tt.expected.MemoryMaxBytes, result.MemoryMaxBytes)

			// Verify throughput calculations
			if result.Duration.Seconds() > 0 {
				expectedStreamDaysPerSec := float64(result.ProcessedDays) / result.Duration.Seconds()
				assert.InDelta(t, expectedStreamDaysPerSec, result.StreamDaysPerSecond, 0.1)

				expectedRowsPerSec := float64(result.TotalDeletedRows) / result.Duration.Seconds()
				assert.InDelta(t, expectedRowsPerSec, result.RowsDeletedPerSecond, 0.1)
			}
		})
	}
}

// TestValidateBenchmarkCase tests benchmark case validation used in benchmark execution.
func TestValidateBenchmarkCase(t *testing.T) {
	tests := []struct {
		name    string
		c       DigestBenchmarkCase
		wantErr bool
	}{
		{
			name: "valid_case",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 100,
				DeleteCap:     1000,
				Pattern:       PatternRandom,
				Samples:       3,
			},
			wantErr: false,
		},
		{
			name: "zero_streams",
			c: DigestBenchmarkCase{
				Streams:       0,
				DaysPerStream: 5,
				RecordsPerDay: 100,
				DeleteCap:     1000,
				Pattern:       PatternRandom,
				Samples:       3,
			},
			wantErr: true,
		},
		{
			name: "zero_delete_cap",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 100,
				DeleteCap:     0,
				Pattern:       PatternRandom,
				Samples:       3,
			},
			wantErr: true,
		},
		{
			name: "zero_samples",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 100,
				DeleteCap:     1000,
				Pattern:       PatternRandom,
				Samples:       0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBenchmarkCase(tt.c)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestValidateResult tests result validation used in benchmark execution.
func TestValidateResult(t *testing.T) {
	validResult := DigestRunResult{
		Candidates:       50,
		ProcessedDays:    25,
		TotalDeletedRows: 1000,

		Duration:             5 * time.Second,
		MemoryMaxBytes:       100 * 1024 * 1024,
		StreamDaysPerSecond:  5.0,
		RowsDeletedPerSecond: 200.0,
	}

	invalidResults := []struct {
		name   string
		result DigestRunResult
	}{
		{
			name: "negative_candidates",
			result: DigestRunResult{
				Candidates:     -1,
				ProcessedDays:  25,
				Duration:       5 * time.Second,
				MemoryMaxBytes: 100 * 1024 * 1024,
			},
		},
		{
			name: "zero_memory",
			result: DigestRunResult{
				Candidates:     50,
				ProcessedDays:  25,
				Duration:       5 * time.Second,
				MemoryMaxBytes: 0,
			},
		},
		{
			name: "negative_duration",
			result: DigestRunResult{
				Candidates:     50,
				ProcessedDays:  25,
				Duration:       -1 * time.Second,
				MemoryMaxBytes: 100 * 1024 * 1024,
			},
		},
	}

	// Test valid result
	err := ValidateResult(validResult)
	assert.NoError(t, err, "valid result should pass validation")

	// Test invalid results
	for _, tt := range invalidResults {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResult(tt.result)
			assert.Error(t, err, "expected validation error for %s", tt.name)
		})
	}
}

// TestCoreConstants validates essential constants used in benchmarks.
func TestCoreConstants(t *testing.T) {
	// Test essential table names for digest operations
	assert.NotEmpty(t, DigestTables, "DigestTables should not be empty")
	assert.Contains(t, DigestTables, "primitive_events", "should contain primitive_events")
	assert.Contains(t, DigestTables, "pending_prune_days", "should contain pending_prune_days")

	// Test essential patterns
	assert.NotEmpty(t, PatternRandom, "PatternRandom should not be empty")

	// Test container name
	assert.NotEmpty(t, DockerPostgresContainer, "DockerPostgresContainer should not be empty")

	// Test core configuration
	assert.Greater(t, DefaultDeleteCap, 0, "DefaultDeleteCap should be positive")
	assert.Greater(t, DefaultSamples, 0, "DefaultSamples should be positive")
}
