package digest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSliceCandidates was removed - batching functionality is no longer used.
// All candidates are now processed in a single call to batch_digest.

// TestAggregateResults tests the result aggregation functionality.
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

						Pattern: "random",
						Samples: 3,
					},
					Candidates:           50,
					ProcessedDays:        25,
					TotalDeletedRows:     1000,
					TotalPreservedRows:   2000,
					Duration:             5 * time.Second,
					MemoryMaxBytes:       1024 * 1024 * 100, // 100MB
					DaysPerSecond:        5.0,
					RowsDeletedPerSecond: 200.0,
					WALBytes:             nil,
				},
			},
			expected: DigestRunResult{
				Case: DigestBenchmarkCase{
					Streams:       10,
					DaysPerStream: 5,
					RecordsPerDay: 100,

					Pattern: "random",
					Samples: 3,
				},
				Candidates:           50,
				ProcessedDays:        25,
				TotalDeletedRows:     1000,
				TotalPreservedRows:   2000,
				Duration:             5 * time.Second,
				MemoryMaxBytes:       1024 * 1024 * 100, // 100MB
				DaysPerSecond:        5.0,
				RowsDeletedPerSecond: 200.0,
				WALBytes:             nil,
			},
		},
		{
			name: "multiple_results",
			results: []DigestRunResult{
				{
					Candidates:           30,
					ProcessedDays:        15,
					TotalDeletedRows:     500,
					TotalPreservedRows:   1000,
					Duration:             3 * time.Second,
					MemoryMaxBytes:       50 * 1024 * 1024, // 50MB
					DaysPerSecond:        5.0,
					RowsDeletedPerSecond: 166.67,
					WALBytes:             nil,
				},
				{
					Candidates:           20,
					ProcessedDays:        10,
					TotalDeletedRows:     300,
					TotalPreservedRows:   800,
					Duration:             2 * time.Second,
					MemoryMaxBytes:       75 * 1024 * 1024, // 75MB
					DaysPerSecond:        5.0,
					RowsDeletedPerSecond: 150.0,
					WALBytes:             nil,
				},
			},
			expected: DigestRunResult{
				Candidates:           50,               // 30 + 20
				ProcessedDays:        25,               // 15 + 10
				TotalDeletedRows:     800,              // 500 + 300
				TotalPreservedRows:   1800,             // 1000 + 800
				Duration:             5 * time.Second,  // 3 + 2
				MemoryMaxBytes:       75 * 1024 * 1024, // max(50MB, 75MB) = 75MB
				DaysPerSecond:        5.0,              // 25 / 5
				RowsDeletedPerSecond: 160.0,            // 800 / 5
				WALBytes:             nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AggregateResults(tt.results)

			if len(tt.results) == 0 {
				assert.Equal(t, DigestRunResult{}, result, "empty input should return zero result")
				return
			}

			assert.Equal(t, tt.expected.Candidates, result.Candidates, "candidates should match")
			assert.Equal(t, tt.expected.ProcessedDays, result.ProcessedDays, "processed days should match")
			assert.Equal(t, tt.expected.TotalDeletedRows, result.TotalDeletedRows, "deleted rows should match")
			assert.Equal(t, tt.expected.TotalPreservedRows, result.TotalPreservedRows, "preserved rows should match")
			assert.Equal(t, tt.expected.Duration, result.Duration, "duration should match")
			assert.Equal(t, tt.expected.MemoryMaxBytes, result.MemoryMaxBytes, "memory should match")

			// Verify calculated throughput metrics
			if result.Duration.Seconds() > 0 {
				expectedDaysPerSec := float64(result.ProcessedDays) / result.Duration.Seconds()
				assert.InDelta(t, expectedDaysPerSec, result.DaysPerSecond, 0.1, "days per second should be calculated correctly")

				expectedRowsPerSec := float64(result.TotalDeletedRows) / result.Duration.Seconds()
				assert.InDelta(t, expectedRowsPerSec, result.RowsDeletedPerSecond, 0.1, "rows per second should be calculated correctly")
			}
		})
	}
}

// TestValidateBenchmarkCase tests benchmark case validation.
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

				Pattern: "random",
				Samples: 3,
			},
			wantErr: false,
		},
		{
			name: "zero_streams",
			c: DigestBenchmarkCase{
				Streams:       0,
				DaysPerStream: 5,
				RecordsPerDay: 100,

				Pattern: "random",
				Samples: 3,
			},
			wantErr: true,
		},
		{
			name: "zero_days",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 0,
				RecordsPerDay: 100,

				Pattern: "random",
				Samples: 3,
			},
			wantErr: true,
		},
		{
			name: "zero_records",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 0,

				Pattern: "random",
				Samples: 3,
			},
			wantErr: true,
		},
		{
			name: "zero_batch_size",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 100,

				Pattern: "random",
				Samples: 3,
			},
			wantErr: true,
		},
		{
			name: "valid_pattern",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 100,

				Pattern: PatternRandom,
				Samples: 3,
			},
			wantErr: false,
		},
		{
			name: "zero_samples",
			c: DigestBenchmarkCase{
				Streams:       10,
				DaysPerStream: 5,
				RecordsPerDay: 100,

				Pattern: "random",
				Samples: 0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBenchmarkCase(tt.c)
			if tt.wantErr {
				assert.Error(t, err, "expected validation error but got none")
			} else {
				assert.NoError(t, err, "expected no validation error but got: %v", err)
			}
		})
	}
}

// TestValidateResult tests result validation.
func TestValidateResult(t *testing.T) {
	tests := []struct {
		name    string
		result  DigestRunResult
		wantErr bool
	}{
		{
			name: "valid_result",
			result: DigestRunResult{
				Candidates:           50,
				ProcessedDays:        25,
				TotalDeletedRows:     1000,
				TotalPreservedRows:   2000,
				Duration:             5 * time.Second,
				MemoryMaxBytes:       100 * 1024 * 1024,
				DaysPerSecond:        5.0,
				RowsDeletedPerSecond: 200.0,
			},
			wantErr: false,
		},
		{
			name: "negative_candidates",
			result: DigestRunResult{
				Candidates:         -1,
				ProcessedDays:      25,
				TotalDeletedRows:   1000,
				TotalPreservedRows: 2000,
				Duration:           5 * time.Second,
				MemoryMaxBytes:     100 * 1024 * 1024,
			},
			wantErr: true,
		},
		{
			name: "zero_memory",
			result: DigestRunResult{
				Candidates:         50,
				ProcessedDays:      25,
				TotalDeletedRows:   1000,
				TotalPreservedRows: 2000,
				Duration:           5 * time.Second,
				MemoryMaxBytes:     0,
			},
			wantErr: true,
		},
		{
			name: "negative_duration",
			result: DigestRunResult{
				Candidates:         50,
				ProcessedDays:      25,
				TotalDeletedRows:   1000,
				TotalPreservedRows: 2000,
				Duration:           -1 * time.Second,
				MemoryMaxBytes:     100 * 1024 * 1024,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResult(tt.result)
			if tt.wantErr {
				assert.Error(t, err, "expected validation error but got none")
			} else {
				assert.NoError(t, err, "expected no validation error but got: %v", err)
			}
		})
	}
}

// TestConvertToSavedResult tests CSV conversion.
func TestConvertToSavedResult(t *testing.T) {
	input := DigestRunResult{
		Case: DigestBenchmarkCase{
			Streams:       10,
			DaysPerStream: 5,
			RecordsPerDay: 100,

			Pattern: "random",
			Samples: 3,
		},
		Candidates:           50,
		ProcessedDays:        25,
		TotalDeletedRows:     1000,
		TotalPreservedRows:   2000,
		Duration:             5 * time.Second,
		MemoryMaxBytes:       100 * 1024 * 1024, // 100MB
		DaysPerSecond:        5.0,
		RowsDeletedPerSecond: 200.0,
		WALBytes:             &[]int64{1024}[0], // 1024 bytes
	}

	result := ConvertToSavedResult(input)

	// Verify conversion
	assert.Equal(t, input.Case.Streams, result.Streams)
	assert.Equal(t, input.Case.DaysPerStream, result.DaysPerStream)
	assert.Equal(t, input.Case.RecordsPerDay, result.RecordsPerDay)
	// BatchSize was removed - we now send all candidates in one call
	assert.Equal(t, input.Case.Pattern, result.Pattern)
	assert.Equal(t, input.Case.Samples, result.Samples)
	assert.Equal(t, input.Candidates, result.Candidates)
	assert.Equal(t, input.ProcessedDays, result.ProcessedDays)
	assert.Equal(t, input.TotalDeletedRows, result.TotalDeleted)
	assert.Equal(t, input.TotalPreservedRows, result.TotalPreserved)
	assert.Equal(t, int64(5000), result.DurationMs) // 5 seconds in milliseconds
	assert.Equal(t, input.DaysPerSecond, result.DaysPerSec)
	assert.Equal(t, input.RowsDeletedPerSecond, result.RowsDeletedPerSec)
	assert.Equal(t, uint64(100*1024*1024), result.MemoryMB*1024*1024) // Memory in bytes
	assert.Equal(t, input.WALBytes, result.WalBytes)
	assert.NotEmpty(t, result.Timestamp)
}

// TestDataGenerationPatterns tests the data generation patterns.
func TestDataGenerationPatterns(t *testing.T) {
	patterns := []string{PatternRandom, PatternDups50, PatternMonotonic, PatternEqual, PatternTimeDup}

	for _, pattern := range patterns {
		t.Run("pattern_"+pattern, func(t *testing.T) {
			// Test that we can create benchmark cases with each pattern
			c := DigestBenchmarkCase{
				Streams:       5,
				DaysPerStream: 2,
				RecordsPerDay: 10,

				Pattern: pattern,
				Samples: 1,
			}

			// Should not panic when creating the case
			err := ValidateBenchmarkCase(c)
			// Pattern validation may not be implemented, so we just check that basic validation passes
			if c.Streams > 0 && c.DaysPerStream > 0 && c.RecordsPerDay > 0 && c.Samples > 0 {
				// These should be valid if the pattern validation isn't implemented
				assert.NoError(t, err, "basic validation should pass for pattern %s", pattern)
			}
		})
	}
}

// TestConstants tests that all constants are properly defined.
func TestConstants(t *testing.T) {
	// Test that all table names are defined
	assert.NotEmpty(t, DigestTables, "DigestTables should not be empty")
	assert.Contains(t, DigestTables, "primitive_events", "should contain primitive_events")
	assert.Contains(t, DigestTables, "pending_prune_days", "should contain pending_prune_days")

	// Test that all patterns are defined
	assert.NotEmpty(t, PatternRandom, "PatternRandom should not be empty")
	assert.NotEmpty(t, PatternDups50, "PatternDups50 should not be empty")
	assert.NotEmpty(t, PatternMonotonic, "PatternMonotonic should not be empty")
	assert.NotEmpty(t, PatternEqual, "PatternEqual should not be empty")
	assert.NotEmpty(t, PatternTimeDup, "PatternTimeDup should not be empty")

	// Test that container name is defined
	assert.NotEmpty(t, DockerPostgresContainer, "DockerPostgresContainer should not be empty")

	// Test that resource limits are reasonable
	assert.Greater(t, MaxWorkers, 0, "MaxWorkers should be positive")
	assert.Greater(t, MaxConcurrency, 0, "MaxConcurrency should be positive")
	// BatchSize was removed - we now send all candidates in one call
	assert.Greater(t, DefaultSamples, 0, "DefaultSamples should be positive")

	// Test that test configurations are reasonable
	assert.Greater(t, SmokeTestStreams, 0, "SmokeTestStreams should be positive")
	assert.Greater(t, MediumTestStreams, 0, "MediumTestStreams should be positive")
	assert.Greater(t, ExtremeTestStreams, 0, "ExtremeTestStreams should be positive")
}

// TestBenchmarkCaseCreation tests creation of benchmark cases.
func TestBenchmarkCaseCreation(t *testing.T) {
	// Test creation with constants
	c := DigestBenchmarkCase{
		Streams:       SmokeTestStreams,
		DaysPerStream: SmokeTestDays,
		RecordsPerDay: SmokeTestRecords,

		Pattern: PatternRandom,
		Samples: DefaultSamples,
	}

	err := ValidateBenchmarkCase(c)
	require.NoError(t, err, "benchmark case with constants should be valid")

	// Test that the case has reasonable values
	assert.Greater(t, c.Streams, 0, "streams should be positive")
	assert.Greater(t, c.DaysPerStream, 0, "days per stream should be positive")
	assert.Greater(t, c.RecordsPerDay, 0, "records per day should be positive")
	// BatchSize validation removed - we now send all candidates in one call
	assert.Greater(t, c.Samples, 0, "samples should be positive")
	assert.NotEmpty(t, c.Pattern, "pattern should not be empty")
}
