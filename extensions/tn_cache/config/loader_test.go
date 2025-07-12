package config

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

// TestDeduplicateDirectivesLogging tests that duplicate directives trigger warning logs
func TestDeduplicateDirectivesLogging(t *testing.T) {
	// Create a buffer to capture log output
	var logBuf bytes.Buffer
	logger := log.New(log.WithWriter(&logBuf))

	// Create loader with logger
	loader := NewLoader(logger)

	// Create test configuration with duplicate directives
	testConfig := map[string]string{
		"enabled": "true",
		"streams_inline": `[
			{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "ststream1",
				"cron_schedule": "0 0 * * * *",
				"from": 1704067200
			},
			{
				"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
				"stream_id": "ststream1",
				"cron_schedule": "0 0 */2 * * *",
				"from": 1705276800
			}
		]`,
	}

	// Process the configuration
	processedConfig, err := loader.LoadAndProcessFromMap(context.Background(), testConfig)
	require.NoError(t, err)

	// Verify only one directive remains after deduplication
	assert.Len(t, processedConfig.Directives, 1)
	assert.Equal(t, "ststream1", processedConfig.Directives[0].StreamID)

	// Verify warning was logged
	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "duplicate cache directive detected")
	assert.Contains(t, logOutput, "0x1234567890abcdef1234567890abcdef12345678:ststream1")
	assert.Contains(t, logOutput, "kept_schedule")
	assert.Contains(t, logOutput, "0 0 * * * *")
	assert.Contains(t, logOutput, "ignored_schedule")
	assert.Contains(t, logOutput, "0 0 */2 * * *")
}

// TestDeduplicateDirectivesMultipleDuplicates tests deduplication with multiple duplicate sets
func TestDeduplicateDirectivesMultipleDuplicates(t *testing.T) {
	// Create a buffer to capture log output
	var logBuf bytes.Buffer
	logger := log.New(log.WithWriter(&logBuf))

	// Create loader with logger
	loader := NewLoader(logger)

	// Create test configuration with multiple sets of duplicates
	testConfig := map[string]string{
		"enabled": "true",
		"streams_inline": `[
			{
				"data_provider": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"stream_id": "ststreama",
				"cron_schedule": "0 0 * * * *"
			},
			{
				"data_provider": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"stream_id": "ststreamb",
				"cron_schedule": "0 0 * * * *"
			},
			{
				"data_provider": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"stream_id": "ststreama",
				"cron_schedule": "0 0 */2 * * *"
			},
			{
				"data_provider": "0xcccccccccccccccccccccccccccccccccccccccc",
				"stream_id": "ststreamc",
				"cron_schedule": "0 0 * * * *"
			},
			{
				"data_provider": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"stream_id": "ststreamb",
				"cron_schedule": "0 0 */3 * * *"
			}
		]`,
	}

	// Process the configuration
	processedConfig, err := loader.LoadAndProcessFromMap(context.Background(), testConfig)
	require.NoError(t, err)

	// Verify only unique directives remain
	assert.Len(t, processedConfig.Directives, 3)

	// Verify warnings were logged for each duplicate
	logOutput := logBuf.String()
	duplicateCount := strings.Count(logOutput, "duplicate cache directive detected")
	assert.Equal(t, 2, duplicateCount, "Expected 2 duplicate warnings")

	// Verify specific duplicates were logged
	assert.Contains(t, logOutput, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:ststreama")
	assert.Contains(t, logOutput, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:ststreamb")
}