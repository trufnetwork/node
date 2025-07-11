package scheduler

import (
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
)

// TestDeduplicateResolvedSpecs tests the deduplication logic for resolved stream specifications
func TestDeduplicateResolvedSpecs(t *testing.T) {
	logger := log.New(log.WithWriter(nil)) // Discard logs during tests
	cacheDB := internal.NewCacheDB(nil, logger)
	scheduler := NewCacheScheduler(NewCacheSchedulerParams{
		Service:         &common.Service{},
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
	})

	// Helper to create directive with optional from timestamp
	createDirective := func(provider, streamID string, from *int64, cronExpr string) config.CacheDirective {
		return config.CacheDirective{
			ID:           provider + "_" + streamID,
			Type:         config.DirectiveSpecific,
			DataProvider: provider,
			StreamID:     streamID,
			Schedule:     config.Schedule{CronExpr: cronExpr},
			TimeRange:    config.TimeRange{From: from},
		}
	}

	tests := []struct {
		name     string
		input    []config.CacheDirective
		expected int // expected number of deduplicated directives
		check    func([]config.CacheDirective) bool
	}{
		{
			name: "no duplicates",
			input: []config.CacheDirective{
				createDirective("0xabc", "stream1", nil, "0 0 * * * *"),
				createDirective("0xabc", "stream2", nil, "0 0 * * * *"),
				createDirective("0xdef", "stream1", nil, "0 0 * * * *"),
			},
			expected: 3,
		},
		{
			name: "duplicate with earlier timestamp wins",
			input: []config.CacheDirective{
				createDirective("0xabc", "stream1", ptrInt64(2000), "0 0 * * * *"),
				createDirective("0xabc", "stream1", ptrInt64(1000), "0 0 0 * * *"), // should win
			},
			expected: 1,
			check: func(result []config.CacheDirective) bool {
				return result[0].TimeRange.From != nil &&
					*result[0].TimeRange.From == 1000 &&
					result[0].Schedule.CronExpr == "0 0 0 * * *"
			},
		},
		{
			name: "duplicate with nil timestamp (0) wins over positive",
			input: []config.CacheDirective{
				createDirective("0xabc", "stream1", ptrInt64(1000), "0 0 * * * *"),
				createDirective("0xabc", "stream1", nil, "0 0 0 * * *"), // should win (nil = 0)
			},
			expected: 1,
			check: func(result []config.CacheDirective) bool {
				return result[0].TimeRange.From == nil &&
					result[0].Schedule.CronExpr == "0 0 0 * * *"
			},
		},
		{
			name: "duplicate with same timestamp - first wins",
			input: []config.CacheDirective{
				createDirective("0xabc", "stream1", ptrInt64(1000), "0 0 * * * *"), // should win
				createDirective("0xabc", "stream1", ptrInt64(1000), "0 0 0 * * *"),
			},
			expected: 1,
			check: func(result []config.CacheDirective) bool {
				return result[0].Schedule.CronExpr == "0 0 * * * *"
			},
		},
		{
			name: "multiple duplicates across providers",
			input: []config.CacheDirective{
				createDirective("0xabc", "stream1", ptrInt64(2000), "0 0 * * * *"),
				createDirective("0xabc", "stream1", ptrInt64(1000), "0 0 0 * * *"), // wins for 0xabc/stream1
				createDirective("0xdef", "stream1", ptrInt64(3000), "0 0 * * * *"),
				createDirective("0xdef", "stream1", nil, "0 */5 * * * *"),          // wins for 0xdef/stream1
				createDirective("0xghi", "stream2", ptrInt64(1500), "0 0 * * * *"), // unique
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scheduler.deduplicateResolvedSpecs(tt.input)

			if len(result) != tt.expected {
				t.Errorf("expected %d deduplicated directives, got %d", tt.expected, len(result))
			}

			if tt.check != nil && !tt.check(result) {
				t.Error("deduplication did not produce expected result")
			}
		})
	}
}

func ptrInt64(v int64) *int64 {
	return &v
}
