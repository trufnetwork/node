package tn_cache

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shouldSkipRefresh checks if we're still in the same cron period
func shouldSkipRefresh(lastRefresh time.Time, cronSchedule string, now time.Time) (bool, error) {
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(cronSchedule)
	if err != nil {
		return false, err
	}
	
	nextScheduled := schedule.Next(lastRefresh)
	return !nextScheduled.Before(now), nil
}

func TestCronPeriodDetection(t *testing.T) {
	// Helper to create time on 2024-01-01 with given hour:minute
	mkTime := func(hourMin string) time.Time {
		t, _ := time.Parse("15:04", hourMin)
		return time.Date(2024, 1, 1, t.Hour(), t.Minute(), 0, 0, time.UTC)
	}

	tests := []struct {
		name       string
		schedule   string
		lastTime   string
		nowTime    string
		shouldSkip bool
	}{
		// Core cases that prove the logic works
		{"hourly - within period", "0 0 * * * *", "14:30", "14:45", true},
		{"hourly - past period", "0 0 * * * *", "14:30", "15:01", false},
		{"5min - within period", "0 */5 * * * *", "14:47", "14:48", true},
		{"5min - past period", "0 */5 * * * *", "14:42", "14:48", false},
		
		// Edge case: exact schedule time
		{"exact time edge case", "0 0 * * * *", "15:00", "15:00", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastRefresh := mkTime(tt.lastTime)
			now := mkTime(tt.nowTime)
			
			skip, err := shouldSkipRefresh(lastRefresh, tt.schedule, now)
			require.NoError(t, err)
			assert.Equal(t, tt.shouldSkip, skip)
		})
	}
}

func TestCronAcrossBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		schedule   string
		last       time.Time
		now        time.Time
		shouldSkip bool
	}{
		// Daily
		{"daily - same day", "0 2 * * *", 
			time.Date(2024, 1, 15, 2, 30, 0, 0, time.UTC),
			time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC), true},
		{"daily - next day", "0 2 * * *",
			time.Date(2024, 1, 15, 2, 30, 0, 0, time.UTC),
			time.Date(2024, 1, 16, 2, 30, 0, 0, time.UTC), false},
		
		// Monthly
		{"monthly - same month", "0 0 1 * *",
			time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), true},
		{"monthly - next month", "0 0 1 * *",
			time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			time.Date(2024, 2, 1, 0, 30, 0, 0, time.UTC), false},
		
		// Yearly
		{"yearly - same year", "0 0 1 1 *",
			time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC), true},
		{"yearly - next year", "0 0 1 1 *",
			time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC), false},
		
		// Special: leap year Feb 29
		{"leap year schedule", "0 0 29 2 *",
			time.Date(2024, 2, 29, 0, 30, 0, 0, time.UTC),
			time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC), true}, // Next is 2028!
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skip, err := shouldSkipRefresh(tt.last, tt.schedule, tt.now)
			require.NoError(t, err)
			assert.Equal(t, tt.shouldSkip, skip)
		})
	}
}

func TestCronInvalidInput(t *testing.T) {
	_, err := shouldSkipRefresh(time.Now(), "invalid cron", time.Now())
	assert.Error(t, err)
}