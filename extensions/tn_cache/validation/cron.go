package validation

import (
	"fmt"

	"github.com/robfig/cron/v3"
	"github.com/trufnetwork/node/extensions/tn_cache/config/sources"
)

// CronScheduleRule validates cron schedule expressions
type CronScheduleRule struct{}

// Name returns the name of this validation rule
func (r *CronScheduleRule) Name() string {
	return "cron_schedule"
}

// Validate checks if the cron_schedule field contains a valid cron expression
func (r *CronScheduleRule) Validate(spec sources.StreamSpec) error {
	return ValidateCronSchedule(spec.CronSchedule)
}

// ValidateCronSchedule validates a cron schedule expression using the robfig/cron parser
func ValidateCronSchedule(schedule string) error {
	if schedule == "" {
		return fmt.Errorf("cron schedule cannot be empty")
	}

	// Use 5-field parser to match scheduler's expectation (no seconds)
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron schedule '%s': %w", schedule, err)
	}

	return nil
}

// ParseCronSchedule parses a cron schedule and returns the parsed schedule object
func ParseCronSchedule(schedule string) (cron.Schedule, error) {
	if err := ValidateCronSchedule(schedule); err != nil {
		return nil, err
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parsed, err := parser.Parse(schedule)
	if err != nil {
		return nil, fmt.Errorf("failed to parse validated cron schedule '%s': %w", schedule, err)
	}

	return parsed, nil
}
