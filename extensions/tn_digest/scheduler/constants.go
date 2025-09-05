package scheduler

import "time"

const (
	// Digest drain mode constants (scheduler-scoped to avoid import cycles)
	DigestDeleteCap                = 100_000
	DigestExpectedRecordsPerStream = 24
	DigestPreservePastDays         = 2
	DrainRunDelay                  = 60 * time.Second // 1 minute
	DrainMaxRuns                   = 100
	DrainMaxConsecutiveFailures    = 5
)
