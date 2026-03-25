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

	// Order event trim constants
	// ~2 days at 1-second blocks, giving the indexer ample time to sync
	TrimOrderEventsPreserveBlocks int64 = 172_800
	TrimOrderEventsDeleteCap            = 100_000
	TrimOrderEventsMaxRuns              = 10
)
