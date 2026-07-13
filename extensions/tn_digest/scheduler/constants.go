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

	// Transaction event trim constants
	// ~2 days at 1-second blocks, so the indexer has synced high-volume
	// write-fee (method 2) ledger rows before they are pruned from node state.
	TrimTxEventsPreserveBlocks int64 = 172_800
	TrimTxEventsDeleteCap      int   = 100_000
	TrimTxEventsMaxRuns        int   = 10

	// TrimTxEventsEnabled gates activation. It ships false so a binary rollout
	// is decoupled from actually pruning: enable only after the Trufscan
	// indexer fallback (trufscan #183) is live in prod, so a pruned tx still
	// resolves on the explorer /tx page.
	TrimTxEventsEnabled bool = false
)
