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

	// Duplicate prune constants.
	//
	// There is no Enabled constant here on purpose. duplicate_prune_config.enabled
	// ships false and is the only gate, so an operator turns the sweep on with a
	// signed exec-sql rather than a binary release. A second gate in Go would mean
	// setting that column and watching nothing happen.
	//
	// The sweep is cyclic: has_more_to_delete means "the cursor has not finished a
	// pass", not "there is more to delete". A firing therefore runs its whole loop
	// rather than stopping early, so these numbers say how much of a pass one
	// firing covers rather than how fast a backlog drains.
	//
	// Mainnet holds ~182,000 primitive streams. At 100 streams a run and 100 runs a
	// firing that is 10,000 streams, so a pass takes ~19 firings: about five days on
	// the six-hourly default. Raising PruneStreamBatchSize shortens that, and the
	// cost is a longer scan inside one consensus transaction -- measure with
	// internal/benchmark/digest before doing it on mainnet.
	PruneDeleteCap       = 100_000
	PruneStreamBatchSize = 100
	PruneDrainMaxRuns    = 100

	// PruneDrainRunDelay paces the runs that actually delete, the way digest's
	// DrainRunDelay paces its own capped deletes.
	PruneDrainRunDelay = 60 * time.Second
	// PruneIdleRunDelay paces the runs that delete nothing. Once the backlog is
	// gone every run is one of those -- the sweep still visits every stream on its
	// cycle -- and a full delay would spend 100 minutes of wall clock a firing
	// moving a cursor. Same value as the inter-run delay the trims use.
	PruneIdleRunDelay                = 5 * time.Second
	PruneDrainMaxConsecutiveFailures = 5
)
