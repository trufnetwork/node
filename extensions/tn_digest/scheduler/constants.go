package scheduler

import "time"

const (
	// Digest drain mode constants (scheduler-scoped to avoid import cycles)
	//
	// DigestDeleteCap bounds a single auto_digest transaction, and the bound that
	// matters is not the row count on its own. kwild issues PREPARE TRANSACTION and
	// then waits for Postgres logical replication to decode that block's whole
	// change set and hand back a commit ID. That wait is a hardcoded 30 seconds
	// (kwil-db node/pg/db.go). Exceeding it is fatal: the node exits, and because
	// the block never commits, the same work is retried on the next firing and
	// fails again. Testnet sat in exactly that loop for five consecutive digest
	// firings, restarting each time and draining nothing.
	//
	// auto_digest turns this cap into a day count as
	// floor((delete_cap * 3) / (expected_records_per_stream * 2)), so 100,000 asked
	// for 6,250 days of history in one transaction. Runs that did commit were
	// changing roughly 43,000 rows; the ones that killed the node were larger. At
	// 10,000 a run covers 625 days and changes at most about 11,000 rows, which is
	// comfortably inside the window, and it matches the default auto_digest itself
	// declares. A backlog simply takes more runs, which DrainMaxRuns already allows
	// for: 100 runs a firing still clears 62,500 days.
	DigestDeleteCap                = 10_000
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
	// Same 30-second replication window as DigestDeleteCap above, but this cap is in
	// different units and cannot simply copy digest's number. batch_prune_duplicates
	// bounds EVENT TIMES, not rows (057 says why: rule 5 makes an event time atomic
	// and primitive_events has no primary key to address a single row by), and one
	// event time expands into the change set twice over:
	//
	//   - every revision at it, since the whole event time goes together
	//   - every marker in that event time's whole DAY, because a digested day that
	//     kept some markers and lost others reads as corruption, so step 3 clears
	//     the day rather than the one marker
	//
	// Measured on mainnet (2% page samples, 2026-09-06): rows per event time average
	// 1.0001 and top out at 2, with 84 of 1,486,639 sampled event times carrying any
	// revision at all. Markers per stream-day average 1.0904 and top out at 4, which
	// is also the structural ceiling since digest writes at most open/high/low/close.
	//
	// So a cap of C changes at most 2C rows plus 4C markers, 6C in the worst case and
	// about 2.1C typically. Duplicate-heavy streams are daily publishers whose days
	// hold one marker each, so they sit at the low end, but the cap has to hold for
	// the intraday streams too. At 5,000 that is 30,000 changes worst case and around
	// 10,500 in practice, which matches what DigestDeleteCap allows. 10,000 would
	// have reached 60,000, past the ~43,000 digest was still committing at before it
	// stopped fitting the window.
	//
	// batch_prune_duplicates returns deleted_rows next to deleted_event_times, so the
	// real ratio is observable per run. Raise this from that measurement, not from a
	// guess.
	PruneDeleteCap       = 5_000
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
