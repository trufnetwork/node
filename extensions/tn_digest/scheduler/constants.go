package scheduler

import "time"

const (
	// Digest drain mode constants (scheduler-scoped to avoid import cycles)
	//
	// DigestDeleteCap bounds a single auto_digest transaction. auto_digest turns it
	// into a day count as floor((delete_cap * 3) / (expected_records_per_stream * 2)),
	// so 10,000 covers 625 days a run and matches the default the action itself
	// declares. A backlog takes more runs, which DrainMaxRuns already allows for.
	//
	// An earlier revision of this comment blamed the testnet crash loop of 2026-09
	// on this cap being too large. That was wrong and is corrected here: the loop was
	// log volume. Testnet ran at debug level on the awslogs driver, and the
	// replication monitor emits one line per decoded row, shipped synchronously over
	// the network -- roughly 2,300 lines a second. That blew the hardcoded 30 second
	// precommit window in kwil-db node/pg/db.go. Setting the log level to info fixed
	// it outright: 40 hours and six firings on the OLD 100,000 cap with no timeouts.
	// The cap was reduced anyway and is kept here, because 625 days a run is a
	// reasonable size on its own, but it was not the fix and should not be cited as
	// one.
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
	// rather than stopping early.
	//
	// These numbers come from what the first mainnet firing actually did, on
	// 2026-09-08. At PruneStreamBatchSize 100 a single auto_prune_duplicates
	// transaction held the consensus path for a median of 16.9 seconds (22 samples,
	// max 18.6 s) against neighbouring blocks at 50 ms. The gateway's read timeout
	// is 20 s, so user reads queued behind it and 1,619 of them returned 503 with
	// rpc_code -32001 across a 19 minute window.
	//
	// The lesson is which knob matters. Run 1 deleted 794 event times in 14.4 s;
	// later runs deleted essentially nothing and still cost 16-18 s. The work is the
	// per-stream history scan across stream_batch_size streams, and PruneDeleteCap
	// bounds deletions only -- it cannot bound the scan, so lowering it does nothing
	// for block time. Size PruneStreamBatchSize against measured block time.
	//
	// A whole pass costs what it costs: ~182,000 primitive streams at 17 s per 100
	// streams is about 8.6 hours of execution however it is sliced. Batching only
	// decides how that is spread, so the shape is many cheap transactions at a low
	// duty cycle rather than a few expensive ones.
	//
	// At 5 streams a run a transaction lands near 0.85 s, which is an ordinary block
	// rather than 85% of the read budget. One run per 10 s is an ~8% duty cycle, and
	// 1,000 runs covers 5,000 streams in about 2.8 hours -- inside the six-hourly
	// firing, with a full pass in roughly 37 firings, about nine days.
	//
	// What this does NOT fix: the per-stream scan is unbounded, because the deletable
	// set is a whole-stream property. One stream holding 1.3 M rows costs the same in
	// a batch of 5 as in a batch of 100. That tail needs a design change -- a cursor
	// within a stream, or a maintained duplicate index -- not a smaller constant.
	// Re-measure with internal/benchmark/digest on a mainnet-shaped fixture before
	// raising any of these; testnet is 212k rows at 1.8% duplicates and never reaches
	// this path.
	PruneDeleteCap       = 1_000
	PruneStreamBatchSize = 5
	PruneDrainMaxRuns    = 1_000

	// PruneDrainRunDelay paces the runs that actually delete. Ten seconds against a
	// ~0.85 s transaction is the duty cycle described above; the old 60 s was chosen
	// when a run was rare and expensive rather than frequent and cheap.
	PruneDrainRunDelay = 10 * time.Second
	// PruneIdleRunDelay paces the runs that delete nothing. Once the backlog is
	// gone every run is one of those -- the sweep still visits every stream on its
	// cycle -- so this stays short.
	PruneIdleRunDelay                = 5 * time.Second
	PruneDrainMaxConsecutiveFailures = 5
)
