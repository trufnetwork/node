package scheduler

import "time"

// SettlementPollFrequency is the default interval for checking unsettled markets
const SettlementPollFrequency = 5 * time.Minute

// MaxMarketsPerRun limits the number of markets processed in a single scheduler run
const MaxMarketsPerRun = 10

// MaxRetryAttempts is the default number of retry attempts per market
const MaxRetryAttempts = 3

// RetryBackoffInitial is the initial backoff duration for retries
const RetryBackoffInitial = 2 * time.Second

// RetryBackoffMax is the maximum backoff duration for retries
const RetryBackoffMax = 30 * time.Second

// PermanentFailureReprobeCooldown is how long a market whose settlement failed
// with a permanent (deterministic) error is quarantined before the scheduler
// re-probes it once. A permanent failure — e.g. a malformed, immutable
// attestation — recurs identically on every attempt, so re-broadcasting it each
// poll only burns nonces and commits failed txs to blocks. Quarantining bounds
// that to at most one failed tx per cooldown per stuck market, while still
// auto-recovering a market an operator repairs (re-attestation): after the
// cooldown, one probe is allowed through, and a success clears the quarantine.
const PermanentFailureReprobeCooldown = 1 * time.Hour

// CaptureFullPollWindow is how long past settle_time an uncaptured market is
// requested on every poll. A capture whose block timestamp lands just under
// settle_time does not count, so this window must keep the full poll rate.
const CaptureFullPollWindow = 1 * time.Hour

// CaptureBackoffSlot is the request rate for a market still uncaptured after
// CaptureFullPollWindow: at most one request_attestation per slot. Slots are
// counted from settle_time, not from the last request, so every book of a
// ladder (they share a settle_time) becomes due on the same poll. A v2 market
// whose print is late reverts its request until the print lands or the
// deadline passes; without this it reverted every poll for up to 90 days.
const CaptureBackoffSlot = 1 * time.Hour

// CaptureBackoffPruneAfter drops a request record this long after the request.
// It only bounds memory for markets that left the due list without a capture
// being seen here: a record that old is before the current slot, so dropping
// it never changes whether a market is due.
const CaptureBackoffPruneAfter = 3 * time.Hour
