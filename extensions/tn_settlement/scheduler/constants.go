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
