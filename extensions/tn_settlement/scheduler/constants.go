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
