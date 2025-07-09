package tn_cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
)

const (
	// DefaultMaxBlockAge is 1 hour
	DefaultMaxBlockAge = 3600
	// Health check interval
	healthCheckInterval = 30 * time.Second
	// Health endpoint
	healthEndpoint = "http://localhost:8484/api/v1/health"
)

// SyncChecker monitors if the node is synced enough to perform cache operations
type SyncChecker struct {
	logger      log.Logger
	maxBlockAge int64 // in seconds, 0 = disabled
	endpoint    string // health endpoint URL
	
	// Atomic fields for lock-free reads
	isSyncing   atomic.Bool
	blockTime   atomic.Int64
	lastChecked atomic.Int64
	
	cancel context.CancelFunc
}

// NewSyncChecker creates a new sync checker
func NewSyncChecker(logger log.Logger, maxBlockAge int64) *SyncChecker {
	if maxBlockAge == 0 {
		maxBlockAge = DefaultMaxBlockAge
	} else if maxBlockAge < -1 {
		logger.Warn("invalid max_block_age value, using default", "provided", maxBlockAge, "default", DefaultMaxBlockAge)
		maxBlockAge = DefaultMaxBlockAge
	}
	return &SyncChecker{
		logger:      logger.New("sync_checker"),
		maxBlockAge: maxBlockAge,
		endpoint:    healthEndpoint,
	}
}

// Start begins monitoring sync status
func (sc *SyncChecker) Start(ctx context.Context) {
	sc.logger.Info("starting sync checker", "max_block_age", sc.maxBlockAge)
	
	ctx, cancel := context.WithCancel(ctx)
	sc.cancel = cancel
	
	// Initial check
	sc.updateStatus(ctx)
	
	// Monitor in background
	go sc.monitor(ctx)
}

// Stop halts sync monitoring
func (sc *SyncChecker) Stop() {
	if sc.cancel != nil {
		sc.cancel()
	}
}

// CanExecute checks if cache operations should proceed
// Returns false if:
// - Node is actively syncing
// - Block age exceeds maxBlockAge (even during network halts)
// This ensures cache freshness even when the network stops producing blocks
func (sc *SyncChecker) CanExecute() (bool, string) {
	// Always allow if sync checking is disabled
	if sc.maxBlockAge <= 0 {
		return true, ""
	}
	
	// Check if actively syncing
	if sc.isSyncing.Load() {
		return false, "node is syncing"
	}
	
	// Check block age
	blockTime := sc.blockTime.Load()
	if blockTime == 0 {
		return false, "no block time available"
	}
	
	blockAge := time.Now().Unix() - blockTime
	if blockAge > sc.maxBlockAge {
		return false, fmt.Sprintf("block too old (%ds)", blockAge)
	}
	
	return true, ""
}

// monitor runs the background monitoring loop
func (sc *SyncChecker) monitor(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sc.updateStatus(ctx)
		}
	}
}

// updateStatus queries current sync status from health endpoint with retry logic
func (sc *SyncChecker) updateStatus(ctx context.Context) {
	// Retry configuration
	const maxRetries = 3
	initialBackoff := 100 * time.Millisecond
	
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms
			backoff := initialBackoff * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		
		err := sc.doUpdateStatus(ctx)
		if err == nil {
			// Success
			return
		}
		
		lastErr = err
		// Log retry attempts
		if attempt < maxRetries-1 {
			sc.logger.Debug("retrying health check", "attempt", attempt+1, "error", err)
		}
	}
	
	// All retries failed
	sc.logger.Debug("health check failed after retries", "error", lastErr)
}

// doUpdateStatus performs the actual status update request
func (sc *SyncChecker) doUpdateStatus(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	
	req, err := http.NewRequestWithContext(ctx, "GET", sc.endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create health request: %w", err)
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health endpoint error: status %d", resp.StatusCode)
	}
	
	var health struct {
		Services struct {
			User struct {
				Syncing   bool  `json:"syncing"`
				BlockTime int64 `json:"block_time"`
			} `json:"user"`
		} `json:"services"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("failed to decode health response: %w", err)
	}
	
	// Update atomic fields
	sc.isSyncing.Store(health.Services.User.Syncing)
	// Convert milliseconds to seconds
	blockTimeSeconds := health.Services.User.BlockTime / 1000
	
	// Validate block time
	currentTime := time.Now().Unix()
	if blockTimeSeconds < 0 {
		sc.logger.Warn("invalid negative block time", "block_time_ms", health.Services.User.BlockTime)
		return fmt.Errorf("invalid negative block time: %d ms", health.Services.User.BlockTime)
	}
	// Allow up to 5 minutes in the future to account for clock drift
	if blockTimeSeconds > currentTime+300 {
		sc.logger.Warn("block time too far in future", "block_time", blockTimeSeconds, "current_time", currentTime)
		return fmt.Errorf("block time too far in future: %d", blockTimeSeconds)
	}
	
	sc.blockTime.Store(blockTimeSeconds)
	sc.lastChecked.Store(currentTime)
	
	if health.Services.User.Syncing {
		sc.logger.Debug("node is syncing")
	} else if sc.maxBlockAge > 0 {
		age := time.Now().Unix() - blockTimeSeconds
		sc.logger.Debug("sync status", "block_age", age, "max_age", sc.maxBlockAge)
	}
	
	return nil
}


