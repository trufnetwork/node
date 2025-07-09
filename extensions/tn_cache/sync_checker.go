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

// updateStatus queries current sync status from health endpoint
func (sc *SyncChecker) updateStatus(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	
	req, err := http.NewRequestWithContext(ctx, "GET", sc.endpoint, nil)
	if err != nil {
		sc.logger.Debug("failed to create health request", "error", err)
		return
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		sc.logger.Debug("health check failed", "error", err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		sc.logger.Debug("health endpoint error", "status", resp.StatusCode)
		return
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
		sc.logger.Debug("failed to decode health response", "error", err)
		return
	}
	
	// Update atomic fields
	sc.isSyncing.Store(health.Services.User.Syncing)
	// Convert milliseconds to seconds
	blockTimeSeconds := health.Services.User.BlockTime / 1000
	sc.blockTime.Store(blockTimeSeconds)
	sc.lastChecked.Store(time.Now().Unix())
	
	if health.Services.User.Syncing {
		sc.logger.Debug("node is syncing")
	} else if sc.maxBlockAge > 0 {
		age := time.Now().Unix() - blockTimeSeconds
		sc.logger.Debug("sync status", "block_age", age, "max_age", sc.maxBlockAge)
	}
}


