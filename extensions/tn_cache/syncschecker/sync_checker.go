package syncschecker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
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
	maxBlockAge int64  // in seconds, 0 = disabled
	endpoint    string // health endpoint URL

	status struct {
		mu          sync.Mutex
		isSyncing   bool
		blockTime   int64
		lastChecked int64
	}

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

	if sc.maxBlockAge <= 0 {
		sc.logger.Info("sync checking disabled - skipping monitoring")
		return
	}

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
	if sc.maxBlockAge <= 0 {
		return true, ""
	}

	sc.status.mu.Lock()
	defer sc.status.mu.Unlock()

	// Check if actively syncing
	if sc.status.isSyncing {
		return false, "node is syncing"
	}

	// Check block age
	blockTime := sc.status.blockTime
	if blockTime == 0 {
		return false, "no block time available"
	}

	currentTime := time.Now().Unix()
	blockAge := currentTime - blockTime
	if blockAge > sc.maxBlockAge {
		return false, fmt.Sprintf("block age %d seconds exceeds max %d", blockAge, sc.maxBlockAge)
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
	sc.status.mu.Lock()
	defer sc.status.mu.Unlock()

	client := retryablehttp.NewClient()
	client.RetryMax = 3
	client.RetryWaitMin = 1 * time.Second
	client.RetryWaitMax = 5 * time.Second

	req, err := retryablehttp.NewRequest("GET", sc.endpoint, nil)
	if err != nil {
		sc.logger.Error("failed to create health request", "error", err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		sc.logger.Error("failed to fetch health status", "error", err)
		return
	}
	defer resp.Body.Close()

	var health struct {
		Services struct {
			User struct {
				Syncing   bool `json:"syncing"`
				BlockTime int  `json:"block_time"`
			} `json:"user"`
		} `json:"services"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		sc.logger.Error("failed to parse health response", "error", err)
		return
	}

	sc.status.isSyncing = health.Services.User.Syncing
	sc.status.blockTime = int64(health.Services.User.BlockTime / 1000) // Convert ms to s
	sc.status.lastChecked = time.Now().Unix()
}
