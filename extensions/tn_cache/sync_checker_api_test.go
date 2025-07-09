package tn_cache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

// TestSyncCheckerMillisecondsConversion verifies sync checker handles API's millisecond timestamps
func TestSyncCheckerMillisecondsConversion(t *testing.T) {
	// Use current time minus 30 minutes for a valid recent block.
	// The sync checker validates blocks against maxBlockAge (3600s = 1 hour).
	// 30 minutes (1800s) ensures we're well within the valid range while
	// providing buffer for test execution timing variations.
	currentTime := time.Now().Unix()
	recentBlockTime := (currentTime - 1800) * 1000 // 30 minutes ago in milliseconds
	
	// Real API response with millisecond timestamp
	healthResponse := fmt.Sprintf(`{
		"services": {
			"user": {
				"block_time": %d,
				"syncing": false
			}
		}
	}`, recentBlockTime)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(healthResponse))
	}))
	defer server.Close()

	sc := NewSyncChecker(log.New(log.WithWriter(nil)), 3600)
	sc.endpoint = server.URL

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	time.Sleep(100 * time.Millisecond)

	// Verify milliseconds converted to seconds
	expectedBlockTime := recentBlockTime / 1000
	require.Equal(t, expectedBlockTime, sc.blockTime.Load())
	
	// Should allow execution with recent block
	canExecute, _ := sc.CanExecute()
	require.True(t, canExecute)
}