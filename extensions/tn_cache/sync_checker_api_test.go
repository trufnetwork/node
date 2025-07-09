package tn_cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

// TestSyncCheckerMillisecondsConversion verifies sync checker handles API's millisecond timestamps
func TestSyncCheckerMillisecondsConversion(t *testing.T) {
	// Real API response with millisecond timestamp
	healthResponse := `{
		"services": {
			"user": {
				"block_time": 1752082707307,
				"syncing": false
			}
		}
	}`

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
	require.Equal(t, int64(1752082707), sc.blockTime.Load())
	
	// Should allow execution with recent block
	canExecute, _ := sc.CanExecute()
	require.True(t, canExecute)
}