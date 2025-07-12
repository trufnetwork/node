package syncschecker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

func TestSyncChecker_CanExecute(t *testing.T) {
	tests := []struct {
		name        string
		maxBlockAge int64
		health      map[string]any
		wantOK      bool
		wantReason  string
	}{
		{
			name:        "zero gets default value",
			maxBlockAge: 0, // Should use DefaultMaxBlockAge
			health:      nil,
			wantOK:      true,
			wantReason:  "",
		},
		{
			name:        "negative disables checking",
			maxBlockAge: -1,
			health:      nil, // server won't be called
			wantOK:      true,
			wantReason:  "",
		},
		{
			name:        "syncing blocks execution",
			maxBlockAge: 3600,
			health: map[string]any{
				"services": map[string]any{
					"user": map[string]any{
						"syncing":    true,
						"block_time": time.Now().Unix() * 1000, // Convert to milliseconds
					},
				},
			},
			wantOK:     false,
			wantReason: "node is syncing",
		},
		{
			name:        "old block blocks execution",
			maxBlockAge: 3600,
			health: map[string]any{
				"services": map[string]any{
					"user": map[string]any{
						"syncing":    false,
						"block_time": (time.Now().Unix() - 7200) * 1000, // 2 hours old, in milliseconds
					},
				},
			},
			wantOK:     false,
			wantReason: "block too old",
		},
		{
			name:        "recent block allows execution",
			maxBlockAge: 3600,
			health: map[string]any{
				"services": map[string]any{
					"user": map[string]any{
						"syncing":    false,
						"block_time": (time.Now().Unix() - 1800) * 1000, // 30 minutes old, in milliseconds
					},
				},
			},
			wantOK:     true,
			wantReason: "",
		},
		{
			name:        "network halt scenario - synced but old block",
			maxBlockAge: 3600,
			health: map[string]any{
				"services": map[string]any{
					"user": map[string]any{
						"syncing":    false,                             // Not actively syncing
						"block_time": (time.Now().Unix() - 7200) * 1000, // Old block during halt, in milliseconds
					},
				},
			},
			wantOK:     false, // Still blocks because block is too old
			wantReason: "block too old",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test server if health data provided
			var server *httptest.Server
			if tt.health != nil {
				server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					json.NewEncoder(w).Encode(tt.health)
				}))
				defer server.Close()
			}

			// Create sync checker
			logger := log.DiscardLogger
			sc := NewSyncChecker(logger, tt.maxBlockAge)

			// Verify default value handling
			if tt.maxBlockAge == 0 {
				assert.Equal(t, int64(DefaultMaxBlockAge), sc.maxBlockAge, "Zero should use default")
			} else {
				assert.Equal(t, tt.maxBlockAge, sc.maxBlockAge, "Custom value should be preserved")
			}

			// If server exists, update endpoint and populate state
			if server != nil {
				sc.endpoint = server.URL
				ctx := context.Background()
				sc.updateStatus(ctx)
				// Give it time to process
				time.Sleep(10 * time.Millisecond)
			}

			// Test CanExecute
			gotOK, gotReason := sc.CanExecute()
			assert.Equal(t, tt.wantOK, gotOK)
			if tt.wantReason != "" {
				assert.Contains(t, gotReason, tt.wantReason)
			}
		})
	}
}

// TestSyncChecker_DefaultMaxBlockAge merged into TestSyncChecker_CanExecute

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
