package syncschecker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
						"syncing":    false, // Not actively syncing
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

func TestSyncChecker_DefaultMaxBlockAge(t *testing.T) {
	logger := log.DiscardLogger
	
	// Test that 0 gets replaced with default
	sc := NewSyncChecker(logger, 0)
	assert.Equal(t, int64(DefaultMaxBlockAge), sc.maxBlockAge)
	
	// Test that custom value is preserved
	sc = NewSyncChecker(logger, 1800)
	assert.Equal(t, int64(1800), sc.maxBlockAge)
}