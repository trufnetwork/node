package tn_vacuum

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

func TestCountRepackedTables(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   int
	}{
		{
			name: "no eligible tables",
			output: "INFO: database \"kwild_test_db\" skipped: pg_repack 1.5.3 is not installed in the database\n" +
				"INFO: database \"postgres\" skipped: pg_repack 1.5.3 is not installed in the database",
			want: 0,
		},
		{
			name:   "single table",
			output: "INFO: repacking table \"main\".\"primitive_events\"",
			want:   1,
		},
		{
			name: "multiple tables",
			output: "INFO: repacking table \"main\".\"primitive_events\"\n" +
				"INFO: repacking table \"main\".\"streams\"\n" +
				"INFO: repacking table \"main\".\"taxonomies\"",
			want: 3,
		},
		{
			name:   "empty output",
			output: "",
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countRepackedTables(tt.output); got != tt.want {
				t.Fatalf("countRepackedTables() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDetectPgRepackSoftFailure(t *testing.T) {
	tests := []struct {
		name    string
		stderr  string
		expects bool
	}{
		{
			name:    "version mismatch",
			stderr:  "INFO: database \"kwild\" skipped: program 'pg_repack 1.5.0' does not match database library 'pg_repack 1.5.2'",
			expects: true,
		},
		{
			name:    "extension missing",
			stderr:  "INFO: database \"kwild\" skipped: pg_repack 1.5.0 is not installed in the database",
			expects: false,
		},
		{
			name:    "no issues",
			stderr:  "INFO: repacking database \"kwild\"\nINFO: repacking table \"public\".\"foo\"",
			expects: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := detectPgRepackSoftFailure(tt.stderr)
			if tt.expects && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expects && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

// fakePgRepack writes a stand-in for the pg_repack binary. It records that it
// started, then waits; on SIGINT it records that too, the way pg_repack drops
// its trigger and log table when interrupted.
func fakePgRepack(t *testing.T) (binary, started, interrupted string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "pg_repack")
	started = filepath.Join(dir, "started")
	interrupted = filepath.Join(dir, "interrupted")
	script := fmt.Sprintf("#!/bin/sh\ntrap 'touch %s; exit 1' INT\ntouch %s\nwhile :; do sleep 0.05; done\n", interrupted, started)
	require.NoError(t, os.WriteFile(binary, []byte(script), 0o755))
	return binary, started, interrupted
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestRunInterruptsPgRepackOnCancel(t *testing.T) {
	binary, started, interrupted := fakePgRepack(t)
	m := &pgRepackMechanism{
		logger:         log.DiscardLogger,
		binaryPath:     binary,
		clearLeftovers: func(context.Context, DBConnConfig, log.Logger) error { return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Run(ctx, RunRequest{DB: DBConnConfig{Database: "kwild"}})
		done <- err
	}()
	require.Eventually(t, func() bool { return fileExists(started) }, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
	require.True(t, fileExists(interrupted), "a cancelled pg_repack must get SIGINT, the signal it cleans up on")
}

func TestRunSkipsPgRepackWhenLeftoversCannotBeCleared(t *testing.T) {
	binary, started, _ := fakePgRepack(t)
	clearErr := errors.New("lock timeout")
	m := &pgRepackMechanism{
		logger:         log.DiscardLogger,
		binaryPath:     binary,
		clearLeftovers: func(context.Context, DBConnConfig, log.Logger) error { return clearErr },
	}

	report, err := m.Run(context.Background(), RunRequest{DB: DBConnConfig{Database: "kwild"}})
	require.ErrorIs(t, err, clearErr)
	require.Equal(t, StatusFailed, report.Status)
	require.False(t, fileExists(started), "pg_repack must not run on top of an interrupted run's trigger and log tables")
}
