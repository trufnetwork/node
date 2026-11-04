package tn_vacuum

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
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

func TestLoadConfigReadsPgRepackRole(t *testing.T) {
	svc := &common.Service{LocalConfig: &config.Config{Extensions: map[string]map[string]string{
		ExtensionName: {
			ConfigKeyPgRepackUser:     " repacker ",
			ConfigKeyPgRepackPassword: "pass word",
		},
	}}}
	cfg, err := LoadConfig(svc)
	require.NoError(t, err)
	require.Equal(t, "repacker", cfg.PgRepackUser)
	require.Equal(t, "pass word", cfg.PgRepackPassword)

	cfg, err = LoadConfig(&common.Service{LocalConfig: &config.Config{}})
	require.NoError(t, err)
	require.Empty(t, cfg.PgRepackUser, "without the key, the role is chosen when the mechanism starts")
}

func TestPgRepackRoleFromConfig(t *testing.T) {
	ctx := context.Background()
	kwild := DBConnConfig{User: "kwild", Password: "kwild-pass", Database: "kwild"}

	role, err := pgRepackRole(ctx, MechanismDeps{DB: kwild, PgRepackUser: "repacker", PgRepackPassword: "repacker-pass"})
	require.NoError(t, err)
	require.Equal(t, &repackRole{user: "repacker", password: "repacker-pass"}, role)

	role, err = pgRepackRole(ctx, MechanismDeps{DB: kwild, PgRepackUser: "kwild"})
	require.NoError(t, err)
	require.Nil(t, role, "naming kwild's own user keeps pg_repack on kwild's connection settings")
}

// recordingPgRepack writes a stand-in for the pg_repack binary that records
// its arguments, one per line, and the password it was given.
func recordingPgRepack(t *testing.T) (binary, argsFile, passwordFile string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "pg_repack")
	argsFile = filepath.Join(dir, "args")
	passwordFile = filepath.Join(dir, "password")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %s\nprintf '%%s' \"$PGPASSWORD\" > %s\n", argsFile, passwordFile)
	require.NoError(t, os.WriteFile(binary, []byte(script), 0o755))
	return binary, argsFile, passwordFile
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func TestRunConnectsAsPgRepackRole(t *testing.T) {
	kwild := DBConnConfig{Host: "db", Port: "5432", User: "kwild", Password: "kwild-pass", Database: "kwild"}

	for _, tc := range []struct {
		name         string
		role         *repackRole
		wantUser     string
		wantPassword string
		wantSkipFlag bool
	}{
		{name: "kwild's own user", wantUser: "kwild", wantPassword: "kwild-pass"},
		{
			name:         "own role",
			role:         &repackRole{user: "tn_repack", password: "repack-pass"},
			wantUser:     "tn_repack",
			wantPassword: "repack-pass",
			wantSkipFlag: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binary, argsFile, passwordFile := recordingPgRepack(t)
			var cleared DBConnConfig
			m := &pgRepackMechanism{
				logger:     log.DiscardLogger,
				binaryPath: binary,
				role:       tc.role,
				clearLeftovers: func(_ context.Context, db DBConnConfig, _ log.Logger) error {
					cleared = db
					return nil
				},
			}

			report, err := m.Run(context.Background(), RunRequest{DB: kwild})
			require.NoError(t, err)
			require.Equal(t, StatusOK, report.Status)

			args := readLines(t, argsFile)
			require.Contains(t, args, "--username="+tc.wantUser)
			require.Contains(t, args, "--dbname=kwild")
			require.Contains(t, args, "--host=db")
			if tc.wantSkipFlag {
				require.Contains(t, args, "--no-superuser-check")
			} else {
				require.NotContains(t, args, "--no-superuser-check")
			}
			password, err := os.ReadFile(passwordFile)
			require.NoError(t, err)
			require.Equal(t, tc.wantPassword, string(password))

			require.Equal(t, tc.wantUser, cleared.User, "leftovers are cleared as the same role")
			require.Equal(t, tc.wantPassword, cleared.Password)
			require.Equal(t, "kwild", cleared.Database)
		})
	}
}

func TestBuildConnStringKeepsValuesWhole(t *testing.T) {
	for _, password := range []string{
		"pass word",
		"it's",
		`back\slash`,
		`ends in \`,
		`\'`,
		"a=b c=d",
	} {
		t.Run(password, func(t *testing.T) {
			db := DBConnConfig{Host: "db", Port: "5433", User: "re packer", Password: password, Database: "kwild"}
			cfg, err := pgx.ParseConfig(buildConnString(db))
			require.NoError(t, err)
			require.Equal(t, "db", cfg.Host)
			require.EqualValues(t, 5433, cfg.Port)
			require.Equal(t, "kwild", cfg.Database)
			require.Equal(t, "re packer", cfg.User)
			require.Equal(t, password, cfg.Password)
		})
	}

	cfg, err := pgx.ParseConfig(buildConnString(DBConnConfig{User: "kwild", Database: "kwild"}))
	require.NoError(t, err)
	require.Equal(t, DefaultPostgresHost, cfg.Host)
	require.EqualValues(t, 5432, cfg.Port)
}
