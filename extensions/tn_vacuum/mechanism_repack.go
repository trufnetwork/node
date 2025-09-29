package tn_vacuum

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/trufnetwork/kwil-db/core/log"
)

var ErrPgRepackUnavailable = errors.New("pg_repack binary not found in PATH")

type pgRepackMechanism struct {
	logger     log.Logger
	binaryPath string
	db         DBConnConfig
}

func NewPgRepackMechanism() Mechanism {
	return &pgRepackMechanism{}
}

func (m *pgRepackMechanism) Name() string { return "pg_repack" }

func (m *pgRepackMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	m.logger = deps.Logger.New("mechanism.pg_repack")
	m.db = deps.DB
	path, err := exec.LookPath("pg_repack")
	if err != nil {
		m.logger.Warn("pg_repack binary not found; vacuum runs will fail until available", "error", err)
		return ErrPgRepackUnavailable
	}
	m.binaryPath = path
	m.logger.Info("pg_repack binary detected", "path", path)
	if err := ensurePgRepackExtension(ctx, deps.DB, m.logger); err != nil {
		return fmt.Errorf("ensure pg_repack extension: %w", err)
	}
	return nil
}

func (m *pgRepackMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	startTime := time.Now()
	report := &RunReport{
		Mechanism: m.Name(),
		Status:    StatusOK,
	}

	if m.binaryPath == "" {
		return nil, fmt.Errorf("pg_repack unavailable: %w", ErrPgRepackUnavailable)
	}
	db := req.DB
	if db.Database == "" {
		db = m.db
	}
	if db.Database == "" {
		return nil, fmt.Errorf("pg_repack requires database name")
	}

	args := []string{fmt.Sprintf("--dbname=%s", db.Database), "--all"}
	if db.Host != "" {
		args = append(args, fmt.Sprintf("--host=%s", db.Host))
	}
	if db.Port != "" {
		args = append(args, fmt.Sprintf("--port=%s", db.Port))
	}
	if db.User != "" {
		args = append(args, fmt.Sprintf("--username=%s", db.User))
	}

	cmd := exec.CommandContext(ctx, m.binaryPath, args...)
	env := os.Environ()
	if db.Password != "" {
		env = append(env, fmt.Sprintf("PGPASSWORD=%s", db.Password))
	}
	cmd.Env = env

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	m.logger.Info("pg_repack starting", "args", args)
	if err := cmd.Run(); err != nil {
		report.Duration = time.Since(startTime)
		report.Status = StatusFailed
		report.Error = err.Error()
		m.logger.Warn("pg_repack failed", "error", err, "stderr", stderr.String(), "duration", report.Duration)
		return report, fmt.Errorf("pg_repack execution failed: %w", err)
	}

	report.Duration = time.Since(startTime)
	// Parse stdout to count tables if possible (pg_repack outputs "INFO: repacking table...")
	tablesProcessed := strings.Count(stdout.String(), "INFO: repacking table")
	report.TablesProcessed = tablesProcessed

	m.logger.Info("pg_repack completed", "stdout", stdout.String(), "duration", report.Duration, "tables", tablesProcessed)
	return report, nil
}

func (m *pgRepackMechanism) Close(ctx context.Context) error {
	if m.logger != nil {
		m.logger.Info("pg_repack mechanism closed")
	}
	return nil
}

func ensurePgRepackExtension(ctx context.Context, db DBConnConfig, logger log.Logger) error {
	if db.Database == "" {
		return fmt.Errorf("missing database name for pg_repack extension setup")
	}
	connStr := buildConnString(db)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		logger.Warn("failed to connect to database for pg_repack extension", "error", err)
		return fmt.Errorf("pg_repack extension connection: %w", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_repack"); err != nil {
		logger.Warn("failed to create pg_repack extension", "error", err)
		return fmt.Errorf("create pg_repack extension: %w", err)
	}
	logger.Info("pg_repack extension ensured")
	return nil
}

func buildConnString(db DBConnConfig) string {
	host := db.Host
	if host == "" {
		host = DefaultPostgresHost
	}
	port := db.Port
	if port == "" {
		port = DefaultPostgresPort
	}
	parts := []string{
		fmt.Sprintf("host=%s", host),
		fmt.Sprintf("port=%s", port),
		fmt.Sprintf("dbname=%s", db.Database),
		DefaultSSLMode,
	}
	if db.User != "" {
		parts = append(parts, fmt.Sprintf("user=%s", db.User))
	}
	if db.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", db.Password))
	}
	return strings.Join(parts, " ")
}
