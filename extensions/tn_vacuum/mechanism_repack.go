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

// pgRepackStopGrace is how long a cancelled pg_repack gets to drop its trigger
// and log table before it is killed. pg_repack cleans up on SIGINT only; a
// SIGKILL leaves both behind.
const pgRepackStopGrace = 30 * time.Second

// leftoverLockTimeout bounds how long clearing an interrupted run may wait for a
// table lock, since block execution queues behind that wait.
const leftoverLockTimeout = "5s"

type pgRepackMechanism struct {
	logger     log.Logger
	binaryPath string
	db         DBConnConfig
	// role is set when pg_repack connects as its own role instead of kwild's
	// database user. That role need not be a superuser, so pg_repack skips its
	// own superuser check, and an interrupted run is cleared one object at a
	// time, which needs no superuser either, instead of by creating the
	// extension again.
	role *repackRole
	// clearLeftovers runs before every pg_repack; tests replace it.
	clearLeftovers func(ctx context.Context, db DBConnConfig, logger log.Logger) error
}

// repackRole is the role pg_repack connects as in place of kwild's own user.
type repackRole struct {
	user     string
	password string
}

func NewPgRepackMechanism() Mechanism {
	return &pgRepackMechanism{clearLeftovers: clearInterruptedRepack}
}

func (m *pgRepackMechanism) Name() string { return "pg_repack" }

func (m *pgRepackMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	m.logger = deps.Logger.New("mechanism.pg_repack")
	m.db = deps.DB
	path, err := exec.LookPath("pg_repack")
	if err != nil {
		m.logger.Error("pg_repack binary not found; extension cannot start", "error", err)
		return ErrPgRepackUnavailable
	}
	m.binaryPath = path
	m.logger.Info("pg_repack binary detected", "path", path)
	role, err := pgRepackRole(ctx, deps)
	if err != nil {
		return fmt.Errorf("choose the pg_repack role: %w", err)
	}
	m.role = role
	if role != nil {
		m.clearLeftovers = clearInterruptedRepackAsOwner
		m.logger.Info("pg_repack runs as its own role", "user", role.user)
	}
	if err := ensurePgRepackExtension(ctx, m.conn(deps.DB), m.logger); err != nil {
		if role != nil {
			return fmt.Errorf("ensure pg_repack extension as %s (a superuser creates it with the node's postgres-roles.sql): %w", role.user, err)
		}
		return fmt.Errorf("ensure pg_repack extension: %w", err)
	}
	return nil
}

// conn returns db with the user and password pg_repack connects with.
func (m *pgRepackMechanism) conn(db DBConnConfig) DBConnConfig {
	if m.role != nil {
		db.User = m.role.user
		db.Password = m.role.password
	}
	return db
}

// pgRepackRole returns the role pg_repack connects as, or nil when it uses
// kwild's own database user. A configured pg_repack_user is used as given.
// Otherwise pg_repack keeps kwild's user while that user is a superuser, as it
// always has, and uses DefaultPgRepackUser once it is not: a database set up
// with postgres-roles.sql reserves the pg_repack extension to that role.
func pgRepackRole(ctx context.Context, deps MechanismDeps) (*repackRole, error) {
	user := deps.PgRepackUser
	if user == "" {
		super, err := isSuperuser(ctx, deps.DB)
		if err != nil {
			return nil, err
		}
		if super {
			return nil, nil
		}
		user = DefaultPgRepackUser
	}
	if user == deps.DB.User {
		return nil, nil
	}
	return &repackRole{user: user, password: deps.PgRepackPassword}, nil
}

// isSuperuser reports whether db's user is a Postgres superuser.
func isSuperuser(ctx context.Context, db DBConnConfig) (bool, error) {
	conn, err := pgx.Connect(ctx, buildConnString(db))
	if err != nil {
		return false, fmt.Errorf("connect to read the database role: %w", err)
	}
	defer conn.Close(ctx)

	var super bool
	if err := conn.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil {
		return false, fmt.Errorf("read whether the database role is a superuser: %w", err)
	}
	return super, nil
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
	db = m.conn(db)

	if m.clearLeftovers != nil {
		if err := m.clearLeftovers(ctx, db, m.logger); err != nil {
			report.Duration = time.Since(startTime)
			report.Status = StatusFailed
			report.Error = err.Error()
			m.logger.Warn("pg_repack skipped: could not clear an interrupted run", "error", err)
			return report, err
		}
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
	if m.role != nil {
		// pg_repack stops for a role that is not a superuser unless told to
		// skip its check; the role's grants cover what pg_repack does.
		args = append(args, "--no-superuser-check")
	}

	if req.PgRepackJobs > 0 {
		args = append(args, fmt.Sprintf("--jobs=%d", req.PgRepackJobs))
	}
	// Always skip reordering to minimize swap time; logical data remains unchanged.
	args = append(args, "--no-order")

	cmd := exec.CommandContext(ctx, m.binaryPath, args...)
	// On cancellation send SIGINT, which pg_repack handles by dropping its trigger
	// and log table, and kill it only if it has not exited after the grace period.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = pgRepackStopGrace
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
	output := stdout.String() + stderr.String()
	if err := detectPgRepackSoftFailure(stderr.String()); err != nil {
		report.Status = StatusFailed
		report.Error = err.Error()
		m.logger.Warn("pg_repack reported incompatibility", "stderr", stderr.String(), "duration", report.Duration)
		return report, err
	}
	tablesProcessed := countRepackedTables(output)
	report.TablesProcessed = tablesProcessed

	if tablesProcessed == 0 {
		m.logger.Info("pg_repack completed with no eligible tables", "duration", report.Duration)
		return report, nil
	}

	m.logger.Info("pg_repack completed", "stdout", stdout.String(), "stderr", stderr.String(), "duration", report.Duration, "tables", tablesProcessed)
	return report, nil
}

func countRepackedTables(output string) int {
	return strings.Count(output, "INFO: repacking table")
}

func detectPgRepackSoftFailure(stderr string) error {
	lowered := strings.ToLower(stderr)
	switch {
	case strings.Contains(lowered, "does not match database library"):
		return fmt.Errorf("pg_repack version mismatch: %s", summarizePgRepackError(stderr))
	default:
		return nil
	}
}

func summarizePgRepackError(stderr string) string {
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	if len(lines) == 0 {
		return ""
	}
	return strings.TrimSpace(lines[len(lines)-1])
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

// leftoverCountSQL counts what a pg_repack run leaves when it is killed before it
// can clean up: repack_trigger on a source table, and the tables it creates in the
// repack schema. A fresh pg_repack extension owns no tables there.
const leftoverCountSQL = `SELECT
	(SELECT count(*) FROM pg_trigger WHERE tgname = 'repack_trigger' AND NOT tgisinternal),
	(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	  WHERE n.nspname = 'repack' AND c.relkind = 'r')`

// clearInterruptedRepack removes the trigger and log tables an interrupted
// pg_repack run leaves behind. The trigger copies every write to its table into a
// repack.log_<oid> table until someone drops it, and a later --all run repacks
// those log tables as well, so the triggers chain and one write becomes many.
// Dropping the extension with CASCADE removes all of it, and the extension is
// created again in the same transaction. Nothing in the repack schema is
// consensus state.
func clearInterruptedRepack(ctx context.Context, db DBConnConfig, logger log.Logger) error {
	conn, err := pgx.Connect(ctx, buildConnString(db))
	if err != nil {
		return fmt.Errorf("connect to clear pg_repack leftovers: %w", err)
	}
	defer conn.Close(ctx)

	var triggers, tables int64
	if err := conn.QueryRow(ctx, leftoverCountSQL).Scan(&triggers, &tables); err != nil {
		return fmt.Errorf("count pg_repack leftovers: %w", err)
	}
	if triggers == 0 && tables == 0 {
		return nil
	}
	logger.Warn("clearing what an interrupted pg_repack run left behind", "triggers", triggers, "tables", tables)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin clearing pg_repack leftovers: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, stmt := range []string{
		"SET LOCAL lock_timeout = '" + leftoverLockTimeout + "'",
		"DROP EXTENSION IF EXISTS pg_repack CASCADE",
		"CREATE EXTENSION pg_repack",
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clear pg_repack leftovers: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit clearing pg_repack leftovers: %w", err)
	}
	return nil
}

// leftoverDropsSQL lists, in the order to run them, the statements that clear
// what an interrupted pg_repack run left behind without dropping the
// extension: repack_trigger on each table outside the repack schema, then the
// tables in the repack schema, then the key types pg_repack created there.
// The extension creates no tables or standalone types of its own.
const leftoverDropsSQL = `SELECT stmt FROM (
	SELECT 1 AS step, format('DROP TRIGGER repack_trigger ON %I.%I', n.nspname, c.relname) AS stmt
	FROM pg_trigger t
	JOIN pg_class c ON c.oid = t.tgrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE t.tgname = 'repack_trigger' AND NOT t.tgisinternal AND n.nspname <> 'repack'
	UNION ALL
	SELECT 2, format('DROP TABLE IF EXISTS repack.%I', c.relname)
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = 'repack' AND c.relkind = 'r'
	UNION ALL
	SELECT 3, format('DROP TYPE IF EXISTS repack.%I', c.relname)
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = 'repack' AND c.relkind = 'c'
) leftovers ORDER BY step, stmt`

// clearInterruptedRepackAsOwner clears an interrupted run for a pg_repack role
// that is not a superuser, which may not drop and create the extension. The
// role owns what its own runs create in the repack schema, and it may drop
// the trigger from kwild's tables as a member of their owner.
func clearInterruptedRepackAsOwner(ctx context.Context, db DBConnConfig, logger log.Logger) error {
	conn, err := pgx.Connect(ctx, buildConnString(db))
	if err != nil {
		return fmt.Errorf("connect to clear pg_repack leftovers: %w", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin clearing pg_repack leftovers: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, _ := tx.Query(ctx, leftoverDropsSQL)
	stmts, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return fmt.Errorf("list pg_repack leftovers: %w", err)
	}
	if len(stmts) == 0 {
		return nil
	}
	logger.Warn("clearing what an interrupted pg_repack run left behind", "objects", len(stmts))

	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+leftoverLockTimeout+"'"); err != nil {
		return fmt.Errorf("clear pg_repack leftovers: %w", err)
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clear pg_repack leftovers: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit clearing pg_repack leftovers: %w", err)
	}
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
