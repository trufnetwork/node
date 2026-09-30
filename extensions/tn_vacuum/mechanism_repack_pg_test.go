//go:build kwiltest

package tn_vacuum

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/trufnetwork/kwil-db/core/log"
)

// startRepackPostgres starts the Postgres image nodes run, which ships the
// pg_repack extension.
func startRepackPostgres(t *testing.T) DBConnConfig {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/trufnetwork/kwil-postgres:16.8-1",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "kwil",
				"POSTGRES_USER":     "kwil",
				"POSTGRES_PASSWORD": "kwil",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	return DBConnConfig{Host: host, Port: port.Port(), User: "kwil", Password: "kwil", Database: "kwil"}
}

func countOne(t *testing.T, conn *pgx.Conn, query string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, conn.QueryRow(context.Background(), query).Scan(&n))
	return n
}

// leaveInterruptedRepack runs what pg_repack runs on a table before it copies
// it, taken from pg_repack's own repack.tables view, and stops there, as a
// killed run does: a primary key type, a log table, and repack_trigger.
func leaveInterruptedRepack(t *testing.T, conn *pgx.Conn, table string) {
	t.Helper()
	ctx := context.Background()
	var pktype, logTable, trigger string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT create_pktype, create_log, create_trigger FROM repack.tables WHERE relname = $1", table,
	).Scan(&pktype, &logTable, &trigger))
	for _, stmt := range []string{pktype, logTable, trigger} {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err)
	}
}

func TestClearInterruptedRepack(t *testing.T) {
	ctx := context.Background()
	db := startRepackPostgres(t)
	conn, err := pgx.Connect(ctx, buildConnString(db))
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, stmt := range []string{
		"CREATE EXTENSION pg_repack",
		"CREATE SCHEMA main",
		"CREATE TABLE main.events (id INT PRIMARY KEY, v TEXT)",
		"INSERT INTO main.events SELECT g, 'r' || g FROM generate_series(1, 3) g",
	} {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err)
	}

	// Nothing to clear: the extension is left as it is.
	var extOID uint32
	require.NoError(t, conn.QueryRow(ctx, "SELECT oid FROM pg_extension WHERE extname = 'pg_repack'").Scan(&extOID))
	require.NoError(t, clearInterruptedRepack(ctx, db, log.DiscardLogger))
	require.EqualValues(t, extOID, countOne(t, conn, "SELECT oid::bigint FROM pg_extension WHERE extname = 'pg_repack'"),
		"with nothing left behind, the extension must not be dropped")

	// A run interrupted on main.events, then a later --all run interrupted on
	// that run's log table: the triggers chain.
	leaveInterruptedRepack(t, conn, "main.events")
	var eventsOID uint32
	require.NoError(t, conn.QueryRow(ctx, "SELECT 'main.events'::regclass::oid").Scan(&eventsOID))
	firstLog := fmt.Sprintf("repack.log_%d", eventsOID)
	leaveInterruptedRepack(t, conn, firstLog)
	var firstLogOID uint32
	require.NoError(t, conn.QueryRow(ctx, "SELECT $1::regclass::oid", firstLog).Scan(&firstLogOID))
	secondLog := fmt.Sprintf("repack.log_%d", firstLogOID)

	_, err = conn.Exec(ctx, "INSERT INTO main.events VALUES (4, 'r4')")
	require.NoError(t, err)
	require.EqualValues(t, 1, countOne(t, conn, "SELECT count(*) FROM "+firstLog), "one write lands in the first log table")
	require.EqualValues(t, 1, countOne(t, conn, "SELECT count(*) FROM "+secondLog), "and again in the second")

	require.NoError(t, clearInterruptedRepack(ctx, db, log.DiscardLogger))

	require.EqualValues(t, 0, countOne(t, conn,
		"SELECT count(*) FROM pg_trigger WHERE tgname = 'repack_trigger' AND NOT tgisinternal"))
	require.EqualValues(t, 0, countOne(t, conn,
		"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'repack' AND c.relkind = 'r'"))
	require.EqualValues(t, 1, countOne(t, conn, "SELECT count(*) FROM pg_extension WHERE extname = 'pg_repack'"),
		"the extension is created again")
	require.EqualValues(t, 4, countOne(t, conn, "SELECT count(*) FROM main.events"), "the table's own rows stay")
	_, err = conn.Exec(ctx, "INSERT INTO main.events VALUES (5, 'r5')")
	require.NoError(t, err, "writes work once the trigger is gone")

	// A run can also die after dropping its trigger but before dropping its log
	// table. A table left in the repack schema on its own is still cleared.
	var pktype, logTable string
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT create_pktype, create_log FROM repack.tables WHERE relname = 'main.events'",
	).Scan(&pktype, &logTable))
	for _, stmt := range []string{pktype, logTable} {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	require.NoError(t, clearInterruptedRepack(ctx, db, log.DiscardLogger))
	require.EqualValues(t, 0, countOne(t, conn,
		"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'repack' AND c.relkind = 'r'"),
		"a log table without its trigger is cleared too")
}
