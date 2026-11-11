//go:build kwiltest

package tn_vacuum

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/pg"
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

// startNodePostgres starts the Postgres image the node composes run, as they
// run it: a "postgres" superuser, and the "kwild" role and database that the
// image's init script creates, with trust authentication.
func startNodePostgres(t *testing.T) (testcontainers.Container, DBConnConfig) {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/trufnetwork/kwil-postgres:16.8-2",
			ExposedPorts: []string{"5432/tcp"},
			Env:          map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust"},
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
	return container, DBConnConfig{Host: host, Port: port.Port(), User: "kwild", Database: "kwild"}
}

// execInContainer runs cmd in the container and returns its exit code and output.
func execInContainer(t *testing.T, container testcontainers.Container, cmd ...string) (int, string) {
	t.Helper()
	code, reader, err := container.Exec(context.Background(), cmd, tcexec.Multiplexed())
	require.NoError(t, err)
	out, err := io.ReadAll(reader)
	require.NoError(t, err)
	return code, string(out)
}

// runPostgresRoles runs deployments/postgres-roles.sql as the postgres
// superuser, as the node composes do before kwild starts.
func runPostgresRoles(t *testing.T, container testcontainers.Container) {
	t.Helper()
	require.NoError(t, container.CopyFileToContainer(context.Background(),
		"../../deployments/postgres-roles.sql", "/tmp/postgres-roles.sql", 0o644))
	code, out := execInContainer(t, container, "psql", "--username=postgres", "--no-psqlrc",
		"--set=ON_ERROR_STOP=1", "--file=/tmp/postgres-roles.sql")
	require.Equal(t, 0, code, out)
}

func connectAs(t *testing.T, db DBConnConfig) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), buildConnString(db))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func requirePermissionDenied(t *testing.T, err error, stmt string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "%s: want a permission error, got %v", stmt, err)
	require.Equal(t, "42501", pgErr.Code, "%s: %s", stmt, pgErr.Message)
}

func queryBool(t *testing.T, conn *pgx.Conn, query string, args ...any) bool {
	t.Helper()
	var b bool
	require.NoError(t, conn.QueryRow(context.Background(), query, args...).Scan(&b))
	return b
}

func TestPostgresRolesRunPgRepackWithoutSuperuser(t *testing.T) {
	ctx := context.Background()
	container, kwild := startNodePostgres(t)

	// A node set up before the roles existed: kwild is a superuser and made
	// the event trigger and the publication, and tn_vacuum made the pg_repack
	// extension as kwild.
	asKwild := connectAs(t, kwild)
	for _, stmt := range []string{
		`CREATE FUNCTION set_replica_identity() RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN
        SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE TABLE'
    LOOP
        EXECUTE 'ALTER TABLE ' || obj.object_identity || ' REPLICA IDENTITY FULL';
    END LOOP;
END;
$$`,
		`CREATE EVENT TRIGGER set_replica_identity_on_create ON ddl_command_end
			WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION set_replica_identity()`,
		"CREATE PUBLICATION kwild_repl FOR ALL TABLES",
		"CREATE EXTENSION pg_repack",
		"CREATE SCHEMA main",
		"CREATE TABLE main.events (id INT PRIMARY KEY, v TEXT)",
		"INSERT INTO main.events SELECT g, 'r' || g FROM generate_series(1, 100) g",
	} {
		_, err := asKwild.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	require.NoError(t, asKwild.Close(ctx))

	runPostgresRoles(t, container)
	asKwild = connectAs(t, kwild)
	const triggerOID = "SELECT oid::bigint FROM pg_event_trigger WHERE evtname = 'set_replica_identity_on_create'"
	const copyTriggerOID = "SELECT oid::bigint FROM pg_event_trigger WHERE evtname = 'set_replica_identity_on_repack_copy'"
	trigger := countOne(t, asKwild, triggerOID)
	copyTrigger := countOne(t, asKwild, copyTriggerOID)
	runPostgresRoles(t, container) // and again, as on every start
	require.Equal(t, trigger, countOne(t, asKwild, triggerOID), "a second run keeps the event trigger as it is")
	require.Equal(t, copyTrigger, countOne(t, asKwild, copyTriggerOID),
		"a second run keeps the event trigger for pg_repack's copies as it is")

	require.False(t, queryBool(t, asKwild, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"))
	// The event trigger's function is read through the trigger, from the
	// catalog: kwild may not look up names in the function's schema.
	const triggerFunction = "pg_event_trigger e JOIN pg_proc p ON p.oid = e.evtfoid " +
		"JOIN pg_namespace n ON n.oid = p.pronamespace WHERE e.evtname = 'set_replica_identity_on_create'"
	const copyTriggerFunction = "pg_event_trigger e JOIN pg_proc p ON p.oid = e.evtfoid " +
		"JOIN pg_namespace n ON n.oid = p.pronamespace WHERE e.evtname = 'set_replica_identity_on_repack_copy'"
	for name, query := range map[string]string{
		"extension":         "SELECT r.rolsuper FROM pg_extension e JOIN pg_roles r ON r.oid = e.extowner WHERE e.extname = 'pg_repack'",
		"event trigger":     "SELECT r.rolsuper FROM pg_event_trigger e JOIN pg_roles r ON r.oid = e.evtowner WHERE e.evtname = 'set_replica_identity_on_create'",
		"function":          "SELECT (SELECT rolsuper FROM pg_roles WHERE oid = p.proowner) FROM " + triggerFunction,
		"function's schema": "SELECT (SELECT rolsuper FROM pg_roles WHERE oid = n.nspowner) FROM " + triggerFunction,
		"copy trigger":      "SELECT r.rolsuper FROM pg_event_trigger e JOIN pg_roles r ON r.oid = e.evtowner WHERE e.evtname = 'set_replica_identity_on_repack_copy'",
		"copy function":     "SELECT (SELECT rolsuper FROM pg_roles WHERE oid = p.proowner) FROM " + copyTriggerFunction,
		"copy schema":       "SELECT (SELECT rolsuper FROM pg_roles WHERE oid = n.nspowner) FROM " + copyTriggerFunction,
		"publication":       "SELECT r.rolsuper FROM pg_publication p JOIN pg_roles r ON r.oid = p.pubowner WHERE p.pubname = 'kwild_repl'",
	} {
		require.True(t, queryBool(t, asKwild, query), "a superuser owns the %s", name)
	}
	require.True(t, queryBool(t, asKwild,
		"SELECT n.nspname = 'tn_replica_identity' AND p.proname = 'set_replica_identity' FROM "+triggerFunction),
		"the event trigger calls the function in the superuser's schema")
	require.True(t, queryBool(t, asKwild, "SELECT to_regprocedure('public.set_replica_identity()') IS NULL"),
		"the function kwild made in its public schema is gone")
	// The event trigger's function runs for every role that creates a table,
	// so it looks names up in pg_catalog only, not in schemas kwild owns.
	require.True(t, queryBool(t, asKwild,
		"SELECT p.proconfig = ARRAY['search_path=pg_catalog, pg_temp'] FROM "+triggerFunction))
	// Read from the catalog by name: kwild may not look up names in the repack schema.
	require.True(t, queryBool(t, asKwild,
		`SELECT r.rolname = 'tn_repack' FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_roles r ON r.oid = p.proowner
		WHERE n.nspname = 'repack' AND p.proname = 'repack_trigger'`),
		"tn_repack owns pg_repack's trigger function")

	// kwild may not run programs, read the server's files, use pg_repack, or
	// drop the event triggers, their functions or the functions' schema.
	for _, stmt := range []string{
		"COPY (SELECT 1) TO PROGRAM 'true'",
		"COPY (SELECT 1) TO '/tmp/kwild-copy'",
		"SELECT pg_read_file('PG_VERSION')",
		"SELECT repack.version()",
		"DROP EVENT TRIGGER set_replica_identity_on_create",
		"DROP FUNCTION tn_replica_identity.set_replica_identity() CASCADE",
		"DROP EVENT TRIGGER set_replica_identity_on_repack_copy",
		"DROP FUNCTION tn_replica_identity.set_repack_copy_replica_identity() CASCADE",
		"DROP SCHEMA tn_replica_identity CASCADE",
	} {
		_, err := asKwild.Exec(ctx, stmt)
		requirePermissionDenied(t, err, stmt)
	}
	// It may still restore a snapshot, which runs as a replica session.
	_, err := asKwild.Exec(ctx, "SET session_replication_role = replica")
	require.NoError(t, err)
	_, err = asKwild.Exec(ctx, "RESET session_replication_role")
	require.NoError(t, err)
	// And the tables it creates still get full replica identity.
	_, err = asKwild.Exec(ctx, "CREATE TABLE main.later (id INT PRIMARY KEY)")
	require.NoError(t, err)
	require.True(t, queryBool(t, asKwild, "SELECT relreplident = 'f' FROM pg_class WHERE oid = 'main.later'::regclass"))

	// Once kwild is not a superuser, tn_vacuum runs pg_repack as tn_repack.
	role, err := pgRepackRole(ctx, MechanismDeps{DB: kwild})
	require.NoError(t, err)
	require.Equal(t, &repackRole{user: DefaultPgRepackUser}, role)
	repacker := kwild
	repacker.User = role.user
	asRepacker := connectAs(t, repacker)
	require.NoError(t, ensurePgRepackExtension(ctx, repacker, log.DiscardLogger))

	// A run interrupted as tn_repack leaves its trigger on kwild's table, and
	// kwild's writes reach the log table through it.
	leaveInterruptedRepack(t, asRepacker, "main.events")
	var eventsOID uint32
	require.NoError(t, asKwild.QueryRow(ctx, "SELECT 'main.events'::regclass::oid").Scan(&eventsOID))
	_, err = asKwild.Exec(ctx, "INSERT INTO main.events VALUES (101, 'r101')")
	require.NoError(t, err)
	require.EqualValues(t, 1, countOne(t, asRepacker, fmt.Sprintf("SELECT count(*) FROM repack.log_%d", eventsOID)))

	var extOID uint32
	require.NoError(t, asKwild.QueryRow(ctx, "SELECT oid FROM pg_extension WHERE extname = 'pg_repack'").Scan(&extOID))
	require.NoError(t, clearInterruptedRepackAsOwner(ctx, repacker, log.DiscardLogger))
	require.EqualValues(t, 0, countOne(t, asKwild,
		"SELECT count(*) FROM pg_trigger WHERE tgname = 'repack_trigger' AND NOT tgisinternal"))
	require.EqualValues(t, 0, countOne(t, asKwild,
		"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'repack' AND c.relkind IN ('r', 'c')"))
	require.EqualValues(t, extOID, countOne(t, asKwild, "SELECT oid::bigint FROM pg_extension WHERE extname = 'pg_repack'"),
		"the extension stays")
	require.NoError(t, clearInterruptedRepackAsOwner(ctx, repacker, log.DiscardLogger), "nothing left to clear")

	// pg_repack itself runs as tn_repack.
	var before uint32
	require.NoError(t, asKwild.QueryRow(ctx, "SELECT pg_relation_filenode('main.events')").Scan(&before))
	code, out := execInContainer(t, container, "pg_repack", "--no-superuser-check", "--username=tn_repack",
		"--dbname=kwild", "--all", "--no-order")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, `repacking table "main.events"`)
	var after uint32
	require.NoError(t, asKwild.QueryRow(ctx, "SELECT pg_relation_filenode('main.events')").Scan(&after))
	require.NotEqual(t, before, after, "the table was rewritten")
	require.EqualValues(t, 101, countOne(t, asKwild, "SELECT count(*) FROM main.events"))
	require.EqualValues(t, 0, countOne(t, asKwild,
		"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'repack' AND c.relkind IN ('r', 'c')"))
}

// pg_repack runs as kwild while kwild is a superuser, and an interrupted run
// is cleared by creating the extension again. Once the roles script has made
// kwild a regular role, pg_repack runs as tn_repack, which may not drop the
// extension and clears an interrupted run one object at a time.
func TestPgRepackRoleFollowsKwildsRole(t *testing.T) {
	ctx := context.Background()
	container, kwild := startNodePostgres(t)
	binary, argsFile, _ := recordingPgRepack(t)
	t.Setenv("PATH", filepath.Dir(binary)+string(os.PathListSeparator)+os.Getenv("PATH"))

	asKwild := connectAs(t, kwild)
	for _, stmt := range []string{
		"CREATE SCHEMA main",
		"CREATE TABLE main.events (id INT PRIMARY KEY, v TEXT)",
	} {
		_, err := asKwild.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	const extOID = "SELECT oid::bigint FROM pg_extension WHERE extname = 'pg_repack'"
	const leftoverTriggers = "SELECT count(*) FROM pg_trigger WHERE tgname = 'repack_trigger' AND NOT tgisinternal"

	// kwild is still the superuser that the image's init script made it.
	role, err := pgRepackRole(ctx, MechanismDeps{DB: kwild})
	require.NoError(t, err)
	require.Nil(t, role, "pg_repack keeps kwild's own user while that user is a superuser")

	m := NewPgRepackMechanism().(*pgRepackMechanism)
	require.NoError(t, m.Prepare(ctx, MechanismDeps{Logger: log.DiscardLogger, DB: kwild}))
	leaveInterruptedRepack(t, asKwild, "main.events")
	ext := countOne(t, asKwild, extOID)
	_, err = m.Run(ctx, RunRequest{DB: kwild})
	require.NoError(t, err)
	require.Zero(t, countOne(t, asKwild, leftoverTriggers))
	require.NotEqual(t, ext, countOne(t, asKwild, extOID), "kwild clears the run by creating the extension again")
	args := readLines(t, argsFile)
	require.Contains(t, args, "--username=kwild")
	require.NotContains(t, args, "--no-superuser-check")

	require.NoError(t, asKwild.Close(ctx))
	runPostgresRoles(t, container)
	asKwild = connectAs(t, kwild)
	repacker := kwild
	repacker.User = DefaultPgRepackUser

	m = NewPgRepackMechanism().(*pgRepackMechanism)
	require.NoError(t, m.Prepare(ctx, MechanismDeps{Logger: log.DiscardLogger, DB: kwild}))
	leaveInterruptedRepack(t, connectAs(t, repacker), "main.events")
	ext = countOne(t, asKwild, extOID)
	_, err = m.Run(ctx, RunRequest{DB: kwild})
	require.NoError(t, err)
	require.Zero(t, countOne(t, asKwild, leftoverTriggers))
	require.Equal(t, ext, countOne(t, asKwild, extOID), "tn_repack leaves the extension in place")
	args = readLines(t, argsFile)
	require.Contains(t, args, "--username="+DefaultPgRepackUser)
	require.Contains(t, args, "--no-superuser-check")
}

// pgRepackRun is the exit code and output of a pg_repack run in a container.
type pgRepackRun struct {
	code int
	out  string
	err  error
}

// startPgRepack runs pg_repack in the container as tn_vacuum runs it on a
// database set up with postgres-roles.sql, and returns at once. The run's
// result arrives on the channel.
func startPgRepack(container testcontainers.Container) <-chan pgRepackRun {
	done := make(chan pgRepackRun, 1)
	go func() {
		code, reader, err := container.Exec(context.Background(), []string{"pg_repack", "--no-superuser-check",
			"--username=" + DefaultPgRepackUser, "--dbname=kwild", "--all", "--no-order"}, tcexec.Multiplexed())
		var out []byte
		if err == nil {
			out, err = io.ReadAll(reader)
		}
		done <- pgRepackRun{code: code, out: string(out), err: err}
	}()
	return done
}

// pg_repack copies a table with CREATE TABLE ... AS, and until it swaps the
// copy in, it replays on the copy the writes the table takes. The publication
// covers every table, and Postgres refuses an UPDATE or DELETE on a published
// table without a replica identity. The copy has no primary key, so the roles
// script gives it full replica identity.
func TestPgRepackReplaysUpdatesAndDeletesUnderPublication(t *testing.T) {
	ctx := context.Background()
	container, kwild := startNodePostgres(t)
	runPostgresRoles(t, container)

	asKwild := connectAs(t, kwild)
	for _, stmt := range []string{
		"CREATE SCHEMA main",
		"CREATE TABLE main.events (id INT PRIMARY KEY, v TEXT)",
		"INSERT INTO main.events SELECT g, 'r' || g FROM generate_series(1, 100) g",
		// The CREATE TABLE trigger gave the table full replica identity, like
		// the copy will get. Set it back to the default, so that the check after
		// the swap shows the table keeps its own setting, not the copy's.
		"ALTER TABLE main.events REPLICA IDENTITY DEFAULT",
	} {
		_, err := asKwild.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	require.True(t, queryBool(t, asKwild,
		"SELECT puballtables AND pubupdate AND pubdelete FROM pg_publication WHERE pubname = 'kwild_repl'"),
		"the publication covers every table and publishes updates and deletes")
	var eventsOID, before uint32
	require.NoError(t, asKwild.QueryRow(ctx,
		"SELECT 'main.events'::regclass::oid, pg_relation_filenode('main.events')").Scan(&eventsOID, &before))

	// pg_repack swaps the copy in only once the transactions that were open
	// when it began to copy have ended. Holding one open keeps it replaying
	// writes on the copy until the writes below have reached it.
	hold, err := connectAs(t, kwild).Begin(ctx)
	require.NoError(t, err)
	_, err = hold.Exec(ctx, "SELECT 1")
	require.NoError(t, err)

	run := startPgRepack(container)

	// kwild may not look up names in the repack schema, so it reads the copy
	// from the catalog. The copy is there once pg_repack has filled it.
	var copyReplident string
	deadline := time.Now().Add(time.Minute)
	for {
		err := asKwild.QueryRow(ctx, `SELECT c.relreplident::text FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'repack' AND c.relname = $1`, fmt.Sprintf("table_%d", eventsOID)).Scan(&copyReplident)
		if err == nil {
			break
		}
		require.ErrorIs(t, err, pgx.ErrNoRows)
		select {
		case r := <-run:
			t.Fatalf("pg_repack ended before it copied main.events: exit %d, %v\n%s", r.code, r.err, r.out)
		default:
		}
		require.True(t, time.Now().Before(deadline), "pg_repack did not copy main.events within a minute")
		time.Sleep(100 * time.Millisecond)
	}

	for _, stmt := range []string{
		"UPDATE main.events SET v = 'u' || id WHERE id <= 10",
		"DELETE FROM main.events WHERE id > 90",
		"INSERT INTO main.events VALUES (101, 'r101')",
	} {
		_, err := asKwild.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	require.NoError(t, hold.Commit(ctx))

	var r pgRepackRun
	select {
	case r = <-run:
	case <-time.After(2 * time.Minute):
		t.Fatal("pg_repack did not finish within two minutes")
	}
	require.NoError(t, r.err)
	require.Equal(t, 0, r.code, r.out)
	require.Contains(t, r.out, `repacking table "main.events"`)
	require.Equal(t, "f", copyReplident, "pg_repack's copy of main.events has full replica identity")

	var after uint32
	require.NoError(t, asKwild.QueryRow(ctx, "SELECT pg_relation_filenode('main.events')").Scan(&after))
	require.NotEqual(t, before, after, "the table was rewritten")
	require.EqualValues(t, 91, countOne(t, asKwild, "SELECT count(*) FROM main.events"))
	require.EqualValues(t, 10, countOne(t, asKwild, "SELECT count(*) FROM main.events WHERE v = 'u' || id"),
		"the updates made during the run are kept")
	require.EqualValues(t, 0, countOne(t, asKwild, "SELECT count(*) FROM main.events WHERE id BETWEEN 91 AND 100"),
		"the deletes made during the run are kept")
	require.EqualValues(t, 1, countOne(t, asKwild, "SELECT count(*) FROM main.events WHERE id = 101"),
		"the insert made during the run is kept")
	require.True(t, queryBool(t, asKwild, "SELECT relreplident = 'd' FROM pg_class WHERE oid = 'main.events'::regclass"),
		"the table keeps its own replica identity, not the copy's")
	require.EqualValues(t, 0, countOne(t, asKwild,
		"SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'repack' AND c.relkind IN ('r', 'c')"))
}

// kwild stops at startup unless set_replica_identity_on_create fires on
// CREATE TABLE alone and runs kwild's own function, so the copies pg_repack
// makes have a trigger of their own. Running the roles script again adds that
// trigger to a node set up before it, and puts either trigger back when its
// tags have changed.
func TestPostgresRolesKeepReplicaIdentityTriggersAsKwildChecks(t *testing.T) {
	ctx := context.Background()
	container, kwild := startNodePostgres(t)
	runPostgresRoles(t, container)
	superuser := kwild
	superuser.User = "postgres"
	asSuperuser := connectAs(t, superuser)

	type trigger struct {
		Enabled string
		Tags    []string
	}
	readTrigger := func(name string) trigger {
		t.Helper()
		var tr trigger
		require.NoError(t, asSuperuser.QueryRow(ctx,
			"SELECT evtenabled::text, evttags FROM pg_event_trigger WHERE evtname = $1 AND evtevent = 'ddl_command_end'",
			name).Scan(&tr.Enabled, &tr.Tags))
		return tr
	}
	asKwildChecks := trigger{Enabled: "O", Tags: []string{"CREATE TABLE"}}
	forRepackCopies := trigger{Enabled: "O", Tags: []string{"CREATE TABLE AS"}}

	// A node set up by an earlier roles script has no trigger for pg_repack's
	// copies, and that script kept a trigger whose tags had changed as long as
	// it ran the right function.
	for _, stmt := range []string{
		"DROP EVENT TRIGGER set_replica_identity_on_repack_copy",
		"DROP FUNCTION tn_replica_identity.set_repack_copy_replica_identity()",
		"DROP EVENT TRIGGER set_replica_identity_on_create",
		`CREATE EVENT TRIGGER set_replica_identity_on_create ON ddl_command_end
			WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS') EXECUTE FUNCTION tn_replica_identity.set_replica_identity()`,
	} {
		_, err := asSuperuser.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	runPostgresRoles(t, container)
	require.Equal(t, asKwildChecks, readTrigger("set_replica_identity_on_create"))
	require.Equal(t, forRepackCopies, readTrigger("set_replica_identity_on_repack_copy"))

	// The same for the trigger on pg_repack's copies, also when disabled.
	for _, stmt := range []string{
		"DROP EVENT TRIGGER set_replica_identity_on_repack_copy",
		`CREATE EVENT TRIGGER set_replica_identity_on_repack_copy ON ddl_command_end
			WHEN TAG IN ('CREATE TABLE AS', 'SELECT INTO') EXECUTE FUNCTION tn_replica_identity.set_repack_copy_replica_identity()`,
		"ALTER EVENT TRIGGER set_replica_identity_on_repack_copy DISABLE",
	} {
		_, err := asSuperuser.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	runPostgresRoles(t, container)
	require.Equal(t, asKwildChecks, readTrigger("set_replica_identity_on_create"))
	require.Equal(t, forRepackCopies, readTrigger("set_replica_identity_on_repack_copy"))

	const triggerOIDs = "SELECT array_agg(oid::bigint ORDER BY evtname) FROM pg_event_trigger " +
		"WHERE evtname IN ('set_replica_identity_on_create', 'set_replica_identity_on_repack_copy')"
	var oids, oidsAgain []int64
	require.NoError(t, asSuperuser.QueryRow(ctx, triggerOIDs).Scan(&oids))
	require.Len(t, oids, 2)
	runPostgresRoles(t, container)
	require.NoError(t, asSuperuser.QueryRow(ctx, triggerOIDs).Scan(&oidsAgain))
	require.Equal(t, oids, oidsAgain, "a run with nothing to repair keeps both triggers")

	// A table that CREATE TABLE ... AS makes in pg_repack's schema gets full
	// replica identity. One kwild makes in its own schemas keeps the default,
	// as it has on every node so far.
	repacker := kwild
	repacker.User = DefaultPgRepackUser
	asRepacker := connectAs(t, repacker)
	_, err := asRepacker.Exec(ctx, "CREATE TABLE repack.probe AS SELECT 1 AS x")
	require.NoError(t, err)
	require.True(t, queryBool(t, asRepacker, "SELECT relreplident = 'f' FROM pg_class WHERE oid = 'repack.probe'::regclass"))
	_, err = asRepacker.Exec(ctx, "DROP TABLE repack.probe")
	require.NoError(t, err)
	asKwild := connectAs(t, kwild)
	for _, stmt := range []string{"CREATE SCHEMA main", "CREATE TABLE main.probe AS SELECT 1 AS x"} {
		_, err := asKwild.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	require.True(t, queryBool(t, asKwild, "SELECT relreplident = 'd' FROM pg_class WHERE oid = 'main.probe'::regclass"))

	// kwild starts, which it does only when its own trigger is as it checks.
	db, err := pg.NewDB(ctx, &pg.DBConfig{PoolConfig: pg.PoolConfig{
		ConnConfig: pg.ConnConfig{Host: kwild.Host, Port: kwild.Port, User: kwild.User, DBName: kwild.Database},
		MaxConns:   4,
	}})
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
