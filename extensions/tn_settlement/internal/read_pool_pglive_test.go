//go:build pglive

package internal

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

// testConnString returns the pgx connection string for the liveness test. It is
// overridable via TN_TEST_PG_CONNSTRING and defaults to the parameters of the
// node's kwil-testing Postgres container.
func testConnString() string {
	if s := os.Getenv("TN_TEST_PG_CONNSTRING"); s != "" {
		return s
	}
	return "host=127.0.0.1 port=52853 user=kwild database=kwil_test_db sslmode=disable"
}

// TestReadPool_LockTimeoutBreaksBlockedRead proves the core liveness property of
// the settlement/cache interpreter-deadlock fix: a ReadPool SELECT blocked by
// another connection's AccessExclusiveLock — exactly what an in-block DDL
// (ALTER/DROP/etc.) holds — fails fast via lock_timeout instead of hanging
// forever. Under the old code the equivalent read (through the engine
// interpreter) would hold the interpreter lock until block commit, deadlocking
// the chain.
//
// Requires a reachable Postgres; skips cleanly otherwise. Run with:
//
//	go test -tags 'kwiltest pglive' ./extensions/tn_settlement/internal/ \
//	  -run TestReadPool_LockTimeoutBreaksBlockedRead -count=1
func TestReadPool_LockTimeoutBreaksBlockedRead(t *testing.T) {
	ctx := context.Background()
	connStr := testConnString()

	probe, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Skipf("no reachable Postgres (%v); set TN_TEST_PG_CONNSTRING to run", err)
	}
	defer probe.Close(context.Background())

	const tbl = "tn_settlement_locktimeout_probe"
	mustExec := func(conn *pgx.Conn, sqlText string) {
		_, e := conn.Exec(ctx, sqlText)
		require.NoError(t, e)
	}
	mustExec(probe, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
	mustExec(probe, fmt.Sprintf("CREATE TABLE %s (id int)", tbl))
	mustExec(probe, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", tbl))
	defer func() {
		_, _ = probe.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
	}()

	// The pool under test.
	pool, err := newReadPoolFromConnString(ctx, connStr, log.New())
	require.NoError(t, err)
	defer pool.Close()

	// Sanity: an unobstructed read succeeds.
	rs, err := pool.Execute(ctx, fmt.Sprintf("SELECT id FROM %s", tbl))
	require.NoError(t, err)
	require.Len(t, rs.Rows, 1)

	// Hold an AccessExclusiveLock on the table from a separate connection — as an
	// in-block ALTER/DROP would — without committing.
	locker, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer locker.Close(context.Background())
	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	defer lockTx.Rollback(context.Background())
	_, err = lockTx.Exec(ctx, fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", tbl))
	require.NoError(t, err)

	// The blocked read must return (fail-fast) rather than hang. lock_timeout is
	// settlementReadLockTimeout; allow a generous ceiling before declaring a hang.
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, e := pool.Execute(context.Background(), fmt.Sprintf("SELECT id FROM %s", tbl))
		done <- e
	}()

	select {
	case e := <-done:
		require.Error(t, e, "read should fail on lock_timeout while the table is exclusively locked")
		require.Contains(t, e.Error(), "lock timeout")
		require.GreaterOrEqual(t, time.Since(start), settlementReadLockTimeout-time.Second,
			"read returned before lock_timeout could have elapsed")
	case <-time.After(settlementReadLockTimeout + 10*time.Second):
		t.Fatal("ReadPool read did not return: lock_timeout is not bounding the wait (deadlock reproduced)")
	}
}
