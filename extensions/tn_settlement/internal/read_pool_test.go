package internal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

// TestNewReadPool_FailsClosedOnUnreachableDB verifies that when the independent
// read pool cannot be built (no reachable database), NewReadPool returns an
// error rather than panicking or hanging. The caller (engineReadyHook) treats
// this as fail-closed: readDB stays nil and settlement reads error out, instead
// of silently falling back to the engine interpreter and reintroducing the
// deadlock this pool exists to prevent.
func TestNewReadPool_FailsClosedOnUnreachableDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Port 1 has nothing listening: connection is refused immediately.
	_, err := newReadPoolFromConnString(ctx,
		"host=127.0.0.1 port=1 user=nobody database=nodb sslmode=disable connect_timeout=1",
		log.New())
	require.Error(t, err)
}

// TestBuildReadPoolConfig_SetsLockTimeout guards the settlement half of the
// deadlock fix: the read pool's connections must carry a bounded lock_timeout so
// a poll read blocked by in-block DDL fails fast instead of deadlocking block
// production. This is a no-DB regression guard (the pglive liveness test that
// exercises the real behaviour is not run in the default CI tag set), so if a
// future edit drops or clobbers the lock_timeout wiring, this fails loudly.
func TestBuildReadPoolConfig_SetsLockTimeout(t *testing.T) {
	cfg, err := buildReadPoolConfig("host=127.0.0.1 port=5432 user=u database=d sslmode=disable")
	require.NoError(t, err)
	require.Equal(t, "3000", cfg.ConnConfig.RuntimeParams["lock_timeout"],
		"settlement read pool must set lock_timeout=3000ms (3s) as a connection startup parameter")
	// Keep the wired value in sync with the source-of-truth constant.
	require.Equal(t, lockTimeoutMillis(), cfg.ConnConfig.RuntimeParams["lock_timeout"])
}
