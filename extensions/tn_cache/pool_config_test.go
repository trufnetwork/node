package tn_cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildCachePoolConfig_SetsLockTimeout guards the cache half of the
// settlement/cache interpreter-deadlock fix. The cache's stream/taxonomy reads
// run through the engine interpreter on this independent pool, so a bounded
// lock_timeout on its connections is the only thing that prevents an in-block
// DDL (AccessExclusiveLock) on a cache-read table from deadlocking block
// production. If a future edit drops or clobbers the lock_timeout wiring, this
// fails loudly.
func TestBuildCachePoolConfig_SetsLockTimeout(t *testing.T) {
	cfg, err := buildCachePoolConfig("host=127.0.0.1 port=5432 user=u database=d sslmode=disable")
	require.NoError(t, err)
	require.Equal(t, "3000", cfg.ConnConfig.RuntimeParams["lock_timeout"],
		"cache pool must set lock_timeout=3000ms (3s) as a connection startup parameter")
	require.Equal(t, cacheLockTimeoutMillis, cfg.ConnConfig.RuntimeParams["lock_timeout"])
}
