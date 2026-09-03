package tn_digest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	digestinternal "github.com/trufnetwork/node/extensions/tn_digest/internal"
)

// The duplicate prune sweep shares the extension with digest but nothing else:
// its own table, its own enabled flag, its own schedule and its own cron. These
// tests are mostly about that separation, because the failure it protects against
// is one feature's config change silently stopping the other.

// Nothing prunes until an operator turns duplicate_prune_config.enabled on, and
// the migration ships it false. With digest off as well the extension builds no
// scheduler at all.
func TestPrune_DefaultDisabled_NoSchedulerOnLeaderAcquire(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetPruneConfig(false, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("pruneA")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	assert.Nil(t, ext.Scheduler())
}

// Pruning does not depend on digest being on. An operator draining duplicates on a
// network where digest is off should get the sweep and nothing else.
func TestPrune_LeaderAcquire_StartsPruneWithDigestOff(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetPruneConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("pruneB")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())
	assert.True(t, ext.Scheduler().PruneRunning())
	assert.False(t, ext.Scheduler().Running())

	_ = ext.Scheduler().StopPrune()
}

func TestPrune_LoseLeadership_StopsPrune(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetPruneConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("pruneC")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())
	require.True(t, ext.Scheduler().PruneRunning())

	digestLeaderLose(context.Background(), app, makeBlock(2, []byte("someone else")))
	assert.False(t, ext.Scheduler().PruneRunning())
}

// The enable path an operator actually takes: set enabled through a signed
// exec-sql and wait for the next config reload to pick it up.
func TestPrune_Reload_EnablesAndStarts_WhenBecomesEnabled(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetPruneConfig(false, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1)
	ext.SetLastCheckedHeight(1)
	identity := []byte("pruneD")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	fdb := &fakeDB{pruneEnabled: true, pruneSchedule: "*/5 * * * *"}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, nil, &fakeAccounts{}, log.New()))

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.Nil(t, ext.Scheduler())

	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))
	require.NotNil(t, ext.Scheduler())
	assert.True(t, ext.Scheduler().PruneRunning())
	assert.False(t, ext.Scheduler().Running(), "digest is off and should have stayed off")

	_ = ext.Scheduler().StopPrune()
}

// The way back is the same knob. Setting enabled false stops the sweep without a
// binary release, which is the reason the gate lives in the table rather than in a
// Go constant.
func TestPrune_Reload_DisablesAndStops_WhenBecomesDisabled(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetPruneConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1)
	ext.SetLastCheckedHeight(1)
	identity := []byte("pruneE")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())
	require.True(t, ext.Scheduler().PruneRunning())

	fdb := &fakeDB{pruneEnabled: false, pruneSchedule: "*/5 * * * *"}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, nil, &fakeAccounts{}, log.New()))
	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))

	assert.False(t, ext.Scheduler().PruneRunning())
	assert.False(t, ext.PruneEnabled())
}

// The reason the sweep gets its own cron and its own context. A digest schedule
// change stops and restarts the digest cron; on a shared one that would cancel a
// prune drain partway through a six-hour sweep, and there is no signal that would
// tell anyone it had happened.
func TestPrune_SurvivesADigestConfigChange(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetPruneConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1)
	ext.SetLastCheckedHeight(1)
	identity := []byte("pruneF")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())
	require.True(t, ext.Scheduler().Running())
	require.True(t, ext.Scheduler().PruneRunning())

	// Digest moves to a different schedule; the prune row is unchanged.
	fdb := &fakeDB{
		enabled: true, schedule: "0 9 * * *",
		pruneEnabled: true, pruneSchedule: "*/5 * * * *",
	}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, nil, &fakeAccounts{}, log.New()))
	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))

	assert.Equal(t, "0 9 * * *", ext.Schedule())
	assert.True(t, ext.Scheduler().PruneRunning(), "the prune sweep should not notice a digest config change")

	_ = ext.Scheduler().StopPrune()
	_ = ext.Scheduler().Stop()
}

// The other direction of the same separation. Turning digest off stops the digest
// cron; the sweep is a different feature answering to a different row and has to
// keep going.
func TestPrune_SurvivesDigestBeingTurnedOff(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetPruneConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1)
	ext.SetLastCheckedHeight(1)
	identity := []byte("pruneH")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())
	require.True(t, ext.Scheduler().Running())
	require.True(t, ext.Scheduler().PruneRunning())

	fdb := &fakeDB{
		enabled: false, schedule: "*/5 * * * *",
		pruneEnabled: true, pruneSchedule: "*/5 * * * *",
	}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, nil, &fakeAccounts{}, log.New()))
	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))

	assert.False(t, ext.Scheduler().Running())
	assert.True(t, ext.Scheduler().PruneRunning(), "turning digest off should not stop the sweep")

	_ = ext.Scheduler().StopPrune()
}

// A node whose binary is ahead of its migrations reads no duplicate_prune_config
// at all. That has to leave the sweep off rather than fail the reload, or every
// end-block on such a node would signal the retry worker.
func TestPrune_MissingConfigLeavesTheSweepOff(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetPruneConfig(false, "")
	ext.SetReloadIntervalBlocks(1)
	ext.SetLastCheckedHeight(1)
	identity := []byte("pruneG")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	// pruneSchedule empty means the fake answers no row.
	fdb := &fakeDB{enabled: true, schedule: "*/5 * * * *"}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, nil, &fakeAccounts{}, log.New()))

	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())
	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))

	assert.False(t, ext.PruneEnabled())
	assert.False(t, ext.Scheduler().PruneRunning())
	assert.True(t, ext.Scheduler().Running(), "digest should be unaffected")

	_ = ext.Scheduler().Stop()
}
