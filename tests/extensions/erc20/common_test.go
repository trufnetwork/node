//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	evmsync "github.com/trufnetwork/kwil-db/node/exts/evm-sync"
)

// Common deterministic values used across ERC20 bridge tests
const (
	TestChain   = "sepolia"
	TestEscrowA = "0x1111111111111111111111111111111111111111"
	TestEscrowB = "0x2222222222222222222222222222222222222222"
	TestERC20   = "0x2222222222222222222222222222222222222222"
	TestUserA   = "0xabc0000000000000000000000000000000000001"
	TestUserB   = "0xabc0000000000000000000000000000000000002"
	TestAmount1 = "1000000000000000000" // 1.0 tokens
	TestAmount2 = "2000000000000000000" // 2.0 tokens
)

// seedAndRun is a helper that handles seed script setup and runs the test with proper isolation
func seedAndRun(t TestingT, name string, extraSeed string, fn kwilTesting.TestFunc) {
	seedScripts := migrations.GetSeedScriptPaths()

	if extraSeed != "" {
		_, thisFile, _, _ := runtime.Caller(1)
		extraSeedPath := filepath.Join(filepath.Dir(thisFile), extraSeed)
		seedScripts = append(seedScripts, extraSeedPath)
	}

	// Wrap the test function to add singleton reset and cleanup
	wrappedFn := func(ctx context.Context, platform *kwilTesting.Platform) error {
		// STEP 1: Reset singleton and prepare for seeding
		if err := ensureSingletonReset(ctx, platform); err != nil {
			return err
		}

		// STEP 2: If using simple_mock.sql, ensure the seeded instance is set up
		if extraSeed == "simple_mock.sql" {
			app := &common.App{DB: platform.DB, Engine: platform.Engine}
			if _, err := erc20shim.ForTestingForceSyncInstance(ctx, app, TestChain, TestEscrowA, TestERC20, 18); err == nil {
				_ = erc20shim.ForTestingInitializeExtension(ctx, app)
			}
		}

		// STEP 3: Register cleanup (runs after transaction rollback)
		addSeedCleanup(t, platform, extraSeed)

		// STEP 4: Run the actual test inside a transaction for rollback isolation
		tx, err := platform.DB.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback(ctx)

		txPlatform := &kwilTesting.Platform{
			Engine:   platform.Engine,
			DB:       tx,
			Deployer: platform.Deployer,
			Logger:   platform.Logger,
		}

		return fn(ctx, txPlatform)
	}

	// Ensure the ERC20 test singleton is clean BEFORE seeds run so prepare() uses a fresh cache
	erc20shim.ForTestingResetSingleton()

	testutils.RunSchemaTest(t.(*testing.T), kwilTesting.SchemaTest{
		Name:          name,
		SeedScripts:   seedScripts,
		FunctionTests: []kwilTesting.TestFunc{wrappedFn},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}

// engCtx creates a standard EngineContext for testing
func engCtx(ctx context.Context, platform *kwilTesting.Platform, caller string, height int64, overrideAuthz bool) *common.EngineContext {
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: height},
			Signer:       platform.Deployer,
			Caller:       caller,
			TxID:         platform.Txid(),
		},
		OverrideAuthz: overrideAuthz,
	}
}

// TestingT interface for test functions (matches testutils)
type TestingT interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Cleanup(func())
}

// ensureSingletonReset ensures the ERC20 singleton is reset and initialized for test isolation
func ensureSingletonReset(ctx context.Context, platform *kwilTesting.Platform) error {
	app := &common.App{DB: platform.DB, Engine: platform.Engine}

	// Reset singleton to ensure clean state
	erc20shim.ForTestingResetSingleton()

	// Initialize extension to load any DB state into singleton
	err := erc20shim.ForTestingInitializeExtension(ctx, app)
	if err != nil {
		return fmt.Errorf("failed to initialize ERC20 extension: %w", err)
	}

	return nil
}

// addSeedCleanup adds cleanup to deactivate instances used in seeds
//
// This function is called after the test transaction has been rolled back, so it MUST
// use mechanisms that do not rely on a still-open DB connection to tear down listeners/pollers.
// We explicitly unregister pollers/listeners using deterministic IDs, then call the
// transactional cleanup helper as a best-effort DB cleanup.
func addSeedCleanup(t TestingT, platform *kwilTesting.Platform, extraSeed string) {
	// If using simple_mock.sql, add cleanup to deactivate the seeded instance
	if extraSeed == "simple_mock.sql" {
		t.Cleanup(func() {
			// Unregister poller and listener by unique names (does not require DB)
			id := erc20shim.ForTestingGetInstanceID(TestChain, TestEscrowA)
			// state poller unique name format: "erc20_state_poll_" + id
			pollerName := "erc20_state_poll_" + id.String()
			_ = evmsync.StatePoller.UnregisterPoll(pollerName)
			// transfer listener unique name available via shim
			transferName := erc20shim.ForTestingTransferListenerTopic(*id)
			_ = evmsync.EventSyncer.UnregisterListener(transferName)

			// Best-effort DB cleanup (may log a warning if conn closed)
			testerc20.DeactivateCurrentInstanceTx(t.(*testing.T), platform, TestEscrowA)
		})
	}
}

// deactivateSeededInstance deactivates the instance created by simple_mock.sql if it exists
//
// This function is called BEFORE the test transaction starts to ensure a clean slate.
// It prevents "already active" errors when running multiple ERC20 tests consecutively.
//
// For simple_mock.sql seeds, it uses the outer (non-transactional) platform DB to ensure
// deactivation persists and resets the singleton to reflect the DB state.
//
// This is different from cleanup which happens AFTER the transaction is rolled back.
func deactivateSeededInstance(ctx context.Context, platform *kwilTesting.Platform, extraSeed string) error {
	if extraSeed == "simple_mock.sql" {
		// Proactively unregister any previously registered poller/listener (no DB required)
		id := erc20shim.ForTestingGetInstanceID(TestChain, TestEscrowA)
		pollerName := "erc20_state_poll_" + id.String()
		_ = evmsync.StatePoller.UnregisterPoll(pollerName)
		transferName := erc20shim.ForTestingTransferListenerTopic(*id)
		_ = evmsync.EventSyncer.UnregisterListener(transferName)
	}
	return nil
}
