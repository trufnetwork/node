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
		// For tests that DO NOT use simple_mock.sql, allow deactivation to ensure clean slate
		if extraSeed != "simple_mock.sql" {
			if err := deactivateSeededInstance(ctx, platform, extraSeed); err != nil {
				return fmt.Errorf("failed to deactivate seeded instance: %w", err)
			}
		}

		// Enable idempotent prepare for active instances during test
		erc20shim.ForTestingAllowPrepareOnActive(true)
		t.Cleanup(func() { erc20shim.ForTestingAllowPrepareOnActive(false) })

		// Add cleanup for seeded instances
		addSeedCleanup(t, platform, extraSeed)

		// Run the actual test inside a transaction to ensure rollback isolation
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
func addSeedCleanup(t TestingT, platform *kwilTesting.Platform, extraSeed string) {
	// If using simple_mock.sql, add cleanup to deactivate the seeded instance
	if extraSeed == "simple_mock.sql" {
		t.Cleanup(func() {
			testerc20.DeactivateCurrentInstanceTx(t.(*testing.T), platform, TestEscrowA)
		})
	}
}

// deactivateSeededInstance deactivates the instance created by simple_mock.sql if it exists
func deactivateSeededInstance(ctx context.Context, platform *kwilTesting.Platform, extraSeed string) error {
	if extraSeed == "simple_mock.sql" {
		// Try to deactivate the seeded instance to allow reuse
		app := &common.App{DB: platform.DB, Engine: platform.Engine}
		id, err := erc20shim.ForTestingForceSyncInstance(ctx, app, TestChain, TestEscrowA, TestERC20, 18)
		if err != nil {
			// Instance might not exist or might already be deactivated, ignore error
			return nil
		}

		// Call the meta extension's disable method
		txCtx := &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: 1},
			Signer:       platform.Deployer,
			Caller:       "0x0000000000000000000000000000000000000000",
			TxID:         platform.Txid(),
		}
		engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}

		_, err = platform.Engine.Call(engCtx, platform.DB, "kwil_erc20_meta", "disable", []any{id}, func(row *common.Row) error {
			return nil
		})
		if err != nil {
			// Instance might already be deactivated, ignore error
			return nil
		}

		// Reset and re-initialize the singleton to reflect the deactivated state
		erc20shim.ForTestingResetSingleton()
		err = erc20shim.ForTestingInitializeExtension(ctx, app)
		if err != nil {
			return fmt.Errorf("failed to re-initialize singleton: %w", err)
		}
	}
	return nil
}
