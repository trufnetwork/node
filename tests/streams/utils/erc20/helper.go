// Package erc20 provides helper functions for ERC-20 bridge testing.
// Follows the same patterns as query_test.go for consistency and compatibility.
package erc20

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

// WithERC20TestSetup is a helper function that sets up the test environment with ERC-20 bridge
// This version works with transaction-based isolation for perfect test isolation
func WithERC20TestSetup(escrowAddr string) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use the shared platform's DB and Engine (could be transaction-scoped)
		app := &common.App{DB: platform.DB, Engine: platform.Engine}

		// Reset the entire singleton to ensure test isolation
		erc20shim.ForTestingResetSingleton()

		// Initialize extension with synced instance
		_, err := erc20shim.ForTestingForceSyncInstance(ctx, app, "sepolia", escrowAddr, "0x2222222222222222222222222222222222222222", 18)
		if err != nil {
			return fmt.Errorf("failed to force sync ERC20 instance for escrow %s: %w", escrowAddr, err)
		}

		err = erc20shim.ForTestingInitializeExtension(ctx, app)
		if err != nil {
			return fmt.Errorf("failed to initialize ERC20 extension: %w", err)
		}

		return nil
	}
}

// WithERC20TestSetupTx is a transaction-aware version that works with WithTx for perfect isolation
func WithERC20TestSetupTx(escrowAddr string) func(t *testing.T, txPlatform *kwilTesting.Platform) {
	return func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Use the transaction-scoped platform
		app := &common.App{DB: txPlatform.DB, Engine: txPlatform.Engine}

		// Step 1: Reset the entire singleton for this test
		erc20shim.ForTestingResetSingleton()

		// Step 2: Initialize extension with synced instance using transaction-scoped DB
		_, err := erc20shim.ForTestingForceSyncInstance(context.Background(), app, "sepolia", escrowAddr, "0x2222222222222222222222222222222222222222", 18)
		if err != nil {
			t.Fatalf("failed to force sync ERC20 instance for escrow %s: %v", escrowAddr, err)
		}

		// Step 3: Initialize extension with synced instance using transaction-scoped DB
		err = erc20shim.ForTestingInitializeExtension(context.Background(), app)
		if err != nil {
			t.Fatalf("failed to initialize ERC20 extension: %v", err)
		}

		// Step 4: Set up cleanup to deactivate the instance after the test
		// This ensures the next test can reuse the same escrow address
		t.Cleanup(func() {
			DeactivateCurrentInstanceTx(t, txPlatform, escrowAddr)
		})
	}
}

// CreditUserBalance injects a realistic ERC-20 transfer to credit the user's balance.
// This simulates a user depositing tokens into the bridge.
func CreditUserBalance(ctx context.Context, platform *kwilTesting.Platform, escrowAddr, userAddr, amount string) error {
	// Use the platform's DB and Engine (could be transaction-scoped)
	app := &common.App{DB: platform.DB, Engine: platform.Engine}
	return InjectERC20Transfer(
		ctx, app, "sepolia", escrowAddr, "0x2222222222222222222222222222222222222222", userAddr, escrowAddr, amount, 10, nil)
}

// GetUserBalance queries the user's current balance via the extension.
func GetUserBalance(ctx context.Context, platform *kwilTesting.Platform, userAddr string) (string, error) {
	txCtx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       platform.Deployer,
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         platform.Txid(),
	}
	engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}

	var balance string
	r, err := platform.Engine.Call(engCtx, platform.DB, "", "get_balance", []any{userAddr}, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return fmt.Errorf("expected 1 column, got %d", len(row.Values))
		}
		balance = fmt.Sprintf("%v", row.Values[0])
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("engine call error: %w", err)
	}
	if r != nil && r.Error != nil {
		return "", fmt.Errorf("engine execution error: %w", r.Error)
	}
	return balance, nil
}

// CallLockAdmin executes the lock_admin system method with OverrideAuthz.
// This simulates an admin locking user tokens.
func CallLockAdmin(ctx context.Context, platform *kwilTesting.Platform, userAddr, amount string) error {
	txCtx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       platform.Deployer,
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         platform.Txid(),
	}
	engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}

	// Convert amount string to Decimal
	amt, err := strconv.ParseInt(amount, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse amount: %w", err)
	}
	amtDec, err := types.NewDecimalFromBigInt(big.NewInt(amt), 0)
	if err != nil {
		return fmt.Errorf("failed to create decimal: %w", err)
	}
	amtDec.SetPrecisionAndScale(78, 0)

	_, err = platform.Engine.Call(engCtx, platform.DB, "sepolia_bridge", "lock_admin", []any{userAddr, amtDec}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine call error: %w", err)
	}
	return nil
}

// DeactivateCurrentInstanceTx deactivates the ERC20 instance for the given escrow.
// This should be called in test cleanup to ensure the next test can use the same escrow.
// It sets active=false in the DB and refreshes the singleton state.
func DeactivateCurrentInstanceTx(t *testing.T, txPlatform *kwilTesting.Platform, escrowAddr string) {
	app := &common.App{DB: txPlatform.DB, Engine: txPlatform.Engine}

	// Get the instance ID by calling ForTestingForceSyncInstance (it's idempotent)
	id, err := erc20shim.ForTestingForceSyncInstance(context.Background(), app, "sepolia", escrowAddr, "0x2222222222222222222222222222222222222222", 18)
	if err != nil {
		t.Logf("Warning: failed to get instance ID for deactivation: %v", err)
		return
	}

	// Call the meta extension's disable method to set active=false
	txCtx := &common.TxContext{
		Ctx:          context.Background(),
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       txPlatform.Deployer,
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         txPlatform.Txid(),
	}
	engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}

	// Call the meta extension's disable method
	_, err = txPlatform.Engine.Call(engCtx, txPlatform.DB, "kwil_erc20_meta", "disable", []any{id}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		t.Logf("Warning: failed to deactivate instance %s: %v", id, err)
		// Don't fail the test for cleanup issues, just log
	}

	// Reset and re-initialize the singleton to reflect the deactivated state
	erc20shim.ForTestingResetSingleton()
	err = erc20shim.ForTestingInitializeExtension(context.Background(), app)
	if err != nil {
		t.Logf("Warning: failed to re-initialize singleton after deactivation: %v", err)
		// Don't fail the test for cleanup issues, just log
	}
}
