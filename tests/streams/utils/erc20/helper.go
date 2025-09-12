//go:build kwiltest

// Package erc20 provides helper functions for ERC-20 bridge testing.
// Follows the same patterns as query_test.go for consistency and compatibility.
package erc20

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

// WithERC20TestSetupTx is a transaction-aware version that works with WithTx for perfect isolation
func WithERC20TestSetup(chain, alias string, escrowAddr string) func(t *testing.T, txPlatform *kwilTesting.Platform) {
	return func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Use the transaction-scoped platform

		// Step 1: Set up cleanup to deactivate the instance after the test
		// This ensures the next test can reuse the same escrow address
		t.Cleanup(func() {
			erc20shim.ForTestingDisableInstance(context.Background(), txPlatform, chain, escrowAddr, alias)
			erc20shim.ForTestingResetSingleton()
		})

		erc20shim.ForTestingSeedAndActivateInstance(context.Background(), txPlatform, chain, escrowAddr, "0x2222222222222222222222222222222222222222", 18, 60, alias)
	}
}

// CreditUserBalance injects a realistic ERC-20 transfer to credit the user's balance.
// This simulates a user depositing tokens into the bridge.
func CreditUserBalance(ctx context.Context, platform *kwilTesting.Platform, extensionAlias, escrowAddr, userAddr, amount string) error {
	// Use the platform's DB and Engine (could be transaction-scoped)
	return InjectERC20Transfer(
		ctx, platform, extensionAlias, escrowAddr, "0x2222222222222222222222222222222222222222", userAddr, escrowAddr, amount, 10, nil)
}

// GetUserBalance queries the user's current balance via the extension.
func GetUserBalance(ctx context.Context, platform *kwilTesting.Platform, extensionAlias, userAddr string) (string, error) {
	txCtx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       platform.Deployer,
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         platform.Txid(),
	}
	engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}

	var balance string
	r, err := platform.Engine.Call(engCtx, platform.DB, extensionAlias, "balance", []any{userAddr}, func(row *common.Row) error {
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
func CallLockAdmin(ctx context.Context, platform *kwilTesting.Platform, extensionAlias, userAddr, amount string) error {
	txCtx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       platform.Deployer,
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         platform.Txid(),
	}
	engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}
	// Convert amount string to Decimal using big.Int
	bn := new(big.Int)
	if _, ok := bn.SetString(amount, 10); !ok {
		return fmt.Errorf("failed to parse amount: %s", amount)
	}
	amtDec, err := types.NewDecimalFromBigInt(bn, 0)
	if err != nil {
		return fmt.Errorf("failed to create decimal: %w", err)
	}
	amtDec.SetPrecisionAndScale(78, 0)

	_, err = platform.Engine.Call(engCtx, platform.DB, extensionAlias, "lock_admin", []any{userAddr, amtDec}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine call error: %w", err)
	}
	return nil
}
