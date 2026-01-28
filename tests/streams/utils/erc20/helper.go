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

		if err := erc20shim.ForTestingSeedAndActivateInstance(
			context.Background(), txPlatform, chain, escrowAddr,
			"0x2222222222222222222222222222222222222222", 18, 60, alias,
		); err != nil {
			t.Fatalf("seed/activate ERC20 instance failed: %v", err)
		}
	}
}

// CreditUserBalance injects a synthetic escrow deposit to credit the user's balance.
// This simulates a user depositing tokens into the bridge via the RewardDistributor contract.
func CreditUserBalance(ctx context.Context, platform *kwilTesting.Platform, extensionAlias, escrowAddr, userAddr, amount string) error {
	// Use the platform's DB and Engine (could be transaction-scoped)
	return InjectERC20Transfer(
		ctx, platform, extensionAlias, escrowAddr, "0x2222222222222222222222222222222222222222", userAddr, userAddr, amount, 10, nil)
}

// GetUserBalance queries the user's current balance via the extension.
// IMPORTANT: The balance() action requires TWO parameters: instance_id (UUID) and user_address (text).
// We compute the instance ID deterministically from the extension configuration.
func GetUserBalance(ctx context.Context, platform *kwilTesting.Platform, extensionAlias, userAddr string) (string, error) {
	// CRITICAL: Get instance ID from extension configuration
	// The balance() precompile signature is: balance(id UUID, user TEXT)
	// We need to pass the instance ID as the first parameter

	// For test extensions, we need to know the chain and escrow address
	// Extract from known test configurations
	var chainName, escrowAddr string
	switch extensionAlias {
	case "hoodi_tt":
		chainName = "hoodi"
		escrowAddr = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	case "hoodi_tt2":
		chainName = "hoodi"
		escrowAddr = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	case "sepolia_bridge":
		chainName = "sepolia"
		escrowAddr = "0x80d9b3b6941367917816d36748c88b303f7f1415"
	case "erc20_bridge_test":
		// Test-only alias used in tests/extensions/erc20/ tests
		chainName = "sepolia"
		escrowAddr = "0x1111111111111111111111111111111111111111"
	case "hoodi_bridge_test":
		// Test-only alias used in tests/extensions/erc20/erc20_bridge_hoodi_test.go
		chainName = "hoodi"
		escrowAddr = "0x3333333333333333333333333333333333333333"
	default:
		return "", fmt.Errorf("unknown extension alias: %s", extensionAlias)
	}

	// Compute instance ID deterministically (same as ForTestingForceSyncInstance)
	instanceID := erc20shim.ForTestingGetInstanceID(chainName, escrowAddr)

	txCtx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       platform.Deployer,
		Caller:       "0x0000000000000000000000000000000000000000",
		TxID:         platform.Txid(),
	}
	engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}

	var balance string
	// Pass instance ID as FIRST parameter, user address as SECOND
	r, err := platform.Engine.Call(engCtx, platform.DB, "kwil_erc20_meta", "balance", []any{instanceID, userAddr}, func(row *common.Row) error {
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

	r, err := platform.Engine.Call(engCtx, platform.DB, extensionAlias, "lock_admin", []any{userAddr, amtDec}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine call error: %w", err)
	}
	if r != nil && r.Error != nil {
		return fmt.Errorf("engine execution error: %w", r.Error)
	}
	return nil
}
