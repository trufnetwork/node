//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants for Hoodi bridge (no withdrawal fees)
const (
	testHoodiChain         = "hoodi"
	testHoodiEscrow        = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7" // Real Hoodi bridge proxy
	testHoodiERC20         = "0x263ce78fef26600e4e428cebc91c2a52484b4fbf" // Real TRUF token on Hoodi
	testHoodiExtensionName = "hoodi_tt"                                  // Extension name from migrations
)

var (
	hoodiPointCounter int64  = 6000  // Start from 6000, increment for each balance injection
	hoodiPrevPoint    *int64         // Track previous point for deposit chaining
)

func mustParseHoodiBigInt(s string) *big.Int {
	val := new(big.Int)
	_, ok := val.SetString(s, 10)
	if !ok {
		panic(fmt.Sprintf("failed to parse big.Int: %q", s))
	}
	return val
}

// TestHoodiWithdrawalNoFees is the main test suite for Hoodi bridge withdrawals (no fees)
// This test validates the hoodi_tt_bridge_tokens action defined in 001-actions.sql
func TestHoodiWithdrawalNoFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "HOODI_WITHDRAWAL_NOFEE01_NoWithdrawalFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupHoodiWithdrawalTestEnvironment(t),
			testHoodiWithdrawalNoFee(t),
			testHoodiWithdrawalInsufficientBalance(t),
			testHoodiWithdrawalLeaderReceivesNoFees(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupHoodiWithdrawalTestEnvironment sets up the Hoodi bridge test environment
func setupHoodiWithdrawalTestEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset the previous point tracker for deposit chaining
		hoodiPrevPoint = nil

		// Use the system admin address (derived from private key 0x00...01)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		// Sync and initialize Hoodi bridge extension
		// The hoodi_tt instance is created by migrations (000-extension.sql)
		// ForTestingForceSyncInstance ensures it's synced in DB
		_, err := erc20shim.ForTestingForceSyncInstance(
			ctx,
			platform,
			testHoodiChain,
			testHoodiEscrow,
			testHoodiERC20,
			18, // TRUF decimals
		)
		if err != nil {
			return fmt.Errorf("failed to sync Hoodi bridge instance: %w", err)
		}

		// Initialize extension to load instances into singleton
		// This is CRITICAL - without this, lock()/unlock() will fail with "not synced"
		err = erc20shim.ForTestingInitializeExtension(ctx, platform)
		if err != nil {
			return fmt.Errorf("failed to initialize extension: %w", err)
		}

		return nil
	}
}

// Test 1: hoodi_tt_bridge_tokens charges no fee
func testHoodiWithdrawalNoFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Re-initialize extension in this test (singleton might have been reset)
		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xc111111111111111111111111111111111111111")
		userAddr := &userAddrVal

		// Give user 100 TRUF
		err = giveHoodiBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getHoodiBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		t.Logf("DEBUG: User balance after deposit: %s", initialBalance.String())
		require.Equal(t, "100000000000000000000", initialBalance.String(), "initial balance should be 100 TRUF (got %s)", initialBalance.String())

		// Generate leader
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Withdraw 10 TRUF (should deduct only 10 TRUF, no fee)
		withdrawAmount := "10000000000000000000" // 10 TRUF
		err = executeHoodiWithdrawalWithLeader(ctx, platform, userAddr, pub, userAddr.Address(), withdrawAmount)
		require.NoError(t, err, "hoodi_tt_bridge_tokens should succeed")

		// Verify balance decreased by exactly 10 TRUF (no fee)
		finalBalance, err := getHoodiBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		withdrawAmountBig, _ := new(big.Int).SetString(withdrawAmount, 10)
		expectedBalance := new(big.Int).Sub(initialBalance, withdrawAmountBig)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by exactly 10 TRUF (no fee), expected %s but got %s",
			expectedBalance, finalBalance)

		t.Logf("✅ hoodi_tt_bridge_tokens correctly deducted only 10 TRUF (no fee)")
		return nil
	}
}

// Test 2: Insufficient balance for withdrawal fails
func testHoodiWithdrawalInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Re-initialize extension in this test (singleton might have been reset)
		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xc222222222222222222222222222222222222222")
		userAddr := &userAddrVal

		// Give user only 5 TRUF (insufficient for 10 TRUF withdrawal)
		err = giveHoodiBalance(ctx, platform, userAddr.Address(), "5000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Generate leader
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Try to withdraw 10 TRUF (should fail - only has 5 TRUF)
		withdrawAmount := "10000000000000000000" // 10 TRUF
		err = executeHoodiWithdrawalWithLeader(ctx, platform, userAddr, pub, userAddr.Address(), withdrawAmount)
		require.Error(t, err, "hoodi_tt_bridge_tokens should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for withdrawal",
			"error should mention insufficient balance, got: %v", err)

		t.Logf("✅ hoodi_tt_bridge_tokens correctly rejects insufficient balance (5 TRUF < 10 TRUF needed)")
		return nil
	}
}

// Test 3: Leader receives no fees
func testHoodiWithdrawalLeaderReceivesNoFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Re-initialize extension in this test (singleton might have been reset)
		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xc333333333333333333333333333333333333333")
		userAddr := &userAddrVal

		// Give user 100 TRUF
		// Reset deposit chain for this test to avoid cross-test interference
		oldPrevPoint := hoodiPrevPoint
		hoodiPrevPoint = nil
		t.Logf("DEBUG test3: Reset prev point from %v to nil", oldPrevPoint)
		err = giveHoodiBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Check balance
		bal, err := getHoodiBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get balance")
		t.Logf("DEBUG test3: User balance after deposit: %s (expected 100 TRUF)", bal.String())

		// Generate leader keys
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Get leader address
		leaderSigner := crypto.EthereumAddressFromPubKey(pub)
		leaderAddr := fmt.Sprintf("0x%x", leaderSigner)

		// Get initial leader balance (should be 0)
		initialLeaderBalance, err := getHoodiBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get initial leader balance")

		// Withdraw 10 TRUF with specific leader
		withdrawAmount := "10000000000000000000" // 10 TRUF
		err = executeHoodiWithdrawalWithLeader(ctx, platform, userAddr, pub, userAddr.Address(), withdrawAmount)
		require.NoError(t, err, "hoodi_tt_bridge_tokens with leader should succeed")

		// Verify leader balance stays at 0 (no fee transfer)
		finalLeaderBalance, err := getHoodiBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get final leader balance")

		require.Equal(t, 0, initialLeaderBalance.Cmp(finalLeaderBalance),
			"Leader should receive no fee, balance should stay %s but got %s",
			initialLeaderBalance, finalLeaderBalance)

		t.Logf("✅ Leader correctly received no fee (balance: %s → %s)",
			initialLeaderBalance, finalLeaderBalance)
		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// giveHoodiBalance credits TRUF balance to a wallet using ERC20 inject with deposit chaining
func giveHoodiBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	hoodiPointCounter++
	currentPoint := hoodiPointCounter

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testHoodiChain,
		testHoodiEscrow,
		testHoodiERC20,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		hoodiPrevPoint, // Chain to previous deposit (nil for first)
	)

	if err == nil {
		// Update previous point for next deposit
		p := currentPoint
		hoodiPrevPoint = &p
	}

	return err
}

// getHoodiBalance retrieves the TRUF balance for a wallet
func getHoodiBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testHoodiExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

// callHoodiWithdrawalAction calls the hoodi_tt_bridge_tokens action
func callHoodiWithdrawalAction(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, recipient string, amount string) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   2, // Use height 2 to ensure it's after initial setup
			Proposer: leaderPub,
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	// Call hoodi_tt_bridge_tokens action (defined in 001-actions.sql)
	res, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"",
		"hoodi_tt_bridge_tokens", // Hoodi TT bridge action (no fees)
		[]any{recipient, amount},
		func(row *common.Row) error { return nil },
	)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

// executeHoodiWithdrawalWithLeader executes a withdrawal with a specific leader
func executeHoodiWithdrawalWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, recipient string, amount string) error {
	return callHoodiWithdrawalAction(ctx, platform, signer, leaderPub, recipient, amount)
}
