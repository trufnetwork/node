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

// Test constants for withdrawal fees
const (
	testWithdrawalChain         = "sepolia"
	testWithdrawalEscrow        = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	testWithdrawalERC20         = "0x2222222222222222222222222222222222222222"
	testWithdrawalExtensionName = "sepolia_bridge"       // Extension name for balance queries in tests
	withdrawalFeeAmount         = "40000000000000000000" // 40 TRUF with 18 decimals
)

var (
	fortyTRUFWithdrawal          = mustParseWithdrawalBigInt(withdrawalFeeAmount) // 40 TRUF as big.Int
	withdrawalPointCounter int64 = 5000                                           // Start from 5000, increment for each balance injection
)

func mustParseWithdrawalBigInt(s string) *big.Int {
	val := new(big.Int)
	val.SetString(s, 10)
	return val
}

// TestWithdrawalFees is the main test suite for withdrawal transaction fees
func TestWithdrawalFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "WITHDRAWAL_FEE01_WithdrawalFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupWithdrawalTestEnvironment(t),
			testWithdrawalPaysFee(t),
			testWithdrawalInsufficientBalance(t),
			testWithdrawalLeaderReceivesFees(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupWithdrawalTestEnvironment sets up the test environment
func setupWithdrawalTestEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use the system admin address (derived from private key 0x00...01)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		// Sync and initialize bridge extension (alias already exists from migrations)
		// ForTestingForceSyncInstance ensures the instance exists in DB and is marked as synced
		_, err := erc20shim.ForTestingForceSyncInstance(
			ctx,
			platform,
			testWithdrawalChain,
			testWithdrawalEscrow,
			testWithdrawalERC20,
			18, // decimals
		)
		if err != nil {
			return fmt.Errorf("failed to sync bridge instance: %w", err)
		}

		// Initialize extension to load instances into singleton
		err = erc20shim.ForTestingInitializeExtension(ctx, platform)
		if err != nil {
			return fmt.Errorf("failed to initialize extension: %w", err)
		}

		return nil
	}
}

// Test 1: Withdrawal pays 40 TRUF fee
func testWithdrawalPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Re-initialize extension in this test (singleton might have been reset)
		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xb111111111111111111111111111111111111111")
		userAddr := &userAddrVal

		// Give user 100 TRUF
		err = giveWithdrawalBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getWithdrawalBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Generate leader for fee recipient
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Withdraw 10 TRUF with specific leader (should pay 40 TRUF fee, total 50 TRUF deducted)
		withdrawAmount := "10000000000000000000" // 10 TRUF
		// Use the user's own address as recipient (self-withdrawal to L1)
		err = executeWithdrawalWithLeader(ctx, platform, userAddr, pub, userAddr.Address(), withdrawAmount)
		require.NoError(t, err, "withdrawal should succeed")

		// Verify balance decreased by 50 TRUF (10 TRUF withdrawal + 40 TRUF fee)
		finalBalance, err := getWithdrawalBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		withdrawAmountBig, _ := new(big.Int).SetString(withdrawAmount, 10)
		expectedDeduction := new(big.Int).Add(withdrawAmountBig, fortyTRUFWithdrawal)
		expectedBalance := new(big.Int).Sub(initialBalance, expectedDeduction)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 50 TRUF (10 withdrawal + 40 fee), expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 2: Insufficient balance for withdrawal + fee fails
func testWithdrawalInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Re-initialize extension in this test (singleton might have been reset)
		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xb222222222222222222222222222222222222222")
		userAddr := &userAddrVal

		// Give user only 30 TRUF (insufficient for 10 TRUF withdrawal + 40 TRUF fee)
		err = giveWithdrawalBalance(ctx, platform, userAddr.Address(), "30000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Generate leader for fee recipient
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Try to withdraw 10 TRUF (should fail - needs 50 TRUF total)
		withdrawAmount := "10000000000000000000" // 10 TRUF
		err = executeWithdrawalWithLeader(ctx, platform, userAddr, pub, userAddr.Address(), withdrawAmount)
		require.Error(t, err, "withdrawal should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for withdrawal",
			"error should mention insufficient balance")

		return nil
	}
}

// Test 3: Leader receives fees correctly
func testWithdrawalLeaderReceivesFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Re-initialize extension in this test (singleton might have been reset)
		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xb333333333333333333333333333333333333333")
		userAddr := &userAddrVal

		// Give user 100 TRUF
		err = giveWithdrawalBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Generate leader keys
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Get leader address
		leaderSigner := crypto.EthereumAddressFromPubKey(pub)
		leaderAddr := fmt.Sprintf("0x%x", leaderSigner)

		// Get initial leader balance (should be 0)
		initialLeaderBalance, err := getWithdrawalBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get initial leader balance")

		// Withdraw 10 TRUF with specific leader
		withdrawAmount := "10000000000000000000" // 10 TRUF
		err = executeWithdrawalWithLeader(ctx, platform, userAddr, pub, userAddr.Address(), withdrawAmount)
		require.NoError(t, err, "withdrawal with leader should succeed")

		// Verify leader balance increased by 40 TRUF
		finalLeaderBalance, err := getWithdrawalBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get final leader balance")

		expectedLeaderBalance := new(big.Int).Add(initialLeaderBalance, fortyTRUFWithdrawal)
		require.Equal(t, 0, expectedLeaderBalance.Cmp(finalLeaderBalance),
			"Leader should receive 40 TRUF fee, expected %s but got %s", expectedLeaderBalance, finalLeaderBalance)

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// createEngineContext creates a standard EngineContext for withdrawal tests
func createEngineContext(ctx context.Context, platform *kwilTesting.Platform, signerBytes []byte, caller string, height int64) *common.EngineContext {
	// Generate a leader public key for fee collection
	_, leaderPubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		panic(fmt.Sprintf("failed to generate leader key: %v", err))
	}
	leaderPub := leaderPubGeneric.(*crypto.Secp256k1PublicKey)

	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:           ctx,
			BlockContext:  &common.BlockContext{Height: height, Proposer: leaderPub},
			Signer:        signerBytes,
			Caller:        caller,
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		},
	}
}

// giveWithdrawalBalance credits TRUF balance to a wallet using ERC20 inject
func giveWithdrawalBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	withdrawalPointCounter++
	return testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testWithdrawalChain,
		testWithdrawalEscrow,
		testWithdrawalERC20,
		wallet,
		wallet,
		amountStr,
		withdrawalPointCounter,
		nil,
	)
}

// getWithdrawalBalance retrieves the TRUF balance for a wallet
func getWithdrawalBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testWithdrawalExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

// callWithdrawalAction is the base implementation - calls the sepolia_bridge_tokens action
func callWithdrawalAction(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, recipient string, amount string) error {
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

	res, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"",
		"sepolia_bridge_tokens", // Test-specific action (ethereum_bridge_tokens also becomes this in tests)
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

// executeWithdrawal executes a withdrawal with a randomly generated leader
func executeWithdrawal(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, recipient string, amount string) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	return callWithdrawalAction(ctx, platform, signer, pub, recipient, amount)
}

// executeWithdrawalWithLeader executes a withdrawal with a specific leader (for testing fee recipient)
func executeWithdrawalWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, recipient string, amount string) error {
	return callWithdrawalAction(ctx, platform, signer, leaderPub, recipient, amount)
}
