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

// Test constants for both Hoodi bridges
const (
	// hoodi_tt bridge (first bridge)
	testTTChain         = "hoodi"
	testTTEscrow        = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testTTERC20         = "0x263ce78fef26600e4e428cebc91c2a52484b4fbf"
	testTTExtensionName = "hoodi_tt"

	// hoodi_tt2 bridge (second bridge)
	// Must match the escrow address in migrations/erc20-bridge/000-extension.sql
	testTT2Chain         = "hoodi"
	testTT2Escrow        = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	testTT2ERC20         = "0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd"
	testTT2ExtensionName = "hoodi_tt2"
)

var (
	// Separate counters and prev pointers for each bridge
	// Each bridge has its own ordered-sync topic (per escrow address)
	ttPointCounter  int64  = 7000
	tt2PointCounter int64  = 8000
	ttPrevPoint     *int64
	tt2PrevPoint    *int64
)

// TestHoodiBridgeCoexistence verifies that hoodi_tt and hoodi_tt2 can co-exist without interfering
func TestHoodiBridgeCoexistence(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "HOODI_COEXIST01_BridgeCoexistence",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupBridgeCoexistenceEnvironment(t),
			testTTDepositOnlyAffectsTTBalance(t),
			testTT2DepositOnlyAffectsTT2Balance(t),
			testWithdrawalsAreIsolated(t),
			testCrossContamination(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupBridgeCoexistenceEnvironment sets up both Hoodi bridge instances
func setupBridgeCoexistenceEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point trackers for fresh test
		ttPrevPoint = nil
		tt2PrevPoint = nil

		// Use the system admin address
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		// Sync hoodi_tt bridge instance
		_, err := erc20shim.ForTestingForceSyncInstance(
			ctx,
			platform,
			testTTChain,
			testTTEscrow,
			testTTERC20,
			18, // decimals
		)
		if err != nil {
			return fmt.Errorf("failed to sync hoodi_tt instance: %w", err)
		}

		// Sync hoodi_tt2 bridge instance
		_, err = erc20shim.ForTestingForceSyncInstance(
			ctx,
			platform,
			testTT2Chain,
			testTT2Escrow,
			testTT2ERC20,
			18, // decimals
		)
		if err != nil {
			return fmt.Errorf("failed to sync hoodi_tt2 instance: %w", err)
		}

		// Initialize extension to load both instances into singleton
		err = erc20shim.ForTestingInitializeExtension(ctx, platform)
		if err != nil {
			return fmt.Errorf("failed to initialize extension: %w", err)
		}

		return nil
	}
}

// Test 1: Deposit to TT only affects TT balance
func testTTDepositOnlyAffectsTTBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point trackers - each test runs in isolated container with fresh DB
		ttPrevPoint = nil
		tt2PrevPoint = nil

		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xc555555555555555555555555555555555555555")
		userAddr := &userAddrVal

		// Give user 100 TT tokens via hoodi_tt bridge
		err = giveTTBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give TT balance")

		// Check TT balance (should be 100)
		ttBalance, err := getTTBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT balance")
		require.Equal(t, "100000000000000000000", ttBalance.String(), "TT balance should be 100 tokens")

		// Check TT2 balance (should be 0)
		tt2Balance, err := getTT2Balance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT2 balance")
		require.Equal(t, "0", tt2Balance.String(), "TT2 balance should be 0 tokens")

		t.Logf("✅ TT deposit correctly affected only TT balance (TT: 100, TT2: 0)")
		return nil
	}
}

// Test 2: Deposit to TT2 only affects TT2 balance
func testTT2DepositOnlyAffectsTT2Balance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point trackers - each test runs in isolated container with fresh DB
		ttPrevPoint = nil
		tt2PrevPoint = nil

		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xc666666666666666666666666666666666666666")
		userAddr := &userAddrVal

		// Give user 200 TT2 tokens via hoodi_tt2 bridge
		err = giveTT2Balance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err, "failed to give TT2 balance")

		// Check TT2 balance (should be 200)
		tt2Balance, err := getTT2Balance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT2 balance")
		require.Equal(t, "200000000000000000000", tt2Balance.String(), "TT2 balance should be 200 tokens")

		// Check TT balance (should be 0)
		ttBalance, err := getTTBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT balance")
		require.Equal(t, "0", ttBalance.String(), "TT balance should be 0 tokens")

		t.Logf("✅ TT2 deposit correctly affected only TT2 balance (TT: 0, TT2: 200)")
		return nil
	}
}

// Test 3: Withdrawals are isolated between bridges
func testWithdrawalsAreIsolated(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point trackers - each test runs in isolated container with fresh DB
		ttPrevPoint = nil
		tt2PrevPoint = nil

		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xc777777777777777777777777777777777777777")
		userAddr := &userAddrVal

		// Give user 100 TT and 100 TT2
		err = giveTTBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give TT balance")

		err = giveTT2Balance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give TT2 balance")

		// Verify initial balances
		ttBalance, err := getTTBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT balance")
		tt2Balance, err := getTT2Balance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT2 balance")
		require.Equal(t, "100000000000000000000", ttBalance.String())
		require.Equal(t, "100000000000000000000", tt2Balance.String())

		// Generate leader
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Withdraw 10 TT tokens
		err = executeTTWithdrawal(ctx, platform, userAddr, pub, userAddr.Address(), "10000000000000000000")
		require.NoError(t, err, "TT withdrawal should succeed")

		// Verify TT balance decreased by 10, TT2 unchanged
		ttBalanceAfter, err := getTTBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT balance after withdrawal")
		tt2BalanceAfter, err := getTT2Balance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get TT2 balance after withdrawal")
		require.Equal(t, "90000000000000000000", ttBalanceAfter.String(), "TT balance should be 90")
		require.Equal(t, "100000000000000000000", tt2BalanceAfter.String(), "TT2 balance should remain 100")

		// Withdraw 20 TT2 tokens
		err = executeTT2Withdrawal(ctx, platform, userAddr, pub, userAddr.Address(), "20000000000000000000")
		require.NoError(t, err, "TT2 withdrawal should succeed")

		// Verify TT2 balance decreased by 20, TT unchanged
		ttBalanceFinal, err := getTTBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final TT balance")
		tt2BalanceFinal, err := getTT2Balance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final TT2 balance")
		require.Equal(t, "90000000000000000000", ttBalanceFinal.String(), "TT balance should remain 90")
		require.Equal(t, "80000000000000000000", tt2BalanceFinal.String(), "TT2 balance should be 80")

		t.Logf("✅ Withdrawals correctly isolated (TT: 100→90, TT2: 100→80)")
		return nil
	}
}

// Test 4: Cross-contamination test - operations on one bridge don't affect the other
func testCrossContamination(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset point trackers - each test runs in isolated container with fresh DB
		ttPrevPoint = nil
		tt2PrevPoint = nil

		err := erc20shim.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to re-initialize extension")

		user1AddrVal := util.Unsafe_NewEthereumAddressFromString("0xc888888888888888888888888888888888888888")
		user1Addr := &user1AddrVal

		user2AddrVal := util.Unsafe_NewEthereumAddressFromString("0xc999999999999999999999999999999999999999")
		user2Addr := &user2AddrVal

		// User1: deposit 50 TT
		err = giveTTBalance(ctx, platform, user1Addr.Address(), "50000000000000000000")
		require.NoError(t, err)

		// User2: deposit 75 TT2
		err = giveTT2Balance(ctx, platform, user2Addr.Address(), "75000000000000000000")
		require.NoError(t, err)

		// Verify User1 has 50 TT and 0 TT2
		user1TT, err := getTTBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err, "failed to get User1 TT balance")
		user1TT2, err := getTT2Balance(ctx, platform, user1Addr.Address())
		require.NoError(t, err, "failed to get User1 TT2 balance")
		require.Equal(t, "50000000000000000000", user1TT.String())
		require.Equal(t, "0", user1TT2.String())

		// Verify User2 has 0 TT and 75 TT2
		user2TT, err := getTTBalance(ctx, platform, user2Addr.Address())
		require.NoError(t, err, "failed to get User2 TT balance")
		user2TT2, err := getTT2Balance(ctx, platform, user2Addr.Address())
		require.NoError(t, err, "failed to get User2 TT2 balance")
		require.Equal(t, "0", user2TT.String())
		require.Equal(t, "75000000000000000000", user2TT2.String())

		// Generate leader
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// User1 withdraws 5 TT
		err = executeTTWithdrawal(ctx, platform, user1Addr, pub, user1Addr.Address(), "5000000000000000000")
		require.NoError(t, err)

		// Verify User1's TT decreased, User2's TT2 unchanged
		user1TTAfter, err := getTTBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err, "failed to get User1 TT balance after withdrawal")
		user2TT2After, err := getTT2Balance(ctx, platform, user2Addr.Address())
		require.NoError(t, err, "failed to get User2 TT2 balance after withdrawal")
		require.Equal(t, "45000000000000000000", user1TTAfter.String(), "User1 TT should be 45")
		require.Equal(t, "75000000000000000000", user2TT2After.String(), "User2 TT2 should remain 75")

		// User2 withdraws 10 TT2
		err = executeTT2Withdrawal(ctx, platform, user2Addr, pub, user2Addr.Address(), "10000000000000000000")
		require.NoError(t, err)

		// Verify User2's TT2 decreased, User1's TT unchanged
		user1TTFinal, err := getTTBalance(ctx, platform, user1Addr.Address())
		require.NoError(t, err, "failed to get final User1 TT balance")
		user2TT2Final, err := getTT2Balance(ctx, platform, user2Addr.Address())
		require.NoError(t, err, "failed to get final User2 TT2 balance")
		require.Equal(t, "45000000000000000000", user1TTFinal.String(), "User1 TT should remain 45")
		require.Equal(t, "65000000000000000000", user2TT2Final.String(), "User2 TT2 should be 65")

		t.Logf("✅ No cross-contamination detected (User1 TT: 50→45, User2 TT2: 75→65)")
		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// TT Bridge helpers
func giveTTBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	ttPointCounter++
	currentPoint := ttPointCounter

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTTChain,
		testTTEscrow,
		testTTERC20,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		ttPrevPoint,
	)

	if err == nil {
		p := currentPoint
		ttPrevPoint = &p
	}

	return err
}

func getTTBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testTTExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

func executeTTWithdrawal(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, recipient string, amount string) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   2,
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
		"hoodi_tt_bridge_tokens",
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

// TT2 Bridge helpers
func giveTT2Balance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	tt2PointCounter++
	currentPoint := tt2PointCounter

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTT2Chain,
		testTT2Escrow,
		testTT2ERC20,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		tt2PrevPoint,
	)

	if err == nil {
		p := currentPoint
		tt2PrevPoint = &p
	}

	return err
}

func getTT2Balance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testTT2ExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

func executeTT2Withdrawal(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, recipient string, amount string) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   2,
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
		"hoodi_tt2_bridge_tokens",
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
