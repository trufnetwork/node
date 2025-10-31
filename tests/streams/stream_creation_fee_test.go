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
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants - must match erc20-bridge/000-extension.sql configuration
const (
	testChain         = "sepolia"
	testEscrow        = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3" // From erc20-bridge/000-extension.sql
	testERC20         = "0x2222222222222222222222222222222222222222"
	testExtensionName = "sepolia_bridge"
	feeAmount         = "2000000000000000000" // 2 TRUF with 18 decimals
)

var (
	twoTRUF            = mustParseBigInt(feeAmount) // 2 TRUF as big.Int
	pointCounter int64 = 10                         // Start from 10, increment for each balance injection
)

func mustParseBigInt(s string) *big.Int {
	val := new(big.Int)
	val.SetString(s, 10)
	return val
}

// TestStreamCreationFees is the main test suite for stream creation transaction fees
func TestStreamCreationFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "STREAM_FEE01_StreamCreationFees",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTestEnvironment(t),
			testExemptWalletNoFee(t),
			testNonExemptWalletPaysFee(t),
			testInsufficientBalance(t),
			testRoleChangeAffectsFee(t),
			testBatchCreationPerStreamFee(t),
			testLeaderReceivesFees(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupTestEnvironment grants network_writer role to a default test address (like Taskfile.yml does)
// Note: ERC20 bridge is already initialized by 0_test_only/bootstrap_erc20.sql
//
// Data Provider Creation Scenarios in Tests:
// 1. setup.CreateDataProvider() - Whitelisted (with network_writer role) - NO FEES
// 2. setup.CreateDataProviderWithoutRole() - Non-whitelisted (without role) - PAYS FEES
func setupTestEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use the system admin address (derived from private key 0x00...01)
		// This address has permission to manage system roles, just like in Taskfile.yml
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		// Grant network_writers_manager role so system admin can manage network_writer roles
		// This allows the test to use procedure.RevokeRoles() without needing bypass
		err := setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", systemAdmin.Address())
		if err != nil {
			return fmt.Errorf("failed to grant network_writers_manager to system admin: %w", err)
		}

		// Also grant network_writer role to system admin (like Taskfile: grant_roles text:system text:network_writer text[]:{{.DEV_DB_OWNER}})
		// This is needed so test infrastructure can create streams for setup
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", systemAdmin.Address())
		if err != nil {
			return fmt.Errorf("failed to grant network_writer to system admin: %w", err)
		}

		return nil
	}
}

// Test 1: Exempt wallet creates stream without paying fee
func testExemptWalletNoFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		exemptAddrVal := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		exemptAddr := &exemptAddrVal

		// Register data provider (this also grants network_writer role and registers in data_providers table)
		err := setup.CreateDataProvider(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to create data provider")

		// Give balance to verify it doesn't change
		err = giveBalance(ctx, platform, exemptAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Create stream as exempt user
		err = createStream(ctx, platform, exemptAddr, "st000000000000000000000000000001", "primitive")
		require.NoError(t, err, "stream creation should succeed for exempt wallet")

		// Verify balance unchanged
		finalBalance, err := getBalance(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, initialBalance, finalBalance, "Balance should not change for exempt wallet")

		return nil
	}
}

// Test 2: Non-exempt wallet pays 2 TRUF fee
func testNonExemptWalletPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted, will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Create stream
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000002", "primitive")
		require.NoError(t, err, "stream creation should succeed")

		// Verify balance decreased by 2 TRUF
		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, twoTRUF)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 2 TRUF, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 3: Insufficient balance fails
func testInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted, will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user only 1 TRUF (insufficient)
		err = giveBalance(ctx, platform, userAddr.Address(), "1000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Try to create stream (should fail)
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000003", "primitive")
		require.Error(t, err, "stream creation should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for stream creation",
			"error should mention insufficient balance")

		return nil
	}
}

// Test 4: Role change affects fee behavior
func testRoleChangeAffectsFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted, will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// First call: User pays fee (not exempt)
		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000004", "primitive")
		require.NoError(t, err, "first stream creation should succeed")

		balanceAfterFirst, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterFirst := new(big.Int).Sub(initialBalance, twoTRUF)
		require.Equal(t, 0, expectedAfterFirst.Cmp(balanceAfterFirst), "First call should charge 2 TRUF fee")

		// Grant role
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", userAddr.Address())
		require.NoError(t, err, "failed to grant role")

		// Second call: User is now exempt
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000005", "primitive")
		require.NoError(t, err, "second stream creation should succeed")

		balanceAfterSecond, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		require.Equal(t, 0, balanceAfterFirst.Cmp(balanceAfterSecond), "Second call should not charge fee (now exempt)")

		// Revoke role using bypass (same pattern as AddMemberToRoleBypass)
		err = revokeRoleBypass(ctx, platform, "system", "network_writer", userAddr.Address())
		require.NoError(t, err, "failed to revoke role")

		// Third call: User pays fee again
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000006", "primitive")
		require.NoError(t, err, "third stream creation should succeed")

		balanceAfterThird, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterThird := new(big.Int).Sub(balanceAfterSecond, twoTRUF)
		require.Equal(t, 0, expectedAfterThird.Cmp(balanceAfterThird), "Third call should charge fee (role revoked)")

		return nil
	}
}

// Test 5: Batch stream creation charges fee per stream (not per call)
func testBatchCreationPerStreamFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted, will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Create 3 streams in batch
		streamIds := []string{
			"st000000000000000000000000000007",
			"st000000000000000000000000000008",
			"st000000000000000000000000000009",
		}
		streamTypes := []string{"primitive", "primitive", "primitive"}

		err = createStreams(ctx, platform, userAddr, streamIds, streamTypes)
		require.NoError(t, err, "batch stream creation should succeed")

		// Verify balance decreased by 6 TRUF (2 TRUF × 3 streams)
		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		numStreams := int64(len(streamIds)) // 3 streams
		expectedFee := new(big.Int).Mul(twoTRUF, big.NewInt(numStreams))
		expectedBalance := new(big.Int).Sub(initialBalance, expectedFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Batch should charge 2 TRUF per stream (3 streams = 6 TRUF), expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 6: Leader receives fees
func testLeaderReceivesFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted, will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Setup leader
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)
		leaderSigner := crypto.EthereumAddressFromPubKey(pub)
		leaderAddr := fmt.Sprintf("0x%x", leaderSigner)

		// Give user 100 TRUF
		err = giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give user balance")

		// Give leader initial balance (so we can track the increase)
		err = giveBalance(ctx, platform, leaderAddr, "10000000000000000000")
		require.NoError(t, err, "failed to give leader balance")

		// Get initial leader balance
		initialLeaderBalance, err := getBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get initial leader balance")

		// Create stream with specific leader
		err = createStreamWithLeader(ctx, platform, userAddr, pub, "st00000000000000000000000000000a", "primitive")
		require.NoError(t, err, "stream creation with leader should succeed")

		// Verify leader balance increased by 2 TRUF
		finalLeaderBalance, err := getBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get final leader balance")

		expectedLeaderBalance := new(big.Int).Add(initialLeaderBalance, twoTRUF)
		require.Equal(t, 0, expectedLeaderBalance.Cmp(finalLeaderBalance),
			"Leader should receive 2 TRUF fee, expected %s but got %s", expectedLeaderBalance, finalLeaderBalance)

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// revokeRoleBypass revokes a role using direct SQL with OverrideAuthz
// This mirrors the pattern used by setup.AddMemberToRoleBypass()
// way to manage roles in tests without complex authorization setup
func revokeRoleBypass(ctx context.Context, platform *kwilTesting.Platform, owner, roleName, wallet string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         platform.Txid(),
		Signer:       []byte("system"),
		Caller:       "0x0000000000000000000000000000000000000000",
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Skip authorization checks (test utility pattern)
	}

	sql := `DELETE FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet`

	err := platform.Engine.Execute(engineContext, platform.DB, sql, map[string]any{
		"$owner":     owner,
		"$role_name": roleName,
		"$wallet":    wallet,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to revoke role: %w", err)
	}

	return nil
}

// giveBalance credits TRUF balance to a wallet using ERC20 inject
func giveBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	pointCounter++ // Use unique point for each injection
	return testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testChain,
		testEscrow,
		testERC20,
		wallet,
		wallet,
		amountStr,
		pointCounter,
		nil,
	)
}

// getBalance retrieves the TRUF balance for a wallet
func getBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

// callCreateStreamsAction is the base implementation - calls the create_streams action
func callCreateStreamsAction(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, streamIds []string, streamTypes []string) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   1,
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
		"create_streams",
		[]any{streamIds, streamTypes},
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

// Convenience wrappers that call callCreateStreamsAction with specific parameters

// createStream creates a single stream with a randomly generated leader
func createStream(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, streamId string, streamType string) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	return callCreateStreamsAction(ctx, platform, signer, pub, []string{streamId}, []string{streamType})
}

// createStreams creates multiple streams with a randomly generated leader
func createStreams(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, streamIds []string, streamTypes []string) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	return callCreateStreamsAction(ctx, platform, signer, pub, streamIds, streamTypes)
}

// createStreamWithLeader creates a single stream with a specific leader (for testing fee recipient)
func createStreamWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, streamId string, streamType string) error {
	return callCreateStreamsAction(ctx, platform, signer, leaderPub, []string{streamId}, []string{streamType})
}
