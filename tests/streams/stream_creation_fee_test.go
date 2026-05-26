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
	"github.com/trufnetwork/node/tests/streams/utils/feefund"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants — must match the `hoodi_tt` USE block in
// erc20-bridge/000-extension.sql. The fee-collection actions (001/003/004)
// call hoodi_tt.balance / hoodi_tt.transfer directly, so this test helper
// credits balance into the matching bridge instance.
const (
	testChain         = "hoodi"
	testEscrow        = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testERC20         = "0x2222222222222222222222222222222222222222"
	testExtensionName = "hoodi_tt"
)

var (
	// perStreamFee is parsed from feefund.StreamCreationFeeWei — the same
	// constant the migration uses, so a fee-schedule change in one place
	// can't drift from test assertions silently. Per issue #3971 create_streams
	// charges 100 TRUF per stream, universally (no role exemption).
	perStreamFee       = mustParseBigInt(feefund.StreamCreationFeeWei)
	pointCounter int64 = 10 // Start from 10, increment for each balance injection
)

func mustParseBigInt(s string) *big.Int {
	val, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(fmt.Sprintf("mustParseBigInt: invalid integer string %q", s))
	}
	return val
}

// TestStreamCreationFees is the main test suite for stream creation transaction fees
func TestStreamCreationFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "STREAM_FEE01_StreamCreationFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTestEnvironment(t),
			testWriterRolePaysFee(t),
			testNonExemptWalletPaysFee(t),
			testInsufficientBalance(t),
			testFeeIndependentOfRole(t),
			testBatchChargesPerStreamFee(t),
			testLeaderReceivesFees(t),
			testUnenrolledWalletStillCharged(t),
		testPermissionlessOnboarding(t),
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

// Test 1: Wallet with network_writer role pays the create_streams per-stream fee.
// The role carries no exemption — funded callers always pay 100 TRUF per stream.
func testWriterRolePaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		writerAddrVal := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		writerAddr := &writerAddrVal

		// Register as data provider (also grants network_writer role).
		err := setup.CreateDataProvider(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to create data provider")

		// Fund wallet with 200 TRUF so the single-stream create fee can be paid.
		err = giveBalance(ctx, platform, writerAddr.Address(), "200000000000000000000")
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		err = createStream(ctx, platform, writerAddr, "st000000000000000000000000000001", "primitive")
		require.NoError(t, err, "stream creation should succeed for funded network_writer")

		finalBalance, err := getBalance(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, perStreamFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"network_writer should pay 100 TRUF per stream, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 2: Non-whitelisted wallet pays the per-stream fee.
// No role gating exists for create_streams — every caller pays 100 TRUF/stream.
func testNonExemptWalletPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted).
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user 200 TRUF (enough for a single-stream create).
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Create stream
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000002", "primitive")
		require.NoError(t, err, "stream creation should succeed")

		// Verify balance decreased by 100 TRUF
		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, perStreamFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 100 TRUF, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 3: Insufficient balance fails (caller has < 100 TRUF, can't pay per-stream fee).
func testInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted).
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user 50 TRUF (insufficient for the 100 TRUF per-stream fee).
		err = giveBalance(ctx, platform, userAddr.Address(), "50000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Try to create stream (should fail)
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000003", "primitive")
		require.Error(t, err, "stream creation should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for stream creation",
			"error should mention insufficient balance")

		return nil
	}
}

// Test 4: network_writer role grant/revoke does NOT change the create_streams fee.
// Every call charges 100 TRUF per stream regardless of role membership.
func testFeeIndependentOfRole(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		userAddr := &userAddrVal

		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give 500 TRUF — three single-stream creates @ 100 TRUF each.
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Without role: charges 100 TRUF.
		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000004", "primitive")
		require.NoError(t, err, "first stream creation should succeed")

		balanceAfterFirst, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterFirst := new(big.Int).Sub(initialBalance, perStreamFee)
		require.Equal(t, 0, expectedAfterFirst.Cmp(balanceAfterFirst), "first create should charge 100 TRUF")

		// Grant role — must NOT exempt.
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", userAddr.Address())
		require.NoError(t, err, "failed to grant role")

		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000005", "primitive")
		require.NoError(t, err, "second stream creation should succeed")

		balanceAfterSecond, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		expectedAfterSecond := new(big.Int).Sub(balanceAfterFirst, perStreamFee)
		require.Equal(t, 0, expectedAfterSecond.Cmp(balanceAfterSecond),
			"network_writer must still pay the 100 TRUF fee — no exemption")

		// Revoke role — fee unchanged.
		err = revokeRoleBypass(ctx, platform, "system", "network_writer", userAddr.Address())
		require.NoError(t, err, "failed to revoke role")

		err = createStream(ctx, platform, userAddr, "st000000000000000000000000000006", "primitive")
		require.NoError(t, err, "third stream creation should succeed")

		balanceAfterThird, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterThird := new(big.Int).Sub(balanceAfterSecond, perStreamFee)
		require.Equal(t, 0, expectedAfterThird.Cmp(balanceAfterThird),
			"third create should charge 100 TRUF (role revoked, fee unchanged)")

		return nil
	}
}

// Test 5: Batched create_streams charges N × perStreamFee (issue #3971).
// Pricing is per-stream, not per-tx — three streams = 300 TRUF.
func testBatchChargesPerStreamFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted).
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give user 500 TRUF (more than the 300 TRUF needed for 3 streams).
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
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

		// Verify balance decreased by N × perStreamFee for the whole batch.
		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		totalFee := new(big.Int).Mul(perStreamFee, big.NewInt(int64(len(streamIds))))
		expectedBalance := new(big.Int).Sub(initialBalance, totalFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Batch of %d streams charges N × 100 TRUF (per-stream, not per-tx); expected %s but got %s",
			len(streamIds), expectedBalance, finalBalance)

		return nil
	}
}

// Test 6: Leader receives the per-stream fee.
func testLeaderReceivesFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role (non-whitelisted).
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Setup leader
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)
		leaderSigner := crypto.EthereumAddressFromPubKey(pub)
		leaderAddr := fmt.Sprintf("0x%x", leaderSigner)

		// Give user 200 TRUF (covers one create at 100 TRUF).
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
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

		// Verify leader balance increased by 100 TRUF
		finalLeaderBalance, err := getBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get final leader balance")

		expectedLeaderBalance := new(big.Int).Add(initialLeaderBalance, perStreamFee)
		require.Equal(t, 0, expectedLeaderBalance.Cmp(finalLeaderBalance),
			"Leader should receive 100 TRUF fee, expected %s but got %s", expectedLeaderBalance, finalLeaderBalance)

		return nil
	}
}

// Test 7: A wallet not enrolled in system:fee_required is still charged.
// Regression check that the phased-rollout exemption has been removed
// from create_streams (issue #3971 universal charging).
func testUnenrolledWalletStillCharged(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")
		userAddr := &userAddrVal

		// Register data provider WITHOUT role. NOT enrolled in fee_required.
		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Fund with 200 TRUF — enough to cover one 100-TRUF create.
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		err = createStream(ctx, platform, userAddr, "st00000000000000000000000000000b", "primitive")
		require.NoError(t, err, "wallet should be able to create stream when funded")

		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, perStreamFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Un-enrolled wallet must still be charged 100 TRUF — exemption removed; expected %s but got %s",
			expectedBalance, finalBalance)

		return nil
	}
}

// Test 8: A wallet with NO data_providers row and NO system:network_writer role
// can create streams — the action auto-registers the data provider on first call.
// Second call from the same wallet succeeds via the ON CONFLICT DO NOTHING path.
func testPermissionlessOnboarding(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		userAddr := &userAddrVal

		// Fund with 300 TRUF — enough for two single-stream creates.
		// Deliberately skip CreateDataProvider / CreateDataProviderWithoutRole.
		err := giveBalance(ctx, platform, userAddr.Address(), "300000000000000000000")
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// First create: auto-registers this wallet as a data provider.
		err = createStream(ctx, platform, userAddr, "st00000000000000000000000000000c", "primitive")
		require.NoError(t, err, "first stream creation should succeed without prior data provider registration")

		dpCount, err := countDataProviders(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to count data providers after first create")
		require.Equal(t, 1, dpCount, "exactly one data_providers row should exist after first create")

		balanceAfterFirst, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get balance after first create")

		expectedAfterFirst := new(big.Int).Sub(initialBalance, perStreamFee)
		require.Equal(t, 0, expectedAfterFirst.Cmp(balanceAfterFirst),
			"first create should charge 100 TRUF, expected %s but got %s", expectedAfterFirst, balanceAfterFirst)

		// Second create: data provider already exists — ON CONFLICT DO NOTHING path.
		err = createStream(ctx, platform, userAddr, "st00000000000000000000000000000d", "primitive")
		require.NoError(t, err, "second stream creation should succeed (ON CONFLICT path)")

		dpCount, err = countDataProviders(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to count data providers after second create")
		require.Equal(t, 1, dpCount, "data_providers row count must remain 1 after ON CONFLICT path")

		balanceAfterSecond, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get balance after second create")

		expectedAfterSecond := new(big.Int).Sub(balanceAfterFirst, perStreamFee)
		require.Equal(t, 0, expectedAfterSecond.Cmp(balanceAfterSecond),
			"second create should charge another 100 TRUF, expected %s but got %s", expectedAfterSecond, balanceAfterSecond)

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

// countDataProviders returns the number of data_providers rows matching the given address.
func countDataProviders(ctx context.Context, platform *kwilTesting.Platform, address string) (int, error) {
	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:          ctx,
			BlockContext: &common.BlockContext{Height: 0},
			TxID:         platform.Txid(),
			Signer:       []byte("system"),
			Caller:       "0x0000000000000000000000000000000000000000",
		},
		OverrideAuthz: true,
	}

	var count int
	err := platform.Engine.Execute(engineCtx, platform.DB,
		`SELECT COUNT(*) AS cnt FROM data_providers WHERE address = $addr`,
		map[string]any{"$addr": address},
		func(row *common.Row) error {
			count = int(row.Values[0].(int64))
			return nil
		},
	)
	return count, err
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
