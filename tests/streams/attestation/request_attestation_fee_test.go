//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants for attestation fees
const (
	testAttestationChain         = "sepolia"
	testAttestationEscrow        = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	testAttestationERC20         = "0x2222222222222222222222222222222222222222"
	testAttestationExtensionName = "sepolia_bridge"
	attestationFeeAmount         = "40000000000000000000" // 40 TRUF with 18 decimals
)

var (
	fortyTRUFAttest               = mustParseAttestationBigInt(attestationFeeAmount) // 40 TRUF as big.Int
	attestationPointCounter int64 = 10000                                            // Start from 10000, increment for each balance injection
)

func mustParseAttestationBigInt(s string) *big.Int {
	val := new(big.Int)
	if _, ok := val.SetString(s, 10); !ok {
		panic(fmt.Sprintf("failed to parse big int: %s", s))
	}
	return val
}

// TestRequestAttestationFees is the main test suite for request_attestation transaction fees
func TestRequestAttestationFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ATTESTATION_FEE01_RequestAttestationFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupAttestationTestEnvironment(t),
			testAttestationNetworkWriterPaysFee(t),
			testAttestationInsufficientBalance(t),
			testAttestationMultipleRequestsChargeFees(t),
			testAttestationLeaderReceivesFees(t),
			testAttestationBalanceCorrectlyDeducted(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupAttestationTestEnvironment creates system admin, registers test action, and creates test stream
func setupAttestationTestEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use the system admin address (derived from private key 0x00...01)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		// Grant network_writers_manager role so system admin can manage network_writer roles
		err := setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", systemAdmin.Address())
		if err != nil {
			return fmt.Errorf("failed to grant network_writers_manager to system admin: %w", err)
		}

		// Register system admin as data provider (this also grants network_writer role)
		err = setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		if err != nil {
			return fmt.Errorf("failed to register system admin as data provider: %w", err)
		}

		// Note: get_record is already registered in 023-attestation-schema.sql with ID=1
		// No need to register it again

		// Create a test stream for attestations
		streamID := "st000000000000000000000000000000"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		if err != nil {
			return fmt.Errorf("failed to create test stream: %w", err)
		}

		return nil
	}
}

// Test 1: Network writer member pays 40 TRUF fee per attestation request
func testAttestationNetworkWriterPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		requesterAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa111111111111111111111111111111111111111")
		requesterAddr := &requesterAddrVal

		// Register as data provider with network_writer role
		err := setup.CreateDataProvider(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give requester 100 TRUF
		err = giveAttestationBalance(ctx, platform, requesterAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getAttestationBalance(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Request attestation (should pay 40 TRUF)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		streamID := "st000000000000000000000000000000"
		err = requestAttestation(ctx, platform, requesterAddr, systemAdmin.Address(), streamID, "get_record")
		require.NoError(t, err, "attestation request should succeed")

		// Verify balance decreased by 40 TRUF
		finalBalance, err := getAttestationBalance(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, fortyTRUFAttest)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 40 TRUF, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 2: Insufficient balance causes error
func testAttestationInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		requesterAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa222222222222222222222222222222222222222")
		requesterAddr := &requesterAddrVal

		// Register as data provider with network_writer role
		err := setup.CreateDataProvider(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give requester only 10 TRUF (insufficient for 40 TRUF fee)
		err = giveAttestationBalance(ctx, platform, requesterAddr.Address(), "10000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Try to request attestation (should fail)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		streamID := "st000000000000000000000000000000"
		err = requestAttestation(ctx, platform, requesterAddr, systemAdmin.Address(), streamID, "get_record")
		require.Error(t, err, "attestation request should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for attestation",
			"error should mention insufficient balance")

		return nil
	}
}

// Test 3: Multiple attestation requests charge 40 TRUF each
func testAttestationMultipleRequestsChargeFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		requesterAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa333333333333333333333333333333333333333")
		requesterAddr := &requesterAddrVal

		// Register as data provider with network_writer role
		err := setup.CreateDataProvider(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give requester 200 TRUF (enough for 5 attestations)
		err = giveAttestationBalance(ctx, platform, requesterAddr.Address(), "200000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getAttestationBalance(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Request 3 attestations with different time ranges (to get different attestation hashes)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		streamID := "st000000000000000000000000000000"
		for i := 0; i < 3; i++ {
			// Vary the time range for each request to get different attestation hashes
			from := int64(i * 1000)
			to := int64(i*1000 + 999)
			err = requestAttestationWithTimeRange(ctx, platform, requesterAddr, systemAdmin.Address(), streamID, "get_record", from, to)
			require.NoError(t, err, "attestation request %d should succeed", i+1)
		}

		// Verify balance decreased by 120 TRUF (3 × 40)
		finalBalance, err := getAttestationBalance(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedFee := new(big.Int).Mul(fortyTRUFAttest, big.NewInt(3))
		expectedBalance := new(big.Int).Sub(initialBalance, expectedFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 120 TRUF (3 × 40), expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 4: Leader receives attestation fees correctly
func testAttestationLeaderReceivesFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		requesterAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa444444444444444444444444444444444444444")
		requesterAddr := &requesterAddrVal

		// Register as data provider with network_writer role
		err := setup.CreateDataProvider(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give requester 100 TRUF
		err = giveAttestationBalance(ctx, platform, requesterAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Generate leader keys
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Get leader address
		leaderSigner := crypto.EthereumAddressFromPubKey(pub)
		leaderAddr := fmt.Sprintf("0x%x", leaderSigner)

		// Give leader initial balance
		err = giveAttestationBalance(ctx, platform, leaderAddr, "10000000000000000000")
		require.NoError(t, err, "failed to give leader balance")

		// Get initial leader balance
		initialLeaderBalance, err := getAttestationBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get initial leader balance")

		// Request attestation with specific leader
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		streamID := "st000000000000000000000000000000"
		err = requestAttestationWithLeader(ctx, platform, requesterAddr, pub, systemAdmin.Address(), streamID, "get_record")
		require.NoError(t, err, "attestation request with leader should succeed")

		// Verify leader balance increased by 40 TRUF
		finalLeaderBalance, err := getAttestationBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get final leader balance")

		expectedLeaderBalance := new(big.Int).Add(initialLeaderBalance, fortyTRUFAttest)
		require.Equal(t, 0, expectedLeaderBalance.Cmp(finalLeaderBalance),
			"Leader should receive 40 TRUF fee, expected %s but got %s", expectedLeaderBalance, finalLeaderBalance)

		return nil
	}
}

// Test 5: Balance is correctly deducted after fee payment
func testAttestationBalanceCorrectlyDeducted(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		requesterAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa555555555555555555555555555555555555555")
		requesterAddr := &requesterAddrVal

		// Register as data provider with network_writer role
		err := setup.CreateDataProvider(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Give requester exactly 80 TRUF (enough for 2 attestations)
		err = giveAttestationBalance(ctx, platform, requesterAddr.Address(), "80000000000000000000")
		require.NoError(t, err, "failed to give balance")

		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		streamID := "st000000000000000000000000000000"

		// First attestation should succeed (time range 0-999)
		err = requestAttestationWithTimeRange(ctx, platform, requesterAddr, systemAdmin.Address(), streamID, "get_record", 0, 999)
		require.NoError(t, err, "first attestation should succeed")

		// Second attestation should succeed (time range 1000-1999)
		err = requestAttestationWithTimeRange(ctx, platform, requesterAddr, systemAdmin.Address(), streamID, "get_record", 1000, 1999)
		require.NoError(t, err, "second attestation should succeed")

		// Third attestation should fail (insufficient balance) (time range 2000-2999)
		err = requestAttestationWithTimeRange(ctx, platform, requesterAddr, systemAdmin.Address(), streamID, "get_record", 2000, 2999)
		require.Error(t, err, "third attestation should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for attestation",
			"error should mention insufficient balance")

		// Verify final balance is 0
		finalBalance, err := getAttestationBalance(ctx, platform, requesterAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, 0, finalBalance.Cmp(big.NewInt(0)),
			"Balance should be 0 after 2 attestations, but got %s", finalBalance)

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// registerAttestationAction registers an action in attestation_actions table
func registerAttestationAction(ctx context.Context, platform *kwilTesting.Platform, actionName string, actionID int) error {
	systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
		Signer:       systemAdmin.Bytes(),
		Caller:       systemAdmin.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	// Register action in attestation_actions table
	insertSQL := `INSERT INTO attestation_actions (action_id, action_name)
	              VALUES ($action_id, $action_name)
	              ON CONFLICT (action_id) DO NOTHING`

	err := platform.Engine.Execute(engineContext, platform.DB, insertSQL, map[string]any{
		"$action_id":   actionID,
		"$action_name": actionName,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to register attestation action: %w", err)
	}

	return nil
}

// giveAttestationBalance credits TRUF balance to a wallet using ERC20 inject
func giveAttestationBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	attestationPointCounter++
	return testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testAttestationChain,
		testAttestationEscrow,
		testAttestationERC20,
		wallet,
		wallet,
		amountStr,
		attestationPointCounter,
		nil,
	)
}

// getAttestationBalance retrieves the TRUF balance for a wallet
func getAttestationBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testAttestationExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

// callRequestAttestationAction is the base implementation - calls the request_attestation action
func callRequestAttestationAction(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, dataProvider string, streamID string, actionName string) error {
	return callRequestAttestationActionWithTimeRange(ctx, platform, signer, leaderPub, dataProvider, streamID, actionName, int64(0), int64(99999))
}

// callRequestAttestationActionWithTimeRange is the base implementation with custom time range
func callRequestAttestationActionWithTimeRange(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, dataProvider string, streamID string, actionName string, from int64, to int64) error {
	// Prepare arguments for get_record action
	// get_record($data_provider TEXT, $stream_id TEXT, $from INT8, $to INT8, $frozen_at INT8, $use_cache BOOL)
	actionArgs := []any{
		dataProvider,
		streamID,
		from,
		to,
		int64(99999), // frozen_at
		false,        // use_cache (will be forced to false by request_attestation)
	}

	// Encode action arguments
	argsBytes, err := tn_utils.EncodeActionArgs(actionArgs)
	if err != nil {
		return fmt.Errorf("failed to encode action args: %w", err)
	}

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
		"request_attestation",
		[]any{
			strings.ToLower(dataProvider),
			streamID,
			actionName,
			argsBytes,
			false,    // encrypt_sig
			int64(0), // max_fee (unused)
		},
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

// requestAttestation requests attestation with a randomly generated leader
func requestAttestation(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, dataProvider string, streamID string, actionName string) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	return callRequestAttestationAction(ctx, platform, signer, pub, dataProvider, streamID, actionName)
}

// requestAttestationWithTimeRange requests attestation with specific time range and randomly generated leader
func requestAttestationWithTimeRange(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, dataProvider string, streamID string, actionName string, from int64, to int64) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	return callRequestAttestationActionWithTimeRange(ctx, platform, signer, pub, dataProvider, streamID, actionName, from, to)
}

// requestAttestationWithLeader requests attestation with a specific leader
func requestAttestationWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, dataProvider string, streamID string, actionName string) error {
	return callRequestAttestationAction(ctx, platform, signer, leaderPub, dataProvider, streamID, actionName)
}
