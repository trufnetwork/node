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
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants
const (
	testInsertChain         = "sepolia"
	testInsertEscrow        = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	testInsertERC20         = "0x2222222222222222222222222222222222222222"
	testInsertExtensionName = "sepolia_bridge"
	insertFeeAmount         = "2000000000000000000" // 2 TRUF with 18 decimals per record
)

var (
	twoTRUFInsert            = mustParseInsertBigInt(insertFeeAmount) // 2 TRUF as big.Int
	insertPointCounter int64 = 1000                                   // Start from 1000, increment for each balance injection
)

func mustParseInsertBigInt(s string) *big.Int {
	val := new(big.Int)
	val.SetString(s, 10)
	return val
}

// TestInsertRecordsFees is the main test suite for insert_records transaction fees
func TestInsertRecordsFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "INSERT_FEE01_InsertRecordsFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupInsertTestEnvironment(t),
			testInsertExemptWalletNoFee(t),
			testInsertNonExemptWalletPaysFee(t),
			testInsertInsufficientBalance(t),
			testInsertRoleChangeAffectsFee(t),
			testInsertBatchChargesPerRecord(t),
			testInsertLeaderReceivesFees(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupInsertTestEnvironment creates system admin, grants roles, and creates test streams
func setupInsertTestEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
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

		// Create all test streams using setup utility (systemAdmin is exempt from fees)
		testStreams := []string{
			"st111111111111111111111111111111", // Test 1
			"st333333333333333333333333333333", // Test 2
			"st444444444444444444444444444444", // Test 3
			"st555555555555555555555555555555", // Test 4
			"st666666666666666666666666666666", // Test 5
			"st777777777777777777777777777777", // Test 6
		}

		for _, streamID := range testStreams {
			streamLocator := types.StreamLocator{
				StreamId:     util.GenerateStreamId(streamID),
				DataProvider: systemAdmin,
			}
			err = setup.CreateStream(ctx, platform, setup.StreamInfo{
				Type:    setup.ContractTypePrimitive,
				Locator: streamLocator,
			})
			if err != nil {
				return fmt.Errorf("failed to create test stream %s: %w", streamID, err)
			}

			// Verify stream was created by checking streams table
			var streamCount int
			tx := &common.TxContext{
				Ctx:          ctx,
				BlockContext: &common.BlockContext{Height: 1},
				TxID:         platform.Txid(),
				Signer:       systemAdmin.Bytes(),
				Caller:       systemAdmin.Address(),
			}
			engineCtx := &common.EngineContext{TxContext: tx}

			_, err = platform.Engine.Call(
				engineCtx,
				platform.DB,
				"",
				"get_stream_id",
				[]any{strings.ToLower(systemAdmin.Address()), streamID},
				func(row *common.Row) error {
					if len(row.Values) > 0 && row.Values[0] != nil {
						streamCount++
					}
					return nil
				},
			)
			if err != nil {
				return fmt.Errorf("failed to verify stream %s: %w", streamID, err)
			}
			if streamCount == 0 {
				return fmt.Errorf("stream %s was not created (get_stream_id returned no results)", streamID)
			}
		}

		return nil
	}
}

// Test 1: Exempt wallet inserts records without paying fee
func testInsertExemptWalletNoFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		exemptAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa111111111111111111111111111111111111111")
		exemptAddr := &exemptAddrVal

		// Register as data provider with network_writer role (this makes them exempt)
		err := setup.CreateDataProvider(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Create stream owned by exempt user (they can create without fee due to network_writer role)
		streamID := "st111111111111111111111111111111"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: *exemptAddr,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err, "failed to create stream")

		// Give balance to verify it doesn't change
		err = giveInsertBalance(ctx, platform, exemptAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getInsertBalance(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Insert 1 record as exempt user (into their own stream)
		err = insertRecord(ctx, platform, exemptAddr, exemptAddr.Address(), streamID, 1000, "10.5")
		require.NoError(t, err, "insert should succeed for exempt wallet")

		// Verify balance unchanged
		finalBalance, err := getInsertBalance(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, initialBalance, finalBalance, "Balance should not change for exempt wallet")

		return nil
	}
}

// Test 2: Non-exempt wallet pays 2 TRUF fee per record
func testInsertNonExemptWalletPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get systemAdmin who owns the stream
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

		// Register systemAdmin as data provider (with network_writer role, so exempt from fees)
		err := setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		require.NoError(t, err, "failed to register systemAdmin as data provider")

		// Create stream owned by systemAdmin
		streamID := "st333333333333333333333333333333"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err, "failed to create stream")

		// Register non-exempt user
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa333333333333333333333333333333333333333")
		userAddr := &userAddrVal
		err = setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Grant write access to user
		err = grantStreamWriteAccess(ctx, platform, systemAdmin.Address(), streamID, userAddr.Address())
		require.NoError(t, err, "failed to grant write access")

		// Give user 100 TRUF
		err = giveInsertBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Insert 1 record (data_provider is systemAdmin who owns the stream)
		err = insertRecord(ctx, platform, userAddr, systemAdmin.Address(), streamID, 1000, "10.5")
		require.NoError(t, err, "insert should succeed")

		// Verify balance decreased by 2 TRUF
		finalBalance, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, twoTRUFInsert)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 2 TRUF, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 3: Insufficient balance fails
func testInsertInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get systemAdmin who owns the stream
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

		// Register systemAdmin as data provider
		err := setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		require.NoError(t, err, "failed to register systemAdmin as data provider")

		// Create stream owned by systemAdmin
		streamID := "st444444444444444444444444444444"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err, "failed to create stream")

		// Register non-exempt user
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa444444444444444444444444444444444444444")
		userAddr := &userAddrVal
		err = setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Grant write access to user
		err = grantStreamWriteAccess(ctx, platform, systemAdmin.Address(), streamID, userAddr.Address())
		require.NoError(t, err, "failed to grant write access")

		// Give user only 1 TRUF (insufficient for 2 TRUF fee)
		err = giveInsertBalance(ctx, platform, userAddr.Address(), "1000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Try to insert record (should fail - data_provider is systemAdmin who owns the stream)
		err = insertRecord(ctx, platform, userAddr, systemAdmin.Address(), streamID, 1000, "10.5")
		require.Error(t, err, "insert should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for write fee",
			"error should mention insufficient balance")

		return nil
	}
}

// Test 4: Role change affects fee behavior
func testInsertRoleChangeAffectsFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get systemAdmin who owns the stream
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

		// Register systemAdmin as data provider
		err := setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		require.NoError(t, err, "failed to register systemAdmin as data provider")

		// Create stream owned by systemAdmin
		streamID := "st555555555555555555555555555555"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err, "failed to create stream")

		// Register non-exempt user
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa555555555555555555555555555555555555555")
		userAddr := &userAddrVal
		err = setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Grant write access to user
		err = grantStreamWriteAccess(ctx, platform, systemAdmin.Address(), streamID, userAddr.Address())
		require.NoError(t, err, "failed to grant write access")

		// Give user 100 TRUF
		err = giveInsertBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// First insert: User pays fee (not exempt)
		initialBalance, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		err = insertRecord(ctx, platform, userAddr, systemAdmin.Address(), streamID, 1000, "10.5")
		require.NoError(t, err, "first insert should succeed")

		balanceAfterFirst, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterFirst := new(big.Int).Sub(initialBalance, twoTRUFInsert)
		require.Equal(t, 0, expectedAfterFirst.Cmp(balanceAfterFirst), "First insert should charge 2 TRUF fee")

		// Grant network_writer role to make user exempt
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", userAddr.Address())
		require.NoError(t, err, "failed to grant role")

		// Second insert: User is now exempt
		err = insertRecord(ctx, platform, userAddr, systemAdmin.Address(), streamID, 1001, "11.5")
		require.NoError(t, err, "second insert should succeed")

		balanceAfterSecond, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		require.Equal(t, 0, balanceAfterFirst.Cmp(balanceAfterSecond), "Second insert should not charge fee (now exempt)")

		// Revoke network_writer role
		err = revokeInsertRoleBypass(ctx, platform, "system", "network_writer", userAddr.Address())
		require.NoError(t, err, "failed to revoke role")

		// Third insert: User pays fee again
		err = insertRecord(ctx, platform, userAddr, systemAdmin.Address(), streamID, 1002, "12.5")
		require.NoError(t, err, "third insert should succeed")

		balanceAfterThird, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedAfterThird := new(big.Int).Sub(balanceAfterSecond, twoTRUFInsert)
		require.Equal(t, 0, expectedAfterThird.Cmp(balanceAfterThird), "Third insert should charge fee (role revoked)")

		return nil
	}
}

// Test 5: Batch insert charges fee per record (not per call)
func testInsertBatchChargesPerRecord(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get systemAdmin who owns the stream
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

		// Register systemAdmin as data provider
		err := setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		require.NoError(t, err, "failed to register systemAdmin as data provider")

		// Create stream owned by systemAdmin
		streamID := "st666666666666666666666666666666"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err, "failed to create stream")

		// Register non-exempt user
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa666666666666666666666666666666666666666")
		userAddr := &userAddrVal
		err = setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Grant write access to user
		err = grantStreamWriteAccess(ctx, platform, systemAdmin.Address(), streamID, userAddr.Address())
		require.NoError(t, err, "failed to grant write access")

		// Give user 100 TRUF
		err = giveInsertBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Insert 5 records in one call (data_provider is systemAdmin who owns the stream)
		numRecords := 5
		err = insertMultipleRecords(ctx, platform, userAddr, systemAdmin.Address(), streamID, 2000, numRecords)
		require.NoError(t, err, "batch insert should succeed")

		// Verify balance decreased by 10 TRUF (5 records × 2 TRUF)
		finalBalance, err := getInsertBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		feeForFiveRecords := new(big.Int).Mul(twoTRUFInsert, big.NewInt(int64(numRecords)))
		expectedBalance := new(big.Int).Sub(initialBalance, feeForFiveRecords)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 10 TRUF (5 records × 2 TRUF), expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 6: Leader receives fees correctly
func testInsertLeaderReceivesFees(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Get systemAdmin who owns the stream
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

		// Register systemAdmin as data provider
		err := setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		require.NoError(t, err, "failed to register systemAdmin as data provider")

		// Create stream owned by systemAdmin
		streamID := "st777777777777777777777777777777"
		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err, "failed to create stream")

		// Register non-exempt user
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0xa777777777777777777777777777777777777777")
		userAddr := &userAddrVal
		err = setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to register data provider")

		// Grant write access to user
		err = grantStreamWriteAccess(ctx, platform, systemAdmin.Address(), streamID, userAddr.Address())
		require.NoError(t, err, "failed to grant write access")

		// Give user 100 TRUF
		err = giveInsertBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// Generate leader keys
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err, "failed to generate leader key")
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		// Get leader address
		leaderSigner := crypto.EthereumAddressFromPubKey(pub)
		leaderAddr := fmt.Sprintf("0x%x", leaderSigner)

		// Give leader initial balance
		err = giveInsertBalance(ctx, platform, leaderAddr, "10000000000000000000")
		require.NoError(t, err, "failed to give leader balance")

		// Get initial leader balance
		initialLeaderBalance, err := getInsertBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get initial leader balance")

		// Insert record with specific leader (data_provider is systemAdmin who owns the stream)
		err = insertRecordWithLeader(ctx, platform, userAddr, pub, systemAdmin.Address(), streamID, 3000, "15.5")
		require.NoError(t, err, "insert with leader should succeed")

		// Verify leader balance increased by 2 TRUF
		finalLeaderBalance, err := getInsertBalance(ctx, platform, leaderAddr)
		require.NoError(t, err, "failed to get final leader balance")

		expectedLeaderBalance := new(big.Int).Add(initialLeaderBalance, twoTRUFInsert)
		require.Equal(t, 0, expectedLeaderBalance.Cmp(finalLeaderBalance),
			"Leader should receive 2 TRUF fee, expected %s but got %s", expectedLeaderBalance, finalLeaderBalance)

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// revokeInsertRoleBypass revokes a role using direct SQL with OverrideAuthz
func revokeInsertRoleBypass(ctx context.Context, platform *kwilTesting.Platform, owner, roleName, wallet string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         platform.Txid(),
		Signer:       []byte("system"),
		Caller:       "0x0000000000000000000000000000000000000000",
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	sql := `DELETE FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet`

	// Normalize to lowercase to match AddMemberToRoleBypass behavior
	err := platform.Engine.Execute(engineContext, platform.DB, sql, map[string]any{
		"$owner":     strings.ToLower(owner),
		"$role_name": strings.ToLower(roleName),
		"$wallet":    strings.ToLower(wallet),
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to revoke role: %w", err)
	}

	return nil
}

// giveInsertBalance credits TRUF balance to a wallet using ERC20 inject
func giveInsertBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	insertPointCounter++
	return testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testInsertChain,
		testInsertEscrow,
		testInsertERC20,
		wallet,
		wallet,
		amountStr,
		insertPointCounter,
		nil,
	)
}

// getInsertBalance retrieves the TRUF balance for a wallet
func getInsertBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testInsertExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

// grantStreamWriteAccess grants write access to a wallet for a specific stream by directly inserting into metadata table
func grantStreamWriteAccess(ctx context.Context, platform *kwilTesting.Platform, dataProvider string, streamID string, wallet string) error {
	systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
		Signer:       systemAdmin.Bytes(),
		Caller:       systemAdmin.Address(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Skip authorization checks (test utility pattern)
	}

	// First, get the stream_ref by calling the get_stream_id action
	var streamRef *int
	result, err := platform.Engine.Call(
		engineContext,
		platform.DB,
		"",
		"get_stream_id",
		[]any{strings.ToLower(dataProvider), streamID},
		func(row *common.Row) error {
			if len(row.Values) > 0 && row.Values[0] != nil {
				if val, ok := row.Values[0].(int64); ok {
					intVal := int(val)
					streamRef = &intVal
				}
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to call get_stream_id: %w", err)
	}
	if result != nil && result.Error != nil {
		return fmt.Errorf("get_stream_id error: %w", result.Error)
	}
	if streamRef == nil {
		return fmt.Errorf("stream not found: data_provider=%s stream_id=%s", dataProvider, streamID)
	}

	// Insert allow_write_wallet metadata using direct SQL
	// Use uuid_generate_v5 to create a deterministic UUID based on stream and wallet
	insertSQL := `INSERT INTO metadata (row_id, stream_ref, metadata_key, value_ref, created_at, disabled_at)
	              VALUES (
	                  uuid_generate_v5(
	                      uuid_generate_kwil('grant_write_access_test'),
	                      'allow_write_' || $stream_ref::TEXT || '_' || $wallet
	                  )::UUID,
	                  $stream_ref,
	                  'allow_write_wallet',
	                  $wallet,
	                  $height,
	                  NULL
	              )`

	err = platform.Engine.Execute(engineContext, platform.DB, insertSQL, map[string]any{
		"$stream_ref": *streamRef,
		"$wallet":     strings.ToLower(wallet),
		"$height":     int64(1),
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to grant write access: %w", err)
	}

	return nil
}

// callInsertRecordsAction is the base implementation - calls the insert_records action
func callInsertRecordsAction(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, dataProviders []string, streamIds []string, eventTimes []int64, values []*kwilTypes.Decimal) error {
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
		"insert_records",
		[]any{dataProviders, streamIds, eventTimes, values},
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

// insertRecord inserts a single record with a randomly generated leader
func insertRecord(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, dataProvider string, streamId string, eventTime int64, value string) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	// Parse value as decimal
	decValue, err := kwilTypes.ParseDecimalExplicit(value, 36, 18)
	if err != nil {
		return fmt.Errorf("failed to parse value as decimal: %w", err)
	}

	return callInsertRecordsAction(ctx, platform, signer, pub, []string{dataProvider}, []string{streamId}, []int64{eventTime}, []*kwilTypes.Decimal{decValue})
}

// insertMultipleRecords inserts multiple records with a randomly generated leader
func insertMultipleRecords(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, dataProvider string, streamId string, startEventTime int64, count int) error {
	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	dataProviders := make([]string, count)
	streamIds := make([]string, count)
	eventTimes := make([]int64, count)
	values := make([]*kwilTypes.Decimal, count)

	for i := 0; i < count; i++ {
		dataProviders[i] = dataProvider
		streamIds[i] = streamId
		eventTimes[i] = startEventTime + int64(i)

		// Parse value as decimal
		valueStr := fmt.Sprintf("%d.5", 10+i)
		decValue, err := kwilTypes.ParseDecimalExplicit(valueStr, 36, 18)
		if err != nil {
			return fmt.Errorf("failed to parse value as decimal: %w", err)
		}
		values[i] = decValue
	}

	return callInsertRecordsAction(ctx, platform, signer, pub, dataProviders, streamIds, eventTimes, values)
}

// insertRecordWithLeader inserts a single record with a specific leader (for testing fee recipient)
func insertRecordWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, dataProvider string, streamId string, eventTime int64, value string) error {
	// Parse value as decimal
	decValue, err := kwilTypes.ParseDecimalExplicit(value, 36, 18)
	if err != nil {
		return fmt.Errorf("failed to parse value as decimal: %w", err)
	}

	return callInsertRecordsAction(ctx, platform, signer, leaderPub, []string{dataProvider}, []string{streamId}, []int64{eventTime}, []*kwilTypes.Decimal{decValue})
}
