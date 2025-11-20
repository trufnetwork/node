//go:build kwiltest

package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	sdkTypes "github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestTransactionInputActions tests all transaction input data retrieval actions
func TestTransactionInputActions(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "TX_INPUT_01_AllActions",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			runTransactionInputActionsTest(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func runTransactionInputActionsTest(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		systemAdminVal := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		systemAdmin := &systemAdminVal
		platform.Deployer = systemAdmin.Bytes()

		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", systemAdmin.Address()))
		require.NoError(t, setup.CreateDataProvider(ctx, platform, systemAdmin.Address()))

		// Test 1: get_transaction_streams
		t.Log("Test 1: Create streams and verify get_transaction_streams")

		stream1 := util.GenerateStreamId("test_stream_1")
		stream2 := util.GenerateStreamId("test_stream_2")

		txID1 := strings.ToLower(platform.Txid())
		tx1 := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 100,
			},
			Signer:        systemAdmin.Bytes(),
			Caller:        systemAdmin.Address(),
			TxID:          txID1,
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx1 := &common.EngineContext{TxContext: tx1}
		_, err := platform.Engine.Call(engineCtx1, platform.DB, "", "create_streams", []any{
			[]string{stream1.String(), stream2.String()},
			[]string{"primitive", "composed"},
		}, func(row *common.Row) error { return nil })
		require.NoError(t, err)

		formattedTxID1 := "0x" + txID1
		t.Logf("Created streams with tx_id: %s", formattedTxID1)

		// Call get_transaction_streams
		rows1 := make([]*common.Row, 0)
		err = callViewAction(ctx, platform, systemAdmin.Address(), "get_transaction_streams", []any{formattedTxID1}, func(row *common.Row) error {
			rows1 = append(rows1, row)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, rows1, 2, "should return 2 streams")

		// Verify both streams are returned with correct types
		stream1Found := false
		stream2Found := false
		for _, row := range rows1 {
			streamID := row.Values[0].(string)
			streamType := row.Values[2].(string)
			dataProvider := row.Values[1].(string)

			require.Equal(t, strings.ToLower(systemAdmin.Address()), dataProvider, "data_provider should match")
			require.NotNil(t, row.Values[3], "created_at should not be nil")

			if streamID == stream1.String() {
				require.Equal(t, "primitive", streamType, "stream1 should be primitive")
				stream1Found = true
			} else if streamID == stream2.String() {
				require.Equal(t, "composed", streamType, "stream2 should be composed")
				stream2Found = true
			}
		}
		require.True(t, stream1Found, "stream1 should be found")
		require.True(t, stream2Found, "stream2 should be found")

		t.Log("✅ get_transaction_streams works correctly")

		// Test 2: get_transaction_records
		t.Log("Test 2: Insert records and verify get_transaction_records")

		primitiveStream := util.GenerateStreamId("test_primitive")
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type: setup.ContractTypePrimitive,
			Locator: sdkTypes.StreamLocator{
				StreamId:     primitiveStream,
				DataProvider: *systemAdmin,
			},
		}))

		value1, err := kwilTypes.ParseDecimalExplicit("100.5", 36, 18)
		require.NoError(t, err)

		txID2 := strings.ToLower(platform.Txid())
		tx2 := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 101,
			},
			Signer:        systemAdmin.Bytes(),
			Caller:        systemAdmin.Address(),
			TxID:          txID2,
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx2 := &common.EngineContext{TxContext: tx2}
		_, err = platform.Engine.Call(engineCtx2, platform.DB, "", "insert_records", []any{
			[]string{strings.ToLower(systemAdmin.Address())},
			[]string{primitiveStream.String()},
			[]int64{1700000000},
			[]*kwilTypes.Decimal{value1},
		}, func(row *common.Row) error { return nil })
		require.NoError(t, err)

		formattedTxID2 := "0x" + txID2
		t.Logf("Inserted records with tx_id: %s", formattedTxID2)

		// Call get_transaction_records
		rows2 := make([]*common.Row, 0)
		err = callViewAction(ctx, platform, systemAdmin.Address(), "get_transaction_records", []any{formattedTxID2}, func(row *common.Row) error {
			rows2 = append(rows2, row)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, rows2, 1, "should return 1 record")
		require.Equal(t, primitiveStream.String(), rows2[0].Values[1].(string), "stream_id should match")
		require.Equal(t, int64(1700000000), rows2[0].Values[2].(int64), "event_time should match")

		t.Log("✅ get_transaction_records works correctly")

		// Test 3: Edge cases
		t.Log("Test 3: Edge cases")

		// Non-existent tx_id
		rows3 := make([]*common.Row, 0)
		err = callViewAction(ctx, platform, systemAdmin.Address(), "get_transaction_streams", []any{"0xnonexistent"}, func(row *common.Row) error {
			rows3 = append(rows3, row)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, rows3, 0, "should return empty for non-existent tx_id")

		// Empty tx_id should error
		err = callViewAction(ctx, platform, systemAdmin.Address(), "get_transaction_records", []any{""}, func(row *common.Row) error {
			return nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "tx_id is required")

		t.Log("✅ Edge cases handled correctly")
		t.Log("✅✅✅ All transaction input data actions work correctly!")

		return nil
	}
}

func callViewAction(ctx context.Context, platform *kwilTesting.Platform, caller string, action string, args []any, fn func(*common.Row) error) error {
	address, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return err
	}
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 100,
		},
		Signer:        address.Bytes(),
		Caller:        strings.ToLower(caller),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", action, args, fn)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}
