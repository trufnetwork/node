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
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestTruflationInsertRecordsFees verifies truflation_insert_records charges the flat
// 1 TRUF per-transaction write fee for every caller (issue #3805 universal charging),
// mirroring insert_records. The fee is per-tx (not per-record) and comes straight out of
// the caller's TRUF balance; an unfunded caller's insert reverts on the balance check.
// (Reuses helpers giveInsertBalance / getInsertBalance / oneTRUFInsert from
// insert_records_fee_test.go — same package.)
func TestTruflationInsertRecordsFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "TRUFLATION_INSERT_FEE01_Fees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testTruflationInsertBatchChargesFlatFee(t),
			testTruflationInsertInsufficientBalance(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Test 1: A batch of N truflation records in one call charges a flat 1 TRUF (per-tx,
// not per-record) and the fee comes out of the caller's balance.
func testTruflationInsertBatchChargesFlatFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		writerVal := util.Unsafe_NewEthereumAddressFromString("0xb111111111111111111111111111111111111111")
		writer := &writerVal
		require.NoError(t, setup.CreateDataProvider(ctx, platform, writer.Address()), "register data provider")

		streamID := util.GenerateStreamId("truflation_fee_flat")
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: types.StreamLocator{StreamId: streamID, DataProvider: *writer},
		}), "create stream")

		require.NoError(t, giveInsertBalance(ctx, platform, writer.Address(), "100000000000000000000"), "fund writer")
		initial, err := getInsertBalance(ctx, platform, writer.Address())
		require.NoError(t, err, "get initial balance")

		// Insert 5 records in one tx — must still charge exactly 1 TRUF (flat).
		require.NoError(t, callTruflationInsert(ctx, platform, writer, writer.Address(), streamID.String(), 1000, 5),
			"truflation insert should succeed for a funded caller")

		final, err := getInsertBalance(ctx, platform, writer.Address())
		require.NoError(t, err, "get final balance")

		expected := new(big.Int).Sub(initial, oneTRUFInsert)
		require.Equal(t, 0, expected.Cmp(final),
			"batch of 5 truflation records must charge a flat 1 TRUF (per-tx, not per-record); expected %s but got %s",
			expected, final)
		return nil
	}
}

// Test 2: An unfunded caller's truflation insert reverts on the balance check — there is
// no exemption; every caller must be able to pay the fee.
func testTruflationInsertInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		writerVal := util.Unsafe_NewEthereumAddressFromString("0xb222222222222222222222222222222222222222")
		writer := &writerVal
		require.NoError(t, setup.CreateDataProvider(ctx, platform, writer.Address()), "register data provider")

		streamID := util.GenerateStreamId("truflation_fee_insufficient")
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: types.StreamLocator{StreamId: streamID, DataProvider: *writer},
		}), "create stream")

		// Give only 0.5 TRUF — below the flat 1 TRUF fee.
		require.NoError(t, giveInsertBalance(ctx, platform, writer.Address(), "500000000000000000"), "fund writer")

		err := callTruflationInsert(ctx, platform, writer, writer.Address(), streamID.String(), 1000, 1)
		require.Error(t, err, "truflation insert must fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for write fee",
			"error should mention insufficient balance")
		return nil
	}
}

// callTruflationInsert calls truflation_insert_records with `count` records for one stream,
// signed by `signer`, with a deterministic block proposer so @leader_sender (the fee
// recipient) resolves.
func callTruflationInsert(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, dataProvider, streamID string, startEventTime int64, count int) error {
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	dataProviders := make([]string, count)
	streamIds := make([]string, count)
	eventTimes := make([]int64, count)
	values := make([]*kwilTypes.Decimal, count)
	truflationCreatedAts := make([]string, count)
	for i := 0; i < count; i++ {
		dataProviders[i] = dataProvider
		streamIds[i] = streamID
		eventTimes[i] = startEventTime + int64(i)
		dec, err := kwilTypes.ParseDecimalExplicit(fmt.Sprintf("%d.5", 10+i), 36, 18)
		if err != nil {
			return err
		}
		values[i] = dec
		truflationCreatedAts[i] = "2024-01-01T00:00:00Z"
	}

	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: 1, Proposer: pub},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "", "truflation_insert_records",
		[]any{dataProviders, streamIds, eventTimes, values, truflationCreatedAts},
		func(row *common.Row) error { return nil })
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}
