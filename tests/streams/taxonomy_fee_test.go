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
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants for taxonomy fees
const (
	// Flat 1 TRUF per insert_taxonomy transaction (issue #3805): child count
	// no longer multiplies the fee.
	taxonomyFeeAmount = "1000000000000000000" // 1 TRUF with 18 decimals per tx
)

var (
	oneTRUFTaxonomy = mustParseBigInt(taxonomyFeeAmount) // 1 TRUF as big.Int, using shared helper from stream_creation_fee_test.go
)

// TestTaxonomyFees is the main test suite for insert_taxonomy transaction fees
func TestTaxonomyFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "TAXONOMY_FEE01_TaxonomyFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTaxonomyTestEnvironment(t),
			testTaxonomyWriterRolePaysFee(t),
			testTaxonomyNonExemptWalletPaysFee(t),
			testTaxonomyInsufficientBalance(t),
			testTaxonomyMultipleChildrenChargesFlatFee(t),
			testTaxonomyUnenrolledWalletWritesFree(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// setupTaxonomyTestEnvironment grants network_writer role to system admin
func setupTaxonomyTestEnvironment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use the system admin address (derived from private key 0x00...01)
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		// Grant network_writers_manager role
		err := setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", systemAdmin.Address())
		if err != nil {
			return fmt.Errorf("failed to grant network_writers_manager to system admin: %w", err)
		}

		// Grant network_writer role to system admin
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", systemAdmin.Address())
		if err != nil {
			return fmt.Errorf("failed to grant network_writer to system admin: %w", err)
		}

		return nil
	}
}

// Test 1: Wallet with network_writer role still pays insert_taxonomy fees.
// 300 TRUF in → 100 (composed) + 100 (child) + 1 (1-child taxonomy) = 201 TRUF spent.
func testTaxonomyWriterRolePaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		writerAddrVal := util.Unsafe_NewEthereumAddressFromString("0x2111111111111111111111111111111111111111")
		writerAddr := &writerAddrVal

		// Register as data provider (also grants network_writer role; no longer exempts).
		err := setup.CreateDataProvider(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to create data provider")

		// Enroll wallet in fee_required (phased rollout per #3805 — only
		// enrolled wallets pay; the test asserts fees are charged).
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "fee_required", writerAddr.Address())
		require.NoError(t, err, "failed to enroll in fee_required role")

		err = giveBalance(ctx, platform, writerAddr.Address(), "300000000000000000000") // 300 TRUF
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		composedStreamId := util.GenerateStreamId("taxonomy_writer_composed")
		childStreamId := util.GenerateStreamId("taxonomy_writer_child")

		err = createStream(ctx, platform, writerAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		err = createStream(ctx, platform, writerAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		err = insertTaxonomy(ctx, platform, writerAddr,
			writerAddr.Address(), composedStreamId.String(),
			[]string{writerAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		finalBalance, err := getBalance(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		// 100 (composed create) + 100 (child create) + 1 (taxonomy w/ 1 child) = 201 TRUF.
		totalFee := mustParseBigInt("201000000000000000000")
		expectedBalance := new(big.Int).Sub(initialBalance, totalFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"network_writer should pay 201 TRUF total, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 2: Non-exempt wallet (without network_writer role) pays a flat 1 TRUF
// per insert_taxonomy tx. Fund exactly 201 TRUF: 100 (composed) + 100 (child) + 1 (taxonomy).
func testTaxonomyNonExemptWalletPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		nonExemptAddrVal := util.Unsafe_NewEthereumAddressFromString("0x3222222222222222222222222222222222222222")
		nonExemptAddr := &nonExemptAddrVal

		// Register data provider WITHOUT role (non-whitelisted - will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Enroll wallet in fee_required so taxonomy writes are charged.
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "fee_required", nonExemptAddr.Address())
		require.NoError(t, err, "failed to enroll in fee_required role")

		// Give exactly 201 TRUF: 100 (composed) + 100 (child) + 1 (taxonomy)
		exactFund := mustParseBigInt("201000000000000000000") // 201 TRUF
		err = giveBalance(ctx, platform, nonExemptAddr.Address(), exactFund.String())
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		require.Equal(t, exactFund, initialBalance, "Initial balance should be 201 TRUF")

		composedStreamId := util.GenerateStreamId("taxonomy_nonexempt_composed")
		childStreamId := util.GenerateStreamId("taxonomy_nonexempt_child")

		// Create composed stream (costs 100 TRUF)
		err = createStream(ctx, platform, nonExemptAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		// Create child stream (costs 100 TRUF)
		err = createStream(ctx, platform, nonExemptAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// Balance after stream creation should be 1 TRUF (201 - 100 - 100)
		balanceAfterStreams, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, oneTRUFTaxonomy, balanceAfterStreams, "Balance should be 1 TRUF after creating streams")

		// Insert taxonomy (1 child, flat 1 TRUF fee)
		err = insertTaxonomy(ctx, platform, nonExemptAddr,
			nonExemptAddr.Address(), composedStreamId.String(),
			[]string{nonExemptAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		// Verify balance is now 0 (1 TRUF taxonomy fee charged)
		finalBalance, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		require.Equal(t, big.NewInt(0), finalBalance, "Final balance should be 0 after paying all fees")

		return nil
	}
}

// Test 3: Insufficient balance test - wallet with less than required fee
func testTaxonomyInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		insufficientAddrVal := util.Unsafe_NewEthereumAddressFromString("0x4333333333333333333333333333333333333333")
		insufficientAddr := &insufficientAddrVal

		// Register data provider WITHOUT role
		err := setup.CreateDataProviderWithoutRole(ctx, platform, insufficientAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Enroll wallet so the insufficient-balance ERROR fires on the
		// taxonomy step (un-enrolled wallets bypass the balance check).
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "fee_required", insufficientAddr.Address())
		require.NoError(t, err, "failed to enroll in fee_required role")

		// Give exactly 200 TRUF: enough for two create_stream calls (100 + 100)
		// but nothing left over for the 1 TRUF taxonomy fee.
		twoHundredTRUF := mustParseBigInt("200000000000000000000")
		err = giveBalance(ctx, platform, insufficientAddr.Address(), twoHundredTRUF.String())
		require.NoError(t, err, "failed to give balance")

		// Create streams (costs 200 TRUF total, leaving 0)
		composedStreamId := util.GenerateStreamId("taxonomy_insufficient_composed")
		childStreamId := util.GenerateStreamId("taxonomy_insufficient_child")

		err = createStream(ctx, platform, insufficientAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		err = createStream(ctx, platform, insufficientAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// Should have 0 TRUF left (200 - 100 - 100 = 0), not enough for the 1 TRUF taxonomy fee
		remainingBalance, err := getBalance(ctx, platform, insufficientAddr.Address())
		require.NoError(t, err, "failed to get remaining balance")
		require.Equal(t, big.NewInt(0), remainingBalance, "Should have 0 TRUF left after creating streams")

		// Attempt to insert taxonomy - should fail due to insufficient balance
		err = insertTaxonomy(ctx, platform, insufficientAddr,
			insufficientAddr.Address(), composedStreamId.String(),
			[]string{insufficientAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)

		require.Error(t, err, "taxonomy insertion should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for taxonomies creation", "Error should mention insufficient balance")
		require.Contains(t, err.Error(), "Required: 1 TRUF", "Error should mention 1 TRUF requirement")

		return nil
	}
}

// Test 4: Multi-child taxonomy charges a flat 1 TRUF regardless of child count.
// This is the key invariant of issue #3805 — pricing is per-tx, not per-child.
func testTaxonomyMultipleChildrenChargesFlatFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		multiAddrVal := util.Unsafe_NewEthereumAddressFromString("0x5444444444444444444444444444444444444444")
		multiAddr := &multiAddrVal

		// Register data provider WITHOUT role
		err := setup.CreateDataProviderWithoutRole(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Enroll wallet in fee_required so the multi-child taxonomy is
		// charged at the flat 1 TRUF.
		err = setup.AddMemberToRoleBypass(ctx, platform, "system", "fee_required", multiAddr.Address())
		require.NoError(t, err, "failed to enroll in fee_required role")

		// Give exactly 401 TRUF: 100 (composed) + 300 (3 children) + 1 (taxonomy, flat).
		// If the migration were still per-child, the 3-child taxonomy would
		// cost 3 TRUF and this test would fail with insufficient balance.
		exactFund := mustParseBigInt("401000000000000000000")
		err = giveBalance(ctx, platform, multiAddr.Address(), exactFund.String())
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		require.Equal(t, exactFund, initialBalance, "Initial balance should be 401 TRUF")

		// Create streams (costs 100 + 300 = 400 TRUF total)
		composedStreamId := util.GenerateStreamId("taxonomy_multi_composed")
		child1StreamId := util.GenerateStreamId("taxonomy_multi_child1")
		child2StreamId := util.GenerateStreamId("taxonomy_multi_child2")
		child3StreamId := util.GenerateStreamId("taxonomy_multi_child3")

		// Create composed stream (costs 100 TRUF)
		err = createStream(ctx, platform, multiAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		// Create 3 child streams (costs 300 TRUF total, 100 per create_stream call)
		for _, childId := range []util.StreamId{child1StreamId, child2StreamId, child3StreamId} {
			err = createStream(ctx, platform, multiAddr, childId.String(), "primitive")
			require.NoError(t, err, "failed to create child stream")
		}

		// Balance after stream creation should be 1 TRUF (401 - 400)
		balanceAfterStreams, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, oneTRUFTaxonomy, balanceAfterStreams, "Balance should be 1 TRUF after creating streams")

		// Insert taxonomy with 3 children — must still charge exactly 1 TRUF.
		err = insertTaxonomy(ctx, platform, multiAddr,
			multiAddr.Address(), composedStreamId.String(),
			[]string{multiAddr.Address(), multiAddr.Address(), multiAddr.Address()},
			[]string{child1StreamId.String(), child2StreamId.String(), child3StreamId.String()},
			[]string{"0.3", "0.3", "0.4"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		// Verify balance is now 0 — 3-child taxonomy charged only 1 TRUF (flat).
		finalBalance, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		require.Equal(t, big.NewInt(0), finalBalance, "Final balance should be 0 — taxonomy fee is flat 1 TRUF regardless of child count")

		return nil
	}
}

// Test 5: A wallet not enrolled in system:fee_required pays the universal
// stream creation fee (100 TRUF/stream) but the taxonomy insertion is free.
// Phased rollout of #3805 — insert_taxonomy fee only fires for enrolled wallets.
func testTaxonomyUnenrolledWalletWritesFree(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		freeAddrVal := util.Unsafe_NewEthereumAddressFromString("0x6555555555555555555555555555555555555555")
		freeAddr := &freeAddrVal

		err := setup.CreateDataProviderWithoutRole(ctx, platform, freeAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Fund with exactly 200 TRUF for two stream creations (100 each, universal).
		// NOT enrolled in fee_required — taxonomy itself should be free.
		err = giveBalance(ctx, platform, freeAddr.Address(), "200000000000000000000")
		require.NoError(t, err, "failed to give balance")

		composedStreamId := util.GenerateStreamId("taxonomy_free_composed")
		childStreamId := util.GenerateStreamId("taxonomy_free_child")

		err = createStream(ctx, platform, freeAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		err = createStream(ctx, platform, freeAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// After 2 stream creations (200 TRUF spent), balance should be 0
		balanceAfterStreams, err := getBalance(ctx, platform, freeAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, big.NewInt(0), balanceAfterStreams, "Balance should be 0 after creating streams")

		// Taxonomy insertion should succeed for free (not enrolled in fee_required)
		err = insertTaxonomy(ctx, platform, freeAddr,
			freeAddr.Address(), composedStreamId.String(),
			[]string{freeAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "un-enrolled wallet should insert taxonomy for free")

		finalBalance, err := getBalance(ctx, platform, freeAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, big.NewInt(0), finalBalance, "un-enrolled wallet must not be charged for taxonomy insertion")

		return nil
	}
}

// insertTaxonomy directly calls the insert_taxonomy action with proper context
func insertTaxonomy(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress,
	dataProvider string, streamId string,
	childDataProviders []string, childStreamIds []string, weights []string, startDate *int64) error {

	// Generate random leader
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	// Convert weights to decimals
	var weightDecimals []*kwilTypes.Decimal
	for _, w := range weights {
		dec, err := kwilTypes.ParseDecimalExplicit(w, 36, 18)
		if err != nil {
			return fmt.Errorf("error parsing weight %s: %w", w, err)
		}
		weightDecimals = append(weightDecimals, dec)
	}

	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   1,
			Proposer: pub,
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
		"insert_taxonomy",
		[]any{dataProvider, streamId, childDataProviders, childStreamIds, weightDecimals, startDate},
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
