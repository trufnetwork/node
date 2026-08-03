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
	"github.com/trufnetwork/node/tests/streams/utils/feefund"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

var (
	// taxonomyFeePerChild is parsed from feefund.TaxonomyFeePerChildWei — the
	// same shared constant the test helpers fund from, so the migration's fee
	// and the tests' expectations cannot drift apart. Per issue #3972 the fee
	// is 10 TRUF per child stream, not a flat amount per transaction.
	taxonomyFeePerChild = mustParseBigInt(feefund.TaxonomyFeePerChildWei) // 10 TRUF as big.Int, using shared helper from stream_creation_fee_test.go

	// threeChildTaxonomyFee is what a 3-child taxonomy costs: 3 × 10 TRUF.
	threeChildTaxonomyFee = new(big.Int).Mul(taxonomyFeePerChild, big.NewInt(3))
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
			testTaxonomyMultipleChildrenChargesPerChildFee(t),
			testTaxonomyUnenrolledWalletStillPaysFee(t),
			testTaxonomyPartialBalanceRejectsExtraChild(t),
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
// 300 TRUF in → 100 (composed) + 100 (child) + 10 (1-child taxonomy) = 210 TRUF spent.
func testTaxonomyWriterRolePaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		writerAddrVal := util.Unsafe_NewEthereumAddressFromString("0x2111111111111111111111111111111111111111")
		writerAddr := &writerAddrVal

		// Register as data provider (also grants network_writer role; no longer exempts).
		err := setup.CreateDataProvider(ctx, platform, writerAddr.Address())
		require.NoError(t, err, "failed to create data provider")

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

		// 100 (composed create) + 100 (child create) + 10 (taxonomy w/ 1 child) = 210 TRUF.
		totalFee := mustParseBigInt("210000000000000000000")
		expectedBalance := new(big.Int).Sub(initialBalance, totalFee)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"network_writer should pay 210 TRUF total, expected %s but got %s", expectedBalance, finalBalance)

		return nil
	}
}

// Test 2: Non-exempt wallet (without network_writer role) pays 10 TRUF per
// child stream. Fund exactly 210 TRUF: 100 (composed) + 100 (child) + 10 (1-child taxonomy).
func testTaxonomyNonExemptWalletPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		nonExemptAddrVal := util.Unsafe_NewEthereumAddressFromString("0x3222222222222222222222222222222222222222")
		nonExemptAddr := &nonExemptAddrVal

		// Register data provider WITHOUT role (non-whitelisted - will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Give exactly 210 TRUF: 100 (composed) + 100 (child) + 10 (1-child taxonomy)
		exactFund := mustParseBigInt("210000000000000000000") // 210 TRUF
		err = giveBalance(ctx, platform, nonExemptAddr.Address(), exactFund.String())
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		require.Equal(t, exactFund, initialBalance, "Initial balance should be 210 TRUF")

		composedStreamId := util.GenerateStreamId("taxonomy_nonexempt_composed")
		childStreamId := util.GenerateStreamId("taxonomy_nonexempt_child")

		// Create composed stream (costs 100 TRUF)
		err = createStream(ctx, platform, nonExemptAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		// Create child stream (costs 100 TRUF)
		err = createStream(ctx, platform, nonExemptAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// Balance after stream creation should be 10 TRUF (210 - 100 - 100)
		balanceAfterStreams, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, taxonomyFeePerChild, balanceAfterStreams, "Balance should be 10 TRUF after creating streams")

		// Insert taxonomy (1 child → 1 × 10 TRUF)
		err = insertTaxonomy(ctx, platform, nonExemptAddr,
			nonExemptAddr.Address(), composedStreamId.String(),
			[]string{nonExemptAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		// Verify balance is now 0 (10 TRUF taxonomy fee charged)
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

		// Give exactly 200 TRUF: enough for two create_stream calls (100 + 100)
		// but nothing left over for the 10 TRUF taxonomy fee.
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

		// Should have 0 TRUF left (200 - 100 - 100 = 0), not enough for the 10 TRUF taxonomy fee
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
		require.Contains(t, err.Error(), "Required: 10 TRUF per child stream", "Error should mention the per-child requirement")

		return nil
	}
}

// Test 4: Multi-child taxonomy charges 10 TRUF × child count.
// This is the key invariant of issue #3972 — pricing is per-child, not per-tx.
func testTaxonomyMultipleChildrenChargesPerChildFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		multiAddrVal := util.Unsafe_NewEthereumAddressFromString("0x5444444444444444444444444444444444444444")
		multiAddr := &multiAddrVal

		// Register data provider WITHOUT role
		err := setup.CreateDataProviderWithoutRole(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Give exactly 430 TRUF: 100 (composed) + 300 (3 children) + 30 (3-child taxonomy).
		// If the migration were still flat per-tx, the taxonomy would cost 1 TRUF
		// and this test would end with 29 TRUF left instead of 0.
		exactFund := mustParseBigInt("430000000000000000000")
		err = giveBalance(ctx, platform, multiAddr.Address(), exactFund.String())
		require.NoError(t, err, "failed to give balance")

		initialBalance, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		require.Equal(t, exactFund, initialBalance, "Initial balance should be 430 TRUF")

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

		// Balance after stream creation should be 30 TRUF (430 - 400)
		balanceAfterStreams, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, threeChildTaxonomyFee, balanceAfterStreams, "Balance should be 30 TRUF after creating streams")

		// Insert taxonomy with 3 children — must charge exactly 3 × 10 = 30 TRUF.
		err = insertTaxonomy(ctx, platform, multiAddr,
			multiAddr.Address(), composedStreamId.String(),
			[]string{multiAddr.Address(), multiAddr.Address(), multiAddr.Address()},
			[]string{child1StreamId.String(), child2StreamId.String(), child3StreamId.String()},
			[]string{"0.3", "0.3", "0.4"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		// Verify balance is now 0 — the 3-child taxonomy charged 30 TRUF.
		finalBalance, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		require.Equal(t, big.NewInt(0), finalBalance, "Final balance should be 0 — taxonomy fee is 10 TRUF per child, so 3 children cost 30 TRUF")

		return nil
	}
}

// Test 5: A wallet with no fee-related role membership still pays the
// insert_taxonomy fee. Regression check that the phased-rollout exemption
// has been removed (issue #3805 universal charging) — after two 100-TRUF
// stream creates, the 10-TRUF per-child taxonomy fee is charged like everybody else.
func testTaxonomyUnenrolledWalletStillPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddrVal := util.Unsafe_NewEthereumAddressFromString("0x6555555555555555555555555555555555555555")
		userAddr := &userAddrVal

		err := setup.CreateDataProviderWithoutRole(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Fund exactly 210 TRUF: 100 (composed) + 100 (child) + 10 (1-child taxonomy).
		// No fee-related role membership — the taxonomy fee applies anyway.
		err = giveBalance(ctx, platform, userAddr.Address(), "210000000000000000000")
		require.NoError(t, err, "failed to give balance")

		composedStreamId := util.GenerateStreamId("taxonomy_unenrolled_composed")
		childStreamId := util.GenerateStreamId("taxonomy_unenrolled_child")

		err = createStream(ctx, platform, userAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		err = createStream(ctx, platform, userAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// After 2 stream creations (200 TRUF spent), balance should be 10 TRUF.
		balanceAfterStreams, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, taxonomyFeePerChild, balanceAfterStreams, "Balance should be 10 TRUF after creating streams")

		// Taxonomy insertion is charged the universal per-child fee — no exemption.
		err = insertTaxonomy(ctx, platform, userAddr,
			userAddr.Address(), composedStreamId.String(),
			[]string{userAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "un-enrolled wallet should insert taxonomy, paying the universal fee")

		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, big.NewInt(0), finalBalance, "un-enrolled wallet must pay the 10 TRUF taxonomy fee — exemption removed")

		return nil
	}
}

// Test 6: A wallet funded for a 1-child taxonomy cannot afford a 2-child one.
// The balance is non-zero, so this exercises the partial-funding rejection path
// rather than the zero-balance one in Test 3 — and it fails only if the fee
// actually multiplies by child count. Under a flat per-tx fee the insert would
// succeed.
func testTaxonomyPartialBalanceRejectsExtraChild(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		partialAddrVal := util.Unsafe_NewEthereumAddressFromString("0x7666666666666666666666666666666666666666")
		partialAddr := &partialAddrVal

		err := setup.CreateDataProviderWithoutRole(ctx, platform, partialAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Fund 310 TRUF: 300 for three stream creates, then exactly 10 left —
		// enough for one child, not the two the taxonomy attaches.
		err = giveBalance(ctx, platform, partialAddr.Address(), "310000000000000000000")
		require.NoError(t, err, "failed to give balance")

		composedStreamId := util.GenerateStreamId("taxonomy_partial_composed")
		child1StreamId := util.GenerateStreamId("taxonomy_partial_child1")
		child2StreamId := util.GenerateStreamId("taxonomy_partial_child2")

		err = createStream(ctx, platform, partialAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		for _, childId := range []util.StreamId{child1StreamId, child2StreamId} {
			err = createStream(ctx, platform, partialAddr, childId.String(), "primitive")
			require.NoError(t, err, "failed to create child stream")
		}

		balanceAfterStreams, err := getBalance(ctx, platform, partialAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, taxonomyFeePerChild, balanceAfterStreams, "Balance should be 10 TRUF after creating streams")

		// 2 children → 20 TRUF required, only 10 held.
		err = insertTaxonomy(ctx, platform, partialAddr,
			partialAddr.Address(), composedStreamId.String(),
			[]string{partialAddr.Address(), partialAddr.Address()},
			[]string{child1StreamId.String(), child2StreamId.String()},
			[]string{"0.5", "0.5"},
			nil)

		require.Error(t, err, "a 2-child taxonomy must be rejected when only one child's fee is held")
		require.Contains(t, err.Error(), "Insufficient balance for taxonomies creation", "Error should mention insufficient balance")

		// The rejected call must not have taken the caller's TRUF.
		finalBalance, err := getBalance(ctx, platform, partialAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, taxonomyFeePerChild, finalBalance, "a rejected taxonomy must leave the balance untouched")

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
