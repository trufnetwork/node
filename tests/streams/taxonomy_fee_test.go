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
	taxonomyFeeAmount = "2000000000000000000" // 2 TRUF with 18 decimals per child stream
)

var (
	twoTRUFTaxonomy = mustParseBigInt(taxonomyFeeAmount) // 2 TRUF as big.Int, using shared helper from stream_creation_fee_test.go
)

// TestTaxonomyFees is the main test suite for insert_taxonomy transaction fees
func TestTaxonomyFees(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "TAXONOMY_FEE01_TaxonomyFees",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			setupTaxonomyTestEnvironment(t),
			testTaxonomyExemptWalletNoFee(t),
			testTaxonomyNonExemptWalletPaysFee(t),
			testTaxonomyInsufficientBalance(t),
			testTaxonomyMultipleChildrenFee(t),
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

// Test 1: Exempt wallet (with network_writer role) inserts taxonomy without paying fee
func testTaxonomyExemptWalletNoFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		exemptAddrVal := util.Unsafe_NewEthereumAddressFromString("0x2111111111111111111111111111111111111111")
		exemptAddr := &exemptAddrVal

		// Register data provider (this grants network_writer role - exempt from fees)
		err := setup.CreateDataProvider(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to create data provider")

		// Give balance to verify it doesn't change
		err = giveBalance(ctx, platform, exemptAddr.Address(), "100000000000000000000") // 100 TRUF
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Create streams using direct engine calls (like stream_creation_fee_test does)
		composedStreamId := util.GenerateStreamId("taxonomy_exempt_composed")
		childStreamId := util.GenerateStreamId("taxonomy_exempt_child")

		// Create composed stream
		err = createStream(ctx, platform, exemptAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		// Create child stream
		err = createStream(ctx, platform, exemptAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// Insert taxonomy (1 child = 2 TRUF fee, but should be exempt)
		err = insertTaxonomy(ctx, platform, exemptAddr,
			exemptAddr.Address(), composedStreamId.String(),
			[]string{exemptAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed for exempt wallet")

		// Verify balance unchanged (no fee charged)
		finalBalance, err := getBalance(ctx, platform, exemptAddr.Address())
		require.NoError(t, err, "failed to get final balance")
		require.Equal(t, initialBalance, finalBalance, "Balance should not change for exempt wallet (no fee)")

		return nil
	}
}

// Test 2: Non-exempt wallet (without network_writer role) pays 2 TRUF per child stream
func testTaxonomyNonExemptWalletPaysFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		nonExemptAddrVal := util.Unsafe_NewEthereumAddressFromString("0x3222222222222222222222222222222222222222")
		nonExemptAddr := &nonExemptAddrVal

		// Register data provider WITHOUT role (non-whitelisted - will pay fees)
		err := setup.CreateDataProviderWithoutRole(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Give 6 TRUF: 2 TRUF (composed stream fee) + 2 TRUF (child stream fee) + 2 TRUF (taxonomy fee)
		sixTRUF := mustParseBigInt("6000000000000000000") // 6 TRUF
		err = giveBalance(ctx, platform, nonExemptAddr.Address(), sixTRUF.String())
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		require.Equal(t, sixTRUF, initialBalance, "Initial balance should be 6 TRUF")

		// Create streams using direct engine calls (each costs 2 TRUF)
		composedStreamId := util.GenerateStreamId("taxonomy_nonexempt_composed")
		childStreamId := util.GenerateStreamId("taxonomy_nonexempt_child")

		// Create composed stream (costs 2 TRUF)
		err = createStream(ctx, platform, nonExemptAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		// Create child stream (costs 2 TRUF)
		err = createStream(ctx, platform, nonExemptAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// Balance after stream creation should be 2 TRUF (6 - 2 - 2)
		balanceAfterStreams, err := getBalance(ctx, platform, nonExemptAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		require.Equal(t, twoTRUFTaxonomy, balanceAfterStreams, "Balance should be 2 TRUF after creating streams")

		// Insert taxonomy (1 child = 2 TRUF fee)
		err = insertTaxonomy(ctx, platform, nonExemptAddr,
			nonExemptAddr.Address(), composedStreamId.String(),
			[]string{nonExemptAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		// Verify balance is now 0 (2 TRUF taxonomy fee charged)
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

		// Give 5 TRUF: Enough for streams (2+2=4) but not enough for taxonomy fee (need 2 more)
		fiveTRUF := mustParseBigInt("5000000000000000000") // 5 TRUF
		err = giveBalance(ctx, platform, insufficientAddr.Address(), fiveTRUF.String())
		require.NoError(t, err, "failed to give balance")

		// Create streams (costs 4 TRUF total, leaving 1 TRUF)
		composedStreamId := util.GenerateStreamId("taxonomy_insufficient_composed")
		childStreamId := util.GenerateStreamId("taxonomy_insufficient_child")

		err = createStream(ctx, platform, insufficientAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		err = createStream(ctx, platform, insufficientAddr, childStreamId.String(), "primitive")
		require.NoError(t, err, "failed to create child stream")

		// Should have 1 TRUF left (5 - 2 - 2 = 1), not enough for 2 TRUF taxonomy fee
		remainingBalance, err := getBalance(ctx, platform, insufficientAddr.Address())
		require.NoError(t, err, "failed to get remaining balance")
		oneTRUF := mustParseBigInt("1000000000000000000")
		require.Equal(t, oneTRUF, remainingBalance, "Should have 1 TRUF left after creating streams")

		// Attempt to insert taxonomy - should fail due to insufficient balance
		err = insertTaxonomy(ctx, platform, insufficientAddr,
			insufficientAddr.Address(), composedStreamId.String(),
			[]string{insufficientAddr.Address()},
			[]string{childStreamId.String()},
			[]string{"1.0"},
			nil)

		require.Error(t, err, "taxonomy insertion should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance for taxonomies creation", "Error should mention insufficient balance")
		require.Contains(t, err.Error(), "Required: 2 TRUF", "Error should mention 2 TRUF requirement")

		return nil
	}
}

// Test 4: Multiple children - fee should be 2 TRUF per child
func testTaxonomyMultipleChildrenFee(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		multiAddrVal := util.Unsafe_NewEthereumAddressFromString("0x5444444444444444444444444444444444444444")
		multiAddr := &multiAddrVal

		// Register data provider WITHOUT role
		err := setup.CreateDataProviderWithoutRole(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to create data provider without role")

		// Give 14 TRUF: 2 (composed) + 6 (3 children streams) + 6 (taxonomy fee for 3 children)
		fourteenTRUF := mustParseBigInt("14000000000000000000") // 14 TRUF
		err = giveBalance(ctx, platform, multiAddr.Address(), fourteenTRUF.String())
		require.NoError(t, err, "failed to give balance")

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get initial balance")
		require.Equal(t, fourteenTRUF, initialBalance, "Initial balance should be 14 TRUF")

		// Create streams (costs 2 + 6 = 8 TRUF total)
		composedStreamId := util.GenerateStreamId("taxonomy_multi_composed")
		child1StreamId := util.GenerateStreamId("taxonomy_multi_child1")
		child2StreamId := util.GenerateStreamId("taxonomy_multi_child2")
		child3StreamId := util.GenerateStreamId("taxonomy_multi_child3")

		// Create composed stream (costs 2 TRUF)
		err = createStream(ctx, platform, multiAddr, composedStreamId.String(), "composed")
		require.NoError(t, err, "failed to create composed stream")

		// Create 3 child streams (costs 6 TRUF total)
		for _, childId := range []util.StreamId{child1StreamId, child2StreamId, child3StreamId} {
			err = createStream(ctx, platform, multiAddr, childId.String(), "primitive")
			require.NoError(t, err, "failed to create child stream")
		}

		// Balance after stream creation should be 6 TRUF (14 - 8)
		balanceAfterStreams, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get balance after stream creation")
		sixTRUF := mustParseBigInt("6000000000000000000")
		require.Equal(t, sixTRUF, balanceAfterStreams, "Balance should be 6 TRUF after creating streams")

		// Insert taxonomy with 3 children (should charge 6 TRUF total)
		err = insertTaxonomy(ctx, platform, multiAddr,
			multiAddr.Address(), composedStreamId.String(),
			[]string{multiAddr.Address(), multiAddr.Address(), multiAddr.Address()},
			[]string{child1StreamId.String(), child2StreamId.String(), child3StreamId.String()},
			[]string{"0.3", "0.3", "0.4"},
			nil)
		require.NoError(t, err, "taxonomy insertion should succeed")

		// Verify balance is now 0 (6 TRUF taxonomy fee charged)
		finalBalance, err := getBalance(ctx, platform, multiAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		require.Equal(t, big.NewInt(0), finalBalance, "Final balance should be 0 after paying all fees")

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
