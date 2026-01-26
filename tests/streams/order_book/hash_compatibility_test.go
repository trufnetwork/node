//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"testing"
	"time"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestHashCompatibility tests that market hash matches attestation hash
// THIS IS THE CRITICAL TEST FOR AUTOMATIC SETTLEMENT TO WORK
func TestHashCompatibility(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_HashCompatibility",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testHashCompatibility_MarketAndAttestation(t),
			testHashCompatibility_SameComponentsProduceSameHash(t),
			testHashCompatibility_DifferentComponentsProduceDifferentHash(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Test constants for bridges
const (
	// TRUF bridge (hoodi_tt) - market creation fee is ALWAYS collected from here
	testTRUFChainHashCompat         = "hoodi"
	testTRUFEscrowHashCompat        = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testTRUFERC20HashCompat         = "0x263ce78fef26600e4e428cebc91c2a52484b4fbf"
	testTRUFExtensionNameHashCompat = "hoodi_tt"

	// USDC bridge (hoodi_tt2) - market collateral for trading
	testUSDCChainHashCompat         = "hoodi"
	testUSDCEscrowHashCompat        = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	testUSDCERC20HashCompat         = "0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd"
	testUSDCExtensionNameHashCompat = "hoodi_tt2"
)

var (
	hashCompatBalanceCounter int64 = 300
	hashCompatLastPoint      *int64
)

// testHashCompatibility_MarketAndAttestation is THE critical test
// It verifies that a market created with query_components produces the SAME hash
// as an attestation requested with the same components.
// This is ESSENTIAL for automatic settlement to work.
func testHashCompatibility_MarketAndAttestation(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// CRITICAL: Reset balance chain pointer for this test
		hashCompatLastPoint = nil

		// Setup
		deployer := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		platform.Deployer = deployer.Bytes()

		// Initialize ERC20 extension and attestation helper
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		// Give balance
		err = giveBalanceChainedHashCompat(ctx, platform, deployer.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create data provider
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		// Use a simple stream ID (exactly 32 characters)
		streamID := "sthashcompat00000000000000000000"
		dataProvider := deployer.Address()

		// Create primitive stream and insert data
		engineCtx := helper.NewEngineContext()

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"},
			nil)
		require.NoError(t, err)

		// Insert outcome data
		eventTime := int64(1000)
		valueStr := "1.000000000000000000" // 1.0 = YES outcome
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(valueStr, 36, 18)
		require.NoError(t, err)

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{eventTime},
				[]*kwilTypes.Decimal{valueDecimal},
			},
			nil)
		require.NoError(t, err)

		// ======================================================================
		// STEP 1: Create market with query_components
		// ======================================================================

		// Build query components for get_record action
		// Args: data_provider, stream_id, from, to, frozen_at, use_cache
		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			dataProvider,
			streamID,
			int64(500),  // from
			int64(1500), // to
			nil,         // frozen_at (NULL = latest)
			false,       // use_cache
		})
		require.NoError(t, err)

		// Encode query components using ABI
		queryComponents, err := encodeQueryComponentsABIHashCompat(dataProvider, streamID, "get_record", argsBytes)
		require.NoError(t, err)

		// Create market
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var marketQueryID int
		var marketHash []byte

		// Use separate engine context for market creation
		marketCtx := helper.NewEngineContext()
		res, err := platform.Engine.Call(marketCtx, platform.DB, "", "create_market",
			[]any{testUSDCExtensionNameHashCompat, queryComponents, settleTime, int64(5), int64(100)},
			func(row *common.Row) error {
				marketQueryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err, "engine call failed")
		if res != nil && res.Error != nil {
			t.Logf("create_market error: %v", res.Error)
			require.NoError(t, res.Error, "create_market action failed")
		}

		// Get the market hash
		err = callGetMarketInfoHashCompat(ctx, platform, &deployer, marketQueryID, func(row *common.Row) error {
			marketHash = row.Values[0].([]byte)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, marketHash, 32, "market hash should be 32 bytes")

		t.Logf("Market hash: %x", marketHash)

		// ======================================================================
		// STEP 2: Request attestation with same components
		// ======================================================================

		var requestTxID string
		var attestationHash []byte

		// Use separate engine context for attestation request
		attestCtx := helper.NewEngineContext()
		res, err = platform.Engine.Call(attestCtx, platform.DB, "", "request_attestation",
			[]any{
				dataProvider,
				streamID,
				"get_record",
				argsBytes,
				false, // encrypt_sig
				nil,   // max_fee
			},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err, "engine call failed")
		if res != nil && res.Error != nil {
			t.Logf("request_attestation error: %v", res.Error)
			require.NoError(t, res.Error, "request_attestation action failed")
		}

		require.NotEmpty(t, requestTxID, "request_tx_id should not be empty")
		require.Len(t, attestationHash, 32, "attestation hash should be 32 bytes")

		t.Logf("Attestation hash: %x", attestationHash)

		// ======================================================================
		// STEP 3: CRITICAL ASSERTION - Hashes MUST match
		// ======================================================================

		require.Equal(t, marketHash, attestationHash,
			"Market hash MUST equal attestation hash for automatic settlement to work!\n"+
				"Market hash:      %x\n"+
				"Attestation hash: %x",
			marketHash, attestationHash)

		t.Logf("✅ SUCCESS: Market hash matches attestation hash!")

		return nil
	}
}

// testHashCompatibility_SameComponentsProduceSameHash tests determinism
func testHashCompatibility_SameComponentsProduceSameHash(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// CRITICAL: Reset balance chain pointer for this test
		hashCompatLastPoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedHashCompat(ctx, platform, deployer.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create query components
		dataProvider := deployer.Address()
		streamID := "stsamecomponents0000000000000000"
		actionID := "get_record"
		argsBytes := []byte{0x01, 0x02, 0x03, 0x04}

		queryComponents, err := encodeQueryComponentsABIHashCompat(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		// Create two markets with SAME components (will fail on second due to duplicate hash)
		settleTime1 := time.Now().Add(1 * time.Hour).Unix()
		var hash1 []byte
		var queryID1 int

		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        deployer.Bytes(),
			Caller:        deployer.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testUSDCExtensionNameHashCompat, queryComponents, settleTime1, int64(5), int64(100)},
			func(row *common.Row) error {
				queryID1 = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if res != nil && res.Error != nil {
			require.NoError(t, res.Error)
		}

		// Get hash from first market
		err = callGetMarketInfoHashCompat(ctx, platform, &deployer, queryID1, func(row *common.Row) error {
			hash1 = row.Values[0].([]byte)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, hash1, 32, "hash should be 32 bytes")

		// Try to create second market with same components (will fail due to duplicate hash)
		settleTime2 := time.Now().Add(2 * time.Hour).Unix()
		tx2 := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    2,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        deployer.Bytes(),
			Caller:        deployer.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx2 := &common.EngineContext{TxContext: tx2}

		res, err = platform.Engine.Call(engineCtx2, platform.DB, "", "create_market",
			[]any{testUSDCExtensionNameHashCompat, queryComponents, settleTime2, int64(5), int64(100)},
			nil)
		require.NoError(t, err, "engine call should not error")
		require.NotNil(t, res, "result should not be nil")
		require.Error(t, res.Error, "should fail due to duplicate hash")
		require.Contains(t, res.Error.Error(), "already exists", "error should mention duplicate")

		t.Logf("✅ Same components produce same hash (duplicate correctly rejected)")

		return nil
	}
}

// testHashCompatibility_DifferentComponentsProduceDifferentHash tests uniqueness
func testHashCompatibility_DifferentComponentsProduceDifferentHash(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// CRITICAL: Reset balance chain pointer for this test
		hashCompatLastPoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedHashCompat(ctx, platform, deployer.Address(), "100000000000000000000")
		require.NoError(t, err)

		dataProvider := deployer.Address()
		streamID1 := "stdiffcomp1000000000000000000000"
		streamID2 := "stdiffcomp2000000000000000000000"
		actionID := "get_record"
		argsBytes := []byte{0x00}

		// Create two markets with DIFFERENT stream IDs
		components1, err := encodeQueryComponentsABIHashCompat(dataProvider, streamID1, actionID, argsBytes)
		require.NoError(t, err)

		components2, err := encodeQueryComponentsABIHashCompat(dataProvider, streamID2, actionID, argsBytes)
		require.NoError(t, err)

		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var queryID1, queryID2 int

		// Create first market
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		require.NoError(t, err)
		pub := pubGeneric.(*crypto.Secp256k1PublicKey)

		tx1 := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        deployer.Bytes(),
			Caller:        deployer.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx1 := &common.EngineContext{TxContext: tx1}

		res, err := platform.Engine.Call(engineCtx1, platform.DB, "", "create_market",
			[]any{testUSDCExtensionNameHashCompat, components1, settleTime, int64(5), int64(100)},
			func(row *common.Row) error {
				queryID1 = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if res != nil && res.Error != nil {
			require.NoError(t, res.Error)
		}

		// Create second market
		tx2 := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    2,
				Timestamp: time.Now().Unix(),
				Proposer:  pub,
			},
			Signer:        deployer.Bytes(),
			Caller:        deployer.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx2 := &common.EngineContext{TxContext: tx2}

		res, err = platform.Engine.Call(engineCtx2, platform.DB, "", "create_market",
			[]any{testUSDCExtensionNameHashCompat, components2, settleTime, int64(5), int64(100)},
			func(row *common.Row) error {
				queryID2 = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if res != nil && res.Error != nil {
			require.NoError(t, res.Error)
		}

		// Get hashes from both markets
		var hash1, hash2 []byte
		err = callGetMarketInfoHashCompat(ctx, platform, &deployer, queryID1, func(row *common.Row) error {
			hash1 = row.Values[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		err = callGetMarketInfoHashCompat(ctx, platform, &deployer, queryID2, func(row *common.Row) error {
			hash2 = row.Values[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		// Hashes MUST be different
		require.NotEqual(t, hash1, hash2, "different components should produce different hashes")

		t.Logf("✅ Different components produce different hashes")
		t.Logf("Hash 1: %x", hash1)
		t.Logf("Hash 2: %x", hash2)

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// giveBalanceChainedHashCompat injects balance to BOTH bridges with proper chaining:
// 1. hoodi_tt (TRUF) for market creation fees
// 2. hoodi_tt2 (USDC) for market collateral/trading
func giveBalanceChainedHashCompat(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	// Inject TRUF balance for market creation fees (chained)
	hashCompatBalanceCounter++
	currentPoint := hashCompatBalanceCounter

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTRUFChainHashCompat,
		testTRUFEscrowHashCompat,
		testTRUFERC20HashCompat,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		hashCompatLastPoint,
	)
	if err != nil {
		return fmt.Errorf("failed to inject TRUF balance: %w", err)
	}

	p := currentPoint
	hashCompatLastPoint = &p

	// Inject USDC balance for market collateral (chained)
	hashCompatBalanceCounter++
	currentPoint = hashCompatBalanceCounter

	err = testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testUSDCChainHashCompat,
		testUSDCEscrowHashCompat,
		testUSDCERC20HashCompat,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		hashCompatLastPoint,
	)
	if err != nil {
		return fmt.Errorf("failed to inject USDC balance: %w", err)
	}

	p = currentPoint
	hashCompatLastPoint = &p

	return nil
}

func encodeQueryComponentsABIHashCompat(dataProvider, streamID, actionID string, args []byte) ([]byte, error) {
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, err
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, err
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, err
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, err
	}

	abiArgs := gethAbi.Arguments{
		{Type: addressType, Name: "data_provider"},
		{Type: bytes32Type, Name: "stream_id"},
		{Type: stringType, Name: "action_id"},
		{Type: bytesType, Name: "args"},
	}

	dpAddr := gethCommon.HexToAddress(dataProvider)
	var streamIDBytes [32]byte
	copy(streamIDBytes[:], []byte(streamID))

	encoded, err := abiArgs.Pack(dpAddr, streamIDBytes, actionID, args)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

func callGetMarketInfoHashCompat(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, resultFn func(*common.Row) error) error {
	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: 1},
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
		"get_market_info",
		[]any{queryID},
		resultFn,
	)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}
