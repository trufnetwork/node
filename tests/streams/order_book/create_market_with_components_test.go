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
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants for bridges
const (
	// TRUF bridge (hoodi_tt) - market creation fee is ALWAYS collected from here
	testTRUFChainComponents         = "hoodi"
	testTRUFEscrowComponents        = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testTRUFERC20Components         = "0x263ce78fef26600e4e428cebc91c2a52484b4fbf"
	testTRUFExtensionNameComponents = "hoodi_tt"

	// USDC bridge (hoodi_tt2) - market collateral for trading
	testUSDCChainComponents         = "hoodi"
	testUSDCEscrowComponents        = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	testUSDCERC20Components         = "0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd"
	testUSDCExtensionNameComponents = "hoodi_tt2"

)

// Balance tracking for chained deposits (prevents multi-user balance issues)
// Note: Uses separate counter from market_creation_test.go to avoid conflicts
var (
	balancePointCounterComponents int64 = 200 // Start at 200 to avoid conflicts
	lastBalancePointComponents    *int64
)

// TestCreateMarketWithQueryComponents tests market creation with query_components
func TestCreateMarketWithQueryComponents(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_CreateMarketWithComponents",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testCreateMarketWithValidComponents(t),
			testCreateMarketStoresHashAndComponents(t),
			testCreateMarketRejectsDuplicateHash(t),
			testCreateMarketRejectsEmptyComponents(t),
			testCreateMarketRejectsMalformedABI(t),
			testGetMarketInfoReturnsComponentsAndBridge(t),
			testCreateMarketWithDifferentBridges(t),
		},
	}, testutils.GetTestOptionsWithCache())
}
// testCreateMarketWithValidComponents tests successful market creation with query_components
func testCreateMarketWithValidComponents(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000") // 100 TRUF
		require.NoError(t, err)

		// Encode query components
		dataProvider := userAddr.Address()
		streamID := "stbtcusd00000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"

		queryComponents, err := encodeQueryComponentsForTests(dataProvider, streamID, actionID, nil)
		require.NoError(t, err)

		// Create market
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var queryID int
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(100), func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err, "create_market should succeed with valid query_components")
		require.Greater(t, queryID, 0, "query_id should be positive")

		return nil
	}
}

// testCreateMarketStoresHashAndComponents verifies that both hash and components are stored
func testCreateMarketStoresHashAndComponents(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create query components
		dataProvider := userAddr.Address()
		streamID := "ststorage00000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x01, 0x02, 0x03}

		queryComponents, err := encodeQueryComponentsABI(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		// Create market
		settleTime := time.Now().Add(2 * time.Hour).Unix()
		var queryID int
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(100), func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)

		// Verify market info returns both hash and components
		var storedHash []byte
		var storedComponents []byte
		var storedBridge string
		err = callGetMarketInfoWithComponents(ctx, platform, &userAddr, queryID, func(row *common.Row) error {
			storedHash = row.Values[0].([]byte)
			storedComponents = row.Values[1].([]byte)
			storedBridge = row.Values[2].(string)
			return nil
		})
		require.NoError(t, err)

		// Verify hash is 32 bytes
		require.Len(t, storedHash, 32, "hash should be 32 bytes")

		// Verify components match what we sent
		require.Equal(t, queryComponents, storedComponents, "stored components should match input")

		// Verify bridge is correct
		require.Equal(t, testUSDCExtensionNameComponents, storedBridge, "bridge should match")

		return nil
	}
}

// testCreateMarketRejectsDuplicateHash tests that duplicate hash is rejected
func testCreateMarketRejectsDuplicateHash(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create query components
		dataProvider := userAddr.Address()
		streamID := "stduplicate000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0xAA, 0xBB}

		queryComponents, err := encodeQueryComponentsABI(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		// Create first market (should succeed)
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(100), nil)
		require.NoError(t, err, "first market creation should succeed")

		// Try to create duplicate (should fail)
		settleTime2 := time.Now().Add(2 * time.Hour).Unix()
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime2, int64(5), int64(100), nil)
		require.Error(t, err, "duplicate hash should be rejected")
		require.Contains(t, err.Error(), "already exists", "error should mention duplicate")

		return nil
	}
}

// testCreateMarketRejectsEmptyComponents tests that empty query_components is rejected
func testCreateMarketRejectsEmptyComponents(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Try to create market with empty components
		emptyComponents := []byte{}
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, emptyComponents, settleTime, int64(5), int64(100), nil)
		require.Error(t, err, "empty query_components should be rejected")
		require.Contains(t, err.Error(), "query_components is required", "error should mention query_components")

		return nil
	}
}

// testCreateMarketRejectsMalformedABI tests that malformed ABI encoding is rejected
func testCreateMarketRejectsMalformedABI(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create malformed ABI data
		malformedABI := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00}

		// Try to create market with malformed ABI
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, malformedABI, settleTime, int64(5), int64(100), nil)
		require.Error(t, err, "malformed ABI should be rejected")
		// Error should mention decoding or invalid components
		t.Logf("Malformed ABI error: %v", err)

		return nil
	}
}

// testGetMarketInfoReturnsComponentsAndBridge tests get_market_info returns query_components and bridge
func testGetMarketInfoReturnsComponentsAndBridge(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create query components
		dataProvider := userAddr.Address()
		streamID := "stgetinfo00000000000000000000000000" // Exactly 32 chars
		actionID := "get_index"
		argsBytes := []byte{0x11, 0x22, 0x33, 0x44}

		queryComponents, err := encodeQueryComponentsABI(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		// Create market
		settleTime := time.Now().Add(3 * time.Hour).Unix()
		var queryID int
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime, int64(10), int64(50), func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)

		// Get market info and verify all fields
		var hash []byte
		var components []byte
		var bridge string
		var storedSettleTime int64
		var maxSpread int64
		var minOrderSize int64
		err = callGetMarketInfoWithComponents(ctx, platform, &userAddr, queryID, func(row *common.Row) error {
			hash = row.Values[0].([]byte)
			components = row.Values[1].([]byte)
			bridge = row.Values[2].(string)
			storedSettleTime = row.Values[3].(int64)
			// row.Values[4] = settled (bool)
			// row.Values[5] = winning_outcome
			// row.Values[6] = settled_at
			maxSpread = row.Values[7].(int64)
			minOrderSize = row.Values[8].(int64)
			return nil
		})
		require.NoError(t, err)

		// Verify all fields
		require.Len(t, hash, 32, "hash should be 32 bytes")
		require.Equal(t, queryComponents, components, "components should match")
		require.Equal(t, testUSDCExtensionNameComponents, bridge, "bridge should match")
		require.Equal(t, settleTime, storedSettleTime, "settle_time should match")
		require.Equal(t, int64(10), maxSpread, "max_spread should match")
		require.Equal(t, int64(50), minOrderSize, "min_order_size should match")

		return nil
	}
}

// testCreateMarketWithDifferentBridges tests market creation with different bridge parameters
func testCreateMarketWithDifferentBridges(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		lastBalancePointComponents = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Initialize and give balance
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)
		err = giveBalanceChainedComponents(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create query components
		dataProvider := userAddr.Address()
		streamID := "stbridgetest000000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x00}

		queryComponents, err := encodeQueryComponentsABI(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		// Create market with testUSDCExtensionNameComponents bridge
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var queryID int
		err = callCreateMarketWithComponents(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(100), func(row *common.Row) error {
			queryID = int(row.Values[0].(int64))
			return nil
		})
		require.NoError(t, err)

		// Verify bridge is stored correctly
		var storedBridge string
		err = callGetMarketInfoWithComponents(ctx, platform, &userAddr, queryID, func(row *common.Row) error {
			storedBridge = row.Values[2].(string) // bridge is 3rd field
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, testUSDCExtensionNameComponents, storedBridge, "bridge should be stored correctly")

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// giveBalanceChainedComponents injects balance to BOTH bridges with proper chaining:
// 1. hoodi_tt (TRUF) for market creation fees
// 2. hoodi_tt2 (USDC) for market collateral/trading
func giveBalanceChainedComponents(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	// Inject TRUF balance for market creation fees (chained)
	balancePointCounterComponents++
	currentPoint := balancePointCounterComponents

	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTRUFChainComponents,
		testTRUFEscrowComponents,
		testTRUFERC20Components,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		lastBalancePointComponents, // Chain to previous
	)
	if err != nil {
		return fmt.Errorf("failed to inject TRUF balance: %w", err)
	}

	p := currentPoint
	lastBalancePointComponents = &p

	// Inject USDC balance for market collateral (chained)
	balancePointCounterComponents++
	currentPoint = balancePointCounterComponents

	err = testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testUSDCChainComponents,
		testUSDCEscrowComponents,
		testUSDCERC20Components,
		wallet,
		wallet,
		amountStr,
		currentPoint,
		lastBalancePointComponents, // Chain to TRUF injection
	)
	if err != nil {
		return fmt.Errorf("failed to inject USDC balance: %w", err)
	}

	p = currentPoint
	lastBalancePointComponents = &p

	return nil
}

func encodeQueryComponentsABI(dataProvider, streamID, actionID string, args []byte) ([]byte, error) {
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create address type: %w", err)
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes32 type: %w", err)
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create string type: %w", err)
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create bytes type: %w", err)
	}

	abiArgs := gethAbi.Arguments{
		{Type: addressType, Name: "data_provider"},
		{Type: bytes32Type, Name: "stream_id"},
		{Type: stringType, Name: "action_id"},
		{Type: bytesType, Name: "args"},
	}

	// Convert data provider to address
	dpAddr := gethCommon.HexToAddress(dataProvider)

	// Convert stream ID to bytes32 (pad with zeros on the right)
	var streamIDBytes [32]byte
	copy(streamIDBytes[:], []byte(streamID))

	// Pack the ABI
	encoded, err := abiArgs.Pack(dpAddr, streamIDBytes, actionID, args)
	if err != nil {
		return nil, fmt.Errorf("failed to pack ABI: %w", err)
	}

	return encoded, nil
}

func callCreateMarketWithComponents(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryComponents []byte, settleTime int64, maxSpread int64, minOrderSize int64, resultFn func(*common.Row) error) error {
	// Generate leader key for fee transfers
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)

	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
			Proposer:  pub,
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
		"create_market",
		[]any{testUSDCExtensionNameComponents, queryComponents, settleTime, maxSpread, minOrderSize},
		resultFn,
	)
	// Log NOTICE messages for debugging
	if res != nil && len(res.Logs) > 0 {
		fmt.Printf("=== create_market logs ===\n")
		for i, log := range res.Logs {
			fmt.Printf("  [%d] %s\n", i, log)
		}
		fmt.Printf("==========================\n")
	}
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

func callGetMarketInfoWithComponents(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, resultFn func(*common.Row) error) error {
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
