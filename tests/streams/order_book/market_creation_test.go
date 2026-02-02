//go:build kwiltest

package order_book

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"testing"
	"time"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants - match existing erc20-bridge configuration
const (
	// TRUF bridge (hoodi_tt) - market creation fee is ALWAYS collected from here
	testTRUFChain         = "hoodi"
	testTRUFEscrow        = "0x878d6aaeb6e746033f50b8dc268d54b4631554e7"
	testTRUFERC20         = "0x263ce78fef26600e4e428cebc91c2a52484b4fbf"
	testTRUFExtensionName = "hoodi_tt"

	// USDC bridge (hoodi_tt2) - market collateral for trading
	// IMPORTANT: Must match 000-extension.sql registration (source of truth)
	testUSDCChain         = "hoodi"
	testUSDCEscrow        = "0x80D9B3b6941367917816d36748C88B303f7F1415"
	testUSDCERC20         = "0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd"
	testUSDCExtensionName = "hoodi_tt2"

	// Aliases for other test files that don't use create_market
	testChain         = testUSDCChain
	testEscrow        = testUSDCEscrow
	testERC20         = testUSDCERC20
	testExtensionName = testUSDCExtensionName

	marketFee = "2000000000000000000" // 2 TRUF with 18 decimals
)

var (
	twoTRUF                = mustParseBigInt(marketFee)
	trufPointCounter int64 = 100   // Counter for TRUF bridge (hoodi_tt)
	usdcPointCounter int64 = 10000 // Counter for USDC bridge (hoodi_tt2) - far apart to avoid conflicts
)

func mustParseBigInt(s string) *big.Int {
	val := new(big.Int)
	if _, ok := val.SetString(s, 10); !ok {
		panic(fmt.Sprintf("mustParseBigInt: invalid integer string: %s", s))
	}
	return val
}

// TestCreateMarket is the main test suite for market creation
func TestCreateMarket(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_01_CreateMarket",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testCreateMarketHappyPath(t),
			testCreateMarketValidation(t),
			testCreateMarketDuplicateHash(t),
			testCreateMarketInsufficientBalance(t),
			testGetMarketInfo(t),
			testListMarkets(t),
			testMarketExists(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testCreateMarketHappyPath tests successful market creation
func testCreateMarketHappyPath(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Give user balance
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000") // 100 TRUF
		require.NoError(t, err, "failed to give balance")

		// Encode query components
		dataProvider := userAddr.Address()
		streamID := "sthappypath000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x01, 0x02, 0x03}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err, "failed to encode query components")

		// Set settlement time to 1 hour from now
		settleTime := time.Now().Add(1 * time.Hour).Unix()

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Create market
		var queryID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(20), func(row *common.Row) error {
			require.Len(t, row.Values, 1, "expected 1 return value")
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err, "create_market should succeed")
		require.Greater(t, queryID, int64(0), "query_id should be positive")

		// Verify balance decreased by 2 TRUF
		finalBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get final balance")

		expectedBalance := new(big.Int).Sub(initialBalance, twoTRUF)
		require.Equal(t, 0, expectedBalance.Cmp(finalBalance),
			"Balance should decrease by 2 TRUF fee")

		// Verify market was created via get_market_info
		var storedHash []byte
		var storedComponents []byte
		var storedBridge string
		var storedSettleTime int64
		var settled bool
		err = callGetMarketInfo(ctx, platform, &userAddr, int(queryID), func(row *common.Row) error {
			storedHash = row.Values[0].([]byte)
			storedComponents = row.Values[1].([]byte)
			storedBridge = row.Values[2].(string)
			storedSettleTime = row.Values[3].(int64)
			settled = row.Values[4].(bool)
			return nil
		})
		require.NoError(t, err, "get_market_info should succeed")
		require.Len(t, storedHash, 32, "hash should be 32 bytes")
		require.Equal(t, queryComponents, storedComponents, "components should match")
		require.Equal(t, testUSDCExtensionName, storedBridge, "bridge should match")
		require.Equal(t, settleTime, storedSettleTime, "settle_time should match")
		require.False(t, settled, "market should not be settled")

		return nil
	}
}

// testCreateMarketValidation tests validation errors
func testCreateMarketValidation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give balance
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Encode valid query components
		dataProvider := userAddr.Address()
		streamID := "stvalidation0000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x01}

		validComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		futureTime := time.Now().Add(1 * time.Hour).Unix()

		// Test: Empty query_components
		emptyComponents := []byte{}
		err = callCreateMarket(ctx, platform, &userAddr, emptyComponents, futureTime, int64(5), int64(20), nil)
		require.Error(t, err, "should fail with empty query_components")
		require.Contains(t, err.Error(), "query_components", "error should mention query_components")

		// Test: Settlement time in the past
		pastTime := time.Now().Add(-1 * time.Hour).Unix()
		err = callCreateMarket(ctx, platform, &userAddr, validComponents, pastTime, int64(5), int64(20), nil)
		require.Error(t, err, "should fail with past settlement time")
		require.Contains(t, err.Error(), "future", "error should mention future")

		// Test: Invalid max_spread (too high)
		err = callCreateMarket(ctx, platform, &userAddr, validComponents, futureTime, int64(100), int64(20), nil)
		require.Error(t, err, "should fail with max_spread > 50")
		require.Contains(t, err.Error(), "spread", "error should mention spread")

		// Test: Invalid max_spread (zero)
		err = callCreateMarket(ctx, platform, &userAddr, validComponents, futureTime, int64(0), int64(20), nil)
		require.Error(t, err, "should fail with max_spread = 0")

		// Test: Invalid min_order_size (zero)
		err = callCreateMarket(ctx, platform, &userAddr, validComponents, futureTime, int64(5), int64(0), nil)
		require.Error(t, err, "should fail with min_order_size = 0")

		return nil
	}
}

// testCreateMarketDuplicateHash tests duplicate hash rejection
func testCreateMarketDuplicateHash(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Give balance
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Encode query components
		dataProvider := userAddr.Address()
		streamID := "stduplicate0000000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x01}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		settleTime := time.Now().Add(1 * time.Hour).Unix()

		// Create first market (should succeed)
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(20), nil)
		require.NoError(t, err, "first market creation should succeed")

		// Try to create duplicate (should fail due to UNIQUE constraint)
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime+3600, int64(5), int64(20), nil)
		require.Error(t, err, "duplicate hash should be rejected")

		return nil
	}
}

// testCreateMarketInsufficientBalance tests insufficient balance handling
func testCreateMarketInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Give only 1 TRUF (less than required 2 TRUF fee)
		err := giveBalance(ctx, platform, userAddr.Address(), "1000000000000000000")
		require.NoError(t, err)

		// Encode query components
		dataProvider := userAddr.Address()
		streamID := "stinsufficient000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x01}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		settleTime := time.Now().Add(1 * time.Hour).Unix()

		// Try to create market (should fail due to insufficient TRUF balance)
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(20), nil)
		require.Error(t, err, "should fail with insufficient TRUF balance")
		require.Contains(t, err.Error(), "Insufficient TRUF balance", "error should mention insufficient TRUF balance")

		return nil
	}
}

// testGetMarketInfo tests get_market_info action
func testGetMarketInfo(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Give balance and create market
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Encode query components
		dataProvider := userAddr.Address()
		streamID := "stgetmarketinfo00000000000000000" // Exactly 32 chars
		actionID := "get_index"
		argsBytes := []byte{0x01, 0x02}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		settleTime := time.Now().Add(2 * time.Hour).Unix()

		var queryID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, int64(10), int64(50), func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test get_market_info
		var hash []byte
		var components []byte
		var bridge string
		var storedSettleTime int64
		var maxSpread int64
		var minOrderSize int64
		err = callGetMarketInfo(ctx, platform, &userAddr, int(queryID), func(row *common.Row) error {
			hash = row.Values[0].([]byte)
			components = row.Values[1].([]byte)
			bridge = row.Values[2].(string)
			storedSettleTime = row.Values[3].(int64)
			// settled = row.Values[4].(bool)
			// winningOutcome = row.Values[5]
			// settledAt = row.Values[6]
			maxSpread = row.Values[7].(int64)
			minOrderSize = row.Values[8].(int64)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, hash, 32, "hash should be 32 bytes")
		require.Equal(t, queryComponents, components, "components should match")
		require.Equal(t, testUSDCExtensionName, bridge, "bridge should match")
		require.Equal(t, settleTime, storedSettleTime)
		require.Equal(t, int64(10), maxSpread)
		require.Equal(t, int64(50), minOrderSize)

		// Test non-existent market
		err = callGetMarketInfo(ctx, platform, &userAddr, 99999, nil)
		require.Error(t, err, "should fail for non-existent market")
		require.Contains(t, err.Error(), "not found")

		return nil
	}
}

// testListMarkets tests list_markets action
func testListMarkets(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Give balance
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Create 3 markets
		for i := 0; i < 3; i++ {
			dataProvider := userAddr.Address()
			// Generate exactly 32-char stream ID
			streamID := fmt.Sprintf("stlistmarkets%02d00000000000000000", i)[:32]
			actionID := "get_record"
			argsBytes := []byte{byte(i)}
			queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
			require.NoError(t, err)
			settleTime := time.Now().Add(time.Duration(i+1) * time.Hour).Unix()
			err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(20), nil)
			require.NoError(t, err)
		}

		// List all markets
		count := 0
		err = callListMarkets(ctx, platform, &userAddr, nil, 100, 0, func(row *common.Row) error {
			count++
			return nil
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, 3, "should have at least 3 markets")

		return nil
	}
}

// testMarketExists tests market_exists action
func testMarketExists(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Give balance
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Encode query components
		dataProvider := userAddr.Address()
		streamID := "stmarketexists000000000000000000" // Exactly 32 chars
		actionID := "get_record"
		argsBytes := []byte{0x01}

		queryComponents, err := encodeQueryComponents(dataProvider, streamID, actionID, argsBytes)
		require.NoError(t, err)

		// Create a market
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var queryID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, int64(5), int64(20), func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Get the hash from get_market_info
		var queryHash []byte
		err = callGetMarketInfo(ctx, platform, &userAddr, int(queryID), func(row *common.Row) error {
			queryHash = row.Values[0].([]byte)
			return nil
		})
		require.NoError(t, err)

		// Test market_exists for existing market
		var exists bool
		err = callMarketExists(ctx, platform, &userAddr, queryHash, func(row *common.Row) error {
			exists = row.Values[0].(bool)
			return nil
		})
		require.NoError(t, err)
		require.True(t, exists, "market should exist")

		// Test market_exists for non-existent market
		nonExistentHash := sha256.Sum256([]byte("non_existent_market"))
		err = callMarketExists(ctx, platform, &userAddr, nonExistentHash[:], func(row *common.Row) error {
			exists = row.Values[0].(bool)
			return nil
		})
		require.NoError(t, err)
		require.False(t, exists, "market should not exist")

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

// giveBalance injects balance to BOTH bridges:
// 1. hoodi_tt (TRUF) for market creation fees
// 2. hoodi_tt2 (USDC) for market collateral/trading
func giveBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	// Reset ordered-sync state to start fresh (clears last_processed_point)
	orderedsync.ForTestingReset()

	// Inject TRUF balance (use separate counter per bridge, starting fresh each test)
	trufPointCounter++
	fmt.Printf("DEBUG giveBalance: Injecting TRUF at point %d\n", trufPointCounter)
	err := testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testTRUFChain,
		testTRUFEscrow,
		testTRUFERC20,
		wallet,
		wallet,
		amountStr,
		trufPointCounter,
		nil, // No chaining
	)
	if err != nil {
		return fmt.Errorf("failed to inject TRUF: %w", err)
	}

	// Inject USDC balance (use separate counter)
	usdcPointCounter++
	fmt.Printf("DEBUG giveBalance: Injecting USDC at point %d\n", usdcPointCounter)
	err = testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testUSDCChain,
		testUSDCEscrow,
		testUSDCERC20,
		wallet,
		wallet,
		amountStr,
		usdcPointCounter,
		nil, // No chaining - separate topic from TRUF
	)
	if err != nil {
		return fmt.Errorf("failed to inject USDC: %w", err)
	}

	return nil
}

func getBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	// Market creation tests check TRUF balance (hoodi_tt) since fee is deducted from there
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testTRUFExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

func callCreateMarket(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryComponents []byte, settleTime int64, maxSpread int64, minOrderSize int64, resultFn func(*common.Row) error) error {
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
		[]any{testUSDCExtensionName, queryComponents, settleTime, maxSpread, minOrderSize},
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

func callGetMarketInfo(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryID int, resultFn func(*common.Row) error) error {
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

func callListMarkets(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, settledFilter *bool, limit int, offset int, resultFn func(*common.Row) error) error {
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
		"list_markets",
		[]any{settledFilter, limit, offset},
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

func callMarketExists(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryHash []byte, resultFn func(*common.Row) error) error {
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
		"market_exists",
		[]any{queryHash},
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

// encodeQueryComponents encodes query components using ABI format
// Format: (address data_provider, bytes32 stream_id, string action_id, bytes args)
func encodeQueryComponents(dataProvider, streamID, actionID string, args []byte) ([]byte, error) {
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
