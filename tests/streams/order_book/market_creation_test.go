//go:build kwiltest

package order_book

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Test constants - match existing erc20-bridge configuration
const (
	testChain         = "sepolia"
	testEscrow        = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	testERC20         = "0x2222222222222222222222222222222222222222"
	testExtensionName = "sepolia_bridge"
	marketFee         = "2000000000000000000" // 2 TRUF with 18 decimals
)

var (
	twoTRUF      = mustParseBigInt(marketFee)
	pointCounter int64 = 100 // Start from 100 to avoid conflicts with other tests
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

		// Generate test query hash
		queryData := []byte("test_data_provider:test_stream:get_record:args")
		queryHash := sha256.Sum256(queryData)

		// Set settlement time to 1 hour from now
		settleTime := time.Now().Add(1 * time.Hour).Unix()

		// Get initial balance
		initialBalance, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err, "failed to get initial balance")

		// Create market
		var queryID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, int64(5), int64(20), func(row *common.Row) error {
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
		var storedSettleTime int64
		var settled bool
		err = callGetMarketInfo(ctx, platform, &userAddr, int(queryID), func(row *common.Row) error {
			storedHash = row.Values[0].([]byte)
			storedSettleTime = row.Values[1].(int64)
			settled = row.Values[2].(bool)
			return nil
		})
		require.NoError(t, err, "get_market_info should succeed")
		require.Equal(t, queryHash[:], storedHash, "hash should match")
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

		validHash := sha256.Sum256([]byte("test_validation"))
		futureTime := time.Now().Add(1 * time.Hour).Unix()

		// Test: Invalid hash length (not 32 bytes)
		shortHash := []byte("too_short") // Less than 32 bytes
		err = callCreateMarket(ctx, platform, &userAddr, shortHash, futureTime, int64(5), int64(20), nil)
		require.Error(t, err, "should fail with invalid hash length")
		require.Contains(t, err.Error(), "32 bytes", "error should mention 32 bytes")

		// Test: Settlement time in the past
		pastTime := time.Now().Add(-1 * time.Hour).Unix()
		err = callCreateMarket(ctx, platform, &userAddr, validHash[:], pastTime, int64(5), int64(20), nil)
		require.Error(t, err, "should fail with past settlement time")
		require.Contains(t, err.Error(), "future", "error should mention future")

		// Test: Invalid max_spread (too high)
		err = callCreateMarket(ctx, platform, &userAddr, validHash[:], futureTime, int64(100), int64(20), nil)
		require.Error(t, err, "should fail with max_spread > 50")
		require.Contains(t, err.Error(), "spread", "error should mention spread")

		// Test: Invalid max_spread (zero)
		err = callCreateMarket(ctx, platform, &userAddr, validHash[:], futureTime, int64(0), int64(20), nil)
		require.Error(t, err, "should fail with max_spread = 0")

		// Test: Invalid min_order_size (zero)
		err = callCreateMarket(ctx, platform, &userAddr, validHash[:], futureTime, int64(5), int64(0), nil)
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

		queryHash := sha256.Sum256([]byte("unique_market_hash"))
		settleTime := time.Now().Add(1 * time.Hour).Unix()

		// Create first market (should succeed)
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, int64(5), int64(20), nil)
		require.NoError(t, err, "first market creation should succeed")

		// Try to create duplicate (should fail due to UNIQUE constraint)
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime+3600, int64(5), int64(20), nil)
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

		queryHash := sha256.Sum256([]byte("insufficient_balance_test"))
		settleTime := time.Now().Add(1 * time.Hour).Unix()

		// Try to create market (should fail due to insufficient balance)
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, int64(5), int64(20), nil)
		require.Error(t, err, "should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance", "error should mention insufficient balance")

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

		queryHash := sha256.Sum256([]byte("get_market_info_test"))
		settleTime := time.Now().Add(2 * time.Hour).Unix()

		var queryID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, int64(10), int64(50), func(row *common.Row) error {
			queryID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test get_market_info
		var hash []byte
		var storedSettleTime int64
		var maxSpread int64
		var minOrderSize int64
		err = callGetMarketInfo(ctx, platform, &userAddr, int(queryID), func(row *common.Row) error {
			hash = row.Values[0].([]byte)
			storedSettleTime = row.Values[1].(int64)
			// settled = row.Values[2].(bool)
			// winningOutcome = row.Values[3]
			// settledAt = row.Values[4]
			maxSpread = row.Values[5].(int64)
			minOrderSize = row.Values[6].(int64)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, queryHash[:], hash)
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
			queryHash := sha256.Sum256([]byte(fmt.Sprintf("list_markets_test_%d", i)))
			settleTime := time.Now().Add(time.Duration(i+1) * time.Hour).Unix()
			err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, int64(5), int64(20), nil)
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

		// Create a market
		queryHash := sha256.Sum256([]byte("market_exists_test"))
		settleTime := time.Now().Add(1 * time.Hour).Unix()
		err = callCreateMarket(ctx, platform, &userAddr, queryHash[:], settleTime, int64(5), int64(20), nil)
		require.NoError(t, err)

		// Test market_exists for existing market
		var exists bool
		err = callMarketExists(ctx, platform, &userAddr, queryHash[:], func(row *common.Row) error {
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

func giveBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amountStr string) error {
	pointCounter++
	return testerc20.InjectERC20Transfer(
		ctx,
		platform,
		testChain,
		testEscrow,
		testERC20,
		wallet,
		wallet,
		amountStr,
		pointCounter,
		nil,
	)
}

func getBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}

func callCreateMarket(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryHash []byte, settleTime int64, maxSpread int64, minOrderSize int64, resultFn func(*common.Row) error) error {
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
		[]any{queryHash, settleTime, maxSpread, minOrderSize},
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
