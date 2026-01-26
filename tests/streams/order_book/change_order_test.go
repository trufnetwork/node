//go:build kwiltest

package order_book

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestChangeOrder(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_05B_ChangeOrder",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// change_bid tests
			testChangeBidSuccess(t),
			testChangeBidHigherPrice(t),
			testChangeBidInsufficientBalance(t),
			testChangeBidTimestampPreservation(t),
			testChangeBidInvalidPrices(t),

			// change_ask tests
			testChangeAskSuccess(t),
			testChangeAskHigherPrice(t),
			testChangeAskTimestampPreservation(t),
			testChangeAskInvalidPrices(t),
			testChangeAskUpsizing(t),
			testChangeAskDownsizing(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Helper Functions
// =============================================================================

func callChangeBid(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress,
	marketID int, outcome bool, oldPrice, newPrice int, newAmount int64) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
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
		"change_bid",
		[]any{marketID, outcome, oldPrice, newPrice, newAmount},
		nil,
	)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

func callChangeAsk(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress,
	marketID int, outcome bool, oldPrice, newPrice int, newAmount int64) error {
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
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
		"change_ask",
		[]any{marketID, outcome, oldPrice, newPrice, newAmount},
		nil,
	)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

func getOrderDetails(ctx context.Context, platform *kwilTesting.Platform,
	marketID int, walletAddr string, outcome bool, price int) (amount int64, timestamp int64, err error) {
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:          platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	query := `SELECT p.amount, p.last_updated
	          FROM ob_positions p
	          INNER JOIN ob_participants part ON p.participant_id = part.id
	          WHERE p.query_id = $query_id
	            AND part.wallet_address = $wallet_addr
	            AND p.outcome = $outcome
	            AND p.price = $price`

	found := false
	err = platform.Engine.Execute(
		engineCtx,
		platform.DB,
		query,
		map[string]any{
			"$query_id":     marketID,
			"$wallet_addr":  decodeAddress(walletAddr),
			"$outcome":      outcome,
			"$price":        price,
		},
		func(row *common.Row) error {
			amount = row.Values[0].(int64)
			timestamp = row.Values[1].(int64)
			found = true
			return nil
		},
	)

	if err != nil {
		return 0, 0, err
	}
	if !found {
		return 0, 0, nil
	}
	return amount, timestamp, nil
}

func decodeAddress(addr string) []byte {
	// Remove 0x prefix and decode hex using standard library
	// This correctly handles both uppercase and lowercase hex characters
	if len(addr) >= 2 && addr[0:2] == "0x" {
		addr = addr[2:]
	}
	result, err := hex.DecodeString(addr)
	if err != nil {
		// In test context, panic is acceptable for invalid addresses
		panic("invalid hex address: " + err.Error())
	}
	return result
}

// =============================================================================
// change_bid Tests
// =============================================================================

func testChangeBidSuccess(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_bid_success0", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place initial buy order: 100 YES @ $0.54
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 54, 100)
		require.NoError(t, err)

		// Verify initial order exists
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -54)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		// Change bid: $0.54 → $0.50
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -54, -50, 100)
		require.NoError(t, err)

		// Verify old order deleted
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -54)
		require.NoError(t, err)
		require.Equal(t, int64(0), amount)

		// Verify new order exists
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -50)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		return nil
	}
}

func testChangeBidHigherPrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")
		err = giveBalance(ctx, platform, userAddr.Address(), "300000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_bid_higher00", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place buy: 100 YES @ $0.50 (locks 50 TRUF)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 100)
		require.NoError(t, err)

		// Change bid to higher price: $0.50 → $0.60 (needs additional 10 TRUF)
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -50, -60, 100)
		require.NoError(t, err)

		// Verify new order at $0.60
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -60)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		return nil
	}
}

func testChangeBidInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		// Give limited balance: market (2) + initial buy (50) + insufficient for upsize
		err = giveBalance(ctx, platform, userAddr.Address(), "102000000000000000000") // 102 TRUF
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_bid_insuffic", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place buy: 100 YES @ $0.50 (locks 50 TRUF)
		// Remaining balance: 102 - 2 - 50 = 50 TRUF
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 100)
		require.NoError(t, err)

		// Try to change to $0.99 (needs 99 TRUF total, additional 49 TRUF)
		// Available: 50 TRUF, Need: 49 TRUF → This would succeed!
		// Let's try $0.01 higher to make it fail
		// Change to price $1.00 would need 100 TRUF total (additional 50 TRUF)
		// But max price is $0.99, so use that with more shares
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -50, -99, 200)
		require.Error(t, err)
		require.Contains(t, err.Error(), "insufficient balance")

		// Verify old order still exists (transaction reverted)
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -50)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		return nil
	}
}

func testChangeBidTimestampPreservation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_bid_timestam", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place initial buy order
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 54, 100)
		require.NoError(t, err)

		// Get original timestamp
		_, originalTimestamp, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -54)
		require.NoError(t, err)
		require.Greater(t, originalTimestamp, int64(0))

		// Change bid (timestamp should be preserved from original order)
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -54, -50, 100)
		require.NoError(t, err)

		// Get new order timestamp
		_, newTimestamp, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, -50)
		require.NoError(t, err)

		// Verify timestamp preserved
		require.Equal(t, originalTimestamp, newTimestamp, "Timestamp should be preserved for FIFO ordering")

		return nil
	}
}

func testChangeBidInvalidPrices(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_bid_invalid0", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 54, 100)
		require.NoError(t, err)

		// Test 1: Same price (old = new)
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -54, -54, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must differ")

		// Test 2: New price positive (sell price for buy order)
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -54, 50, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be negative")

		// Test 3: Price out of range
		err = callChangeBid(ctx, platform, &userAddr, int(marketID), true, -54, -100, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "between -99 and -1")

		return nil
	}
}

// =============================================================================
// change_ask Tests
// =============================================================================

func testChangeAskSuccess(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_ask_success0", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order to get shares
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Verify initial sell order at $0.44 (NO shares)
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		// Change ask: $0.44 → $0.40
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 40, 100)
		require.NoError(t, err)

		// Verify old order deleted
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Equal(t, int64(0), amount)

		// Verify new order exists
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 40)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		return nil
	}
}

func testChangeAskHigherPrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_ask_higher00", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Change ask to higher price: $0.44 → $0.48
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 48, 100)
		require.NoError(t, err)

		// Verify new order at $0.48
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 48)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		return nil
	}
}

func testChangeAskTimestampPreservation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_ask_timestam", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Get original timestamp
		_, originalTimestamp, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Greater(t, originalTimestamp, int64(0))

		// Change ask (timestamp should be preserved from original order)
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 40, 100)
		require.NoError(t, err)

		// Get new order timestamp
		_, newTimestamp, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 40)
		require.NoError(t, err)

		// Verify timestamp preserved
		require.Equal(t, originalTimestamp, newTimestamp, "Timestamp should be preserved for FIFO ordering")

		return nil
	}
}

func testChangeAskInvalidPrices(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_ask_invalid0", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Test 1: Same price
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 44, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must differ")

		// Test 2: New price negative (buy price for sell order)
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, -50, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be positive")

		// Test 3: Price out of range
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 100, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "between 1 and 99")

		return nil
	}
}

func testChangeAskUpsizing(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		err = giveBalance(ctx, platform, userAddr.Address(), "300000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_ask_upsize00", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order: holds 100 YES, sells 100 NO @ $0.44
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Verify initial holdings (100 YES shares at price=0)
		holdings, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, 0)
		require.NoError(t, err)
		require.Equal(t, int64(100), holdings)

		// Cancel the NO sell order to get those shares back as holdings
		err = callCancelOrder(ctx, platform, &userAddr, int(marketID), false, 44)
		require.NoError(t, err)

		// Now we have 100 YES holdings + 100 NO holdings
		noHoldings, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 0)
		require.NoError(t, err)
		require.Equal(t, int64(100), noHoldings)

		// Place a NO sell order at $0.44 with 50 shares (leaving 50 NO in holdings)
		err = callPlaceSellOrder(ctx, platform, &userAddr, int(marketID), false, 44, 50)
		require.NoError(t, err)

		// Verify NO sell order
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Equal(t, int64(50), amount)

		// Verify NO holdings (50 remaining)
		noHoldings, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 0)
		require.NoError(t, err)
		require.Equal(t, int64(50), noHoldings)

		// Upsize sell order: 50 NO @ $0.44 → 100 NO @ $0.40
		// This should pull 50 additional NO shares from holdings
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 40, 100)
		require.NoError(t, err)

		// Verify new sell order has 100 shares
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 40)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		// Verify old order deleted
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Equal(t, int64(0), amount)

		// Verify NO holdings depleted (used for upsize)
		noHoldings, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), noHoldings)

		return nil
	}
}

func testChangeAskDownsizing(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		userAddr := util.Unsafe_NewEthereumAddressFromString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		err = giveBalance(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "stchangetest_change_ask_downsize", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place split limit order: holds 100 YES, sells 100 NO @ $0.44
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, int(marketID), 56, 100)
		require.NoError(t, err)

		// Verify initial sell order
		amount, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Equal(t, int64(100), amount)

		// Verify holdings (100 YES shares at price=0)
		holdings, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, 0)
		require.NoError(t, err)
		require.Equal(t, int64(100), holdings)

		// Downsize sell order: 100 NO @ $0.44 → 50 NO @ $0.40
		// This should return 50 shares to holdings (but NO shares, not YES)
		err = callChangeAsk(ctx, platform, &userAddr, int(marketID), false, 44, 40, 50)
		require.NoError(t, err)

		// Verify new sell order has 50 shares
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 40)
		require.NoError(t, err)
		require.Equal(t, int64(50), amount)

		// Verify old order deleted
		amount, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 44)
		require.NoError(t, err)
		require.Equal(t, int64(0), amount)

		// Verify YES holdings unchanged (still 100)
		holdings, _, err = getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), true, 0)
		require.NoError(t, err)
		require.Equal(t, int64(100), holdings)

		// Verify NO holdings increased by 50 (returned from downsize)
		noHoldings, _, err := getOrderDetails(ctx, platform, int(marketID), userAddr.Address(), false, 0)
		require.NoError(t, err)
		require.Equal(t, int64(50), noHoldings)

		return nil
	}
}
