//go:build kwiltest

package order_book

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Helper: Convert TRUF amount to wei (18 decimals)
// Supports both integer ("100") and decimal ("5.6") formats
// Uses big.Int for precision to avoid floating-point errors
func toWei(trufStr string) *big.Int {
	// Split into integer and fractional parts
	parts := strings.Split(trufStr, ".")
	intPart := parts[0]
	fracPart := ""
	if len(parts) > 1 {
		fracPart = parts[1]
	}

	// Pad fractional part to 18 digits
	if len(fracPart) > 18 {
		panic(fmt.Sprintf("too many decimal places: %s", trufStr))
	}
	fracPart = fracPart + strings.Repeat("0", 18-len(fracPart))

	// Combine and parse as big.Int
	weiStr := intPart + fracPart
	result := new(big.Int)
	if _, ok := result.SetString(weiStr, 10); !ok {
		panic(fmt.Sprintf("invalid TRUF string: %s", trufStr))
	}
	return result
}

// TestPlaceBuyOrder is the main test suite for buy order placement
func TestPlaceBuyOrder(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_02_PlaceBuyOrder",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testBuyOrderSuccessful(t),
			testBuyOrderInsufficientBalance(t),
			testBuyOrderMarketNotFound(t),
			testBuyOrderInvalidPrice(t),
			testBuyOrderInvalidAmount(t),
			testBuyOrderMultipleOrdersDifferentPrices(t),
			testBuyOrderMultipleOrdersSamePrice(t),
			testBuyOrderBalanceChanges(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testBuyOrderSuccessful tests successful buy order placement
func testBuyOrderSuccessful(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Give user 100 TRUF (injected to both TRUF and USDC bridges)
		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err, "failed to give balance")

		// CRITICAL: Load DB instances into singleton cache
		// giveBalance() syncs instances to DATABASE via ForTestingForceSyncInstance
		// But balance()/lock()/unlock() actions check the in-memory SINGLETON cache
		// ForTestingInitializeExtension() loads DB instances into singleton
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err, "failed to initialize ERC20 extension singleton")

		// DEBUG: Check balances AFTER initialization
		trufBal, err := getBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		usdcBal, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		t.Logf("DEBUG: After init - TRUF: %s (expected 100e18), USDC: %s (expected 100e18)", trufBal.String(), usdcBal.String())

		// DEBUG: Try to get balance directly from DB to see if it's there
		balanceStr, err := testerc20.GetUserBalance(ctx, platform, testUSDCExtensionName, userAddr.Address())
		t.Logf("DEBUG: USDC balance from GetUserBalance: %s (err: %v)", balanceStr, err)

		// DEBUG: Check what extension instances exist
		app := &common.App{DB: platform.DB, Engine: platform.Engine}
		var instanceCount int
		_ = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_erc20_meta}SELECT COUNT(*) FROM reward_instances`, nil, func(r *common.Row) error {
			instanceCount = int(r.Values[0].(int64))
			return nil
		})
		t.Logf("DEBUG: Total ERC20 instances in DB: %d", instanceCount)

		// DEBUG: List all instances with sync status
		_ = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_erc20_meta}
			SELECT escrow_address, synced, erc20_address, erc20_decimals
			FROM reward_instances`, nil, func(r *common.Row) error {
			escrow := r.Values[0].([]byte)
			synced := r.Values[1].(bool)
			var erc20Addr []byte
			if r.Values[2] != nil {
				erc20Addr = r.Values[2].([]byte)
			}
			var decimals *int64
			if r.Values[3] != nil {
				d := r.Values[3].(int64)
				decimals = &d
			}
			t.Logf("DEBUG: Instance escrow=0x%x synced=%v erc20=0x%x decimals=%v",
				escrow, synced, erc20Addr, decimals)
			return nil
		})

		// DEBUG: Check user balances table (correct table is 'balances', column is 'address')
		var balanceCount int
		_ = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_erc20_meta}SELECT COUNT(*) FROM balances WHERE address = $addr`,
			map[string]any{"$addr": []byte(userAddr.Bytes())}, func(r *common.Row) error {
			balanceCount = int(r.Values[0].(int64))
			return nil
		})
		t.Logf("DEBUG: User has %d balance records in balances table", balanceCount)

		// DEBUG: Show actual balance values for this user
		_ = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_erc20_meta}
			SELECT b.balance, r.escrow_address, r.erc20_address
			FROM balances b
			JOIN reward_instances r ON b.reward_id = r.id
			WHERE b.address = $addr`,
			map[string]any{"$addr": []byte(userAddr.Bytes())}, func(r *common.Row) error {
			balance := r.Values[0].(*types.Decimal).String()
			escrow := "0x" + hex.EncodeToString(r.Values[1].([]byte))
			erc20 := "0x" + hex.EncodeToString(r.Values[2].([]byte))
			t.Logf("DEBUG: Balance=%s escrow=%s erc20=%s", balance, escrow, erc20)
			return nil
		})

		// DEBUG: Check if ordered-sync namespace exists
		var namespaceExists bool
		err = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `SELECT 1`, nil, func(r *common.Row) error {
			namespaceExists = true
			return nil
		})
		t.Logf("DEBUG: Can execute queries: %v (err: %v)", namespaceExists, err)

		// DEBUG: Check if ordered-sync pending_data were stored
		var pendingCount int
		err = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_ordered_sync}SELECT COUNT(*) FROM pending_data`, nil, func(r *common.Row) error {
			pendingCount = int(r.Values[0].(int64))
			return nil
		})
		t.Logf("DEBUG: Total ordered-sync pending_data: %d (err: %v)", pendingCount, err)

		// DEBUG: Check topics table
		var topicCount int
		err = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_ordered_sync}SELECT COUNT(*) FROM topics`, nil, func(r *common.Row) error {
			topicCount = int(r.Values[0].(int64))
			return nil
		})
		t.Logf("DEBUG: Total ordered-sync topics: %d (err: %v)", topicCount, err)

		// DEBUG: List pending_data points with topic info
		_ = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_ordered_sync}
			SELECT p.point, p.previous_point, t.name, t.last_processed_point
			FROM pending_data p
			JOIN topics t ON p.topic_id = t.id
			ORDER BY p.point`, nil, func(r *common.Row) error {
			point := r.Values[0].(int64)
			var prevPoint *int64
			if r.Values[1] != nil {
				p := r.Values[1].(int64)
				prevPoint = &p
			}
			topicName := r.Values[2].(string)
			var lastProcessed *int64
			if r.Values[3] != nil {
				lp := r.Values[3].(int64)
				lastProcessed = &lp
			}
			var lpStr string
			if lastProcessed != nil {
				lpStr = fmt.Sprintf("%d", *lastProcessed)
			} else {
				lpStr = "nil"
			}
			var prevStr string
			if prevPoint != nil {
				prevStr = fmt.Sprintf("%d", *prevPoint)
			} else {
				prevStr = "nil"
			}
			t.Logf("DEBUG: pending point=%d prev=%s topic=%s last_processed=%s MATCH=%v",
				point, prevStr, topicName, lpStr, (prevPoint == nil && lastProcessed == nil) || (prevPoint != nil && lastProcessed != nil && *prevPoint == *lastProcessed))
			return nil
		})

		// DEBUG: Check finalized status
		_ = app.Engine.ExecuteWithoutEngineCtx(ctx, app.DB, `{kwil_ordered_sync}SELECT topic, point, finalized FROM logs ORDER BY point`, nil, func(r *common.Row) error {
			topic := r.Values[0].(string)
			point := r.Values[1].(int64)
			finalized := r.Values[2].(bool)
			t.Logf("DEBUG: Log topic=%s point=%d finalized=%v", topic, point, finalized)
			return nil
		})

		// Create a market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000001", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err, "create_market should succeed")

		// Get USDC balance before order (collateral is locked from USDC bridge)
		balanceBefore, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Place buy order: 10 YES shares at $0.56
		// Collateral needed: 10 * 56 * 10^16 = 5.6 * 10^18 wei
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 10)
		require.NoError(t, err, "place_buy_order should succeed")

		// Verify USDC balance decreased by 5.6 TRUF
		balanceAfter, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Expected: balanceBefore - 5.6 (collateral locked for order)
		expectedBalance := new(big.Int).Sub(balanceBefore, toWei("5.6")) // Only the 5.6 TRUF (market fee was already deducted)
		require.Equal(t, 0, expectedBalance.Cmp(balanceAfter),
			fmt.Sprintf("Balance should decrease by 5.6 TRUF. Before: %s, After: %s, Expected: %s",
				balanceBefore.String(), balanceAfter.String(), expectedBalance.String()))

		// Verify order exists in ob_positions with negative price
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 1, "should have 1 position")

		pos := positions[0]
		require.Equal(t, int(marketID), pos.QueryID)
		require.True(t, pos.Outcome, "outcome should be TRUE (YES)")
		require.Equal(t, int16(-56), pos.Price, "price should be -56 (negative for buy)")
		require.Equal(t, int64(10), pos.Amount, "amount should be 10")

		return nil
	}
}

// testBuyOrderInsufficientBalance tests error when user has insufficient balance
func testBuyOrderInsufficientBalance(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Give user only 3 TRUF (enough for market creation but not order)
		err := giveBalance(ctx, platform, userAddr.Address(), "3000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000002", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Try to buy 100 shares at $0.99 (requires 99 TRUF - should fail)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 99, 100)
		require.Error(t, err, "place_buy_order should fail with insufficient balance")
		require.Contains(t, err.Error(), "Insufficient balance", "error should mention insufficient balance")

		return nil
	}
}

// testBuyOrderMarketNotFound tests error when market doesn't exist
func testBuyOrderMarketNotFound(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Try to place order on non-existent market (ID 99999)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, 99999, true, 50, 10)
		require.Error(t, err, "place_buy_order should fail for non-existent market")
		require.Contains(t, err.Error(), "Market does not exist", "error should mention market not found")

		return nil
	}
}

// testBuyOrderInvalidPrice tests price validation
func testBuyOrderInvalidPrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000003", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test price = 0 (too low)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 0, 10)
		require.Error(t, err, "price=0 should fail")
		require.Contains(t, err.Error(), "price must be between 1 and 99")

		// Test price = 100 (too high)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 100, 10)
		require.Error(t, err, "price=100 should fail")
		require.Contains(t, err.Error(), "price must be between 1 and 99")

		return nil
	}
}

// testBuyOrderInvalidAmount tests amount validation
func testBuyOrderInvalidAmount(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000004", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Test amount = 0
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 0)
		require.Error(t, err, "amount=0 should fail")
		require.Contains(t, err.Error(), "amount must be positive")

		// Test amount = -10
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, -10)
		require.Error(t, err, "negative amount should fail")
		require.Contains(t, err.Error(), "amount must be positive")

		// Test amount exceeds maximum (1 billion + 1)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 1000000001)
		require.Error(t, err, "amount > 1 billion should fail")
		require.Contains(t, err.Error(), "amount exceeds maximum allowed")

		return nil
	}
}

// testBuyOrderMultipleOrdersDifferentPrices tests user placing multiple orders at different prices
func testBuyOrderMultipleOrdersDifferentPrices(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000005", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place first order: 10 shares at $0.50
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 50, 10)
		require.NoError(t, err)

		// Place second order: 5 shares at $0.60 (different price)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 60, 5)
		require.NoError(t, err)

		// Verify two positions exist
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 2, "should have 2 positions")

		// Sort by price descending (highest buy first)
		if positions[0].Price > positions[1].Price {
			positions[0], positions[1] = positions[1], positions[0]
		}

		// First position: -60 (higher buy price)
		require.Equal(t, int16(-60), positions[0].Price)
		require.Equal(t, int64(5), positions[0].Amount)

		// Second position: -50
		require.Equal(t, int16(-50), positions[1].Price)
		require.Equal(t, int64(10), positions[1].Amount)

		return nil
	}
}

// testBuyOrderMultipleOrdersSamePrice tests UPSERT behavior (orders accumulate)
func testBuyOrderMultipleOrdersSamePrice(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		err := giveBalance(ctx, platform, userAddr.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000006", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Place first order: 10 shares at $0.56
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 10)
		require.NoError(t, err)

		// Place second order: 5 MORE shares at $0.56 (same price - should UPSERT)
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), true, 56, 5)
		require.NoError(t, err)

		// Verify single position with accumulated amount (10 + 5 = 15)
		positions, err := getPositions(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, positions, 1, "should have 1 position (UPSERT)")

		pos := positions[0]
		require.Equal(t, int16(-56), pos.Price)
		require.Equal(t, int64(15), pos.Amount, "amount should be 15 (10+5)")

		return nil
	}
}

// testBuyOrderBalanceChanges verifies exact balance changes
func testBuyOrderBalanceChanges(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		initialBalance := toWei("50")
		err := giveBalance(ctx, platform, userAddr.Address(), initialBalance.String())
		require.NoError(t, err)

		// Initialize ERC20 extension after balance injection
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market (costs 2 TRUF)
		queryComponents, err := encodeQueryComponentsForTests(userAddr.Address(), "sttest00000000000000000000000007", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userAddr, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Get USDC balance after market creation (collateral comes from USDC bridge)
		balanceAfterMarket, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Place order: 20 shares at $0.75 (requires 15 TRUF)
		// Collateral: 20 * 75 * 10^16 = 15 * 10^18
		err = callPlaceBuyOrder(ctx, platform, &userAddr, int(marketID), false, 75, 20)
		require.NoError(t, err)

		// Verify USDC balance decreased by 15 TRUF
		balanceFinal, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		expectedFinal := new(big.Int).Sub(balanceAfterMarket, toWei("15"))
		require.Equal(t, 0, expectedFinal.Cmp(balanceFinal),
			fmt.Sprintf("Balance should decrease by 15 TRUF. After market: %s, Final: %s, Expected: %s",
				balanceAfterMarket.String(), balanceFinal.String(), expectedFinal.String()))

		return nil
	}
}

// ===== HELPER FUNCTIONS =====

type Position struct {
	QueryID       int
	ParticipantID int
	Outcome       bool
	Price         int16
	Amount        int64
	LastUpdated   int64
}

func callPlaceBuyOrder(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, marketID int, outcome bool, price int, amount int64) error {
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
		"place_buy_order",
		[]any{marketID, outcome, price, amount},
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

func getPositions(ctx context.Context, platform *kwilTesting.Platform, marketID int) ([]Position, error) {
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	var positions []Position
	err := platform.Engine.Execute(
		engineCtx,
		platform.DB,
		"SELECT query_id, participant_id, outcome, price, amount, last_updated FROM ob_positions WHERE query_id = $query_id ORDER BY price DESC, last_updated ASC",
		map[string]any{"$query_id": marketID},
		func(row *common.Row) error {
			pos := Position{
				QueryID:       int(row.Values[0].(int64)),
				ParticipantID: int(row.Values[1].(int64)),
				Outcome:       row.Values[2].(bool),
				Price:         int16(row.Values[3].(int64)),
				Amount:        row.Values[4].(int64),
				LastUpdated:   row.Values[5].(int64),
			}
			positions = append(positions, pos)
			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return positions, nil
}

// getUSDCBalance returns the user's USDC bridge balance (hoodi_tt2)
// Used for checking collateral locked in buy/sell orders
func getUSDCBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string) (*big.Int, error) {
	// Import testerc20 is needed
	balanceStr, err := testerc20.GetUserBalance(ctx, platform, testUSDCExtensionName, wallet)
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if _, ok := balance.SetString(balanceStr, 10); !ok {
		return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
	}

	return balance, nil
}
