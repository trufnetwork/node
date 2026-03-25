//go:build kwiltest

package order_book

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// OrderEvent represents a row from ob_order_events
type OrderEvent struct {
	ID             int
	QueryID        int
	ParticipantID  int
	EventType      string
	Outcome        bool
	Price          int
	Amount         int64
	CounterpartyID *int64
	BlockHeight    int64
	BlockTimestamp int64
}

func TestOrderEvents(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_ORDER_EVENTS",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testOrderEventBuyPlaced(t),
			testOrderEventSellPlaced(t),
			testOrderEventSplitPlaced(t),
			testOrderEventCancelled(t),
			testOrderEventDirectFill(t),
			testOrderEventMintFill(t),
			testOrderEventBurnFill(t),
			testOrderEventChangeBid(t),
			testOrderEventChangeAsk(t),
			testOrderEventSettlement(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// getOrderEvents queries ob_order_events for a given market
func getOrderEvents(ctx context.Context, platform *kwilTesting.Platform, marketID int) ([]OrderEvent, error) {
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	var events []OrderEvent
	err := platform.Engine.Execute(
		engineCtx,
		platform.DB,
		"SELECT id, query_id, participant_id, event_type, outcome, price, amount, counterparty_id, block_height, block_timestamp FROM ob_order_events WHERE query_id = $query_id ORDER BY id ASC",
		map[string]any{"$query_id": marketID},
		func(row *common.Row) error {
			evt := OrderEvent{
				ID:            int(row.Values[0].(int64)),
				QueryID:       int(row.Values[1].(int64)),
				ParticipantID: int(row.Values[2].(int64)),
				EventType:     row.Values[3].(string),
				Outcome:       row.Values[4].(bool),
				Price:         int(row.Values[5].(int64)),
				Amount:        row.Values[6].(int64),
				BlockHeight:   row.Values[8].(int64),
				BlockTimestamp: row.Values[9].(int64),
			}
			if row.Values[7] != nil {
				cp := row.Values[7].(int64)
				evt.CounterpartyID = &cp
			}
			events = append(events, evt)
			return nil
		},
	)
	return events, err
}

// getOrderEventsByType filters events by type
func getOrderEventsByType(events []OrderEvent, eventType string) []OrderEvent {
	var filtered []OrderEvent
	for _, e := range events {
		if e.EventType == eventType {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// setupMarket creates a market and returns the market ID
func setupMarket(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, user *util.EthereumAddress, streamSuffix string) int64 {
	queryComponents, err := encodeQueryComponentsForTests(
		user.Address(),
		"sttest00000000000000000000"+streamSuffix,
		"get_record",
		[]byte{0x01},
	)
	require.NoError(t, err)

	settleTime := time.Now().Add(1 * time.Hour).Unix()
	var marketID int64
	err = callCreateMarket(ctx, platform, user, queryComponents, settleTime, 5, 1,
		func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
	require.NoError(t, err)
	return marketID
}

// testOrderEventBuyPlaced verifies that placing a buy order creates a buy_placed event
func testOrderEventBuyPlaced(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xAA00000000000000000000000000000000000001")
		err = giveBalance(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user, "ev000001")

		// Place buy order: YES @ $0.56, 10 shares
		err = callPlaceBuyOrder(ctx, platform, &user, int(marketID), true, 56, 10)
		require.NoError(t, err)

		// Verify event was recorded
		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		buyPlaced := getOrderEventsByType(events, "buy_placed")
		require.Len(t, buyPlaced, 1, "should have exactly 1 buy_placed event")

		evt := buyPlaced[0]
		require.Equal(t, int(marketID), evt.QueryID)
		require.True(t, evt.Outcome, "outcome should be YES (true)")
		require.Equal(t, 56, evt.Price, "price should be 56")
		require.Equal(t, int64(10), evt.Amount, "amount should be 10")
		require.Nil(t, evt.CounterpartyID, "no counterparty for placement")
		require.NotZero(t, evt.BlockTimestamp)

		return nil
	}
}

// testOrderEventSellPlaced verifies that placing a sell order creates a sell_placed event
func testOrderEventSellPlaced(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xBB00000000000000000000000000000000000001")
		err = giveBalance(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user, "ev000002")

		// First buy to get holdings, then sell
		err = callPlaceBuyOrder(ctx, platform, &user, int(marketID), true, 56, 10)
		require.NoError(t, err)

		// We need a counterparty to match the buy so user gets holdings
		// Instead, use split limit order which gives holdings directly
		err = callPlaceSplitLimitOrder(ctx, platform, &user, int(marketID), 60, 20)
		require.NoError(t, err)

		// Now sell some YES holdings
		err = callPlaceSellOrder(ctx, platform, &user, int(marketID), true, 70, 5)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		sellPlaced := getOrderEventsByType(events, "sell_placed")
		require.Len(t, sellPlaced, 1, "should have exactly 1 sell_placed event")

		evt := sellPlaced[0]
		require.Equal(t, int(marketID), evt.QueryID)
		require.True(t, evt.Outcome, "outcome should be YES (true)")
		require.Equal(t, 70, evt.Price)
		require.Equal(t, int64(5), evt.Amount)
		require.Nil(t, evt.CounterpartyID)

		return nil
	}
}

// testOrderEventSplitPlaced verifies that split limit order creates 2 events (YES hold + NO sell)
func testOrderEventSplitPlaced(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xCC00000000000000000000000000000000000001")
		err = giveBalance(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user, "ev000003")

		// Place split limit order: true_price=60, amount=50
		err = callPlaceSplitLimitOrder(ctx, platform, &user, int(marketID), 60, 50)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		splitPlaced := getOrderEventsByType(events, "split_placed")
		require.Len(t, splitPlaced, 2, "should have 2 split_placed events (YES hold + NO sell)")

		// One event for YES at true_price
		var yesEvt, noEvt *OrderEvent
		for i := range splitPlaced {
			if splitPlaced[i].Outcome {
				yesEvt = &splitPlaced[i]
			} else {
				noEvt = &splitPlaced[i]
			}
		}

		require.NotNil(t, yesEvt, "should have YES split_placed event")
		require.Equal(t, 60, yesEvt.Price, "YES price should be true_price=60")
		require.Equal(t, int64(50), yesEvt.Amount)

		require.NotNil(t, noEvt, "should have NO split_placed event")
		require.Equal(t, 40, noEvt.Price, "NO price should be 100-60=40")
		require.Equal(t, int64(50), noEvt.Amount)

		return nil
	}
}

// testOrderEventCancelled verifies that cancelling an order creates a cancelled event
func testOrderEventCancelled(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xDD00000000000000000000000000000000000001")
		err = giveBalance(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user, "ev000004")

		// Place buy order, then cancel it
		err = callPlaceBuyOrder(ctx, platform, &user, int(marketID), true, 45, 20)
		require.NoError(t, err)

		// Cancel the buy order (buy orders have negative price in ob_positions)
		err = callCancelOrder(ctx, platform, &user, int(marketID), true, -45)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		cancelled := getOrderEventsByType(events, "cancelled")
		require.Len(t, cancelled, 1, "should have exactly 1 cancelled event")

		evt := cancelled[0]
		require.Equal(t, int(marketID), evt.QueryID)
		require.True(t, evt.Outcome)
		require.Equal(t, 45, evt.Price, "price should be absolute value")
		require.Equal(t, int64(20), evt.Amount)
		require.Nil(t, evt.CounterpartyID)

		return nil
	}
}

// testOrderEventDirectFill verifies that matching engine creates fill events
func testOrderEventDirectFill(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xEE00000000000000000000000000000000000001")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xEE00000000000000000000000000000000000002")

		err = InjectDualBalance(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = InjectDualBalance(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user1, "ev000005")

		// User1: split limit to get YES holdings and create NO sell @ 40
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 60, 100)
		require.NoError(t, err)

		// User1: sell YES shares @ 55
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 55, 50)
		require.NoError(t, err)

		// User2: buy YES @ 55 should match user1's sell @ 55 (direct match)
		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), true, 55, 30)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		// Check for direct fill events
		buyFills := getOrderEventsByType(events, "direct_buy_fill")
		sellFills := getOrderEventsByType(events, "direct_sell_fill")

		require.Len(t, buyFills, 1, "should have 1 direct_buy_fill event")
		require.Len(t, sellFills, 1, "should have 1 direct_sell_fill event")

		// Buy fill: user2 bought at execution price (= sell price = 55)
		buyFill := buyFills[0]
		require.Equal(t, 55, buyFill.Price, "execution price should be sell price")
		require.Equal(t, int64(30), buyFill.Amount, "matched amount should be 30")
		require.NotNil(t, buyFill.CounterpartyID, "should have counterparty")

		// Sell fill: user1 sold at execution price
		sellFill := sellFills[0]
		require.Equal(t, 55, sellFill.Price)
		require.Equal(t, int64(30), sellFill.Amount)
		require.NotNil(t, sellFill.CounterpartyID)

		// The counterparties should reference each other
		require.NotEqual(t, buyFill.ParticipantID, sellFill.ParticipantID,
			"buyer and seller should be different participants")
		require.Equal(t, int64(sellFill.ParticipantID), *buyFill.CounterpartyID,
			"buyer's counterparty should be seller")
		require.Equal(t, int64(buyFill.ParticipantID), *sellFill.CounterpartyID,
			"seller's counterparty should be buyer")

		// Also verify placement events exist
		buyPlaced := getOrderEventsByType(events, "buy_placed")
		sellPlaced := getOrderEventsByType(events, "sell_placed")
		splitPlaced := getOrderEventsByType(events, "split_placed")

		require.GreaterOrEqual(t, len(buyPlaced), 1, "should have buy_placed events")
		require.GreaterOrEqual(t, len(sellPlaced), 1, "should have sell_placed events")
		require.Len(t, splitPlaced, 2, "should have 2 split_placed events")

		return nil
	}
}

// testOrderEventMintFill verifies mint match creates mint_fill events
func testOrderEventMintFill(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xFF00000000000000000000000000000000000001")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xFF00000000000000000000000000000000000002")

		err = InjectDualBalance(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = InjectDualBalance(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user1, "ev000006")

		// Mint match: user1 buys YES@60, user2 buys NO@40 → complementary (60+40=100)
		err = callPlaceBuyOrder(ctx, platform, &user1, int(marketID), true, 60, 50)
		require.NoError(t, err)

		err = callPlaceBuyOrder(ctx, platform, &user2, int(marketID), false, 40, 30)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		mintFills := getOrderEventsByType(events, "mint_fill")
		require.Len(t, mintFills, 2, "should have 2 mint_fill events (YES buyer + NO buyer)")

		// One event for YES side, one for NO side
		var yesFill, noFill *OrderEvent
		for i := range mintFills {
			if mintFills[i].Outcome {
				yesFill = &mintFills[i]
			} else {
				noFill = &mintFills[i]
			}
		}

		require.NotNil(t, yesFill, "should have YES mint_fill")
		require.Equal(t, 60, yesFill.Price)
		require.Equal(t, int64(30), yesFill.Amount, "minted amount = min(50, 30)")
		require.NotNil(t, yesFill.CounterpartyID)

		require.NotNil(t, noFill, "should have NO mint_fill")
		require.Equal(t, 40, noFill.Price)
		require.Equal(t, int64(30), noFill.Amount)
		require.NotNil(t, noFill.CounterpartyID)

		return nil
	}
}

// testOrderEventBurnFill verifies burn match creates burn_fill events
func testOrderEventBurnFill(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user1 := util.Unsafe_NewEthereumAddressFromString("0xAB00000000000000000000000000000000000001")
		user2 := util.Unsafe_NewEthereumAddressFromString("0xAB00000000000000000000000000000000000002")

		err = InjectDualBalance(ctx, platform, user1.Address(), "500000000000000000000")
		require.NoError(t, err)
		err = InjectDualBalance(ctx, platform, user2.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user1, "ev000007")

		// User1: split@60 → YES holdings(100) + NO sell@40(100)
		err = callPlaceSplitLimitOrder(ctx, platform, &user1, int(marketID), 60, 100)
		require.NoError(t, err)

		// User2: split@40 → YES holdings(100) + NO sell@60(100)
		err = callPlaceSplitLimitOrder(ctx, platform, &user2, int(marketID), 40, 100)
		require.NoError(t, err)

		// User1: sell YES@60 (complementary to user2's NO sell@60? No — burn needs YES@P + NO@(100-P))
		// Burn match: YES sell@70 + NO sell@30 (70+30=100)
		// User1 sells YES@70
		err = callPlaceSellOrder(ctx, platform, &user1, int(marketID), true, 70, 25)
		require.NoError(t, err)

		// User2 sells NO@30 → burn match with user1's YES sell@70 (70+30=100)
		// User2 needs NO holdings first. User2 already has NO sell@60 from split.
		// Cancel that sell to get NO holdings back, then sell at price 30.
		err = callCancelOrder(ctx, platform, &user2, int(marketID), false, 60)
		require.NoError(t, err)

		err = callPlaceSellOrder(ctx, platform, &user2, int(marketID), false, 30, 25)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		burnFills := getOrderEventsByType(events, "burn_fill")
		require.GreaterOrEqual(t, len(burnFills), 2, "should have at least 2 burn_fill events")

		// Verify burn fills have counterparties
		for _, bf := range burnFills {
			require.NotNil(t, bf.CounterpartyID, "burn fill should have counterparty")
			require.Greater(t, bf.Amount, int64(0), "burn amount should be positive")
		}

		return nil
	}
}

// testOrderEventChangeBid verifies change_bid creates bid_changed event
func testOrderEventChangeBid(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xAC00000000000000000000000000000000000001")
		err = giveBalance(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user, "ev000008")

		// Place buy order, then change it
		err = callPlaceBuyOrder(ctx, platform, &user, int(marketID), true, 40, 20)
		require.NoError(t, err)

		// Change bid from -40 to -55
		err = callChangeBid(ctx, platform, &user, int(marketID), true, -40, -55, 15)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		bidChanged := getOrderEventsByType(events, "bid_changed")
		require.Len(t, bidChanged, 1, "should have 1 bid_changed event")

		evt := bidChanged[0]
		require.Equal(t, 55, evt.Price, "new price should be 55 (absolute)")
		require.Equal(t, int64(15), evt.Amount, "new amount should be 15")
		require.Nil(t, evt.CounterpartyID)

		return nil
	}
}

// testOrderEventChangeAsk verifies change_ask creates ask_changed event
func testOrderEventChangeAsk(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xAD00000000000000000000000000000000000001")
		err = giveBalance(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		marketID := setupMarket(t, ctx, platform, &user, "ev000009")

		// Get holdings via split, then sell, then change ask
		err = callPlaceSplitLimitOrder(ctx, platform, &user, int(marketID), 50, 100)
		require.NoError(t, err)

		// Sell YES@60
		err = callPlaceSellOrder(ctx, platform, &user, int(marketID), true, 60, 30)
		require.NoError(t, err)

		// Change ask from 60 to 70
		err = callChangeAsk(ctx, platform, &user, int(marketID), true, 60, 70, 25)
		require.NoError(t, err)

		events, err := getOrderEvents(ctx, platform, int(marketID))
		require.NoError(t, err)

		askChanged := getOrderEventsByType(events, "ask_changed")
		require.Len(t, askChanged, 1, "should have 1 ask_changed event")

		evt := askChanged[0]
		require.Equal(t, 70, evt.Price, "new price should be 70")
		require.Equal(t, int64(25), evt.Amount, "new amount should be 25")
		require.Nil(t, evt.CounterpartyID)

		return nil
	}
}

// testOrderEventSettlement verifies that settlement creates settled events for all positions
func testOrderEventSettlement(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
		platform.Deployer = deployer.Bytes()

		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		err = InjectDualBalance(ctx, platform, deployer.Address(), "1000000000000000000000")
		require.NoError(t, err)

		user2 := util.Unsafe_NewEthereumAddressFromString("0xAE00000000000000000000000000000000000002")
		err = InjectDualBalance(ctx, platform, user2.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Re-initialize after injections to sync singleton
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create data provider and stream
		err = setup.CreateDataProvider(ctx, platform, deployer.Address())
		require.NoError(t, err)

		streamID := "stordereventsettletest0000000000"
		dataProvider := deployer.Address()
		engineCtx := helper.NewEngineContext()

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		// Insert YES outcome data (value > 0 = YES wins)
		valueDecimal, err := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
		require.NoError(t, err)

		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{int64(1000)},
				[]*kwilTypes.Decimal{valueDecimal},
			}, nil)
		require.NoError(t, err)

		// Request and sign attestation
		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			dataProvider, streamID, int64(500), int64(1500), nil, false,
		})
		require.NoError(t, err)

		var requestTxID string
		engineCtx = helper.NewEngineContext()
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_record", argsBytes, false, nil},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				return nil
			})
		require.NoError(t, err)
		require.Nil(t, res.Error, "request_attestation should succeed")

		helper.SignAttestation(requestTxID)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(dataProvider, streamID, "get_record", argsBytes)
		require.NoError(t, err)

		settleTime := time.Now().Add(1 * time.Hour).Unix()
		var queryID int

		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = time.Now().Unix()
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, settleTime, int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		require.Nil(t, createRes.Error)

		// Deployer: split limit → YES holdings + NO sell
		err = callPlaceSplitLimitOrder(ctx, platform, &deployer, queryID, 60, 50)
		require.NoError(t, err)

		// User2: buy NO@40 (matches deployer's NO sell@40 from split — direct fill)
		err = callPlaceBuyOrder(ctx, platform, &user2, queryID, false, 40, 30)
		require.NoError(t, err)

		// User2 also has an open buy YES@30 that won't match
		err = callPlaceBuyOrder(ctx, platform, &user2, queryID, true, 30, 10)
		require.NoError(t, err)

		// Settle the market (YES wins since value was 1.0)
		settleTx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: settleTime + 1,
			},
			Signer:        deployer.Bytes(),
			Caller:        deployer.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		settleEngineCtx := &common.EngineContext{TxContext: settleTx}

		settleRes, err := platform.Engine.Call(settleEngineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.Nil(t, settleRes.Error, "settle_market should succeed")

		// Verify settlement events
		events, err := getOrderEvents(ctx, platform, queryID)
		require.NoError(t, err)

		settled := getOrderEventsByType(events, "settled")
		require.GreaterOrEqual(t, len(settled), 2, "should have settled events for multiple positions")

		// Check settlement events cover all branches:
		// - Winners (YES holdings, price>=0): settle at 98
		// - Losers (NO holdings, wrong outcome): settle at 0
		// - Open buy orders (price<0): refund at abs(price)
		var foundWinnerAt98 bool
		var foundLoserAt0 bool
		var foundOpenBuyAt30 bool
		for _, evt := range settled {
			if evt.Outcome == true && evt.Price == 98 {
				foundWinnerAt98 = true
			}
			if evt.Price == 0 {
				foundLoserAt0 = true
			}
			if evt.Outcome == true && evt.Price == 30 {
				foundOpenBuyAt30 = true
			}
		}
		require.True(t, foundWinnerAt98, "should have winning settlement event with price=98")
		require.True(t, foundLoserAt0, "should have losing settlement event with price=0")
		require.True(t, foundOpenBuyAt30, "should have open buy order settlement event with price=30")

		return nil
	}
}
