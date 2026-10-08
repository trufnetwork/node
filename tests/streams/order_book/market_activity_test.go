//go:build kwiltest

package order_book

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	extauth "github.com/trufnetwork/kwil-db/extensions/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// get_market_activity holds the one definition of a market's traded volume
// that every SDK reads. These tests trade on a real order book and read the
// action back, so each rule of the definition is checked against the event rows
// the matching engine writes rather than against the statement's text.

// activityT0 is the block time the scenarios start from, in unix seconds. Every
// call sets its own height and time, so windows and coverage are exact.
const activityT0 int64 = 1_800_000_000

// marketActivity is one row of get_market_activity.
type marketActivity struct {
	Bridge            string
	VolumeCents       string
	DirectCents       string
	MintBurnCents     string
	UniqueTraders     int64
	FillCount         int64
	DirectFillCount   int64
	SharesTraded      int64
	FirstEventTs      *int64
	LastEventTs       *int64
	CoverageFromBlock int64
	CoverageComplete  bool
}

func TestMarketActivity(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_MARKET_ACTIVITY",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testActivityCountsEachTradeOnce(t),
			testActivityWindowIncludesBothEnds(t),
			testActivityCoverageFollowsTheTrim(t),
			testActivityWithNothingTraded(t),
			testActivityRejectsABadWindow(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// activityScenario is one market carrying one fill of each kind, one block
// apart, plus an earlier market whose only event predates it.
type activityScenario struct {
	earlier, market    int
	direct, mint, burn int64 // block time of each fill
}

// tradeOneOfEachFill builds the scenario every test below reads:
//
//	height 11  A splits at 60 x100: YES held, NO sell@40 x100     no volume
//	height 12  A sells YES@55 x30                                 no volume
//	height 13  B buys YES@55 x30, matching A's sell               direct 55*30 = 1650
//	height 14  C bids YES@70 x10                                  no volume
//	height 15  D bids NO@30 x10, minting against C's bid          mint 100*10 = 1000
//	height 16  B sells YES@60 x10, burning against A's NO@40      burn 100*10 = 1000
//
// The market is created at height 10. The earlier market is created at height 5
// and carries one bid at height 6, so the node holds an event older than the
// market, as it does on a network with other activity.
func tradeOneOfEachFill(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) activityScenario {
	t.Helper()

	lastBalancePoint = nil
	lastTrufBalancePoint = nil
	require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

	a := util.Unsafe_NewEthereumAddressFromString("0xA100000000000000000000000000000000000001")
	b := util.Unsafe_NewEthereumAddressFromString("0xA100000000000000000000000000000000000002")
	c := util.Unsafe_NewEthereumAddressFromString("0xA100000000000000000000000000000000000003")
	d := util.Unsafe_NewEthereumAddressFromString("0xA100000000000000000000000000000000000004")
	for _, wallet := range []util.EthereumAddress{a, b, c, d} {
		require.NoError(t, InjectDualBalance(ctx, platform, wallet.Address(), "500000000000000000000"))
	}

	act := func(signer *util.EthereumAddress, height, ts int64, action string, args ...any) {
		t.Helper()
		require.NoError(t, activityCall(ctx, platform, signer, height, ts, action, args, nil),
			"%s at height %d", action, height)
	}

	s := activityScenario{direct: activityT0 + 30, mint: activityT0 + 50, burn: activityT0 + 60}

	s.earlier = createActivityMarket(t, ctx, platform, &a, "ma0000", 5, activityT0-100)
	act(&a, 6, activityT0-90, "place_buy_order", s.earlier, true, 10, int64(1))

	s.market = createActivityMarket(t, ctx, platform, &a, "ma0001", 10, activityT0)
	m := s.market
	act(&a, 11, activityT0+10, "place_split_limit_order", m, 60, int64(100))
	act(&a, 12, activityT0+20, "place_sell_order", m, true, 55, int64(30))
	act(&b, 13, s.direct, "place_buy_order", m, true, 55, int64(30))
	act(&c, 14, activityT0+40, "place_buy_order", m, true, 70, int64(10))
	act(&d, 15, s.mint, "place_buy_order", m, false, 30, int64(10))
	act(&b, 16, s.burn, "place_sell_order", m, true, 60, int64(10))

	// The rows each exclusion rule is about must exist, or the rules below would
	// pass without being exercised.
	events, err := getOrderEvents(ctx, platform, m)
	require.NoError(t, err)
	byType := map[string]int{}
	noSide := map[string]int{}
	for _, e := range events {
		byType[e.EventType]++
		if !e.Outcome {
			noSide[e.EventType]++
		}
	}
	require.Equal(t, 2, byType["split_placed"], "events: %+v", events)
	require.Equal(t, 1, byType["direct_buy_fill"], "events: %+v", events)
	require.Equal(t, 1, byType["direct_sell_fill"], "events: %+v", events)
	require.Equal(t, 1, noSide["mint_fill"], "events: %+v", events)
	require.Equal(t, 1, noSide["burn_fill"], "events: %+v", events)

	return s
}

// testActivityCountsEachTradeOnce checks every column over a window holding all
// three fills: each trade counts once, a mint or burn at 100 cents a share on its
// YES row, and a split or a placement not at all.
func testActivityCountsEachTradeOnce(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		s := tradeOneOfEachFill(t, ctx, platform)

		got := readActivity(t, ctx, platform, s.market, activityT0, activityT0+100)

		require.Equal(t, marketActivity{
			Bridge:            testUSDCExtensionName,
			VolumeCents:       "3650",
			DirectCents:       "1650",
			MintBurnCents:     "2000",
			UniqueTraders:     4,
			FillCount:         3,
			DirectFillCount:   1,
			SharesTraded:      50,
			FirstEventTs:      &s.direct,
			LastEventTs:       &s.burn,
			CoverageFromBlock: 6,
			CoverageComplete:  true,
		}, got)
		return nil
	}
}

// testActivityWindowIncludesBothEnds reads one-second windows on each fill, and
// the seconds either side of them.
func testActivityWindowIncludesBothEnds(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		s := tradeOneOfEachFill(t, ctx, platform)

		tests := []struct {
			name           string
			from, to       int64
			volume, direct string
			fills, traders int64
			first, last    *int64
		}{
			{"the direct fill's second", s.direct, s.direct, "1650", "1650", 1, 2, &s.direct, &s.direct},
			{"the mint's second", s.mint, s.mint, "1000", "0", 1, 2, &s.mint, &s.mint},
			{"the burn's second", s.burn, s.burn, "1000", "0", 1, 2, &s.burn, &s.burn},
			{"strictly between the first and last fill", s.direct + 1, s.burn - 1, "1000", "0", 1, 2, &s.mint, &s.mint},
			{"up to the second before the first fill", activityT0, s.direct - 1, "0", "0", 0, 0, nil, nil},
			{"from the second after the last fill", s.burn + 1, s.burn + 100, "0", "0", 0, 0, nil, nil},
		}
		for _, tt := range tests {
			got := readActivity(t, ctx, platform, s.market, tt.from, tt.to)

			require.Equal(t, tt.volume, got.VolumeCents, tt.name)
			require.Equal(t, tt.direct, got.DirectCents, tt.name)
			require.Equal(t, tt.fills, got.FillCount, tt.name)
			require.Equal(t, tt.traders, got.UniqueTraders, tt.name)
			require.Equal(t, tt.first, got.FirstEventTs, tt.name)
			require.Equal(t, tt.last, got.LastEventTs, tt.name)
		}
		return nil
	}
}

// testActivityCoverageFollowsTheTrim trims the node's events from the bottom, as
// the leader's scheduler does, and reads how far back the answer can be trusted.
func testActivityCoverageFollowsTheTrim(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		s := tradeOneOfEachFill(t, ctx, platform)
		wide := func(queryID int) marketActivity {
			return readActivity(t, ctx, platform, queryID, activityT0-1000, activityT0+1000)
		}

		got := wide(s.market)
		require.Equal(t, int64(6), got.CoverageFromBlock)
		require.True(t, got.CoverageComplete, "an event older than the market is still held")

		// Nothing is trimmed yet, but no held event is as old as the earlier
		// market, so its row cannot tell that apart from a trim and says so.
		require.False(t, wide(s.earlier).CoverageComplete)

		// Keep blocks 12 and up: the earlier market's bid and A's split go.
		trimOrderEventsAsLeader(t, ctx, platform, 20, 8)
		got = wide(s.market)
		require.Equal(t, int64(12), got.CoverageFromBlock)
		require.False(t, got.CoverageComplete, "the market is older than every held event")
		require.Equal(t, "3650", got.VolumeCents, "no fill was trimmed")

		// Keep blocks 15 and up: the direct fill goes, and the volume with it.
		trimOrderEventsAsLeader(t, ctx, platform, 25, 10)
		got = wide(s.market)
		require.Equal(t, int64(15), got.CoverageFromBlock)
		require.False(t, got.CoverageComplete)
		require.Equal(t, "2000", got.VolumeCents)
		require.Equal(t, int64(0), got.DirectFillCount)
		return nil
	}
}

// testActivityWithNothingTraded reads a market that never traded on a node that
// holds no order events, and a market that does not exist.
func testActivityWithNothingTraded(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))

		creator := util.Unsafe_NewEthereumAddressFromString("0xA200000000000000000000000000000000000001")
		require.NoError(t, InjectDualBalance(ctx, platform, creator.Address(), "500000000000000000000"))
		market := createActivityMarket(t, ctx, platform, &creator, "ma0002", 3, activityT0)

		// MIN over an empty ob_order_events is NULL. The row still comes back,
		// with coverage that says a zero from it cannot be trusted.
		got := readActivity(t, ctx, platform, market, activityT0, activityT0+100)
		require.Equal(t, marketActivity{
			Bridge:            testUSDCExtensionName,
			VolumeCents:       "0",
			DirectCents:       "0",
			MintBurnCents:     "0",
			CoverageFromBlock: 0,
			CoverageComplete:  false,
		}, got)

		rows, err := readActivityRows(ctx, platform, market+1000, activityT0, activityT0+100)
		require.NoError(t, err)
		require.Empty(t, rows, "a market that does not exist has no row")
		return nil
	}
}

// testActivityRejectsABadWindow checks the inputs the action refuses.
func testActivityRejectsABadWindow(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		_, err := readActivityRows(ctx, platform, 1, activityT0+1, activityT0)
		require.ErrorContains(t, err, "to_ts must not be before from_ts")

		_, err = readActivityRows(ctx, platform, nil, activityT0, activityT0)
		require.ErrorContains(t, err, "query_id is required")

		_, err = readActivityRows(ctx, platform, 1, nil, activityT0)
		require.ErrorContains(t, err, "from_ts and to_ts are required")
		return nil
	}
}

// createActivityMarket creates a market in a block at the given height and time,
// settling a day after it.
func createActivityMarket(
	t *testing.T, ctx context.Context, platform *kwilTesting.Platform,
	creator *util.EthereumAddress, streamSuffix string, height, ts int64,
) int {
	t.Helper()

	queryComponents, err := encodeQueryComponentsForTests(
		creator.Address(), "sttest00000000000000000000"+streamSuffix, "get_record", []byte{0x01})
	require.NoError(t, err)

	var marketID int64
	err = activityCall(ctx, platform, creator, height, ts, "create_market",
		[]any{testUSDCExtensionName, queryComponents, ts + 86400, int64(5), int64(1)},
		func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
	require.NoError(t, err)
	return int(marketID)
}

// activityCall runs an action as signer in a block at the given height and time.
// The block has a proposer because create_market pays its fee to the leader.
func activityCall(
	ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress,
	height, ts int64, action string, args []any, resultFn func(*common.Row) error,
) error {
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		return err
	}

	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    height,
			Timestamp: ts,
			Proposer:  pubGeneric.(*crypto.Secp256k1PublicKey),
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "", action, args, resultFn)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

// trimOrderEventsAsLeader runs trim_order_events as the block leader at height,
// which deletes the events of every block below height - preserveBlocks.
func trimOrderEventsAsLeader(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, height, preserveBlocks int64) {
	t.Helper()

	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)
	leader := crypto.EthereumAddressFromPubKey(pub)
	caller, err := extauth.GetIdentifier(coreauth.EthPersonalSignAuth, leader)
	require.NoError(t, err)

	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: height, Proposer: pub},
		Signer:        leader,
		Caller:        caller,
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "",
		"trim_order_events", []any{preserveBlocks, int64(1000)}, nil)
	require.NoError(t, err)
	require.NoError(t, res.Error)
}

// readActivity reads the one row get_market_activity returns for a market that
// exists.
func readActivity(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, queryID int, fromTs, toTs int64) marketActivity {
	t.Helper()

	rows, err := readActivityRows(ctx, platform, queryID, fromTs, toTs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0]
}

// readActivityRows calls get_market_activity. The arguments are any so a test
// can pass NULL.
func readActivityRows(ctx context.Context, platform *kwilTesting.Platform, queryID, fromTs, toTs any) ([]marketActivity, error) {
	reader := util.Unsafe_NewEthereumAddressFromString("0xA300000000000000000000000000000000000001")

	var rows []marketActivity
	err := activityCall(ctx, platform, &reader, 100, activityT0+10_000, "get_market_activity",
		[]any{queryID, fromTs, toTs},
		func(row *common.Row) error {
			a := marketActivity{
				Bridge:            row.Values[0].(string),
				VolumeCents:       row.Values[1].(*kwilTypes.Decimal).String(),
				DirectCents:       row.Values[2].(*kwilTypes.Decimal).String(),
				MintBurnCents:     row.Values[3].(*kwilTypes.Decimal).String(),
				UniqueTraders:     row.Values[4].(int64),
				FillCount:         row.Values[5].(int64),
				DirectFillCount:   row.Values[6].(int64),
				SharesTraded:      row.Values[7].(int64),
				CoverageFromBlock: row.Values[10].(int64),
				CoverageComplete:  row.Values[11].(bool),
			}
			if row.Values[8] != nil {
				first := row.Values[8].(int64)
				a.FirstEventTs = &first
			}
			if row.Values[9] != nil {
				last := row.Values[9].(int64)
				a.LastEventTs = &last
			}
			rows = append(rows, a)
			return nil
		})
	return rows, err
}
