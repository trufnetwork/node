//go:build kwiltest

package order_book

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// The read actions of migration 061 let an indexer page through the order book
// and the fee ledgers. These tests seed each table directly, with known keys,
// NULLs and page boundaries, and read the rows back through the actions. Every
// read checks the column names and declared types, every value, the order, the
// cursor and the page size.

func TestIndexerReadActions(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_INDEXER_READ_ACTIONS",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testIndexerReadsOnAnEmptyNode(t),
			testIndexerReadsMarketsAndPositions(t),
			testIndexerReadsALargeBook(t),
			testIndexerReadsFeeDistributions(t),
			testIndexerReadsTradesAndOrderEvents(t),
			testIndexerReadsOrderEventsAcrossARestart(t),
			testIndexerReadsTransactionEvents(t),
			testIndexerReadsRefuseBadPageSizes(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Columns each action returns, as "name type". INT and INT8 are both int8.
var (
	irMarketColumns = []string{
		"id int8", "hash bytea", "query_components bytea", "settle_time int8", "settled bool",
		"winning_outcome bool", "settled_at int8", "created_at int8", "creator bytea",
		"max_spread int8", "min_order_size int8", "bridge text",
	}
	irOpenInterestColumns = []string{"bridge text", "open_interest int8", "markets_with_oi int8"}
	irPositionColumns     = []string{
		"participant_id int8", "wallet_address bytea", "outcome bool", "price int8", "amount int8",
	}
	irMarketIDColumns     = []string{"query_id int8"}
	irDistributionColumns = []string{
		"id int8", "query_id int8", "bridge text", "total_fees_distributed numeric(78,0)",
		"total_dp_fees numeric(78,0)", "total_validator_fees numeric(78,0)", "total_lp_count int8",
		"block_count int8", "distributed_at int8",
	}
	irDetailColumns = []string{
		"distribution_id int8", "query_id int8", "participant_id int8", "wallet_address bytea",
		"total_reward_percent numeric(10,2)", "reward_amount numeric(78,0)", "distributed_at int8",
		"block_count int8",
	}
	irNetImpactColumns = []string{
		"id int8", "tx_hash bytea", "query_id int8", "participant_id int8", "wallet_address bytea",
		"outcome bool", "shares_change int8", "collateral_change numeric(78,0)", "is_negative bool",
		"timestamp int8",
	}
	irOrderEventColumns = []string{
		"id int8", "tx_hash bytea", "query_id int8", "participant_id int8", "wallet_address bytea",
		"event_type text", "outcome bool", "price int8", "amount int8", "counterparty_id int8",
		"block_height int8", "block_timestamp int8",
	}
	irMaxIDColumns   = []string{"max_net_impact_id int8", "max_order_event_height int8", "max_order_event_id int8"}
	irTxEventColumns = []string{
		"tx_id text", "block_height int8", "method_id int8", "caller text",
		"fee_amount numeric(78,0)", "fee_recipient text", "metadata text",
	}
)

// The participants every scenario seeds, with ids 1, 2 and 3.
var (
	irWallet1 = bytes.Repeat([]byte{0xa1}, 20)
	irWallet2 = bytes.Repeat([]byte{0xb2}, 20)
	irWallet3 = bytes.Repeat([]byte{0xc3}, 20)
)

// irTxHash is a 32-byte transaction hash filled with b.
func irTxHash(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

// irTxID is a transaction id the way transaction_events stores one.
func irTxID(n int) string { return fmt.Sprintf("0x%064x", n) }

func irPtr[T any](v T) *T { return &v }

// =============================================================================
// Scenarios
// =============================================================================

// testIndexerReadsOnAnEmptyNode reads every action on a node that holds no
// order book and no fee ledger.
func testIndexerReadsOnAnEmptyNode(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		empty := []struct {
			action  string
			args    []any
			columns []string
		}{
			{"list_markets_after", []any{nil, nil}, irMarketColumns},
			{"get_open_interest_by_bridge", nil, irOpenInterestColumns},
			{"get_market_positions", []any{int64(1), nil, nil, nil, nil}, irPositionColumns},
			{"list_markets_with_positions", nil, irMarketIDColumns},
			{"list_fee_distributions", []any{nil, nil}, irDistributionColumns},
			{"get_latest_fee_distribution", nil, irDistributionColumns},
			{"list_fee_distribution_details", []any{nil, nil, nil}, irDetailColumns},
			{"list_net_impacts", []any{nil, nil}, irNetImpactColumns},
			{"list_order_events", []any{nil, nil, nil}, irOrderEventColumns},
			{"list_transaction_events", []any{nil, nil, nil, nil}, irTxEventColumns},
		}
		for _, tc := range empty {
			rows, err := irRead(ctx, platform, tc.action, tc.args, tc.columns)
			require.NoError(t, err, tc.action)
			require.Empty(t, rows, tc.action)
		}

		// Every value is NULL on empty tables, and the action still returns its row.
		require.Equal(t, irMaxIDsRow{}, irMaxIDs(t, ctx, platform))
		return nil
	}
}

// testIndexerReadsMarketsAndPositions reads markets in pages, open interest per
// bridge, and the positions of single markets in pages.
func testIndexerReadsMarketsAndPositions(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		irSeedParticipants(t, ctx, platform)
		markets := irSeedMarkets(t, ctx, platform)

		// Market 1 (eth_truf, open) holds every kind of position, and
		// participant 2 holds NO at two prices; market 3 (eth_truf, settled)
		// still holds a YES holding, which open interest must not count;
		// market 4 (eth_usdc, open) holds YES only; market 5 (hoodi_tt, open)
		// holds a NO holding and a YES bid, neither of which is open interest;
		// market 7 (eth_truf, open) holds a YES holding, so eth_truf has two
		// markets with open interest. Markets 2 and 6 have settled and hold
		// nothing.
		irSeedPositions(t, ctx, platform, []irPosition{
			{QueryID: 1, ParticipantID: 2, Outcome: true, Price: 60, Amount: 30},   // YES ask
			{QueryID: 1, ParticipantID: 1, Outcome: true, Price: 0, Amount: 100},   // YES holding
			{QueryID: 1, ParticipantID: 3, Outcome: true, Price: -40, Amount: 50},  // YES bid
			{QueryID: 1, ParticipantID: 2, Outcome: false, Price: 0, Amount: 100},  // NO holding
			{QueryID: 1, ParticipantID: 1, Outcome: false, Price: -45, Amount: 10}, // NO bid
			{QueryID: 1, ParticipantID: 2, Outcome: false, Price: -10, Amount: 7},  // NO bid
			{QueryID: 3, ParticipantID: 1, Outcome: true, Price: 0, Amount: 999},   // settled market
			{QueryID: 4, ParticipantID: 1, Outcome: true, Price: 0, Amount: 10},
			{QueryID: 4, ParticipantID: 1, Outcome: true, Price: 1, Amount: 5},
			{QueryID: 5, ParticipantID: 3, Outcome: false, Price: 0, Amount: 40},
			{QueryID: 5, ParticipantID: 3, Outcome: true, Price: -55, Amount: 20},
			{QueryID: 7, ParticipantID: 2, Outcome: true, Price: 0, Amount: 10},
		})

		// Every column of every market, in id order, NULLs included.
		require.Equal(t, markets, irListMarkets(t, ctx, platform, nil, nil))

		// Pages of four: the cursor is the last id read.
		require.Equal(t, []int64{1, 2, 3, 4}, irMarketIDs(irListMarkets(t, ctx, platform, nil, int64(4))))
		require.Equal(t, []int64{5, 6, 7}, irMarketIDs(irListMarkets(t, ctx, platform, int64(4), int64(4))))
		require.Empty(t, irListMarkets(t, ctx, platform, int64(7), int64(4)))
		require.Equal(t, []int64{3}, irMarketIDs(irListMarkets(t, ctx, platform, int64(2), int64(1))))

		// One row per bridge. YES holdings and asks of open markets count; bids,
		// the NO side and settled markets do not. markets_with_oi counts each
		// market once: eth_truf has two, 1 and 7, though two rows of market 1
		// count. Bridges with nothing open still return a row, of zero.
		require.Equal(t, []irOpenInterest{
			{Bridge: "eth_truf", OpenInterest: 140, Markets: 2},
			{Bridge: "eth_usdc", OpenInterest: 15, Markets: 1},
			{Bridge: "hoodi_tt", OpenInterest: 0, Markets: 0},
			{Bridge: "hoodi_tt2", OpenInterest: 0, Markets: 0},
		}, irOpenInterestByBridge(t, ctx, platform))

		// The whole book of market 1, holdings included, in primary key order,
		// with each holder's wallet.
		book := []irMarketPosition{
			{ParticipantID: 1, Wallet: irWallet1, Outcome: false, Price: -45, Amount: 10},
			{ParticipantID: 1, Wallet: irWallet1, Outcome: true, Price: 0, Amount: 100},
			{ParticipantID: 2, Wallet: irWallet2, Outcome: false, Price: -10, Amount: 7},
			{ParticipantID: 2, Wallet: irWallet2, Outcome: false, Price: 0, Amount: 100},
			{ParticipantID: 2, Wallet: irWallet2, Outcome: true, Price: 60, Amount: 30},
			{ParticipantID: 3, Wallet: irWallet3, Outcome: true, Price: -40, Amount: 50},
		}
		require.Equal(t, book, irMarketPositions(t, ctx, platform, 1, nil, nil, nil, nil))
		require.Empty(t, irMarketPositions(t, ctx, platform, 2, nil, nil, nil, nil), "market 2 holds no position")
		require.Empty(t, irMarketPositions(t, ctx, platform, 77, nil, nil, nil, nil), "no such market")

		// Market 3 has settled but still holds the row seeded above, and
		// list_markets_with_positions lists it below. The action reads the
		// table as it is, with no test on settled, so that row reads back.
		require.Equal(t, []irMarketPosition{
			{ParticipantID: 1, Wallet: irWallet1, Outcome: true, Price: 0, Amount: 999},
		}, irMarketPositions(t, ctx, platform, 3, nil, nil, nil, nil))

		// Pages of one, two and four. The cursor is the last row's
		// (participant_id, outcome, price), so a page that ends inside a
		// participant, or inside one of its outcomes, skips none of the rest.
		for _, size := range []int64{1, 2, 4} {
			var walked []irMarketPosition
			cursor := []any{nil, nil, nil}
			for range 10 {
				page := irMarketPositions(t, ctx, platform, 1, cursor[0], cursor[1], cursor[2], size)
				if len(page) == 0 {
					break
				}
				require.LessOrEqual(t, len(page), int(size))
				walked = append(walked, page...)
				lastRow := page[len(page)-1]
				cursor = []any{lastRow.ParticipantID, lastRow.Outcome, lastRow.Price}
			}
			require.Equal(t, book, walked, "pages of %d", size)
		}
		require.Equal(t, book[3:5], irMarketPositions(t, ctx, platform, 1, int64(2), false, int64(-10), int64(2)))
		require.Empty(t, irMarketPositions(t, ctx, platform, 1, int64(3), true, int64(-40), nil))

		// NULL parts at the end of a cursor start at the first row that
		// matches the parts before them, and a NULL participant at the first
		// row of the market.
		require.Equal(t, book[2:], irMarketPositions(t, ctx, platform, 1, int64(2), nil, nil, nil))
		require.Equal(t, book[4:], irMarketPositions(t, ctx, platform, 1, int64(2), true, nil, nil))
		require.Equal(t, book, irMarketPositions(t, ctx, platform, 1, nil, true, int64(60), nil))

		rows, err := irRead(ctx, platform, "list_markets_with_positions", nil, irMarketIDColumns)
		require.NoError(t, err)
		var withPositions []int64
		for _, r := range rows {
			withPositions = append(withPositions, r[0].(int64))
		}
		require.Equal(t, []int64{1, 3, 4, 5, 7}, withPositions)
		return nil
	}
}

// testIndexerReadsALargeBook reads a market that holds more positions than one
// page: a NULL page size reads 1000 rows, and the next page carries on inside
// one participant's outcome.
func testIndexerReadsALargeBook(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		irSeedParticipants(t, ctx, platform)
		irSeedMarkets(t, ctx, platform)

		// Each participant on both outcomes at every price from -99 to 99, so
		// 3 * 2 * 199 = 1194 rows in market 1, inserted in reverse key order.
		irExec(t, ctx, platform,
			`INSERT INTO main.ob_positions (query_id, participant_id, outcome, price, amount, last_updated)
			 SELECT 1, p, o, pr, 1, 1000
			 FROM generate_series(3, 1, -1) AS p, (VALUES (TRUE), (FALSE)) AS v(o),
			      generate_series(99, -99, -1) AS pr`)

		wallets := [][]byte{irWallet1, irWallet2, irWallet3}
		var book []irMarketPosition
		for participant := int64(1); participant <= 3; participant++ {
			for _, outcome := range []bool{false, true} {
				for price := int64(-99); price <= 99; price++ {
					book = append(book, irMarketPosition{ParticipantID: participant, Wallet: wallets[participant-1],
						Outcome: outcome, Price: price, Amount: 1})
				}
			}
		}

		// The 1000th row is participant 3's YES row at price -95, so the next
		// page starts inside that participant's outcome.
		first := irMarketPositions(t, ctx, platform, 1, nil, nil, nil, nil)
		require.Equal(t, book[:1000], first)
		lastRow := first[len(first)-1]
		require.Equal(t, irMarketPosition{ParticipantID: 3, Wallet: irWallet3, Outcome: true, Price: -95, Amount: 1}, lastRow)
		require.Equal(t, book[1000:],
			irMarketPositions(t, ctx, platform, 1, lastRow.ParticipantID, lastRow.Outcome, lastRow.Price, nil))
		return nil
	}
}

// testIndexerReadsFeeDistributions reads distributions in pages, the newest
// distribution, and per-wallet rewards across a page boundary that splits a
// distribution.
func testIndexerReadsFeeDistributions(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		irSeedParticipants(t, ctx, platform)
		irSeedMarkets(t, ctx, platform)

		// Markets 2 (eth_usdc) and 3 (eth_truf) have settled and paid fees.
		distributions := []irDistribution{
			{ID: 1, QueryID: 2, Bridge: "eth_usdc", TotalFees: "1000000000000000000", DPFees: "25000000000000000",
				ValidatorFees: "25000000000000000", LPCount: 2, BlockCount: 10, DistributedAt: 1_800_000_250},
			{ID: 2, QueryID: 3, Bridge: "eth_truf", TotalFees: "5000000", DPFees: "0",
				ValidatorFees: "0", LPCount: 1, BlockCount: 4, DistributedAt: 1_800_000_350},
			{ID: 3, QueryID: 2, Bridge: "eth_usdc", TotalFees: "7", DPFees: "1",
				ValidatorFees: "1", LPCount: 3, BlockCount: 1, DistributedAt: 1_800_000_400},
		}
		// Inserted in reverse key order, so insertion order cannot produce the order checked below.
		for _, d := range irReversed(distributions) {
			irExec(t, ctx, platform,
				`INSERT INTO main.ob_fee_distributions (id, query_id, total_fees_distributed, total_dp_fees,
				     total_validator_fees, total_lp_count, block_count, distributed_at)
				 VALUES ($1, $2, $3::NUMERIC(78, 0), $4::NUMERIC(78, 0), $5::NUMERIC(78, 0), $6, $7, $8)`,
				d.ID, d.QueryID, d.TotalFees, d.DPFees, d.ValidatorFees, d.LPCount, d.BlockCount, d.DistributedAt)
		}

		// Seeded out of key order, so the order below comes from the action.
		details := []irDetail{
			{DistributionID: 3, ParticipantID: 3, Wallet: irWallet3, RewardPercent: "50.00", RewardAmount: "2"},
			{DistributionID: 1, ParticipantID: 2, Wallet: irWallet2, RewardPercent: "40.00", RewardAmount: "400000000000000000"},
			{DistributionID: 3, ParticipantID: 1, Wallet: irWallet1, RewardPercent: "30.00", RewardAmount: "3"},
			{DistributionID: 1, ParticipantID: 1, Wallet: irWallet1, RewardPercent: "60.00", RewardAmount: "600000000000000000"},
			{DistributionID: 2, ParticipantID: 3, Wallet: irWallet3, RewardPercent: "100.00", RewardAmount: "5000000"},
			{DistributionID: 3, ParticipantID: 2, Wallet: irWallet2, RewardPercent: "20.00", RewardAmount: "2"},
		}
		for _, d := range details {
			irExec(t, ctx, platform,
				`INSERT INTO main.ob_fee_distribution_details (distribution_id, participant_id, wallet_address,
				     reward_amount, total_reward_percent)
				 VALUES ($1, $2, $3, $4::NUMERIC(78, 0), $5::NUMERIC(10, 2))`,
				d.DistributionID, d.ParticipantID, d.Wallet, d.RewardAmount, d.RewardPercent)
		}

		// Every distribution with its market's bridge, then pages of one.
		require.Equal(t, distributions, irListDistributions(t, ctx, platform, nil, nil))
		require.Equal(t, distributions[1:2], irListDistributions(t, ctx, platform, int64(1), int64(1)))
		require.Equal(t, distributions[2:3], irListDistributions(t, ctx, platform, int64(2), int64(1)))
		require.Empty(t, irListDistributions(t, ctx, platform, int64(3), int64(1)))

		latest, err := irReadDistributions(ctx, platform, "get_latest_fee_distribution", nil)
		require.NoError(t, err)
		require.Equal(t, distributions[2:3], latest)

		// Every reward row, in key order, with its distribution's market, time
		// and block count.
		want := []irDetail{
			{DistributionID: 1, QueryID: 2, ParticipantID: 1, Wallet: irWallet1, RewardPercent: "60.00", RewardAmount: "600000000000000000", DistributedAt: 1_800_000_250, BlockCount: 10},
			{DistributionID: 1, QueryID: 2, ParticipantID: 2, Wallet: irWallet2, RewardPercent: "40.00", RewardAmount: "400000000000000000", DistributedAt: 1_800_000_250, BlockCount: 10},
			{DistributionID: 2, QueryID: 3, ParticipantID: 3, Wallet: irWallet3, RewardPercent: "100.00", RewardAmount: "5000000", DistributedAt: 1_800_000_350, BlockCount: 4},
			{DistributionID: 3, QueryID: 2, ParticipantID: 1, Wallet: irWallet1, RewardPercent: "30.00", RewardAmount: "3", DistributedAt: 1_800_000_400, BlockCount: 1},
			{DistributionID: 3, QueryID: 2, ParticipantID: 2, Wallet: irWallet2, RewardPercent: "20.00", RewardAmount: "2", DistributedAt: 1_800_000_400, BlockCount: 1},
			{DistributionID: 3, QueryID: 2, ParticipantID: 3, Wallet: irWallet3, RewardPercent: "50.00", RewardAmount: "2", DistributedAt: 1_800_000_400, BlockCount: 1},
		}
		require.Equal(t, want, irListDetails(t, ctx, platform, nil, nil, nil))

		// Pages of two. The second page ends inside distribution 3, and the
		// third page carries on from the last row's (distribution, participant)
		// rather than from the next distribution.
		require.Equal(t, want[0:2], irListDetails(t, ctx, platform, nil, nil, int64(2)))
		require.Equal(t, want[2:4], irListDetails(t, ctx, platform, int64(1), int64(2), int64(2)))
		require.Equal(t, want[4:6], irListDetails(t, ctx, platform, int64(3), int64(1), int64(2)))
		require.Empty(t, irListDetails(t, ctx, platform, int64(3), int64(3), int64(2)))

		// A NULL participant starts at the first row of the distribution given.
		require.Equal(t, want[2:], irListDetails(t, ctx, platform, int64(2), nil, nil))
		return nil
	}
}

// testIndexerReadsTradesAndOrderEvents reads net impacts and order events in
// pages, their highest ids, and a page at the default size.
func testIndexerReadsTradesAndOrderEvents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		irSeedParticipants(t, ctx, platform)
		irSeedMarkets(t, ctx, platform)

		// collateral_change is nullable; impact 2 leaves it NULL.
		impacts := []irNetImpact{
			{ID: 1, TxHash: irTxHash(0x01), QueryID: 1, ParticipantID: 1, Wallet: irWallet1, Outcome: true,
				SharesChange: 100, CollateralChange: irPtr("50000000000000000000"), IsNegative: true, Timestamp: 1_800_000_010},
			{ID: 2, TxHash: irTxHash(0x01), QueryID: 1, ParticipantID: 1, Wallet: irWallet1, Outcome: false,
				SharesChange: 100, CollateralChange: nil, IsNegative: true, Timestamp: 1_800_000_010},
			{ID: 3, TxHash: irTxHash(0x02), QueryID: 1, ParticipantID: 2, Wallet: irWallet2, Outcome: true,
				SharesChange: -30, CollateralChange: irPtr("18000000000000000000"), IsNegative: false, Timestamp: 1_800_000_020},
			{ID: 4, TxHash: irTxHash(0x03), QueryID: 4, ParticipantID: 3, Wallet: irWallet3, Outcome: false,
				SharesChange: 0, CollateralChange: irPtr("1000000"), IsNegative: false, Timestamp: 1_800_000_030},
		}
		// Inserted in reverse key order, so insertion order cannot produce the order checked below.
		for _, n := range irReversed(impacts) {
			irExec(t, ctx, platform,
				`INSERT INTO main.ob_net_impacts (id, tx_hash, query_id, participant_id, outcome,
				     shares_change, collateral_change, is_negative, timestamp)
				 VALUES ($1, $2, $3, $4, $5, $6, $7::NUMERIC(78, 0), $8, $9)`,
				n.ID, n.TxHash, n.QueryID, n.ParticipantID, n.Outcome, n.SharesChange,
				n.CollateralChange, n.IsNegative, n.Timestamp)
		}

		// Ids start at 3, as they do once old events are trimmed, and jump
		// from 5 to 9, so the cursor has to be a key and not a count. Block 21
		// holds events 4 and 5.
		events := []irOrderEvent{
			{ID: 3, TxHash: irTxHash(0x01), QueryID: 1, ParticipantID: 1, Wallet: irWallet1, EventType: "buy_placed",
				Outcome: true, Price: -40, Amount: 50, CounterpartyID: nil, BlockHeight: 20, BlockTimestamp: 1_800_000_020},
			{ID: 4, TxHash: irTxHash(0x02), QueryID: 1, ParticipantID: 2, Wallet: irWallet2, EventType: "direct_buy_fill",
				Outcome: true, Price: 60, Amount: 30, CounterpartyID: irPtr(int64(1)), BlockHeight: 21, BlockTimestamp: 1_800_000_021},
			{ID: 5, TxHash: irTxHash(0x02), QueryID: 1, ParticipantID: 1, Wallet: irWallet1, EventType: "direct_sell_fill",
				Outcome: true, Price: 60, Amount: 30, CounterpartyID: irPtr(int64(2)), BlockHeight: 21, BlockTimestamp: 1_800_000_021},
			{ID: 9, TxHash: irTxHash(0x03), QueryID: 4, ParticipantID: 3, Wallet: irWallet3, EventType: "cancelled",
				Outcome: false, Price: 45, Amount: 10, CounterpartyID: nil, BlockHeight: 25, BlockTimestamp: 1_800_000_025},
		}
		// Inserted in reverse key order, so insertion order cannot produce the order checked below.
		for _, e := range irReversed(events) {
			irExec(t, ctx, platform,
				`INSERT INTO main.ob_order_events (id, tx_hash, query_id, participant_id, event_type, outcome,
				     price, amount, counterparty_id, block_height, block_timestamp)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
				e.ID, e.TxHash, e.QueryID, e.ParticipantID, e.EventType, e.Outcome, e.Price, e.Amount,
				e.CounterpartyID, e.BlockHeight, e.BlockTimestamp)
		}

		require.Equal(t, impacts, irListNetImpacts(t, ctx, platform, nil, nil))
		require.Equal(t, impacts[1:3], irListNetImpacts(t, ctx, platform, int64(1), int64(2)))
		require.Equal(t, impacts[3:], irListNetImpacts(t, ctx, platform, int64(3), int64(2)))
		require.Empty(t, irListNetImpacts(t, ctx, platform, int64(4), int64(2)))

		require.Equal(t, events, irListOrderEvents(t, ctx, platform, nil, nil, nil))

		// Pages of two. The cursor is the last row's (block_height, id), so the
		// page that ends with event 4 is followed by event 5 from the same block.
		require.Equal(t, events[0:2], irListOrderEvents(t, ctx, platform, nil, nil, int64(2)))
		require.Equal(t, events[2:4], irListOrderEvents(t, ctx, platform, int64(21), int64(4), int64(2)))
		require.Empty(t, irListOrderEvents(t, ctx, platform, int64(25), int64(9), int64(2)))

		// (21, NULL) starts at the first event of block 21.
		require.Equal(t, events[1:], irListOrderEvents(t, ctx, platform, int64(21), nil, nil))

		require.Equal(t, irMaxIDsRow{NetImpact: irPtr(int64(4)), OrderEventHeight: irPtr(int64(25)), OrderEvent: irPtr(int64(9))},
			irMaxIDs(t, ctx, platform))

		// 1001 more events, ids 10 to 1010 in blocks 110 to 1110. A NULL page
		// size reads 1000 of them and the next page the last one.
		irExec(t, ctx, platform,
			`INSERT INTO main.ob_order_events (id, tx_hash, query_id, participant_id, event_type, outcome,
			     price, amount, counterparty_id, block_height, block_timestamp)
			 SELECT g, decode(repeat('ee', 32), 'hex'), 1, 1, 'buy_placed', TRUE, -40, 1, NULL, 100 + g, 1800001000 + g
			 FROM generate_series(10, 1010) AS g`)

		page := irListOrderEvents(t, ctx, platform, int64(25), int64(9), nil)
		require.Len(t, page, 1000)
		for i, e := range page {
			require.Equal(t, int64(10+i), e.ID)
		}
		lastRow := page[len(page)-1]
		last := irListOrderEvents(t, ctx, platform, lastRow.BlockHeight, lastRow.ID, nil)
		require.Equal(t, [][2]int64{{1110, 1010}}, irOrderEventKeys(last))

		maxIDs := irMaxIDs(t, ctx, platform)
		require.Equal(t, irPtr(int64(1110)), maxIDs.OrderEventHeight)
		require.Equal(t, irPtr(int64(1010)), maxIDs.OrderEvent)
		return nil
	}
}

// testIndexerReadsOrderEventsAcrossARestart reads order events with a cursor
// taken before a trim removed every event, after the new events have started
// the ids again at 1.
func testIndexerReadsOrderEventsAcrossARestart(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		irSeedParticipants(t, ctx, platform)
		irSeedMarkets(t, ctx, platform)

		// record adds one event in the given block with id MAX(id) + 1, as
		// ob_record_order_event (044) does.
		record := func(height int64) {
			irExec(t, ctx, platform,
				`INSERT INTO main.ob_order_events (id, tx_hash, query_id, participant_id, event_type, outcome,
				     price, amount, counterparty_id, block_height, block_timestamp)
				 SELECT COALESCE(MAX(id), 0::INT8) + 1, decode(repeat('ee', 32), 'hex'), 1, 1, 'buy_placed',
				     TRUE, -40, 1, NULL, $1::INT8, 1800000000 + $1::INT8
				 FROM main.ob_order_events`, height)
		}

		// Five events in blocks 10 to 13. A reader that has read them all
		// holds the cursor (13, 5).
		for _, height := range []int64{10, 11, 11, 12, 13} {
			record(height)
		}
		read := irListOrderEvents(t, ctx, platform, nil, nil, nil)
		require.Equal(t, [][2]int64{{10, 1}, {11, 2}, {11, 3}, {12, 4}, {13, 5}}, irOrderEventKeys(read))
		cursor := read[len(read)-1]

		// A trim whose cutoff is past block 13 removes every event, so the
		// next id is 1 again.
		irExec(t, ctx, platform, `DELETE FROM main.ob_order_events WHERE block_height < 50`)
		require.Equal(t, irMaxIDsRow{}, irMaxIDs(t, ctx, platform))

		// Seven new events in blocks 60 to 64 take ids 1 to 7, five of them at
		// or below the cursor's id. The newest id, 7, is above the cursor's,
		// so the ids alone look like two new events, while the newest height,
		// 64, is past the cursor's 13.
		for _, height := range []int64{60, 60, 61, 62, 62, 63, 64} {
			record(height)
		}
		require.Equal(t, irMaxIDsRow{OrderEventHeight: irPtr(int64(64)), OrderEvent: irPtr(int64(7))},
			irMaxIDs(t, ctx, platform))

		// The cursor from before the trim reads all seven, in pages of four
		// that split block 62.
		first := irListOrderEvents(t, ctx, platform, cursor.BlockHeight, cursor.ID, int64(4))
		require.Equal(t, [][2]int64{{60, 1}, {60, 2}, {61, 3}, {62, 4}}, irOrderEventKeys(first))
		next := irListOrderEvents(t, ctx, platform, first[3].BlockHeight, first[3].ID, int64(4))
		require.Equal(t, [][2]int64{{62, 5}, {63, 6}, {64, 7}}, irOrderEventKeys(next))
		return nil
	}
}

// testIndexerReadsTransactionEvents reads the fee ledger in pages across rows
// that share a block, and over bounded block ranges.
func testIndexerReadsTransactionEvents(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		caller := "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
		recipient := "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"

		// In (block_height, tx_id) order. Blocks 10 and 12 hold several rows,
		// seeded below out of order so the order comes from the action.
		want := []irTxEvent{
			{TxID: irTxID(0xa), BlockHeight: 10, MethodID: 1, Caller: caller, FeeAmount: "2000000000000000000", FeeRecipient: irPtr(recipient)},
			{TxID: irTxID(0xb), BlockHeight: 10, MethodID: 2, Caller: caller, FeeAmount: "1000000000000000000", FeeRecipient: irPtr(recipient)},
			{TxID: irTxID(0xc), BlockHeight: 10, MethodID: 4, Caller: caller, FeeAmount: "0", Metadata: irPtr(`{"to":"` + recipient + `"}`)},
			{TxID: irTxID(0x1), BlockHeight: 11, MethodID: 2, Caller: caller, FeeAmount: "1000000000000000000", FeeRecipient: irPtr(recipient)},
			{TxID: irTxID(0x2), BlockHeight: 12, MethodID: 2, Caller: caller, FeeAmount: "1000000000000000000", FeeRecipient: irPtr(recipient)},
			{TxID: irTxID(0x3), BlockHeight: 12, MethodID: 7, Caller: caller, FeeAmount: "1000000000000000000", FeeRecipient: irPtr(recipient)},
			{TxID: irTxID(0x4), BlockHeight: 15, MethodID: 6, Caller: caller, FeeAmount: "40000000000000000000", FeeRecipient: irPtr(recipient)},
		}
		for _, i := range []int{5, 1, 2, 0, 6, 4, 3} {
			e := want[i]
			irExec(t, ctx, platform,
				`INSERT INTO main.transaction_events (tx_id, block_height, method_id, caller, fee_amount,
				     fee_recipient, metadata)
				 VALUES ($1, $2, $3, $4, $5::NUMERIC(78, 0), $6, $7)`,
				e.TxID, e.BlockHeight, e.MethodID, e.Caller, e.FeeAmount, e.FeeRecipient, e.Metadata)
		}

		require.Equal(t, want, irListTxEvents(t, ctx, platform, nil, nil, nil, nil))

		// Pages of two: the cursor is the last row's (block_height, tx_id), so
		// the rows of block 10 that come after it are not skipped.
		cursor := []any{nil, nil}
		var walked []irTxEvent
		for range 10 {
			page := irListTxEvents(t, ctx, platform, cursor[0], cursor[1], nil, int64(2))
			if len(page) == 0 {
				break
			}
			require.LessOrEqual(t, len(page), 2)
			walked = append(walked, page...)
			lastRow := page[len(page)-1]
			cursor = []any{lastRow.BlockHeight, lastRow.TxID}
		}
		require.Equal(t, want, walked)
		require.Equal(t, want[1:3], irListTxEvents(t, ctx, platform, int64(10), irTxID(0xa), nil, int64(2)))

		// $to_height is included.
		require.Equal(t, want[:6], irListTxEvents(t, ctx, platform, nil, nil, int64(12), nil))
		require.Equal(t, want[5:6], irListTxEvents(t, ctx, platform, int64(12), irTxID(0x2), int64(12), nil))
		require.Empty(t, irListTxEvents(t, ctx, platform, int64(12), irTxID(0x3), int64(12), nil))

		// (height, NULL) starts at the first row of that block, so blocks 10 to
		// 11 read as (10, NULL, 11).
		require.Equal(t, want[:4], irListTxEvents(t, ctx, platform, int64(10), nil, int64(11), nil))
		require.Equal(t, want[3:4], irListTxEvents(t, ctx, platform, int64(11), nil, nil, int64(1)))
		require.Empty(t, irListTxEvents(t, ctx, platform, int64(15), irTxID(0x4), nil, nil))
		return nil
	}
}

// testIndexerReadsRefuseBadPageSizes checks that each paged action refuses a
// page size outside 1 to 1000 instead of cutting it down, and takes 1 and
// 1000.
func testIndexerReadsRefuseBadPageSizes(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		paged := []struct {
			action  string
			args    func(limit int64) []any
			columns []string
		}{
			{"list_markets_after", func(l int64) []any { return []any{nil, l} }, irMarketColumns},
			{"get_market_positions", func(l int64) []any { return []any{int64(1), nil, nil, nil, l} }, irPositionColumns},
			{"list_fee_distributions", func(l int64) []any { return []any{nil, l} }, irDistributionColumns},
			{"list_fee_distribution_details", func(l int64) []any { return []any{nil, nil, l} }, irDetailColumns},
			{"list_net_impacts", func(l int64) []any { return []any{nil, l} }, irNetImpactColumns},
			{"list_order_events", func(l int64) []any { return []any{nil, nil, l} }, irOrderEventColumns},
			{"list_transaction_events", func(l int64) []any { return []any{nil, nil, nil, l} }, irTxEventColumns},
		}
		for _, tc := range paged {
			for _, limit := range []int64{0, -1, 1001, 5000} {
				_, err := irRead(ctx, platform, tc.action, tc.args(limit), tc.columns)
				require.ErrorContains(t, err, "limit must be between 1 and 1000", "%s with limit %d", tc.action, limit)
			}
			for _, limit := range []int64{1, 1000} {
				_, err := irRead(ctx, platform, tc.action, tc.args(limit), tc.columns)
				require.NoError(t, err, "%s with limit %d", tc.action, limit)
			}
		}

		_, err := irRead(ctx, platform, "get_market_positions", []any{nil, nil, nil, nil, nil}, irPositionColumns)
		require.ErrorContains(t, err, "query_id is required")
		return nil
	}
}

// =============================================================================
// Seeds
// =============================================================================

// irExec runs one statement against the test database directly, bypassing the
// actions, so a scenario controls every key and NULL.
func irExec(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, stmt string, args ...any) {
	t.Helper()
	_, err := platform.DB.Execute(ctx, stmt, args...)
	require.NoError(t, err)
}

func irSeedParticipants(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) {
	t.Helper()
	for i, wallet := range [][]byte{irWallet1, irWallet2, irWallet3} {
		irExec(t, ctx, platform, `INSERT INTO main.ob_participants (id, wallet_address) VALUES ($1, $2)`,
			int64(i+1), wallet)
	}
}

// irMarket is one ob_queries row, as list_markets_after returns it.
type irMarket struct {
	ID              int64
	Hash            []byte
	QueryComponents []byte
	SettleTime      int64
	Settled         bool
	WinningOutcome  *bool
	SettledAt       *int64
	CreatedAt       int64
	Creator         []byte
	MaxSpread       int64
	MinOrderSize    int64
	Bridge          string
}

// irSeedMarkets seeds seven markets on four bridges and returns them in id
// order. Markets 1, 4, 5 and 7 are open; 2, 3 and 6 have settled, and the
// only market on hoodi_tt2 is settled.
func irSeedMarkets(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) []irMarket {
	t.Helper()
	market := func(id int64, bridge string, creator []byte, settled bool, winning *bool, settledAt *int64) irMarket {
		return irMarket{
			ID:              id,
			Hash:            bytes.Repeat([]byte{byte(id)}, 32),
			QueryComponents: []byte{0xc0, byte(id)},
			SettleTime:      1_800_000_000 + id*100,
			Settled:         settled,
			WinningOutcome:  winning,
			SettledAt:       settledAt,
			CreatedAt:       100 + id,
			Creator:         creator,
			MaxSpread:       4 + id,
			MinOrderSize:    19 + id,
			Bridge:          bridge,
		}
	}
	markets := []irMarket{
		market(1, "eth_truf", irWallet1, false, nil, nil),
		market(2, "eth_usdc", irWallet2, true, irPtr(true), irPtr(int64(1_800_000_200))),
		market(3, "eth_truf", irWallet1, true, irPtr(false), irPtr(int64(1_800_000_300))),
		market(4, "eth_usdc", irWallet2, false, nil, nil),
		market(5, "hoodi_tt", irWallet3, false, nil, nil),
		market(6, "hoodi_tt2", irWallet3, true, irPtr(true), irPtr(int64(1_800_000_600))),
		market(7, "eth_truf", irWallet2, false, nil, nil),
	}
	// Seeded in reverse, so the order the action returns is its own.
	for i := len(markets) - 1; i >= 0; i-- {
		m := markets[i]
		irExec(t, ctx, platform,
			`INSERT INTO main.ob_queries (id, hash, query_components, settle_time, settled, winning_outcome,
			     settled_at, created_at, creator, max_spread, min_order_size, bridge)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			m.ID, m.Hash, m.QueryComponents, m.SettleTime, m.Settled, m.WinningOutcome, m.SettledAt,
			m.CreatedAt, m.Creator, m.MaxSpread, m.MinOrderSize, m.Bridge)
	}
	return markets
}

type irPosition struct {
	QueryID, ParticipantID int64
	Outcome                bool
	Price, Amount          int64
}

func irSeedPositions(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, positions []irPosition) {
	t.Helper()
	for i, p := range positions {
		irExec(t, ctx, platform,
			`INSERT INTO main.ob_positions (query_id, participant_id, outcome, price, amount, last_updated)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			p.QueryID, p.ParticipantID, p.Outcome, p.Price, p.Amount, int64(1000+i))
	}
}

// =============================================================================
// Reads
// =============================================================================

// irRead calls a read action as a wallet that holds nothing and returns its
// rows. Every row must carry exactly the given columns, in order.
func irRead(ctx context.Context, platform *kwilTesting.Platform, action string, args []any, columns []string) ([][]any, error) {
	reader := util.Unsafe_NewEthereumAddressFromString("0xD4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4")
	var rows [][]any
	err := callActionQueries(ctx, platform, &reader, action, args, func(row *common.Row) error {
		if len(row.ColumnTypes) != len(row.ColumnNames) {
			return fmt.Errorf("%s returned %d column names and %d types", action, len(row.ColumnNames), len(row.ColumnTypes))
		}
		got := make([]string, len(row.ColumnNames))
		for i, name := range row.ColumnNames {
			got[i] = name + " " + row.ColumnTypes[i].String()
		}
		if !slices.Equal(got, columns) {
			return fmt.Errorf("%s returned columns %v, want %v", action, got, columns)
		}
		rows = append(rows, row.Values)
		return nil
	})
	return rows, err
}

func irOptInt(v any) *int64 {
	if v == nil {
		return nil
	}
	return irPtr(v.(int64))
}

func irOptBool(v any) *bool {
	if v == nil {
		return nil
	}
	return irPtr(v.(bool))
}

func irOptText(v any) *string {
	if v == nil {
		return nil
	}
	return irPtr(v.(string))
}

func irNumeric(v any) string { return v.(*kwilTypes.Decimal).String() }

func irOptNumeric(v any) *string {
	if v == nil {
		return nil
	}
	return irPtr(irNumeric(v))
}

func irListMarkets(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, afterID, limit any) []irMarket {
	t.Helper()
	rows, err := irRead(ctx, platform, "list_markets_after", []any{afterID, limit}, irMarketColumns)
	require.NoError(t, err)
	var markets []irMarket
	for _, r := range rows {
		markets = append(markets, irMarket{
			ID:              r[0].(int64),
			Hash:            r[1].([]byte),
			QueryComponents: r[2].([]byte),
			SettleTime:      r[3].(int64),
			Settled:         r[4].(bool),
			WinningOutcome:  irOptBool(r[5]),
			SettledAt:       irOptInt(r[6]),
			CreatedAt:       r[7].(int64),
			Creator:         r[8].([]byte),
			MaxSpread:       r[9].(int64),
			MinOrderSize:    r[10].(int64),
			Bridge:          r[11].(string),
		})
	}
	return markets
}

func irMarketIDs(markets []irMarket) []int64 {
	var ids []int64
	for _, m := range markets {
		ids = append(ids, m.ID)
	}
	return ids
}

type irOpenInterest struct {
	Bridge       string
	OpenInterest int64
	Markets      int64
}

func irOpenInterestByBridge(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) []irOpenInterest {
	t.Helper()
	rows, err := irRead(ctx, platform, "get_open_interest_by_bridge", nil, irOpenInterestColumns)
	require.NoError(t, err)
	var out []irOpenInterest
	for _, r := range rows {
		out = append(out, irOpenInterest{Bridge: r[0].(string), OpenInterest: r[1].(int64), Markets: r[2].(int64)})
	}
	return out
}

type irMarketPosition struct {
	ParticipantID int64
	Wallet        []byte
	Outcome       bool
	Price         int64
	Amount        int64
}

func irMarketPositions(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, queryID int64,
	afterParticipant, afterOutcome, afterPrice, limit any) []irMarketPosition {
	t.Helper()
	rows, err := irRead(ctx, platform, "get_market_positions",
		[]any{queryID, afterParticipant, afterOutcome, afterPrice, limit}, irPositionColumns)
	require.NoError(t, err)
	var out []irMarketPosition
	for _, r := range rows {
		out = append(out, irMarketPosition{
			ParticipantID: r[0].(int64),
			Wallet:        r[1].([]byte),
			Outcome:       r[2].(bool),
			Price:         r[3].(int64),
			Amount:        r[4].(int64),
		})
	}
	return out
}

type irDistribution struct {
	ID            int64
	QueryID       int64
	Bridge        string
	TotalFees     string
	DPFees        string
	ValidatorFees string
	LPCount       int64
	BlockCount    int64
	DistributedAt int64
}

func irReadDistributions(ctx context.Context, platform *kwilTesting.Platform, action string, args []any) ([]irDistribution, error) {
	rows, err := irRead(ctx, platform, action, args, irDistributionColumns)
	if err != nil {
		return nil, err
	}
	var out []irDistribution
	for _, r := range rows {
		out = append(out, irDistribution{
			ID:            r[0].(int64),
			QueryID:       r[1].(int64),
			Bridge:        r[2].(string),
			TotalFees:     irNumeric(r[3]),
			DPFees:        irNumeric(r[4]),
			ValidatorFees: irNumeric(r[5]),
			LPCount:       r[6].(int64),
			BlockCount:    r[7].(int64),
			DistributedAt: r[8].(int64),
		})
	}
	return out, nil
}

func irListDistributions(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, afterID, limit any) []irDistribution {
	t.Helper()
	out, err := irReadDistributions(ctx, platform, "list_fee_distributions", []any{afterID, limit})
	require.NoError(t, err)
	return out
}

type irDetail struct {
	DistributionID int64
	QueryID        int64
	ParticipantID  int64
	Wallet         []byte
	RewardPercent  string
	RewardAmount   string
	DistributedAt  int64
	BlockCount     int64
}

func irListDetails(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, afterDistribution, afterParticipant, limit any) []irDetail {
	t.Helper()
	rows, err := irRead(ctx, platform, "list_fee_distribution_details",
		[]any{afterDistribution, afterParticipant, limit}, irDetailColumns)
	require.NoError(t, err)
	var out []irDetail
	for _, r := range rows {
		out = append(out, irDetail{
			DistributionID: r[0].(int64),
			QueryID:        r[1].(int64),
			ParticipantID:  r[2].(int64),
			Wallet:         r[3].([]byte),
			RewardPercent:  irNumeric(r[4]),
			RewardAmount:   irNumeric(r[5]),
			DistributedAt:  r[6].(int64),
			BlockCount:     r[7].(int64),
		})
	}
	return out
}

type irNetImpact struct {
	ID               int64
	TxHash           []byte
	QueryID          int64
	ParticipantID    int64
	Wallet           []byte
	Outcome          bool
	SharesChange     int64
	CollateralChange *string
	IsNegative       bool
	Timestamp        int64
}

func irListNetImpacts(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, afterID, limit any) []irNetImpact {
	t.Helper()
	rows, err := irRead(ctx, platform, "list_net_impacts", []any{afterID, limit}, irNetImpactColumns)
	require.NoError(t, err)
	var out []irNetImpact
	for _, r := range rows {
		out = append(out, irNetImpact{
			ID:               r[0].(int64),
			TxHash:           r[1].([]byte),
			QueryID:          r[2].(int64),
			ParticipantID:    r[3].(int64),
			Wallet:           r[4].([]byte),
			Outcome:          r[5].(bool),
			SharesChange:     r[6].(int64),
			CollateralChange: irOptNumeric(r[7]),
			IsNegative:       r[8].(bool),
			Timestamp:        r[9].(int64),
		})
	}
	return out
}

type irOrderEvent struct {
	ID             int64
	TxHash         []byte
	QueryID        int64
	ParticipantID  int64
	Wallet         []byte
	EventType      string
	Outcome        bool
	Price          int64
	Amount         int64
	CounterpartyID *int64
	BlockHeight    int64
	BlockTimestamp int64
}

func irListOrderEvents(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, afterHeight, afterID, limit any) []irOrderEvent {
	t.Helper()
	rows, err := irRead(ctx, platform, "list_order_events", []any{afterHeight, afterID, limit}, irOrderEventColumns)
	require.NoError(t, err)
	var out []irOrderEvent
	for _, r := range rows {
		out = append(out, irOrderEvent{
			ID:             r[0].(int64),
			TxHash:         r[1].([]byte),
			QueryID:        r[2].(int64),
			ParticipantID:  r[3].(int64),
			Wallet:         r[4].([]byte),
			EventType:      r[5].(string),
			Outcome:        r[6].(bool),
			Price:          r[7].(int64),
			Amount:         r[8].(int64),
			CounterpartyID: irOptInt(r[9]),
			BlockHeight:    r[10].(int64),
			BlockTimestamp: r[11].(int64),
		})
	}
	return out
}

// irOrderEventKeys returns the (block_height, id) of each event.
func irOrderEventKeys(events []irOrderEvent) [][2]int64 {
	var keys [][2]int64
	for _, e := range events {
		keys = append(keys, [2]int64{e.BlockHeight, e.ID})
	}
	return keys
}

// irMaxIDsRow is the one row get_order_book_max_ids returns.
type irMaxIDsRow struct {
	NetImpact        *int64
	OrderEventHeight *int64
	OrderEvent       *int64
}

func irMaxIDs(t *testing.T, ctx context.Context, platform *kwilTesting.Platform) irMaxIDsRow {
	t.Helper()
	rows, err := irRead(ctx, platform, "get_order_book_max_ids", nil, irMaxIDColumns)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return irMaxIDsRow{NetImpact: irOptInt(rows[0][0]), OrderEventHeight: irOptInt(rows[0][1]), OrderEvent: irOptInt(rows[0][2])}
}

type irTxEvent struct {
	TxID         string
	BlockHeight  int64
	MethodID     int64
	Caller       string
	FeeAmount    string
	FeeRecipient *string
	Metadata     *string
}

func irListTxEvents(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, afterHeight, afterTxID, toHeight, limit any) []irTxEvent {
	t.Helper()
	rows, err := irRead(ctx, platform, "list_transaction_events",
		[]any{afterHeight, afterTxID, toHeight, limit}, irTxEventColumns)
	require.NoError(t, err)
	var out []irTxEvent
	for _, r := range rows {
		out = append(out, irTxEvent{
			TxID:         r[0].(string),
			BlockHeight:  r[1].(int64),
			MethodID:     r[2].(int64),
			Caller:       r[3].(string),
			FeeAmount:    irNumeric(r[4]),
			FeeRecipient: irOptText(r[5]),
			Metadata:     irOptText(r[6]),
		})
	}
	return out
}

// irReversed returns a copy of s in reverse order.
func irReversed[T any](s []T) []T {
	out := make([]T, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}
