//go:build kwiltest

package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// Wallets of the fee history dataset. Every address is lower case, as
// record_transaction_event stores it.
const (
	feeHistoryPayer   = "0x00000000000000000000000000000000000000a1" // pays most fees
	feeHistoryQuiet   = "0x00000000000000000000000000000000000000a2" // pays 3 fees
	feeHistorySelf    = "0x00000000000000000000000000000000000000a3" // pays some fees and receives some, a few to itself
	feeHistoryLeaderL = "0x00000000000000000000000000000000000000b1" // receives fees
	feeHistoryLeaderM = "0x00000000000000000000000000000000000000b2" // receives fees
	feeHistoryNobody  = "0x00000000000000000000000000000000000000c1" // no fee rows
)

// feeHistoryEvent is one transaction_events row of the dataset.
type feeHistoryEvent struct {
	txID      string
	height    int64
	methodID  int64
	caller    string
	recipient string // "" stores NULL
	fee       string
	metadata  string // "" stores NULL
}

// TestTransactionFeeHistoryReads checks list_transaction_fees and
// get_last_transactions as migration 065 defines them against the definitions
// they replace (027 and 010), which the test installs under other names. Both
// read the same dataset, so for every wallet, mode and page the two must
// return the same rows in the same order.
func TestTransactionFeeHistoryReads(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")
	seeds := append(migrations.GetSeedScriptStatements(), previousFeeHistoryActions(t)...)
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "LEDGER_FEE02_FeeHistoryReads",
		SeedStatements: seeds,
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			runFeeHistoryReadsScenario(t, owner),
		},
	}, testutils.GetTestOptionsWithCache())
}

// previousFeeHistoryActions returns the 027 list_transaction_fees and the 010
// get_last_transactions, renamed list_transaction_fees_027 and
// get_last_transactions_010.
func previousFeeHistoryActions(t *testing.T) []string {
	t.Helper()
	read := func(file string) string {
		for _, p := range migrations.GetSeedScriptPaths() {
			if filepath.Base(p) == file {
				b, err := os.ReadFile(p)
				require.NoError(t, err)
				return string(b)
			}
		}
		t.Fatalf("migration %s not found", file)
		return ""
	}
	extract := func(src, action, newName string) string {
		head := "CREATE OR REPLACE ACTION " + action + "("
		start := strings.Index(src, head)
		require.GreaterOrEqual(t, start, 0, "%s not found", action)
		end := strings.Index(src[start:], "\n};")
		require.Greater(t, end, 0, "end of %s not found", action)
		body := src[start : start+end+len("\n};")]
		return strings.Replace(body, head, "CREATE OR REPLACE ACTION "+newName+"(", 1)
	}
	return []string{
		extract(read("027-transaction-actions.sql"), "list_transaction_fees", "list_transaction_fees_027"),
		extract(read("010-get-latest-write-activity.sql"), "get_last_transactions", "get_last_transactions_010"),
	}
}

// feeHistoryDataset builds the events. Three events share each block, so the
// tx_id tie break decides their order. A fee recipient is NULL on some
// events, and a few fees are zero, which record_transaction_event stores
// without a distribution row. feeHistorySelf pays some fees to itself, so in
// both mode the same event comes from both branches.
func feeHistoryDataset() []feeHistoryEvent {
	txID := func(n int) string {
		sum := sha256.Sum256([]byte(fmt.Sprintf("fee-history-%d", n)))
		return "0x" + hex.EncodeToString(sum[:])
	}
	methods := []int64{1, 2, 4, 6, 8}
	recipients := []string{"", feeHistoryLeaderL, feeHistoryLeaderM, feeHistorySelf}

	var events []feeHistoryEvent
	for i := 0; i < 40; i++ {
		caller := feeHistoryPayer
		if i%5 == 0 {
			caller = feeHistorySelf
		}
		fee := fmt.Sprintf("%d00000000000000000", i+1)
		if i%7 == 0 {
			fee = "0"
		}
		metadata := ""
		if i%3 == 0 {
			metadata = `{"bridge":"hoodi_tt"}`
		}
		events = append(events, feeHistoryEvent{
			txID:      txID(i),
			height:    100 + int64(i/3),
			methodID:  methods[i%len(methods)],
			caller:    caller,
			recipient: recipients[i%len(recipients)],
			fee:       fee,
			metadata:  metadata,
		})
	}
	for i, height := range []int64{50, 104, 200} {
		events = append(events, feeHistoryEvent{
			txID:      txID(100 + i),
			height:    height,
			methodID:  2,
			caller:    feeHistoryQuiet,
			recipient: feeHistoryLeaderL,
			fee:       "1000000000000000000",
		})
	}
	return events
}

func runFeeHistoryReadsScenario(t *testing.T, owner util.EthereumAddress) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform.Deployer = owner.Bytes()

		events := feeHistoryDataset()
		for _, ev := range events {
			require.NoError(t, seedFeeHistoryEvent(ctx, platform, ev))
		}

		res, err := platform.DB.Execute(ctx,
			`SELECT indexdef FROM pg_indexes WHERE schemaname = 'main' AND indexname = 'transaction_events_fee_recipient_idx'`)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1, "migration 065 creates transaction_events_fee_recipient_idx")
		require.Contains(t, res.Rows[0][0], "(fee_recipient, block_height, tx_id)")

		caller := owner.Address()
		wallets := []string{
			feeHistoryPayer, feeHistoryQuiet, feeHistorySelf,
			feeHistoryLeaderL, feeHistoryLeaderM, feeHistoryNobody,
			"0x" + strings.ToUpper(feeHistoryPayer[2:]), // upper case hex
		}
		modes := []any{"paid", "received", "both", "BOTH", nil}
		pages := [][2]any{
			{20, 0}, {5, 0}, {5, 5}, {5, 10}, {3, 22}, {1000, 0}, {7, 100},
			{0, 0}, {-1, -5}, {nil, nil},
			// Offsets near the INT maximum, where offset + limit does not fit.
			{20, int64(math.MaxInt64 - 20)}, {20, int64(math.MaxInt64 - 7)}, {1000, int64(math.MaxInt64)},
		}

		// Every wallet, mode and page returns what the 027 definition returns.
		for _, wallet := range wallets {
			for _, mode := range modes {
				for _, page := range pages {
					args := []any{wallet, mode, page[0], page[1]}
					got, err := feeHistoryRows(ctx, platform, caller, "list_transaction_fees", args)
					require.NoError(t, err)
					want, err := feeHistoryRows(ctx, platform, caller, "list_transaction_fees_027", args)
					require.NoError(t, err)
					require.Equal(t, want, got, "list_transaction_fees%v", args)
				}
			}
		}

		// Every wallet and limit returns what the 010 definition returns,
		// including no wallet at all.
		lastWallets := []any{"", nil}
		for _, wallet := range wallets {
			lastWallets = append(lastWallets, wallet)
		}
		for _, wallet := range lastWallets {
			for _, limit := range []any{1, 3, 6, 20, 100, 0, nil} {
				args := []any{wallet, limit}
				got, err := feeHistoryRows(ctx, platform, caller, "get_last_transactions", args)
				require.NoError(t, err)
				want, err := feeHistoryRows(ctx, platform, caller, "get_last_transactions_010", args)
				require.NoError(t, err)
				require.Equal(t, want, got, "get_last_transactions%v", args)
			}
		}

		// The same checks against the dataset itself, so the comparison above
		// cannot pass on two equally wrong answers.
		for _, mode := range []string{"paid", "received", "both"} {
			for _, wallet := range wallets[:6] {
				want := expectedFeeHistoryTxIDs(events, wallet, mode)
				rows, err := fetchTransactionFeesPage(ctx, platform, caller, wallet, mode, 1000, 0)
				require.NoError(t, err)
				require.Equal(t, want, distinctTxIDs(rows), "%s %s", mode, wallet)

				// Pages of 4 read back the whole list, in order.
				paged := []string{}
				for offset := 0; offset < len(want)+4; offset += 4 {
					rows, err := fetchTransactionFeesPage(ctx, platform, caller, wallet, mode, 4, offset)
					require.NoError(t, err)
					paged = append(paged, distinctTxIDs(rows)...)
				}
				require.Equal(t, want, paged, "paged %s %s", mode, wallet)
			}
		}

		quiet, err := fetchTransactionFeesPage(ctx, platform, caller, feeHistoryQuiet, "paid", 20, 0)
		require.NoError(t, err)
		require.Len(t, quiet, 3)
		require.Equal(t, []int64{200, 104, 50}, []int64{quiet[0].CreatedAt, quiet[1].CreatedAt, quiet[2].CreatedAt})

		nobody, err := fetchTransactionFeesPage(ctx, platform, caller, feeHistoryNobody, "both", 20, 0)
		require.NoError(t, err)
		require.Empty(t, nobody)

		// An event the wallet paid to itself is listed once in both mode.
		self, err := fetchTransactionFeesPage(ctx, platform, caller, feeHistorySelf, "both", 1000, 0)
		require.NoError(t, err)
		require.Len(t, self, len(expectedFeeHistoryTxIDs(events, feeHistorySelf, "both")))

		// In received mode every event names the wallet as its fee recipient,
		// and each fee above zero carries the wallet's distribution row.
		received, err := fetchTransactionFeesPage(ctx, platform, caller, feeHistoryLeaderL, "received", 1000, 0)
		require.NoError(t, err)
		for _, row := range received {
			require.Equal(t, feeHistoryLeaderL, row.FeeRecipient)
			if row.FeeAmount != "0" {
				require.Equal(t, feeHistoryLeaderL, row.DistributionRecipient)
				require.Equal(t, row.FeeAmount, row.DistributionAmount)
			}
		}

		return nil
	}
}

// seedFeeHistoryEvent writes an event and, as record_transaction_event does,
// one distribution row to the fee recipient when there is a recipient and a
// fee above zero.
func seedFeeHistoryEvent(ctx context.Context, platform *kwilTesting.Platform, ev feeHistoryEvent) error {
	var recipient, metadata any
	if ev.recipient != "" {
		recipient = ev.recipient
	}
	if ev.metadata != "" {
		metadata = ev.metadata
	}
	_, err := platform.DB.Execute(ctx,
		`INSERT INTO main.transaction_events (tx_id, block_height, method_id, caller, fee_amount, fee_recipient, metadata)
		 VALUES ($1, $2, $3, $4, $5::NUMERIC(78, 0), $6, $7)`,
		ev.txID, ev.height, ev.methodID, ev.caller, ev.fee, recipient, metadata)
	if err != nil {
		return err
	}
	if ev.recipient == "" || ev.fee == "0" {
		return nil
	}
	_, err = platform.DB.Execute(ctx,
		`INSERT INTO main.transaction_event_distributions (tx_id, sequence, recipient, amount, note)
		 VALUES ($1, 1, $2, $3::NUMERIC(78, 0), NULL)`,
		ev.txID, ev.recipient, ev.fee)
	return err
}

// expectedFeeHistoryTxIDs lists the events a wallet paid, received or both,
// newest first.
func expectedFeeHistoryTxIDs(events []feeHistoryEvent, wallet, mode string) []string {
	var matched []feeHistoryEvent
	for _, ev := range events {
		paid := ev.caller == wallet
		received := ev.recipient == wallet
		if (mode == "paid" && paid) || (mode == "received" && received) || (mode == "both" && (paid || received)) {
			matched = append(matched, ev)
		}
	}
	for i := 1; i < len(matched); i++ {
		for j := i; j > 0; j-- {
			a, b := matched[j-1], matched[j]
			if a.height > b.height || (a.height == b.height && a.txID > b.txID) {
				break
			}
			matched[j-1], matched[j] = b, a
		}
	}
	ids := make([]string, 0, len(matched))
	for _, ev := range matched {
		ids = append(ids, ev.txID)
	}
	return ids
}

// distinctTxIDs returns the tx ids of the rows in order, once per event.
func distinctTxIDs(rows []ledgerRow) []string {
	ids := []string{}
	for _, row := range rows {
		if len(ids) == 0 || ids[len(ids)-1] != row.TxID {
			ids = append(ids, row.TxID)
		}
	}
	return ids
}

func fetchTransactionFeesPage(ctx context.Context, platform *kwilTesting.Platform, caller, wallet, mode string, limit, offset int) ([]ledgerRow, error) {
	var rows []ledgerRow
	err := callView(ctx, platform, caller, "list_transaction_fees", []any{wallet, mode, limit, offset}, func(row *common.Row) error {
		rows = append(rows, ledgerRow{
			TxID:                  row.Values[0].(string),
			CreatedAt:             row.Values[1].(int64),
			Method:                row.Values[2].(string),
			Caller:                row.Values[3].(string),
			FeeAmount:             decimalToString(row.Values[4]),
			FeeRecipient:          stringOrEmpty(row.Values[5]),
			RawMetadata:           stringOrEmpty(row.Values[6]),
			DistributionSequence:  int(row.Values[7].(int64)),
			DistributionRecipient: stringOrEmpty(row.Values[8]),
			DistributionAmount:    decimalToString(row.Values[9]),
		})
		return nil
	})
	return rows, err
}

// feeHistoryRows renders every column of every row, so two actions can be
// compared value for value and in order.
func feeHistoryRows(ctx context.Context, platform *kwilTesting.Platform, caller, action string, args []any) ([]string, error) {
	var rows []string
	err := callView(ctx, platform, caller, action, args, func(row *common.Row) error {
		cols := make([]string, len(row.Values))
		for i, v := range row.Values {
			switch val := v.(type) {
			case nil:
				cols[i] = "NULL"
			case *kwilTypes.Decimal:
				cols[i] = val.String()
			default:
				cols[i] = fmt.Sprint(val)
			}
		}
		rows = append(rows, strings.Join(cols, "|"))
		return nil
	})
	return rows, err
}
