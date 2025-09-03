package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/util"
)

const daySecs int64 = 86400

// --- Context helpers ---------------------------------------------------------

type ctxKit struct {
	addr util.EthereumAddress
	tx   *common.TxContext
	eng  *common.EngineContext
}

func newCtxKit(ctx context.Context, p *kwilTesting.Platform, override bool) (*ctxKit, error) {
	addr, err := util.NewEthereumAddressFromBytes(p.Deployer)
	timestamp := time.Now().Unix()
	if err != nil {
		return nil, errors.Wrap(err, "new ethereum address")
	}
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1, Timestamp: timestamp},
		Signer:       addr.Bytes(),
		Caller:       addr.Address(),
		TxID:         p.Txid(),
	}
	return &ctxKit{
		addr: addr,
		tx:   tx,
		eng:  &common.EngineContext{TxContext: tx, OverrideAuthz: override},
	}, nil
}

func dayRange(day int64) (start, end int64) {
	return day * daySecs, day*daySecs + daySecs
}

// --- One-time platform bootstrap --------------------------------------------

// Wraps a test with signer + provider registration.
// Keeps your existing behavior; centralizes boilerplate.
func WithSignerAndProvider(next func(ctx context.Context, p *kwilTesting.Platform) error) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, p *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		p = procedure.WithSigner(p, deployer.Bytes())
		if err := setup.CreateDataProvider(ctx, p, deployer.Address()); err != nil {
			return errors.Wrap(err, "register data provider")
		}
		return next(ctx, p)
	}
}

// --- Reusable setup helpers --------------------------------------------------

func SetupStreamMD(ctx context.Context, p *kwilTesting.Platform, streamID util.StreamId, height int64, markdown string) error {
	return setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
		Platform:     p,
		StreamId:     streamID,
		Height:       height,
		MarkdownData: markdown,
	})
}

// Option-style wrapper for composing stream setup with a test fn.
func WithStreamMD(streamID util.StreamId, height int64, markdown string, next func(context.Context, *kwilTesting.Platform) error) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, p *kwilTesting.Platform) error {
		if err := SetupStreamMD(ctx, p, streamID, height, markdown); err != nil {
			return errors.Wrap(err, "setup stream from markdown")
		}
		return next(ctx, p)
	}
}

// --- Action callers (generic + typed shims) ---------------------------------

// Generic action call that stringifies row values and validates column count.
func callActionAsStrings(ctx context.Context, p *kwilTesting.Platform, action string, expectedCols int, args ...any) ([]procedure.ResultRow, error) {
	kit, err := newCtxKit(ctx, p, false)
	if err != nil {
		return nil, err
	}

	var out []procedure.ResultRow
	r, err := p.Engine.Call(kit.eng, p.DB, "", action, args, func(row *common.Row) error {
		if len(row.Values) != expectedCols {
			return errors.Errorf("%s: expected %d columns, got %d", action, expectedCols, len(row.Values))
		}
		rowOut := make(procedure.ResultRow, expectedCols)
		for i := 0; i < expectedCols; i++ {
			rowOut[i] = fmt.Sprintf("%v", row.Values[i])
		}
		out = append(out, rowOut)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r != nil && r.Error != nil {
		return nil, errors.Wrap(r.Error, action+" failed")
	}
	return out, nil
}

func callGetDailyOHLC(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64) ([]procedure.ResultRow, error) {
	return callActionAsStrings(ctx, p, "get_daily_ohlc", 4, streamRef, dayIndex)
}

func callBatchDigest(ctx context.Context, p *kwilTesting.Platform, streamRefs []int, dayIndexes []int) ([]procedure.ResultRow, error) {
	return callActionAsStrings(ctx, p, "batch_digest", 4, streamRefs, dayIndexes)
}

func callBatchDigestWithCap(ctx context.Context, p *kwilTesting.Platform, streamRefs []int, dayIndexes []int, deleteCap int) ([]procedure.ResultRow, error) {
	return callActionAsStrings(ctx, p, "batch_digest", 4, streamRefs, dayIndexes, deleteCap)
}

func callAutoDigest(ctx context.Context, p *kwilTesting.Platform, deleteCap int) ([]procedure.ResultRow, error) {
	return callActionAsStrings(ctx, p, "auto_digest", 3, deleteCap, 24 /* expected_records_per_stream */)
}

func callAutoDigestWithPreserve(ctx context.Context, p *kwilTesting.Platform, deleteCap int, preservePastDays int) ([]procedure.ResultRow, error) {
	return callActionAsStrings(ctx, p, "auto_digest", 3, deleteCap, 24 /* expected_records_per_stream */, preservePastDays)
}

// callAutoDigestAtTimestamp calls auto_digest with an explicit BlockContext.Timestamp
// to make preserve window behavior deterministic in tests.
func callAutoDigestAtTimestamp(ctx context.Context, p *kwilTesting.Platform, deleteCap int, expectedRecordsPerStream int, preservePastDays int, timestamp int64) ([]procedure.ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(p.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error creating ethereum address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1, Timestamp: timestamp},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         p.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var out []procedure.ResultRow
	r, err := p.Engine.Call(engineContext, p.DB, "", "auto_digest", []any{
		deleteCap,
		expectedRecordsPerStream,
		preservePastDays,
	}, func(row *common.Row) error {
		if len(row.Values) != 3 {
			return errors.Errorf("auto_digest: expected 3 columns, got %d", len(row.Values))
		}
		out = append(out, procedure.ResultRow{
			fmt.Sprintf("%v", row.Values[0]),
			fmt.Sprintf("%v", row.Values[1]),
			fmt.Sprintf("%v", row.Values[2]),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r != nil && r.Error != nil {
		return nil, errors.Wrap(r.Error, "auto_digest failed")
	}
	return out, nil
}

// --- Small SQL helpers -------------------------------------------------------

func insertPendingDay(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64) error {
	kit, err := newCtxKit(ctx, p, true)
	if err != nil {
		return err
	}
	return p.Engine.Execute(kit.eng, p.DB,
		"INSERT INTO pending_prune_days (stream_ref, day_index) VALUES ($sr, $di) ON CONFLICT DO NOTHING",
		map[string]any{"$sr": streamRef, "$di": dayIndex},
		func(*common.Row) error { return nil })
}

func queryCount(ctx context.Context, p *kwilTesting.Platform, sql string, args map[string]any) (int, error) {
	kit, err := newCtxKit(ctx, p, true)
	if err != nil {
		return 0, err
	}
	var count int
	err = p.Engine.Execute(kit.eng, p.DB, sql, args, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return errors.Errorf("count: expected 1 col, got %d", len(row.Values))
		}
		switch v := row.Values[0].(type) {
		case int64:
			count = int(v)
		case int:
			count = v
		case string:
			var n int
			_, e := fmt.Sscanf(v, "%d", &n)
			if e != nil {
				return e
			}
			count = n
		default:
			return errors.Errorf("unexpected COUNT type: %T", row.Values[0])
		}
		return nil
	})
	return count, err
}

func countPrimitiveEvents(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64) (int, error) {
	start, end := dayRange(dayIndex)
	return queryCount(ctx, p,
		"SELECT COUNT(*) FROM primitive_events WHERE stream_ref=$sr AND event_time >= $ds AND event_time < $de",
		map[string]any{"$sr": streamRef, "$ds": start, "$de": end},
	)
}

func queryEventTypeMap(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64) (map[int64]int, error) {
	kit, err := newCtxKit(ctx, p, true)
	if err != nil {
		return nil, err
	}
	start, end := dayRange(dayIndex)
	out := make(map[int64]int)
	err = p.Engine.Execute(kit.eng, p.DB,
		`SELECT event_time, type FROM primitive_event_type
		 WHERE stream_ref=$sr AND event_time >= $ds AND event_time < $de ORDER BY event_time`,
		map[string]any{"$sr": streamRef, "$ds": start, "$de": end},
		func(row *common.Row) error {
			if len(row.Values) != 2 {
				return errors.Errorf("expected 2 cols, got %d", len(row.Values))
			}
			et, ok1 := row.Values[0].(int64)
			tf, ok2 := row.Values[1].(int64)
			if !ok1 || !ok2 {
				return errors.New("bad types in primitive_event_type")
			}
			out[et] = int(tf)
			return nil
		})
	return out, err
}

// --- Flag verification (unifies 3 nearly identical helpers) ------------------

const (
	FlagOpen  = 1 << 0 // 1
	FlagHigh  = 1 << 1 // 2
	FlagLow   = 1 << 2 // 4
	FlagClose = 1 << 3 // 8
)

func verifyFlagsExact(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64, expected map[int64]int) error {
	got, err := queryEventTypeMap(ctx, p, streamRef, dayIndex)
	if err != nil {
		return err
	}
	// exact keys
	if len(got) != len(expected) {
		return errors.Errorf("expected %d flag rows, got %d", len(expected), len(got))
	}
	for ts, want := range expected {
		have, ok := got[ts]
		if !ok {
			return errors.Errorf("missing flag for event_time %d", ts)
		}
		if have != want {
			return errors.Errorf("wrong flag at %d: want %d, got %d", ts, want, have)
		}
	}
	return nil
}

// --- Assertion sugar ---------------------------------------------------------

func assertMarkdownEquals(t *testing.T, actual []procedure.ResultRow, expected string) {
	t.Helper()
	table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
		Actual:   actual,
		Expected: strings.TrimSpace(expected),
	})
}

// Optional utility for inserting arbitrary records.
type rec struct {
	EventTime int64
	Value     *kwilTypes.Decimal
	CreatedAt int64 // optional; 0 means leave default
}

func insertPrimitiveRecords(ctx context.Context, p *kwilTesting.Platform, streamRef int, rows []rec, clearFirst bool) error {
	kit, err := newCtxKit(ctx, p, true)
	if err != nil {
		return err
	}
	if clearFirst {
		if err := p.Engine.Execute(kit.eng, p.DB, "DELETE FROM primitive_events WHERE stream_ref=$sr",
			map[string]any{"$sr": streamRef}, func(*common.Row) error { return nil }); err != nil {
			return errors.Wrap(err, "clear primitive_events")
		}
	}
	for _, r := range rows {
		sql := "INSERT INTO primitive_events (stream_ref,event_time,value,created_at) VALUES ($sr,$et,$v,$ca)"
		args := map[string]any{"$sr": streamRef, "$et": r.EventTime, "$v": r.Value, "$ca": r.CreatedAt}
		if err := p.Engine.Execute(kit.eng, p.DB, sql, args, func(*common.Row) error { return nil }); err != nil {
			return errors.Wrapf(err, "insert at %d", r.EventTime)
		}
	}
	return nil
}

// --- Additional helpers for specific use cases -------------------------------

func checkPendingPruneDayExists(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64) (bool, error) {
	count, err := queryCount(ctx, p,
		"SELECT COUNT(*) FROM pending_prune_days WHERE stream_ref=$sr AND day_index=$di",
		map[string]any{"$sr": streamRef, "$di": dayIndex},
	)
	return count > 0, err
}

func countMarkers(ctx context.Context, p *kwilTesting.Platform, streamRef int, dayIndex int64) (int, error) {
	start, end := dayRange(dayIndex)
	return queryCount(ctx, p,
		"SELECT COUNT(*) FROM primitive_event_type WHERE stream_ref=$sr AND event_time >= $ds AND event_time < $de",
		map[string]any{"$sr": streamRef, "$ds": start, "$de": end},
	)
}

// Helper function to test auto_digest with different expected_records_per_stream values
func testAutoDigestWithExpectedRecords(p *kwilTesting.Platform, deployer *util.EthereumAddress, expectedRecords int, expectedErrorSubstring string) error {
	txContext := &common.TxContext{
		Ctx:          context.Background(),
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         p.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var result []procedure.ResultRow
	r, err := p.Engine.Call(engineContext, p.DB, "", "auto_digest", []any{
		10,              // delete_cap
		expectedRecords, // expected_records_per_stream
	}, func(row *common.Row) error {
		if len(row.Values) != 3 {
			return errors.Errorf("expected 3 columns, got %d", len(row.Values))
		}
		processedDays := fmt.Sprintf("%v", row.Values[0])
		totalDeleted := fmt.Sprintf("%v", row.Values[1])
		hasMore := fmt.Sprintf("%v", row.Values[2])
		result = append(result, procedure.ResultRow{processedDays, totalDeleted, hasMore})
		return nil
	})

	// If we expect an error but didn't get one, that's a test failure
	if expectedErrorSubstring != "" {
		if err == nil && (r == nil || r.Error == nil) {
			return errors.Errorf("expected error containing '%s', but got no error", expectedErrorSubstring)
		}
		if r != nil && r.Error != nil {
			if !strings.Contains(r.Error.Error(), expectedErrorSubstring) {
				return errors.Errorf("expected error containing '%s', but got: %s", expectedErrorSubstring, r.Error.Error())
			}
		}
	} else {
		// We expect success, so any error is unexpected
		if err != nil {
			return errors.Wrap(err, "unexpected error for valid input")
		}
		if r != nil && r.Error != nil {
			return errors.Wrap(r.Error, "unexpected error for valid input")
		}
	}

	return nil
}
