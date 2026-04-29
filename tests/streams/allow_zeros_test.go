package tests

import (
	"context"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestAllowZerosCombinations exercises the four-cell {filter on, filter off} ×
// {value=0, value≠0} matrix on the insert path, plus the post-create
// mutability path (set_allow_zeros) and the matching prune-enqueue gating.
func TestAllowZerosCombinations(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "allow_zeros_combinations_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testAllowZerosFourCombinations(t),
			testAllowZerosDefaultPreservesBehavior(t),
			testSetAllowZerosToggleMutability(t),
			testAllowZerosGatesPruneEnqueue(t),
			testGetAllowZerosReflectsState(t),
			testNonOwnerCannotToggleAllowZeros(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testAllowZerosFourCombinations: in one platform, create two streams —
// one default (allow_zeros=FALSE), one opted-in (allow_zeros=TRUE) — and
// for each, insert one zero and one non-zero record. Verify get_record
// returns the expected subset.
func testAllowZerosFourCombinations(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x000000000000000000000000000000000000a110")
		platform = procedure.WithSigner(platform, deployer.Bytes())
		require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

		allowZerosOffLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_off_stream"),
			DataProvider: deployer,
		}
		allowZerosOnLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_on_stream"),
			DataProvider: deployer,
		}

		// allow_zeros=false: zeros dropped on insert (today's behavior).
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, allowZerosOffLocator, false))
		// allow_zeros=true: zeros persist alongside non-zero values.
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, allowZerosOnLocator, true))

		// Insert {value=0, value=5} into both streams at distinct event_times.
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, allowZerosOffLocator,
			setup.InsertRecordInput{EventTime: 100, Value: 0}, 1))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, allowZerosOffLocator,
			setup.InsertRecordInput{EventTime: 200, Value: 5}, 1))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, allowZerosOnLocator,
			setup.InsertRecordInput{EventTime: 100, Value: 0}, 1))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, allowZerosOnLocator,
			setup.InsertRecordInput{EventTime: 200, Value: 5}, 1))

		// allow_zeros=false: only non-zero shows up.
		offRows, err := getRecordsRange(ctx, platform, allowZerosOffLocator, 0, 1000, 10)
		require.NoError(t, err)
		require.Equal(t, []rec{{EventTime: 200, Value: 5}}, offRows,
			"allow_zeros=false: zero must be dropped, non-zero must persist")

		// allow_zeros=true: both records show up.
		onRows, err := getRecordsRange(ctx, platform, allowZerosOnLocator, 0, 1000, 10)
		require.NoError(t, err)
		require.Equal(t, []rec{
			{EventTime: 100, Value: 0},
			{EventTime: 200, Value: 5},
		}, onRows, "allow_zeros=true: both zero and non-zero must persist")

		return nil
	}
}

// testAllowZerosDefaultPreservesBehavior: streams created via the
// 2-arg create_stream() (no allow_zeros parameter — relies on DEFAULT
// FALSE) must drop zeros exactly like before this feature shipped.
func testAllowZerosDefaultPreservesBehavior(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x000000000000000000000000000000000000a111")
		platform = procedure.WithSigner(platform, deployer.Bytes())
		require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

		locator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_default_stream"),
			DataProvider: deployer,
		}
		// Use the existing 2-arg helper to confirm DEFAULT FALSE kicks in
		// for old-shape callers (covers SDK backwards-compat).
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: locator,
		}))

		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator,
			setup.InsertRecordInput{EventTime: 100, Value: 0}, 1))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator,
			setup.InsertRecordInput{EventTime: 200, Value: 7}, 1))

		rows, err := getRecordsRange(ctx, platform, locator, 0, 1000, 5)
		require.NoError(t, err)
		require.Equal(t, []rec{{EventTime: 200, Value: 7}}, rows,
			"default-shape create_stream must preserve today's zero-drop behavior")

		return nil
	}
}

// testSetAllowZerosToggleMutability: create a default-FALSE stream,
// confirm zeros are dropped, then call set_allow_zeros(TRUE) and confirm
// subsequent zero inserts persist. Then flip back to FALSE and confirm
// new zeros are dropped again. Earlier-persisted zero stays put (the
// flag is forward-only).
func testSetAllowZerosToggleMutability(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x000000000000000000000000000000000000a112")
		platform = procedure.WithSigner(platform, deployer.Bytes())
		require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

		locator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_toggle_stream"),
			DataProvider: deployer,
		}
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, locator, false))

		// Each stage reads at insert_height+1 so the just-written row is
		// visible. The pattern is more verbose than a global magic
		// height but spells out the read-after-write timing per stage.

		// Stage 1: default off — zero dropped.
		const stage1Insert int64 = 1
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator,
			setup.InsertRecordInput{EventTime: 100, Value: 0}, stage1Insert))
		rows, err := getRecordsRange(ctx, platform, locator, 0, 1000, stage1Insert+1)
		require.NoError(t, err)
		require.Empty(t, rows, "stage 1: zero with allow_zeros=FALSE must be dropped")

		// Toggle on.
		require.NoError(t, setAllowZeros(ctx, platform, locator, true))

		// Stage 2: zeros now persist.
		const stage2Insert int64 = 2
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator,
			setup.InsertRecordInput{EventTime: 200, Value: 0}, stage2Insert))
		rows, err = getRecordsRange(ctx, platform, locator, 0, 1000, stage2Insert+1)
		require.NoError(t, err)
		require.Equal(t, []rec{{EventTime: 200, Value: 0}}, rows,
			"stage 2: zero after allow_zeros=TRUE flip must persist")

		// Toggle off again.
		require.NoError(t, setAllowZeros(ctx, platform, locator, false))

		// Stage 3: new zeros are dropped, but the one from stage 2 stays.
		const stage3Insert int64 = 3
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator,
			setup.InsertRecordInput{EventTime: 300, Value: 0}, stage3Insert))
		rows, err = getRecordsRange(ctx, platform, locator, 0, 1000, stage3Insert+1)
		require.NoError(t, err)
		require.Equal(t, []rec{{EventTime: 200, Value: 0}}, rows,
			"stage 3: zero after flip-back-to-FALSE must be dropped; earlier zero must remain")

		return nil
	}
}

// testAllowZerosGatesPruneEnqueue: with allow_zeros=TRUE, zero-only
// days must be enqueued in pending_prune_days so digest can see them.
// With allow_zeros=FALSE, zero-only days must NOT be enqueued (today's
// behavior, prevents wasted digest cycles for filtered-out values).
func testAllowZerosGatesPruneEnqueue(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x000000000000000000000000000000000000a113")
		platform = procedure.WithSigner(platform, deployer.Bytes())
		require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

		offLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_prune_off_stream"),
			DataProvider: deployer,
		}
		onLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_prune_on_stream"),
			DataProvider: deployer,
		}
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, offLocator, false))
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, onLocator, true))

		// Insert only zeros into both streams at day 0 and day 1.
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, offLocator,
			setup.InsertRecordInput{EventTime: 100, Value: 0}, 1))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, offLocator,
			setup.InsertRecordInput{EventTime: 90000, Value: 0}, 2))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, onLocator,
			setup.InsertRecordInput{EventTime: 100, Value: 0}, 1))
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, onLocator,
			setup.InsertRecordInput{EventTime: 90000, Value: 0}, 2))

		offDays, err := getPendingDays(ctx, platform, deployer.Address(), offLocator.StreamId.String())
		require.NoError(t, err)
		require.Empty(t, offDays, "allow_zeros=FALSE: zero-only days must not be enqueued")

		onDays, err := getPendingDays(ctx, platform, deployer.Address(), onLocator.StreamId.String())
		require.NoError(t, err)
		require.Equal(t, []int{0, 1}, onDays, "allow_zeros=TRUE: zero-only days must be enqueued")

		return nil
	}
}

// testGetAllowZerosReflectsState: get_allow_zeros must return FALSE
// for streams without an explicit row (today's default), TRUE after
// opt-in via create_stream(_, _, true), and round-trip through
// set_allow_zeros toggles.
func testGetAllowZerosReflectsState(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x000000000000000000000000000000000000a114")
		platform = procedure.WithSigner(platform, deployer.Bytes())
		require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

		defaultLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_view_default"),
			DataProvider: deployer,
		}
		optInLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_view_optin"),
			DataProvider: deployer,
		}
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type: setup.ContractTypePrimitive, Locator: defaultLocator,
		}))
		require.NoError(t, createStreamWithAllowZeros(ctx, platform, optInLocator, true))

		v, err := getAllowZeros(ctx, platform, defaultLocator)
		require.NoError(t, err)
		require.False(t, v, "default-shape create: get_allow_zeros must return FALSE")

		v, err = getAllowZeros(ctx, platform, optInLocator)
		require.NoError(t, err)
		require.True(t, v, "opt-in create: get_allow_zeros must return TRUE")

		// Toggle the default stream on, confirm view flips.
		require.NoError(t, setAllowZeros(ctx, platform, defaultLocator, true))
		v, err = getAllowZeros(ctx, platform, defaultLocator)
		require.NoError(t, err)
		require.True(t, v, "after set_allow_zeros(TRUE): view must return TRUE")

		// Toggle off, confirm view flips back.
		require.NoError(t, setAllowZeros(ctx, platform, defaultLocator, false))
		v, err = getAllowZeros(ctx, platform, defaultLocator)
		require.NoError(t, err)
		require.False(t, v, "after set_allow_zeros(FALSE): view must return FALSE")

		return nil
	}
}

// testNonOwnerCannotToggleAllowZeros: a wallet that didn't create the
// stream must not be able to flip its allow_zeros flag, and a failed
// attempt must leave the current state unchanged.
func testNonOwnerCannotToggleAllowZeros(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		owner := util.Unsafe_NewEthereumAddressFromString("0x000000000000000000000000000000000000a115")
		platform = procedure.WithSigner(platform, owner.Bytes())
		require.NoError(t, setup.CreateDataProvider(ctx, platform, owner.Address()))

		defaultLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId("allow_zeros_authz_stream"),
			DataProvider: owner,
		}
		// Create with the default (FALSE).
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type: setup.ContractTypePrimitive, Locator: defaultLocator,
		}))

		prev, err := getAllowZeros(ctx, platform, defaultLocator)
		require.NoError(t, err)
		require.False(t, prev, "precondition: default-shape stream must report allow_zeros=false")

		stranger := util.Unsafe_NewEthereumAddressFromString("0x00000000000000000000000000000000000ba115")
		err = setAllowZerosAs(ctx, platform, defaultLocator, true, stranger)
		require.Error(t, err, "non-owner toggle must be rejected by the action")
		require.Contains(t, err.Error(), "Only stream owner",
			"reject reason should come from the is_stream_owner guard")

		after, err := getAllowZeros(ctx, platform, defaultLocator)
		require.NoError(t, err)
		require.Equal(t, prev, after, "rejected toggle must not change the stored flag")

		return nil
	}
}

// ----- Helpers ---------------------------------------------------------

// rec carries the event_time and a parsed numeric value so assertions
// stay format-agnostic (e.g., the SDK could reformat 0 as "0",
// "0.0", or "0.000000000000000000" without breaking the tests).
type rec struct {
	EventTime int64
	Value     float64
}

// createStreamWithAllowZeros calls create_stream with the third
// allow_zeros argument explicitly. Mirrors setup.UntypedCreateStream
// but extends the positional args list.
func createStreamWithAllowZeros(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, allowZeros bool) error {
	addr, err := util.NewEthereumAddressFromString(locator.DataProvider.Address())
	if err != nil {
		return errors.Wrap(err, "invalid data provider address")
	}

	engineCtx := setup.NewEngineContext(ctx, platform, addr, 1)

	r, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
		[]any{locator.StreamId.String(), string(setup.ContractTypePrimitive), allowZeros},
		func(row *common.Row) error { return nil },
	)
	if err != nil {
		return errors.Wrap(err, "create_stream call failed")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "create_stream action failed")
	}
	return nil
}

// setAllowZeros calls set_allow_zeros signed by the stream owner.
func setAllowZeros(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, value bool) error {
	owner, err := util.NewEthereumAddressFromString(locator.DataProvider.Address())
	if err != nil {
		return errors.Wrap(err, "invalid data provider address")
	}
	return setAllowZerosAs(ctx, platform, locator, value, owner)
}

// setAllowZerosAs calls set_allow_zeros signed by an explicit caller —
// used to assert the owner-only authorization check rejects strangers.
func setAllowZerosAs(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, value bool, caller util.EthereumAddress) error {
	engineCtx := setup.NewEngineContext(ctx, platform, caller, 1)

	r, err := platform.Engine.Call(engineCtx, platform.DB, "", "set_allow_zeros",
		[]any{locator.DataProvider.Address(), locator.StreamId.String(), value},
		func(row *common.Row) error { return nil },
	)
	if err != nil {
		return errors.Wrap(err, "set_allow_zeros call failed")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "set_allow_zeros action failed")
	}
	return nil
}

func getAllowZeros(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator) (bool, error) {
	addr, err := util.NewEthereumAddressFromString(locator.DataProvider.Address())
	if err != nil {
		return false, errors.Wrap(err, "invalid data provider address")
	}
	engineCtx := setup.NewEngineContext(ctx, platform, addr, 1)

	var got bool
	r, err := platform.Engine.Call(engineCtx, platform.DB, "", "get_allow_zeros",
		[]any{locator.DataProvider.Address(), locator.StreamId.String()},
		func(row *common.Row) error {
			if len(row.Values) == 0 || row.Values[0] == nil {
				return nil
			}
			b, ok := row.Values[0].(bool)
			if !ok {
				return errors.Errorf("get_allow_zeros: expected bool, got %T", row.Values[0])
			}
			got = b
			return nil
		},
	)
	if err != nil {
		return false, errors.Wrap(err, "get_allow_zeros call failed")
	}
	if r.Error != nil {
		return false, errors.Wrap(r.Error, "get_allow_zeros action failed")
	}
	return got, nil
}

// getRecordsRange wraps procedure.GetRecord with explicit from/to/height
// int64 values and returns a typed event_time + numeric-value slice.
// The height parameter is the chain height the read is performed at —
// callers should pass a height >= the latest insert height in the test
// so all writes are visible.
func getRecordsRange(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, from, to, height int64) ([]rec, error) {
	rows, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
		Platform:      platform,
		StreamLocator: locator,
		FromTime:      &from,
		ToTime:        &to,
		Height:        height,
	})
	if err != nil {
		return nil, err
	}
	out := make([]rec, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			return nil, errors.Errorf("get_record returned row with %d cols", len(row))
		}
		ts, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "parse event_time")
		}
		v, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, errors.Wrapf(err, "parse value %q as float", row[1])
		}
		out = append(out, rec{EventTime: ts, Value: v})
	}
	return out, nil
}
