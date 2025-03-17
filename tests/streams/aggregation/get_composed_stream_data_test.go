package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/kwilteam/kwil-db/common"
	kwilTypes "github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

/*
   COMPOSED STREAM DATA TEST SUITE

   Taxonomy versions:
     1) time=0 => Parent => Child1
     2) time=4 => Parent => Child2 (Child1 removed)

   Primitive data:
     Child1 => event_times=1,2 => values=10,12
     Child2 => event_times=2,6 => values=100,180

   Because the function doesn't do fine-grained "time validity" for events,
   but rather "if a substream was active at *any* point in [from..to],
   we include all that substream's data within that range."

   Test cases:
     #1 => [1..3]: Only overlaps version=0 => Child1 => times=1,2
     #2 => [1..6]: Overlaps version=0 (Child1) & version=4 (Child2)
                  => Child1(1,2) + Child2(2,6)
     #3 => [7..7]: Overlaps version=4 => only Child2 is active,
                  but Child2 has no event at 7 => gap-fill => last known record at time=6
     #4 => [NULL..3]: Tests behavior with unbounded lower range, should get all events <= 3
     #5 => [2..NULL]: Tests behavior with unbounded upper range, should get all events >= 2
     #6 => [2..2]: Tests exact hit on $from, ensuring anchor logic works correctly
     #7 => [3..5]: Tests anchor behavior when no records exist in the specified range
     #8 => Testing $frozen_at: Ensures records with created_at > frozen_at are excluded
*/

const (
	parentStreamName = "parent_composed"
	child1StreamName = "child1_primitive"
	child2StreamName = "child2_primitive"
)

var (
	parentStreamId = util.GenerateStreamId(parentStreamName)
	child1StreamId = util.GenerateStreamId(child1StreamName)
	child2StreamId = util.GenerateStreamId(child2StreamName)
)

const (
	version1Start = int64(0) // => child1
	version2Start = int64(4) // => child2
)

func TestComposedStreamData(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "composed_stream_data_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithTestSetup(testQuery1_Child1Only(t)),
			WithTestSetup(testQuery2_BothChildren(t)),
			WithTestSetup(testQuery3_GapFillAtTime7(t)),
			WithTestSetup(testQuery4_NoLowerBound(t)),
			WithTestSetup(testQuery5_NoUpperBound(t)),
			WithTestSetup(testQuery6_ExactFromHit(t)),
			WithTestSetup(testQuery7_NoInRangeDataButAnchorBelow(t)),
			WithTestSetup(testQuery8_FrozenAtBehavior(t)),
		},
	}, testutils.GetTestOptions())
}

// 1) Create streams
// 2) Insert data
// 3) Define 2 taxonomy versions
func WithTestSetup(
	testFn func(ctx context.Context, platform *kwilTesting.Platform) error,
) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Create parent (composed)
		if err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     parentStreamId,
				DataProvider: deployer,
			},
			Type: setup.ContractTypeComposed,
		}); err != nil {
			return errors.Wrap(err, "failed creating parent stream")
		}

		// Create child1 (primitive)
		if err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     child1StreamId,
				DataProvider: deployer,
			},
			Type: setup.ContractTypePrimitive,
		}); err != nil {
			return errors.Wrap(err, "failed creating child1")
		}

		// Create child2 (primitive)
		if err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     child2StreamId,
				DataProvider: deployer,
			},
			Type: setup.ContractTypePrimitive,
		}); err != nil {
			return errors.Wrap(err, "failed creating child2")
		}

		// Insert data => Child1 => times=1,2 => values=10,12
		child1Loc := types.StreamLocator{StreamId: child1StreamId, DataProvider: deployer}
		if err := setup.ExecuteInsertRecord(ctx, platform, child1Loc,
			setup.InsertRecordInput{EventTime: 1, Value: 10}, 1); err != nil {
			return err
		}
		if err := setup.ExecuteInsertRecord(ctx, platform, child1Loc,
			setup.InsertRecordInput{EventTime: 2, Value: 12}, 1); err != nil {
			return err
		}

		// Insert data => Child2 => times=2,6 => values=100,180
		child2Loc := types.StreamLocator{StreamId: child2StreamId, DataProvider: deployer}
		if err := setup.ExecuteInsertRecord(ctx, platform, child2Loc,
			setup.InsertRecordInput{EventTime: 2, Value: 100}, 1); err != nil {
			return err
		}
		if err := setup.ExecuteInsertRecord(ctx, platform, child2Loc,
			setup.InsertRecordInput{EventTime: 6, Value: 180}, 1); err != nil {
			return err
		}

		// Define version1 => time=0 => Parent => Child1
		if err := defineTaxonomy(ctx, platform, parentStreamId, deployer, map[util.StreamId]string{
			child1StreamId: "1.0",
		}, version1Start); err != nil {
			return err
		}

		// Define version2 => time=4 => Parent => Child2
		if err := defineTaxonomy(ctx, platform, parentStreamId, deployer, map[util.StreamId]string{
			child2StreamId: "1.0",
		}, version2Start); err != nil {
			return err
		}

		return testFn(ctx, platform)
	}
}

func defineTaxonomy(
	ctx context.Context,
	platform *kwilTesting.Platform,
	parentId util.StreamId,
	deployer util.EthereumAddress,
	children map[util.StreamId]string,
	startTime int64,
) error {
	dataProviders := make([]string, 0, len(children))
	streamIds := make([]string, 0, len(children))
	weights := make([]string, 0, len(children))

	for cid, w := range children {
		dataProviders = append(dataProviders, deployer.Address())
		streamIds = append(streamIds, cid.String())
		weights = append(weights, w)
	}

	return procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform: platform,
		StreamLocator: types.StreamLocator{
			StreamId:     parentId,
			DataProvider: deployer,
		},
		DataProviders: dataProviders,
		StreamIds:     streamIds,
		Weights:       weights,
		StartTime:     &startTime,
		Height:        1,
	})
}

// =========================================================
// get_composed_stream_data caller
// =========================================================
type GetComposedStreamDataInput struct {
	Platform     *kwilTesting.Platform
	DataProvider string
	StreamId     string
	FromTime     *int64
	ToTime       *int64
	FrozenAt     *int64
}
type ComposedStreamDataResult struct {
	EventTime    int64
	Value        kwilTypes.Decimal
	StreamId     string
	DataProvider string
}

func GetComposedStreamData(ctx context.Context, in GetComposedStreamDataInput) ([]ComposedStreamDataResult, error) {
	deployer, err := util.NewEthereumAddressFromBytes(in.Platform.Deployer)
	if err != nil {
		return nil, err
	}
	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 1,
		},
		TxID:   in.Platform.Txid(),
		Signer: in.Platform.Deployer,
		Caller: deployer.Address(),
	}
	engCtx := &common.EngineContext{TxContext: txContext}

	var fzn interface{}
	if in.FrozenAt != nil {
		fzn = *in.FrozenAt
	}

	var rows [][]any
	r, err := in.Platform.Engine.Call(engCtx, in.Platform.DB, "", "get_composed_stream_data", []any{
		in.DataProvider,
		in.StreamId,
		in.FromTime,
		in.ToTime,
		fzn,
	}, func(row *common.Row) error {
		vals := make([]any, len(row.Values))
		copy(vals, row.Values)
		rows = append(rows, vals)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "engine call error")
	}
	if r.Error != nil {
		return nil, r.Error
	}

	out := make([]ComposedStreamDataResult, 0, len(rows))
	for _, rw := range rows {
		if len(rw) != 4 {
			return nil, errors.Errorf("expected 4 columns, got %d", len(rw))
		}

		et, _ := rw[0].(int64)
		val, _ := rw[1].(*kwilTypes.Decimal)
		sId, _ := rw[2].(string)
		dProv, _ := rw[3].(string)

		out = append(out, ComposedStreamDataResult{
			EventTime:    et,
			Value:        *val,
			StreamId:     sId,
			DataProvider: dProv,
		})
	}
	return out, nil
}

// -----------------------------------------------------
// Test #1 => [1..3] => Only Child1 => times=1,2
// -----------------------------------------------------
func testQuery1_Child1Only(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		fromTime := int64(1)
		toTime := int64(3)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
			ToTime:       &toTime,
		})
		if err != nil {
			return err
		}

		// Child1 => times=1,2
		expected := `
        | event_time | value                     | stream_id              | data_provider                              |
        |------------|---------------------------|------------------------|--------------------------------------------|
        | 1          | 10.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        | 2          | 12.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Test #2 => [1..6] => Overlaps both version=0 (Child1) & version=4 (Child2)
// => Child1(1,2), Child2(2,6)
// -----------------------------------------------------
func testQuery2_BothChildren(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		fromTime := int64(1)
		toTime := int64(6)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
			ToTime:       &toTime,
		})
		if err != nil {
			return err
		}

		// Child1 => times=1,2
		// Child2 => times=2,6
		expected := `
        | event_time | value                     | stream_id               | data_provider                              |
        |------------|---------------------------|-------------------------|--------------------------------------------|
        | 1          | 10.000000000000000000     | child1_primitive        | 0x0000000000000000000000000000000000000000 |
        | 2          | 12.000000000000000000     | child1_primitive        | 0x0000000000000000000000000000000000000000 |
        | 2          | 100.000000000000000000    | child2_primitive        | 0x0000000000000000000000000000000000000000 |
        | 6          | 180.000000000000000000    | child2_primitive        | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// Test #3 => [7..7], we do gap-filling, so we get the last known record at time=6
func testQuery3_GapFillAtTime7(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		fromTime := int64(7)
		toTime := int64(7)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
			ToTime:       &toTime,
		})
		if err != nil {
			return err
		}

		// Because of gap-fill, the function includes the last record at time=6
		expected := `
        | event_time | value                     | stream_id          | data_provider                              |
        |------------|---------------------------|--------------------|--------------------------------------------|
        | 6          | 180.000000000000000000    | child2_primitive   | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Test #4 => [NULL..3]: No lower bound => Child1 => skip anchor logic
// By having no $from, we should skip anchor logic and just return all events <= $to
// -----------------------------------------------------
func testQuery4_NoLowerBound(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		toTime := int64(3)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			ToTime:       &toTime,
			FrozenAt:     nil,
		})
		if err != nil {
			return err
		}

		// Child1 has times=1,2, which are <= 3
		expected := `
        | event_time | value                     | stream_id              | data_provider                              |
        |------------|---------------------------|------------------------|--------------------------------------------|
        | 1          | 10.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        | 2          | 12.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Test #5 => [2..NULL]: No upper bound => Both child streams => no limit on upper range
// -----------------------------------------------------
func testQuery5_NoUpperBound(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		fromTime := int64(2)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
		})
		if err != nil {
			return err
		}

		// For $from=2, we have:
		// - Child1 has exact match at time=2 (so no anchor from time=1)
		// - Child2 has times=2,6
		// With no upper bound, we should get all 3 records
		expected := `
        | event_time | value                     | stream_id              | data_provider                              |
        |------------|---------------------------|------------------------|--------------------------------------------|
        | 2          | 12.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        | 2          | 100.000000000000000000    | child2_primitive       | 0x0000000000000000000000000000000000000000 |
        | 6          | 180.000000000000000000    | child2_primitive       | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Test #6 => [2..2]: Exact from hit => Both child streams
// When there's a record exactly at $from, we don't need the anchor record below
// -----------------------------------------------------
func testQuery6_ExactFromHit(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		fromTime := int64(2)
		toTime := int64(2)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
			ToTime:       &toTime,
		})
		if err != nil {
			return err
		}

		// Both Child1 and Child2 have records exactly at time=2, but only 1 child is active
		// No anchor from time=1 needed since we have exact matches
		expected := `
        | event_time | value                     | stream_id              | data_provider                              |
        |------------|---------------------------|------------------------|--------------------------------------------|
        | 2          | 12.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Test #7 => [3..5]: No in-range data, but anchor below
// Child1 has times=1,2 (anchor=2 for $from=3), and no records in range
// Child2 has no records in [3..5] range, but has anchor at time=2
// -----------------------------------------------------
func testQuery7_NoInRangeDataButAnchorBelow(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)
		fromTime := int64(3)
		toTime := int64(5)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
			ToTime:       &toTime,
		})
		if err != nil {
			return err
		}

		// For range [3..5]:
		// - Child1 has no records in range, but anchor at time=2
		// - Child2 has no records in range, but anchor at time=2
		// Since next record for Child2 is at time=6 (outside range), we should see anchors
		expected := `
        | event_time | value                     | stream_id              | data_provider                              |
        |------------|---------------------------|------------------------|--------------------------------------------|
        | 2          | 12.000000000000000000     | child1_primitive       | 0x0000000000000000000000000000000000000000 |
        | 2          | 100.000000000000000000    | child2_primitive       | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Test #8 => Frozen at behavior
// To test frozen_at, we need to add records with different created_at values
// -----------------------------------------------------
func testQuery8_FrozenAtBehavior(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, _ := util.NewEthereumAddressFromBytes(platform.Deployer)

		// Insert a newer version of the Child2 time=6 record with a higher created_at
		// This simulates an update to the record after our test's freeze point
		child2Loc := types.StreamLocator{StreamId: child2StreamId, DataProvider: deployer}
		if err := setup.ExecuteInsertRecord(ctx, platform, child2Loc,
			setup.InsertRecordInput{EventTime: 6, Value: 200}, 10); err != nil {
			return err
		}

		// Now query with frozen_at = 5, which should get the original record (created_at=1)
		// and not the newer version (created_at=10)
		frozenAt := int64(5)
		fromTime := int64(6)
		toTime := int64(6)
		results, err := GetComposedStreamData(ctx, GetComposedStreamDataInput{
			Platform:     platform,
			DataProvider: deployer.Address(),
			StreamId:     parentStreamId.String(),
			FromTime:     &fromTime,
			ToTime:       &toTime,
			FrozenAt:     &frozenAt,
		})
		if err != nil {
			return err
		}

		// Should get original value=180, not the newer value=200
		expected := `
        | event_time | value                     | stream_id              | data_provider                              |
        |------------|---------------------------|------------------------|--------------------------------------------|
        | 6          | 180.000000000000000000    | child2_primitive       | 0x0000000000000000000000000000000000000000 |
        `
		return compareResults(t, results, expected)
	}
}

// -----------------------------------------------------
// Helper: compare actual vs. expected table
// -----------------------------------------------------
func compareResults(t *testing.T, results []ComposedStreamDataResult, expectedMarkdown string) error {
	var actual []procedure.ResultRow
	for _, r := range results {
		actual = append(actual, procedure.ResultRow{
			fmt.Sprintf("%d", r.EventTime),
			r.Value.String(),
			r.StreamId,
			r.DataProvider,
		})
	}

	table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
		Actual:      actual,
		Expected:    expectedMarkdown,
		SortColumns: []string{"event_time", "stream_id"},
		ColumnTransformers: map[string]func(string) string{
			"stream_id": func(realId string) string {
				streamId := util.GenerateStreamId(realId)
				return streamId.String()
			},
		},
	})
	return nil
}
