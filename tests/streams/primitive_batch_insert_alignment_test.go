package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestPrimitiveBatchInsertAlignment ensures that batch insertion via insert_records maps values
// to the correct streams when multiple streams are processed together.
//
// This is a regression test for a bug where get_stream_ids() and helper_lowercase_array()
// were using non-deterministic array aggregation, causing stream_ref arrays to become
// misaligned with input data_provider/stream_id arrays. This resulted in records being
// inserted into the wrong streams during batch operations.
//
// The test creates two streams and inserts interleaved data, then verifies each stream
// contains only its own data by checking specific value/stream_ref combinations.
func TestPrimitiveBatchInsertAlignment(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "primitive_batch_insert_alignment_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testBatchAlignment(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testBatchAlignment(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a stable signer as data provider
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000abc")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		if err := setup.CreateDataProvider(ctx, platform, deployer.Address()); err != nil {
			return errors.Wrap(err, "register data provider")
		}

		// Create two primitive streams under the same provider
		streamId1 := util.GenerateStreamId("primitive_batch_alignment_1")
		streamId2 := util.GenerateStreamId("primitive_batch_alignment_2")

		if err := setup.CreateStreams(ctx, platform, []setup.StreamInfo{
			{Locator: types.StreamLocator{StreamId: streamId1, DataProvider: deployer}, Type: setup.ContractTypePrimitive},
			{Locator: types.StreamLocator{StreamId: streamId2, DataProvider: deployer}, Type: setup.ContractTypePrimitive},
		}); err != nil {
			return errors.Wrap(err, "create streams")
		}

		// Prepare a single batch that interleaves records for both streams
		dataProviders := []string{deployer.Address(), deployer.Address(), deployer.Address(), deployer.Address()}
		streamIds := []string{streamId1.String(), streamId2.String(), streamId1.String(), streamId2.String()}
		eventTimes := []int64{101, 202, 103, 204}

		// Values as NUMERIC(36,18)
		var values []*kwilTypes.Decimal
		for _, v := range []string{"11", "22", "33", "44"} {
			dec, err := kwilTypes.ParseDecimalExplicit(v, 36, 18)
			if err != nil {
				return errors.Wrap(err, "parse decimal")
			}
			values = append(values, dec)
		}

		// Execute insert_records directly in one call
		engineContext := setup.NewEngineContext(ctx, platform, deployer, 1)

		r, err := platform.Engine.Call(engineContext, platform.DB, "", "insert_records", []any{
			dataProviders,
			streamIds,
			eventTimes,
			values,
		}, func(row *common.Row) error { return nil })
		if err != nil {
			return errors.Wrap(err, "insert_records call")
		}
		if r.Error != nil {
			return errors.Wrap(r.Error, "insert_records result")
		}

		// Validate that each stream received the correct values at the correct event_times
		// Stream 1: (101 -> 11), (103 -> 33)
		stream1 := types.StreamLocator{StreamId: streamId1, DataProvider: deployer}
		if err := assertRecord(t, ctx, platform, stream1, 101, "11.000000000000000000"); err != nil {
			return err
		}
		if err := assertRecord(t, ctx, platform, stream1, 103, "33.000000000000000000"); err != nil {
			return err
		}

		// Stream 2: (202 -> 22), (204 -> 44)
		stream2 := types.StreamLocator{StreamId: streamId2, DataProvider: deployer}
		if err := assertRecord(t, ctx, platform, stream2, 202, "22.000000000000000000"); err != nil {
			return err
		}
		if err := assertRecord(t, ctx, platform, stream2, 204, "44.000000000000000000"); err != nil {
			return err
		}

		return nil
	}
}

// assertRecord fetches a single event_time and checks the value.
func assertRecord(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, ts int64, expectedValue string) error {
	from := ts
	to := ts
	rows, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
		Platform:      platform,
		StreamLocator: locator,
		FromTime:      &from,
		ToTime:        &to,
		FrozenAt:      nil,
		Height:        0,
	})
	if err != nil {
		return errors.Wrap(err, "get_record")
	}

	expected := fmt.Sprintf(`
        | event_time | value  |
        | ---------- | ------ |
        | %s         | %s |
    `, strconv.FormatInt(ts, 10), expectedValue)
	table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{Actual: rows, Expected: expected})
	return nil
}
