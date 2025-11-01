package tests

import (
	"context"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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

// Minimal, high-ROI test that verifies pending_prune_days is populated on insert
// and remains idempotent for multiple inserts within the same day.
func TestPendingPruneDaysEnqueue(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "pending_prune_days_enqueue_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testPendingPruneDays(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// Same minimal assertions for the truflation batch path (truflation_insert_records)
func TestPendingPruneDaysEnqueue_Truflation(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "pending_prune_days_enqueue_truflation_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testPendingPruneDaysTruflation(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testPendingPruneDays(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		return runPendingPruneScenario(t, ctx, platform, false)
	}
}

func testPendingPruneDaysTruflation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		return runPendingPruneScenario(t, ctx, platform, true)
	}
}

// runPendingPruneScenario executes the enqueue/idempotency assertions for either primitive or truflation insert paths.
func runPendingPruneScenario(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, isTruflation bool) error {
	deployerHex := "0x0000000000000000000000000000000000000eed"
	streamName := "pending_prune_days_stream"
	if isTruflation {
		deployerHex = "0x0000000000000000000000000000000000000eef"
		streamName = "truflation_pending_prune_stream"
	}
	deployer := util.Unsafe_NewEthereumAddressFromString(deployerHex)
	platform = procedure.WithSigner(platform, deployer.Bytes())

	require.NoError(t, setup.CreateDataProvider(ctx, platform, deployer.Address()))

	streamId := util.GenerateStreamId(streamName)
	locator := types.StreamLocator{StreamId: streamId, DataProvider: deployer}
	require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{Type: setup.ContractTypePrimitive, Locator: locator}))

	// 1) Insert two records in the same day (day_index = 0)
	if isTruflation {
		require.NoError(t, setup.InsertTruflationDataBatch(ctx, setup.InsertTruflationDataInput{
			Platform: platform,
			PrimitiveStream: setup.TruflationStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{StreamLocator: locator},
				Data:                      []setup.InsertTruflationRecordInput{{EventTime: 100, Value: 1, TruflationCreatedAt: "2024-01-01T00:00:00Z"}, {EventTime: 200, Value: 2, TruflationCreatedAt: "2024-01-01T01:00:00Z"}},
			},
			Height: 1,
		}))
	} else {
		require.NoError(t, setup.InsertPrimitiveDataBatch(ctx, setup.InsertPrimitiveDataInput{
			Platform: platform,
			PrimitiveStream: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{StreamLocator: locator},
				Data:                      []setup.InsertRecordInput{{EventTime: 100, Value: 1}, {EventTime: 200, Value: 2}},
			},
			Height: 1,
		}))
	}

	// Expect exactly one entry for day_index 0
	dayIdxs, err := getPendingDays(ctx, platform, deployer.Address(), streamId.String())
	if err != nil {
		return err
	}
	require.Equal(t, []int{0}, dayIdxs)

	// 2) Insert a record in a different day (day_index = 1)
	if isTruflation {
		require.NoError(t, setup.InsertTruflationDataBatch(ctx, setup.InsertTruflationDataInput{
			Platform: platform,
			PrimitiveStream: setup.TruflationStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{StreamLocator: locator},
				Data:                      []setup.InsertTruflationRecordInput{{EventTime: 90000, Value: 3, TruflationCreatedAt: "2024-01-02T00:00:00Z"}},
			},
			Height: 2,
		}))
	} else {
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator, setup.InsertRecordInput{EventTime: 90000, Value: 3}, 2))
	}
	dayIdxs, err = getPendingDays(ctx, platform, deployer.Address(), streamId.String())
	if err != nil {
		return err
	}
	require.Equal(t, []int{0, 1}, dayIdxs)

	// 3) Idempotency: insert another record in day 0; queue should remain {0,1}
	if isTruflation {
		require.NoError(t, setup.InsertTruflationDataBatch(ctx, setup.InsertTruflationDataInput{
			Platform: platform,
			PrimitiveStream: setup.TruflationStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{StreamLocator: locator},
				Data:                      []setup.InsertTruflationRecordInput{{EventTime: 300, Value: 4, TruflationCreatedAt: "2024-01-01T02:00:00Z"}},
			},
			Height: 3,
		}))
	} else {
		require.NoError(t, setup.ExecuteInsertRecord(ctx, platform, locator, setup.InsertRecordInput{EventTime: 300, Value: 4}, 3))
	}
	dayIdxs, err = getPendingDays(ctx, platform, deployer.Address(), streamId.String())
	if err != nil {
		return err
	}
	assert.Equal(t, []int{0, 1}, dayIdxs)

	return nil
}

func getPendingDays(ctx context.Context, platform *kwilTesting.Platform, provider string, streamId string) ([]int, error) {
	rows := []common.Row{}
	err := platform.Engine.Execute(&common.EngineContext{TxContext: &common.TxContext{Ctx: ctx}}, platform.DB,
		"SELECT ppd.day_index FROM pending_prune_days ppd "+
			"JOIN streams s ON s.id = ppd.stream_ref "+
			"JOIN data_providers dp ON dp.id = s.data_provider_id "+
			"WHERE dp.address = $address AND s.stream_id = $sid "+
			"ORDER BY ppd.day_index",
		map[string]any{"address": provider, "sid": streamId},
		func(row *common.Row) error {
			rows = append(rows, *row)
			return nil
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "query pending_prune_days")
	}

	result := []int{}
	for _, r := range rows {
		// Convert to int via string to be type-agnostic
		s := "" + strconv.FormatInt(int64(toInt(r.Values[0])), 10)
		v, _ := strconv.Atoi(s)
		result = append(result, v)
	}
	return result, nil
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case uint:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	case string:
		if n, err := strconv.Atoi(x); err == nil {
			return n
		}
	}
	// Fallback to 0 if unexpected type; tests will fail on mismatch
	return 0
}
