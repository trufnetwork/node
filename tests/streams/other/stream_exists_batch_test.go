package tests

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// STREAM_EXISTS_BATCH01: Directly test stream_exists_batch action with
// a mix of existing and non-existing streams ensuring both true and false
// paths are covered. This gives clear coverage of core existence logic
// without going through higher-level helpers like filter_streams_by_existence.
func TestSTREAM_EXISTS_BATCH01_Direct(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "stream_exists_batch_direct_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testStreamExistsBatchDirect(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testStreamExistsBatchDirect(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a deterministic deployer address so we know the data_provider value
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		err := setup.CreateDataProvider(ctx, platform, deployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Create one real stream and keep another id unused to test the false path.
		existingStreamID := util.GenerateStreamId("existing_stream")
		nonExistingStreamID := util.GenerateStreamId("nonexistent_stream")

		// Deploy the existing primitive stream
		if err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     existingStreamID,
				DataProvider: deployer,
			},
			Type: setup.ContractTypePrimitive,
		}); err != nil {
			return errors.Wrap(err, "failed to create existing stream")
		}

		// Prepare input arrays for stream_exists_batch
		dataProviders := []string{deployer.Address(), deployer.Address()}
		streamIDs := []string{existingStreamID.String(), nonExistingStreamID.String()}

		// Call the action directly through the kwil engine.
		txCtx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 1,
			},
			TxID:   platform.Txid(),
			Signer: platform.Deployer,
			Caller: deployer.Address(),
		}
		engCtx := &common.EngineContext{TxContext: txCtx}

		type result struct {
			DataProvider string
			StreamID     string
			Exists       bool
		}
		var results []result

		callResult, err := platform.Engine.Call(engCtx, platform.DB, "", "stream_exists_batch", []any{dataProviders, streamIDs}, func(row *common.Row) error {
			if len(row.Values) != 3 {
				return errors.Errorf("expected 3 columns, got %d", len(row.Values))
			}
			dp, _ := row.Values[0].(string)
			sid, _ := row.Values[1].(string)
			exists, _ := row.Values[2].(bool)
			results = append(results, result{dp, sid, exists})
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "engine call failed")
		}
		if callResult.Error != nil {
			return errors.Wrap(callResult.Error, "stream_exists_batch execution failed")
		}

		// Expect two rows matching the input order
		assert.Equal(t, 2, len(results), "expected exactly 2 rows returned")
		if len(results) != 2 {
			return errors.Errorf("unexpected result length: %d", len(results))
		}

		// Validate first (existing) entry
		assert.True(t, results[0].Exists, "existing stream should return true")
		assert.Equal(t, existingStreamID.String(), results[0].StreamID)
		// Validate second (non-existing) entry
		assert.False(t, results[1].Exists, "non-existing stream should return false")
		assert.Equal(t, nonExistingStreamID.String(), results[1].StreamID)

		return nil
	}
}
