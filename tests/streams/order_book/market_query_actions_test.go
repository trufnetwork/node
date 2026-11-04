//go:build kwiltest

package order_book

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_attestation"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestMarketQueryActions covers which query actions a market can be created on,
// and that a market on a query action settles whatever its use_cache flag says.
func TestMarketQueryActions(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_MarketQueryActions",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testSettlesWhateverUseCacheSays(t, "get_last_record", int64(3500), true,
				"0x5151515151515151515151515151515151515151", "stusecachelast000000000000000000"),
			testSettlesWhateverUseCacheSays(t, "get_first_record", int64(500), true,
				"0x5252525252525252525252525252525252525252", "stusecachefirst00000000000000000"),
			testSettlesWhateverUseCacheSays(t, "get_last_record", int64(3500), nil,
				"0x5454545454545454545454545454545454545454", "stusecachenull000000000000000000"),
			testCreateMarketRefusesActionOffAllowlist(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testSettlesWhateverUseCacheSays creates a market whose query sets use_cache to
// something other than false and checks it settles. request_attestation runs the
// query with use_cache off, so the capture has to be found under the hash of the
// query as the market stored it.
//
// get_last_record and get_first_record share a signature: (data_provider,
// stream_id, before|after, frozen_at, use_cache).
func testSettlesWhateverUseCacheSays(t *testing.T, action string, bound int64, useCache any, deployerHex, streamID string) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString(deployerHex)
		platform.Deployer = deployer.Bytes()
		dataProvider := deployer.Address()

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		require.NoError(t, giveBalance(ctx, platform, dataProvider, "500000000000000000000"))
		require.NoError(t, setup.CreateDataProvider(ctx, platform, dataProvider))

		engineCtx := helper.NewEngineContext()
		_, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"}, nil)
		require.NoError(t, err)

		value, err := kwilTypes.ParseDecimalExplicit("1.000000000000000000", 36, 18)
		require.NoError(t, err)
		_, err = platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{1000},
				[]*kwilTypes.Decimal{value},
			}, nil)
		require.NoError(t, err)

		argsBytes, err := tn_utils.EncodeActionArgs([]any{dataProvider, streamID, bound, nil, useCache})
		require.NoError(t, err)
		queryComponents, err := encodeQueryComponentsForTests(dataProvider, streamID, action, argsBytes)
		require.NoError(t, err)

		var queryID int
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, int64(100), int64(5), int64(1)},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		require.Nil(t, res.Error, "create_market")

		var marketHash []byte
		err = platform.Engine.Execute(helper.NewEngineContext(), platform.DB,
			`SELECT hash FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				marketHash = append([]byte(nil), row.Values[0].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		require.Len(t, marketHash, 32)

		// The settlement scheduler requests the capture with the market's own
		// arguments, use_cache included.
		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = captureTime
		res, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, action, argsBytes, false, nil},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		require.Nil(t, res.Error, "request_attestation")
		require.Equal(t, marketHash, attestationHash,
			"the capture must be stored under the market's hash")

		// The payload carries the arguments as requested, so the leader's check
		// that the stored hash matches the payload still holds.
		var canonical []byte
		err = platform.Engine.Execute(helper.NewEngineContext(), platform.DB,
			`SELECT result_canonical FROM attestations WHERE request_tx_id = $txid`,
			map[string]any{"txid": requestTxID},
			func(row *common.Row) error {
				canonical = append([]byte(nil), row.Values[0].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		payload, err := tn_attestation.ParseCanonicalPayload(canonical)
		require.NoError(t, err)
		require.Equal(t, argsBytes, payload.Args)
		require.NotEmpty(t, payload.Result)

		helper.SignAttestation(requestTxID)

		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		res, err = platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID}, nil)
		require.NoError(t, err)
		require.Nil(t, res.Error, "settle_market")

		var settled bool
		var winningOutcome *bool
		err = platform.Engine.Execute(helper.NewEngineContext(), platform.DB,
			`SELECT settled, winning_outcome FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				settled = row.Values[0].(bool)
				if row.Values[1] != nil {
					outcome := row.Values[1].(bool)
					winningOutcome = &outcome
				}
				return nil
			})
		require.NoError(t, err)
		require.True(t, settled)
		require.NotNil(t, winningOutcome)
		require.True(t, *winningOutcome, "the stream's only value is 1.0")

		return nil
	}
}

// testCreateMarketRefusesActionOffAllowlist checks that get_change_over_time,
// which no migration defines, leaves the attestation allowlist when the
// attestation schema migration runs again on a network that still lists it, and
// that create_market refuses a market on it, since nothing could ever capture it.
func testCreateMarketRefusesActionOffAllowlist(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		deployer := util.Unsafe_NewEthereumAddressFromString("0x5353535353535353535353535353535353535353")
		platform.Deployer = deployer.Bytes()
		dataProvider := deployer.Address()
		streamID := "stoffallowlist000000000000000000"

		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)
		require.NoError(t, giveBalance(ctx, platform, dataProvider, "500000000000000000000"))

		// A network seeded before the row left the seed list still has it.
		ownerCtx := helper.NewEngineContext()
		ownerCtx.OverrideAuthz = true
		err := platform.Engine.Execute(ownerCtx, platform.DB,
			`INSERT INTO attestation_actions (action_name, action_id) VALUES ('get_change_over_time', 3)`,
			nil, nil)
		require.NoError(t, err)

		schema := ""
		for _, path := range migrations.GetSeedScriptPaths() {
			if filepath.Base(path) == "023-attestation-schema.sql" {
				content, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				schema = string(content)
			}
		}
		require.NotEmpty(t, schema, "023-attestation-schema.sql not found")

		ownerCtx = helper.NewEngineContext()
		ownerCtx.OverrideAuthz = true
		require.NoError(t, platform.Engine.Execute(ownerCtx, platform.DB, schema, nil, nil))

		listed := 0
		err = platform.Engine.Execute(helper.NewEngineContext(), platform.DB,
			`SELECT action_name FROM attestation_actions WHERE action_id = 3 OR action_name = 'get_change_over_time'`,
			nil,
			func(row *common.Row) error {
				listed++
				return nil
			})
		require.NoError(t, err)
		require.Zero(t, listed, "get_change_over_time must not be on the allowlist")

		argsBytes, err := tn_utils.EncodeActionArgs([]any{dataProvider, streamID, int64(0), int64(1000), nil, nil, false})
		require.NoError(t, err)
		queryComponents, err := encodeQueryComponentsForTests(dataProvider, streamID, "get_change_over_time", argsBytes)
		require.NoError(t, err)

		engineCtx := helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, int64(100), int64(5), int64(1)}, nil)
		require.NoError(t, err)
		require.NotNil(t, res.Error, "create_market must refuse an action off the allowlist")
		require.Contains(t, res.Error.Error(), "not allowed for attestation")

		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = captureTime
		res, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{dataProvider, streamID, "get_change_over_time", argsBytes, false, nil}, nil)
		require.NoError(t, err)
		require.NotNil(t, res.Error)
		require.Contains(t, res.Error.Error(), "Action not allowed for attestation: get_change_over_time")

		return nil
	}
}
