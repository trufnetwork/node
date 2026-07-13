//go:build kwiltest

package tn_digest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/accounts"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
	"github.com/trufnetwork/node/internal/migrations"
	digestembed "github.com/trufnetwork/node/tests/extensions/digest"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// TestBuildAndBroadcastAutoDigestTx_VerifiesTxBuildSignAndDBEffect
// High-ROI integration test using in-process engine+DB (kwilTesting):
// - seeds only minimal digest test schema/actions
// - builds and signs an ActionExecution tx for auto_digest
// - verifies payload, executes action via engine in broadcaster
// - asserts a row is inserted into test_digest_action
func TestBuildAndBroadcastAutoDigestTx_VerifiesTxBuildSignAndDBEffect(t *testing.T) {
	// Load embedded test migration SQL
	bts, err := digestembed.TestMigrationSQL.ReadFile("test_migration.sql")
	require.NoError(t, err)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "tn_digest_tx_build_broadcast_test",
		SeedStatements: []string{string(bts)},
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Create accounts service
				accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
				require.NoError(t, err)

				// Prepare EngineOperations
				ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

				// Generate a node signer (secp256k1)
				priv, _, err := crypto.GenerateSecp256k1Key(nil)
				require.NoError(t, err)
				signer := auth.GetNodeSigner(priv)
				require.NotNil(t, signer)

				// Broadcaster stub: verify tx contents and then execute action with a valid EngineContext
				broadcaster := func(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
					payload, err := types.UnmarshalPayload(tx.Body.PayloadType, tx.Body.Payload)
					require.NoError(t, err)
					ae, ok := payload.(*types.ActionExecution)
					require.True(t, ok, "payload should be ActionExecution")
					require.Equal(t, "main", ae.Namespace)
					require.Equal(t, "auto_digest", ae.Action)

					// Execute the auto_digest action (simulate commit) with a valid EngineContext and override authz
					txCtx := &common.TxContext{
						Ctx:          ctx,
						BlockContext: &common.BlockContext{Height: 1},
						Signer:       signer.CompactID(),
						Caller:       "node",
						TxID:         "test-tx",
					}
					engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}
					_, execErr := platform.Engine.Call(engCtx, platform.DB, "main", "auto_digest", []any{}, func(_ *common.Row) error { return nil })
					require.NoError(t, execErr)

					return types.Hash{}, &types.TxResult{Code: uint32(types.CodeOk), Log: "auto_digest:{\"processed_days\":1,\"total_deleted_rows\":0,\"has_more_to_delete\":false}"}, nil
				}

				// Build and broadcast via ops
				ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				err = ops.BuildAndBroadcastAutoDigestTx(ctx2, "tn-test", signer, broadcaster)
				require.NoError(t, err)

				// Verify side-effect: a row exists
				var found bool
				queryErr := platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
					`SELECT id, caller FROM test_digest_action ORDER BY id DESC LIMIT 1`, nil,
					func(row *common.Row) error { found = true; return nil })
				require.NoError(t, queryErr)
				require.True(t, found, "expected a test_digest_action row")

				return nil
			},
		},
	}, nil)
}

// TestBroadcastTrimTransactionEventsTx_VerifiesTxBuildSignAndDBEffect
// Mirrors the auto_digest broadcast test for the retention path:
//   - seeds the real schema and one old high-volume write-fee row (method 2)
//   - builds and signs an ActionExecution tx for trim_transaction_events via the engine-op
//   - the broadcaster stub verifies the payload, executes the action above the
//     cutoff (authz overridden), and returns a NOTICE the parser understands
//   - asserts the parsed result and that the old row was pruned from the DB
func TestBroadcastTrimTransactionEventsTx_VerifiesTxBuildSignAndDBEffect(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "tn_digest_trim_tx_events_broadcast_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				accts, err := accounts.InitializeAccountStore(ctx, platform.DB, log.New())
				require.NoError(t, err)

				ops := internal.NewEngineOperations(platform.Engine, platform.DB, nil, accts, log.New())

				priv, _, err := crypto.GenerateSecp256k1Key(nil)
				require.NoError(t, err)
				signer := auth.GetNodeSigner(priv)
				require.NotNil(t, signer)

				// Seed one old high-volume write-fee row (method 2) to be pruned.
				oldTxID := fmt.Sprintf("0x%064x", 0xAA)
				_, err = platform.DB.Execute(ctx,
					`INSERT INTO main.transaction_events (tx_id, block_height, method_id, caller, fee_amount, fee_recipient, metadata)
					 VALUES ($1, $2, $3, $4, $5::NUMERIC(78, 0), $6, NULL)`,
					oldTxID, int64(100), int64(2), "0x9999999999999999999999999999999999999999",
					"1000000000000000000", "0x1111111111111111111111111111111111111111")
				require.NoError(t, err)

				const preserveBlocks = int64(172_800)
				const deleteCap = 100

				// Broadcaster stub: verify tx contents, then execute the real action
				// at a height above the cutoff (authz overridden, since the node
				// signer is not the namespace owner), returning a parseable NOTICE.
				broadcaster := func(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
					payload, err := types.UnmarshalPayload(tx.Body.PayloadType, tx.Body.Payload)
					require.NoError(t, err)
					ae, ok := payload.(*types.ActionExecution)
					require.True(t, ok, "payload should be ActionExecution")
					require.Equal(t, "main", ae.Namespace)
					require.Equal(t, "trim_transaction_events", ae.Action)
					require.Len(t, ae.Arguments, 1)
					require.Len(t, ae.Arguments[0], 2, "trim takes preserve_blocks + delete_cap")

					txCtx := &common.TxContext{
						Ctx:          ctx,
						BlockContext: &common.BlockContext{Height: 200_000}, // cutoff = 200000 - 172800 = 27200
						Signer:       signer.CompactID(),
						Caller:       "node",
						TxID:         "test-tx",
					}
					engCtx := &common.EngineContext{TxContext: txCtx, OverrideAuthz: true}
					_, execErr := platform.Engine.Call(engCtx, platform.DB, "main", "trim_transaction_events",
						[]any{preserveBlocks, int64(deleteCap)}, func(_ *common.Row) error { return nil })
					require.NoError(t, execErr)

					return types.Hash{}, &types.TxResult{
						Code: uint32(types.CodeOk),
						Log:  "trim_transaction_events: deleted=1 remaining=0 has_more=false",
					}, nil
				}

				ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				result, err := ops.BroadcastTrimTransactionEventsWithRetry(ctx2, "tn-test", signer, broadcaster, preserveBlocks, deleteCap, 3)
				require.NoError(t, err)
				require.Equal(t, 1, result.Deleted)
				require.False(t, result.HasMore)

				// Verify DB side-effect: the old write-fee row is gone.
				res, qErr := platform.DB.Execute(ctx, `SELECT 1 FROM main.transaction_events WHERE tx_id = $1`, oldTxID)
				require.NoError(t, qErr)
				require.Empty(t, res.Rows, "old write-fee row should have been pruned")

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}
