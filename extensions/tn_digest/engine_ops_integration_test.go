package tn_digest

import (
	"context"
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
	digestembed "github.com/trufnetwork/node/tests/extensions/digest"
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
				ops := internal.NewEngineOperations(platform.Engine, platform.DB, accts, log.New())

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
