package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	extauth "github.com/trufnetwork/kwil-db/extensions/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// TestSchedulerTrimActionsLeaderAuthorization verifies that trim_order_events and
// trim_transaction_events are leader-gated (migration 054): only the current block
// leader — the tn_digest scheduler, which signs with the node's leader key — can
// run them. Before 054 both were declared PUBLIC owner, so the scheduler's calls
// were rejected ("action is owner-only") and the trims never ran on mainnet.
func TestSchedulerTrimActionsLeaderAuthorization(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "scheduler_trim_leader_authorization",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			WithSignerAndProvider(func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Create a secp256k1 leader key; the block leader (@leader_sender)
				// is BlockContext.Proposer, and the passing signer is that key's
				// eth address.
				_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
				if err != nil {
					return errors.Wrap(err, "generate secp256k1 key")
				}
				pub, ok := pubGeneric.(*crypto.Secp256k1PublicKey)
				if !ok {
					return errors.New("unexpected pubkey type")
				}

				callWithCtx := func(action string, args []any, signer []byte, authenticator string) (*common.CallResult, error) {
					caller := ""
					if ident, e := extauth.GetIdentifier(authenticator, signer); e == nil {
						caller = ident
					}
					tx := &common.TxContext{
						Ctx:           ctx,
						BlockContext:  &common.BlockContext{Height: 1, Proposer: pub},
						Signer:        signer,
						Caller:        caller,
						TxID:          platform.Txid(),
						Authenticator: authenticator,
					}
					eng := &common.EngineContext{TxContext: tx}
					return platform.Engine.Call(eng, platform.DB, "", action, args, func(*common.Row) error { return nil })
				}

				// (preserve_blocks INT8, delete_cap INT). At height 1 the cutoff is
				// negative, so a call that passes the gate short-circuits cleanly
				// (deleted=0) without needing any seeded events.
				trimArgs := []any{int64(100), int64(10)}
				trimActions := []string{"trim_order_events", "trim_transaction_events"}

				// Non-leader: signer != leader_sender → leader-only error.
				for _, action := range trimActions {
					r, err := callWithCtx(action, trimArgs, platform.Deployer, coreauth.EthPersonalSignAuth)
					if err != nil {
						return errors.Wrapf(err, "%s non-leader call error", action)
					}
					if r == nil || r.Error == nil || !strings.Contains(r.Error.Error(), "Only the current block leader") {
						return errors.Errorf("expected leader-only error for %s when not leader", action)
					}
				}

				// Leader: signer equals the derived leader_sender → gate passes.
				signerGood := crypto.EthereumAddressFromPubKey(pub)
				for _, action := range trimActions {
					r, err := callWithCtx(action, trimArgs, signerGood, coreauth.EthPersonalSignAuth)
					if err != nil {
						return errors.Wrapf(err, "%s leader call error", action)
					}
					if r != nil && r.Error != nil {
						return errors.Wrapf(r.Error, "%s leader call failed", action)
					}
				}

				return nil
			}),
		},
	}, testutils.GetTestOptionsWithCache())
}
