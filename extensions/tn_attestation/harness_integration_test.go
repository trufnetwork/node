//go:build kwiltest

package tn_attestation

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	kcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	databasesize "github.com/trufnetwork/node/extensions/database-size"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	"github.com/trufnetwork/sdk-go/core/util"
)

func init() {
	// Register extension precompiles for tests (except tn_attestation which tests handle individually)
	err := precompiles.RegisterInitializer(tn_cache.ExtensionName, tn_cache.InitializeCachePrecompile)
	if err != nil {
		panic("failed to register tn_cache precompiles: " + err.Error())
	}

	err = precompiles.RegisterInitializer(databasesize.ExtensionName, databasesize.InitializeDatabaseSizePrecompile)
	if err != nil {
		panic("failed to register database_size precompiles: " + err.Error())
	}

	tn_utils.InitializeExtension()
	// Note: tn_attestation precompile is registered by individual tests via ensurePrecompileRegistered()
}

func TestSigningWorkflowWithHarness(t *testing.T) {
	// Integration test covering the complete production signing workflow:
	// request_attestation (SQL) → prepareSigningWork (Go) → submitSignature (Go)
	// → transaction marshaling → sign_attestation (SQL) with leader authorization.
	// Tests that real migrations work correctly with transaction encoding/decoding.
	const (
		testActionName = "harness_attestation_action"
		testActionID   = 21
		attestedValue  = int64(9001)
	)

	// Reset extension singletons before test to avoid conflicts
	orderedsync.ForTestingReset()
	erc20shim.ForTestingResetSingleton()
	erc20shim.ForTestingClearAllInstances(context.Background(), nil)

	// Ensure tn_attestation precompile is registered (needed for queue_for_signing in migrations).
	// Track whether we registered it so we can clean up afterwards and not interfere with
	// other tests that expect to perform the registration themselves.
	registered := precompiles.RegisteredPrecompiles()
	_, alreadyRegistered := registered[ExtensionName]
	ensurePrecompileRegistered(t)
	if !alreadyRegistered {
		defer delete(precompiles.RegisteredPrecompiles(), ExtensionName)
	}

	ownerAddr := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000a22")
	requesterAddrValue := util.Unsafe_NewEthereumAddressFromString("0xabc0000000000000000000000000000000000a22")
	requesterAddr := &requesterAddrValue

	options := &kwilTesting.Options{
		UseTestContainer: true,
		SetupMetaStore:   true,
	}

	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "tn_attestation_signing_harness",
		SeedScripts: migrations.GetSeedScriptPaths(),
		Owner:       ownerAddr.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform.Deployer = ownerAddr.Bytes()

				// Provision a lightweight action and allowlist entry so the request
				// mirrors production: the Go test intentionally exercises the exact SQL
				// path that nodes run when users hit the public API.
				require.NoError(t, setupTestAttestationAction(ctx, platform, testActionName, testActionID))

				// Request the attestation through the live migration. This ensures the
				// canonical payload we inspect later is produced by the SQL we ship.
				dataProvider := []byte("provider-harness")
				streamID := []byte("stream-harness")
				argsBytes, err := tn_utils.EncodeActionArgs([]any{attestedValue})
				require.NoError(t, err)

				engineCtx := newHarnessEngineContext(ctx, platform, requesterAddr)

				var requestTxID string
				var attestationHash []byte
				_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation", []any{
					dataProvider,
					streamID,
					testActionName,
					argsBytes,
					false,
					int64(0),
				}, func(row *common.Row) error {
					if len(row.Values) != 2 {
						return fmt.Errorf("expected 2 return values, got %d", len(row.Values))
					}
					txID, ok := row.Values[0].(string)
					if !ok {
						return fmt.Errorf("expected TEXT return for request_tx_id, got %T", row.Values[0])
					}
					requestTxID = txID
					hash, ok := row.Values[1].([]byte)
					if !ok {
						return fmt.Errorf("expected BYTEA return for attestation_hash, got %T", row.Values[1])
					}
					attestationHash = append([]byte(nil), hash...)
					return nil
				})
				require.NoError(t, err, "request_attestation failed")
				require.NotEmpty(t, requestTxID, "request_attestation should return request_tx_id")
				require.NotEmpty(t, attestationHash, "request_attestation should return attestation hash")

				// At this point we expect a single row inserted into the persisted
				// table. Fetch it back and validate every column so future changes that
				// alter canonical layout or metadata will trip this test.
				stored := fetchAttestationRowHarness(t, ctx, platform, attestationHash)
				require.Equal(t, requestTxID, stored.requestTxID, "request_tx_id should be captured")
				require.Equal(t, attestationHash, stored.attestationHash)
				require.Equal(t, requesterAddr.Bytes(), stored.requester)
				require.NotEmpty(t, stored.resultCanonical, "canonical payload should be stored")
				require.False(t, stored.encryptSig, "encrypt_sig must be false in MVP")
				require.Nil(t, stored.signature, "signature should be NULL before signing")
				require.Nil(t, stored.validatorPubKey, "validator_pubkey should be NULL before signing")
				require.Nil(t, stored.signedHeight, "signed_height should be NULL before signing")

				// The canonical blob should round-trip through the Go parser; we assert
				// the critical fields so the SQL encoder and Go decoder stay in lockstep.
				payload, err := ParseCanonicalPayload(stored.resultCanonical)
				require.NoError(t, err, "canonical payload should be parseable")
				require.Equal(t, uint8(1), payload.Version)
				require.Equal(t, uint8(1), payload.Algorithm)
				require.Equal(t, dataProvider, payload.DataProvider)
				require.Equal(t, streamID, payload.StreamID)
				require.Equal(t, uint16(testActionID), payload.ActionID)
				require.Equal(t, argsBytes, payload.Args)
				require.NotEmpty(t, payload.Result, "query result should be stored")

				// Finally ensure we can derive the digest that the signing service uses;
				// downstream tests rely on this helper, and this assertion guarantees the
				// canonical format remains stable.
				digest := payload.SigningDigest()
				require.Len(t, digest, 32, "digest should be 32 bytes (SHA-256)")

				// Phase 2: Prepare signing work - validator generates signature
				privateKey, _, err := kcrypto.GenerateSecp256k1Key(nil)
				require.NoError(t, err)

				ResetValidatorSignerForTesting()
				t.Cleanup(ResetValidatorSignerForTesting)
				require.NoError(t, InitializeValidatorSigner(privateKey))
				validatorSigner := GetValidatorSigner()
				require.NotNil(t, validatorSigner)

				nodeSigner := auth.GetNodeSigner(privateKey)
				require.NotNil(t, nodeSigner)
				pubKey, ok := nodeSigner.PubKey().(*kcrypto.Secp256k1PublicKey)
				require.True(t, ok, "unexpected validator pubkey type")

				// Setup extension with real dependencies
				service := &common.Service{
					Logger:        log.DiscardLogger,
					GenesisConfig: &config.GenesisConfig{ChainID: "attestation-harness"},
					LocalConfig:   &config.Config{},
				}

				ext := getExtension()
				ext.setService(service)
				ext.setApp(&common.App{
					Engine:   platform.Engine,
					DB:       platform.DB,
					Accounts: &signerAccountsStub{},
					Service:  service,
				})
				ext.setNodeSigner(nodeSigner)

				hashHex := hex.EncodeToString(attestationHash)
				prepared, err := ext.prepareSigningWork(ctx, hashHex)
				require.NoError(t, err)
				require.Len(t, prepared, 1, "expected one prepared signature")

				// Verify signature was generated correctly
				require.Equal(t, requestTxID, prepared[0].RequestTxID, "request_tx_id should be in prepared signature")
				require.Equal(t, hashHex, prepared[0].HashHex)
				require.Equal(t, attestationHash, prepared[0].Hash)
				require.Equal(t, requesterAddr.Bytes(), prepared[0].Requester)
				require.Len(t, prepared[0].Signature, 65, "EVM signature is 65 bytes")
				require.Equal(t, stored.createdHeight, prepared[0].CreatedHeight)

				// Phase 3: Submit signature via production flow (tests transaction marshaling)
				const signHeight = int64(42)

				// Create test broadcaster that unmarshals transaction and executes sign_attestation
				broadcaster := &harnessExecutingBroadcaster{
					t:          t,
					platform:   platform,
					pubKey:     pubKey,
					nodeSigner: nodeSigner,
					signHeight: signHeight,
				}
				ext.setBroadcaster(broadcaster)

				// Use production submitSignature - this marshals the transaction,
				// the broadcaster unmarshals it, and executes the real SQL action
				err = ext.submitSignature(ctx, prepared[0])
				require.NoError(t, err, "submitSignature should succeed")

				// Verify the broadcaster was called and executed successfully
				require.Equal(t, 1, broadcaster.calls, "should broadcast exactly once")

				// Verify signed state in database
				signedRow := fetchAttestationRowHarness(t, ctx, platform, attestationHash)
				require.NotNil(t, signedRow.signature, "signature should be recorded")
				require.Equal(t, prepared[0].Signature, signedRow.signature)
				require.NotNil(t, signedRow.validatorPubKey, "validator pubkey should be recorded")
				require.Equal(t, nodeSigner.CompactID(), signedRow.validatorPubKey, "validator pubkey should match node signer identity")
				require.NotNil(t, signedRow.signedHeight, "signed height should be recorded")
				require.Equal(t, signHeight, *signedRow.signedHeight)

				return nil
			},
		},
	}, options)
}

// setupTestAttestationAction creates a test action and registers it in the attestation allowlist
func setupTestAttestationAction(ctx context.Context, platform *kwilTesting.Platform, actionName string, actionID int) error {
	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:    ctx,
			Signer: platform.Deployer,
			Caller: string(platform.Deployer),
			TxID:   platform.Txid(),
			BlockContext: &common.BlockContext{
				Height: 1,
			},
		},
		OverrideAuthz: true,
	}

	createAction := `
CREATE OR REPLACE ACTION ` + actionName + `(
	$value INT8
) PUBLIC VIEW RETURNS TABLE(result INT8) {
	RETURN NEXT $value;
};`

	if err := platform.Engine.Execute(engineCtx, platform.DB, createAction, nil, nil); err != nil {
		return fmt.Errorf("create action: %w", err)
	}

	insertAllowlist := `
INSERT INTO attestation_actions(action_name, action_id)
VALUES ($action_name, $action_id)
ON CONFLICT (action_name) DO UPDATE SET action_id = EXCLUDED.action_id;`

	params := map[string]any{
		"action_name": actionName,
		"action_id":   actionID,
	}

	if err := platform.Engine.Execute(engineCtx, platform.DB, insertAllowlist, params, nil); err != nil {
		return fmt.Errorf("insert attestation action allowlist: %w", err)
	}

	return nil
}

func newHarnessEngineContext(ctx context.Context, platform *kwilTesting.Platform, requester *util.EthereumAddress) *common.EngineContext {
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:    ctx,
			Signer: requester.Bytes(),
			Caller: requester.Address(),
			TxID:   platform.Txid(),
			BlockContext: &common.BlockContext{
				Height: 1,
			},
		},
	}
}

func fetchAttestationRowHarness(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, hash []byte) harnessAttestationRow {
	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:    ctx,
			Signer: platform.Deployer,
			Caller: string(platform.Deployer),
			TxID:   platform.Txid(),
			BlockContext: &common.BlockContext{
				Height: 1,
			},
		},
		OverrideAuthz: true,
	}

	var rowData harnessAttestationRow
	err := platform.Engine.Execute(engineCtx, platform.DB, `
SELECT request_tx_id, requester, attestation_hash, result_canonical, encrypt_sig, signature, validator_pubkey, signed_height, created_height
FROM attestations
WHERE attestation_hash = $hash;
`, map[string]any{"hash": hash}, func(row *common.Row) error {
		rowData.requestTxID = row.Values[0].(string)
		rowData.requester = append([]byte(nil), row.Values[1].([]byte)...)
		rowData.attestationHash = append([]byte(nil), row.Values[2].([]byte)...)
		rowData.resultCanonical = append([]byte(nil), row.Values[3].([]byte)...)
		rowData.encryptSig = row.Values[4].(bool)
		if row.Values[5] != nil {
			rowData.signature = append([]byte(nil), row.Values[5].([]byte)...)
		}
		if row.Values[6] != nil {
			rowData.validatorPubKey = append([]byte(nil), row.Values[6].([]byte)...)
		}
		if row.Values[7] != nil {
			height := row.Values[7].(int64)
			rowData.signedHeight = &height
		}
		rowData.createdHeight = row.Values[8].(int64)
		return nil
	})
	require.NoError(t, err)
	return rowData
}

type harnessAttestationRow struct {
	requestTxID     string
	requester       []byte
	attestationHash []byte
	resultCanonical []byte
	encryptSig      bool
	signature       []byte
	validatorPubKey []byte
	signedHeight    *int64
	createdHeight   int64
}

type harnessExecutingBroadcaster struct {
	t          *testing.T
	platform   *kwilTesting.Platform
	pubKey     *kcrypto.Secp256k1PublicKey
	nodeSigner auth.Signer
	signHeight int64
	calls      int
}

func (b *harnessExecutingBroadcaster) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	b.calls++

	// Parse transaction payload
	payload := new(ktypes.ActionExecution)
	if err := payload.UnmarshalBinary(tx.Body.Payload); err != nil {
		return ktypes.Hash{}, nil, err
	}

	require.Equal(b.t, "sign_attestation", payload.Action)
	require.Len(b.t, payload.Arguments, 1)
	require.Len(b.t, payload.Arguments[0], 2)

	// Decode arguments
	requestTxID := b.decodeStringArg(payload.Arguments[0][0])
	sigBytes := b.decodeByteArg(payload.Arguments[0][1])

	// Get caller identifier for leader check
	// For leader authorization to work, Signer must be the Ethereum address derived from the proposer's public key
	signer := b.nodeSigner.CompactID()
	caller, err := auth.GetNodeIdentifier(b.pubKey)
	require.NoError(b.t, err)

	// Create engine context with leader as proposer
	txCtx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   b.signHeight,
			Proposer: b.pubKey,
		},
		Signer:        signer,
		Caller:        caller,
		TxID:          b.platform.Txid(),
		Authenticator: auth.Secp256k1Auth,
	}

	// Execute real sign_attestation action from migrations
	res, err := b.platform.Engine.Call(
		&common.EngineContext{TxContext: txCtx},
		b.platform.DB,
		"",
		"sign_attestation",
		[]any{requestTxID, sigBytes},
		func(*common.Row) error { return nil },
	)

	require.NoError(b.t, err)
	require.NotNil(b.t, res)
	if res.Error != nil {
		b.t.Fatalf("sign_attestation failed: %v", res.Error)
	}

	return ktypes.Hash{}, &ktypes.TxResult{Code: uint32(ktypes.CodeOk)}, nil
}

func (b *harnessExecutingBroadcaster) decodeByteArg(arg *ktypes.EncodedValue) []byte {
	val, err := arg.Decode()
	require.NoError(b.t, err)
	switch typed := val.(type) {
	case []byte:
		return typed
	case *[]byte:
		require.NotNil(b.t, typed)
		return *typed
	default:
		b.t.Fatalf("unexpected byte arg type %T", val)
		return nil
	}
}

func (b *harnessExecutingBroadcaster) decodeStringArg(arg *ktypes.EncodedValue) string {
	val, err := arg.Decode()
	require.NoError(b.t, err)
	switch typed := val.(type) {
	case string:
		return typed
	case *string:
		require.NotNil(b.t, typed)
		return *typed
	default:
		b.t.Fatalf("unexpected string arg type %T", val)
		return ""
	}
}

func (b *harnessExecutingBroadcaster) decodeInt64Arg(arg *ktypes.EncodedValue) int64 {
	val, err := arg.Decode()
	require.NoError(b.t, err)
	switch typed := val.(type) {
	case int64:
		return typed
	case *int64:
		require.NotNil(b.t, typed)
		return *typed
	default:
		b.t.Fatalf("unexpected int64 arg type %T", val)
		return 0
	}
}
