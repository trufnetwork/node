//go:build kwiltest

package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestRequestAttestationInsertsCanonicalPayload(t *testing.T) {
	const (
		testActionName = "test_attestation_action"
		testActionID   = 10
	)
	ownerAddr := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000a11")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "ATTESTATION01_RequestInsertion",
		SeedScripts: migrations.GetSeedScriptPaths(),
		Owner:       ownerAddr.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform.Deployer = ownerAddr.Bytes()

				require.NoError(t, setupTestAttestationAction(ctx, platform, testActionName, testActionID))
				runAttestationHappyPath(t, ctx, platform, testActionName, testActionID)
				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}

func setupTestAttestationAction(ctx context.Context, platform *kwilTesting.Platform, actionName string, actionID int) error {
	engineCtx, err := newEngineContext(ctx, platform)
	if err != nil {
		return err
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

	engineCtx, err = newEngineContext(ctx, platform)
	if err != nil {
		return err
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

func runAttestationHappyPath(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, actionName string, actionID int) {
	const attestedValue int64 = 42

	dataProvider := []byte("provider-001")
	streamID := []byte("stream-abc")

	argsBytes, err := tn_utils.EncodeActionArgs([]any{attestedValue})
	require.NoError(t, err, "encode action args")

	engineCtx, err := newEngineContext(ctx, platform)
	require.NoError(t, err)

	var attestationHash []byte
	_, err = platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation", []any{
		dataProvider,
		streamID,
		actionName,
		argsBytes,
		false,
		int64(0),
	}, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return fmt.Errorf("expected single return value, got %d", len(row.Values))
		}
		hash, ok := row.Values[0].([]byte)
		if !ok {
			return fmt.Errorf("expected BYTEA return, got %T", row.Values[0])
		}
		attestationHash = append([]byte(nil), hash...)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, attestationHash, "request_attestation should return attestation hash")

	stored := fetchAttestationRow(t, ctx, platform, attestationHash)

	// Rebuild expected canonical payload
	queryResult, err := tn_utils.EncodeQueryResultCanonical([]*common.Row{
		{Values: []any{attestedValue}},
	})
	require.NoError(t, err)

	expectedCanonical := buildExpectedCanonicalPayload(
		stored.createdHeight,
		dataProvider,
		streamID,
		actionID,
		argsBytes,
		queryResult,
	)

	require.Equal(t, expectedCanonical, stored.resultCanonical, "canonical payload mismatch")
	require.False(t, stored.encryptSig, "encrypt_sig must remain false in MVP")
	require.Nil(t, stored.signature, "signature must be NULL before signing")
	require.Nil(t, stored.validatorPubKey, "validator_pubkey must be NULL before signing")
	require.Nil(t, stored.signedHeight, "signed_height must be NULL before signing")
	require.Equal(t, attestationHash, stored.attestationHash, "returned hash should equal stored hash")
}

type attestationRow struct {
	attestationHash []byte
	resultCanonical []byte
	encryptSig      bool
	signature       []byte
	validatorPubKey []byte
	signedHeight    *int64
	createdHeight   int64
}

func fetchAttestationRow(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, hash []byte) attestationRow {
	engineCtx, err := newEngineContext(ctx, platform)
	require.NoError(t, err)

	var rowData attestationRow
	err = platform.Engine.Execute(engineCtx, platform.DB, `
SELECT attestation_hash, result_canonical, encrypt_sig, signature, validator_pubkey, signed_height, created_height
FROM attestations
WHERE attestation_hash = $hash;
`, map[string]any{"hash": hash}, func(row *common.Row) error {
		rowData.attestationHash = append([]byte(nil), row.Values[0].([]byte)...)
		rowData.resultCanonical = append([]byte(nil), row.Values[1].([]byte)...)
		rowData.encryptSig = row.Values[2].(bool)
		if row.Values[3] != nil {
			rowData.signature = append([]byte(nil), row.Values[3].([]byte)...)
		}
		if row.Values[4] != nil {
			rowData.validatorPubKey = append([]byte(nil), row.Values[4].([]byte)...)
		}
		if row.Values[5] != nil {
			height := row.Values[5].(int64)
			rowData.signedHeight = &height
		}
		rowData.createdHeight = row.Values[6].(int64)
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, rowData.resultCanonical, "attestation row must exist")

	return rowData
}

func buildExpectedCanonicalPayload(
	createdHeight int64,
	dataProvider []byte,
	streamID []byte,
	actionID int,
	argsBytes []byte,
	queryResult []byte,
) []byte {
	versionBytes := []byte{1}
	algoBytes := []byte{1}

	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, uint64(createdHeight))

	actionIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(actionIDBytes, uint16(actionID))

	segments := [][]byte{
		versionBytes,
		algoBytes,
		heightBytes,
		lengthPrefixLittleEndian(dataProvider),
		lengthPrefixLittleEndian(streamID),
		actionIDBytes,
		lengthPrefixLittleEndian(argsBytes),
		lengthPrefixLittleEndian(queryResult),
	}

	return bytes.Join(segments, nil)
}

func lengthPrefixLittleEndian(data []byte) []byte {
	if data == nil {
		data = []byte{}
	}
	prefixed := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(prefixed[:4], uint32(len(data)))
	copy(prefixed[4:], data)
	return prefixed
}

func newEngineContext(ctx context.Context, platform *kwilTesting.Platform) (*common.EngineContext, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return nil, fmt.Errorf("create deployer address: %w", err)
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 1,
		},
		Signer: platform.Deployer,
		Caller: deployer.Address(),
		TxID:   platform.Txid(),
	}

	return &common.EngineContext{
		TxContext: txContext,
	}, nil
}
