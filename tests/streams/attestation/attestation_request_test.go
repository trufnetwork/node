//go:build kwiltest

package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

func TestRequestAttestationInsertsCanonicalPayload(t *testing.T) {
	const testActionName = "test_attestation_action"
	addrs := NewTestAddresses()

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "ATTESTATION01_RequestInsertion",
		SeedScripts: migrations.GetSeedScriptPaths(),
		Owner:       addrs.Owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform.Deployer = addrs.Owner.Bytes()
				helper := NewAttestationTestHelper(t, ctx, platform)

				require.NoError(t, helper.SetupTestAction(testActionName, TestActionIDRequest))
				runAttestationHappyPath(helper, testActionName, TestActionIDRequest)
				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}

func runAttestationHappyPath(helper *AttestationTestHelper, actionName string, actionID int) {
	const attestedValue int64 = 42

	dataProvider := []byte("provider-001")
	streamID := []byte("stream-abc")

	argsBytes, err := tn_utils.EncodeActionArgs([]any{attestedValue})
	require.NoError(helper.t, err, "encode action args")

	engineCtx := helper.NewEngineContext()

	var requestTxID string
	var attestationHash []byte
	_, err = helper.platform.Engine.Call(engineCtx, helper.platform.DB, "", "request_attestation", []any{
		dataProvider,
		streamID,
		actionName,
		argsBytes,
		false,
		int64(0),
	}, func(row *common.Row) error {
		require.Len(helper.t, row.Values, 2, "expected 2 return values (request_tx_id, attestation_hash)")
		requestTxID = row.Values[0].(string)
		attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
		return nil
	})
	require.NoError(helper.t, err)
	require.NotEmpty(helper.t, requestTxID, "request_attestation should return request_tx_id")
	require.NotEmpty(helper.t, attestationHash, "request_attestation should return attestation hash")

	stored := fetchAttestationRow(helper, attestationHash)
	require.Equal(helper.t, requestTxID, stored.requestTxID, "request_tx_id should be captured and stored")

	// Rebuild expected canonical payload
	queryResult, err := tn_utils.EncodeQueryResultCanonical([]*common.Row{
		{Values: []any{attestedValue}},
	})
	require.NoError(helper.t, err)

	expectedCanonical := buildExpectedCanonicalPayload(
		stored.createdHeight,
		dataProvider,
		streamID,
		actionID,
		argsBytes,
		queryResult,
	)

	require.Equal(helper.t, expectedCanonical, stored.resultCanonical, "canonical payload mismatch")
	require.False(helper.t, stored.encryptSig, "encrypt_sig must remain false in MVP")
	require.Nil(helper.t, stored.signature, "signature must be NULL before signing")
	require.Nil(helper.t, stored.validatorPubKey, "validator_pubkey must be NULL before signing")
	require.Nil(helper.t, stored.signedHeight, "signed_height must be NULL before signing")
	require.Equal(helper.t, attestationHash, stored.attestationHash, "returned hash should equal stored hash")
}

type attestationRow struct {
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

func fetchAttestationRow(helper *AttestationTestHelper, hash []byte) attestationRow {
	engineCtx := helper.NewEngineContext()

	var rowData attestationRow
	err := helper.platform.Engine.Execute(engineCtx, helper.platform.DB, `
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
	require.NoError(helper.t, err)
	require.NotNil(helper.t, rowData.resultCanonical, "attestation row must exist")

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
