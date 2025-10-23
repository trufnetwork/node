//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	attestation "github.com/trufnetwork/node/extensions/tn_attestation"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
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

				t.Run("HappyPath", func(t *testing.T) {
					runAttestationHappyPath(helper, testActionName, TestActionIDRequest)
				})

				t.Run("UnauthorizedUserBlocked", func(t *testing.T) {
					runAttestationUnauthorizedBlocked(t, ctx, platform, helper, testActionName)
				})

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}

func runAttestationHappyPath(helper *AttestationTestHelper, actionName string, actionID int) {
	const attestedValue int64 = 42

	dataProviderHex := TestDataProviderHex
	streamID := TestStreamID

	argsBytes, err := tn_utils.EncodeActionArgs([]any{attestedValue})
	require.NoError(helper.t, err, "encode action args")

	engineCtx := helper.NewEngineContext()

	var requestTxID string
	var attestationHash []byte
	_, err = helper.platform.Engine.Call(engineCtx, helper.platform.DB, "", "request_attestation", []any{
		dataProviderHex,
		streamID,
		actionName,
		argsBytes,
		false,
		int64(0),
	}, func(row *common.Row) error {
		require.Len(helper.t, row.Values, 2, "expected request_attestation to return request_tx_id and attestation_hash")
		txID, ok := row.Values[0].(string)
		require.True(helper.t, ok, "request_tx_id should be TEXT")
		hash, ok := row.Values[1].([]byte)
		require.True(helper.t, ok, "attestation_hash should be BYTEA")
		requestTxID = txID
		attestationHash = append([]byte(nil), hash...)
		return nil
	})
	require.NoError(helper.t, err)
	require.NotEmpty(helper.t, requestTxID, "request_attestation should return request_tx_id")
	require.NotEmpty(helper.t, attestationHash, "request_attestation should return attestation hash")

	stored := fetchAttestationRow(helper, attestationHash)
	require.Equal(helper.t, requestTxID, stored.requestTxID, "request_tx_id should be captured and stored")

	// Rebuild expected canonical payload
	valueDecimal := types.MustParseDecimal(fmt.Sprintf("%d.%018d", attestedValue, 0))
	queryRows := []*common.Row{
		{
			Values: []any{
				int64(1),
				valueDecimal,
			},
		},
	}
	canonicalResult, err := tn_utils.EncodeQueryResultCanonical(queryRows)
	require.NoError(helper.t, err)
	resultPayload, err := tn_utils.EncodeDataPointsABI(canonicalResult)
	require.NoError(helper.t, err)

	providerAddr := util.Unsafe_NewEthereumAddressFromString(dataProviderHex)
	expectedCanonical := attestation.BuildCanonicalPayload(
		1,
		0,
		uint64(stored.createdHeight),
		providerAddr.Bytes(),
		[]byte(streamID),
		uint16(actionID),
		argsBytes,
		resultPayload,
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

func runAttestationUnauthorizedBlocked(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, helper *AttestationTestHelper, actionName string) {
	// Create an unauthorized user that does NOT have network_writer role
	unauthorizedAddr := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000009999")

	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(999)})
	require.NoError(t, err, "encode action args")

	// Create a context for the unauthorized user
	unauthorizedCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 1,
			},
			Signer: unauthorizedAddr.Bytes(),
			Caller: unauthorizedAddr.Address(),
			TxID:   platform.Txid(),
		},
	}

	// Try to request attestation as unauthorized user - should fail
	res, err := platform.Engine.Call(unauthorizedCtx, platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		int64(0),
	}, func(row *common.Row) error {
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.NotNil(t, res.Error, "action should return error for unauthorized user")
	require.Contains(t, res.Error.Error(), "does not have the required system:network_writer role",
		"error should indicate missing network_writer role")
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
