//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

func TestGetSignedAttestation(t *testing.T) {
	const testActionName = "test_get_signed_action"
	addrs := NewTestAddresses()

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "ATTESTATION02_GetSignedAttestation",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:       addrs.Owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform.Deployer = addrs.Owner.Bytes()
				helper := NewAttestationTestHelper(t, ctx, platform)

				require.NoError(t, helper.SetupTestAction(testActionName, TestActionIDGet))

				t.Run("HappyPath", func(t *testing.T) {
					testGetSignedHappyPath(helper, testActionName)
				})

				t.Run("NotFound", func(t *testing.T) {
					testGetSignedNotFound(helper)
				})

				t.Run("NotSigned", func(t *testing.T) {
					testGetSignedNotSigned(helper, testActionName)
				})

				t.Run("PayloadStructure", func(t *testing.T) {
					testGetSignedPayloadStructure(helper, testActionName)
				})

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}

func testGetSignedHappyPath(h *AttestationTestHelper, actionName string) {
	requestTxID, _ := h.RequestAttestation(actionName, 100)
	h.SignAttestation(requestTxID)

	var payload []byte
	h.CallAction("get_signed_attestation", []any{requestTxID}, func(row *common.Row) error {
		require.Len(h.t, row.Values, 1, "expected 1 return value")
		payload = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})

	require.NotEmpty(h.t, payload, "payload should not be empty")
	require.Greater(h.t, len(payload), SignatureLength, "payload should be larger than just signature")
}

func testGetSignedNotFound(h *AttestationTestHelper) {
	res := h.CallAction("get_signed_attestation", []any{InvalidTxID}, func(row *common.Row) error {
		return nil
	})
	h.AssertActionError(res, "not found")
}

func testGetSignedNotSigned(h *AttestationTestHelper, actionName string) {
	requestTxID, _ := h.RequestAttestation(actionName, 200)

	res := h.CallAction("get_signed_attestation", []any{requestTxID}, func(row *common.Row) error {
		return nil
	})
	h.AssertActionError(res, "not yet signed")
}

func testGetSignedPayloadStructure(h *AttestationTestHelper, actionName string) {
	requestTxID, _ := h.RequestAttestation(actionName, 300)
	h.SignAttestation(requestTxID)

	var payload []byte
	h.CallAction("get_signed_attestation", []any{requestTxID}, func(row *common.Row) error {
		payload = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})

	require.GreaterOrEqual(h.t, len(payload), SignatureLength, "payload must be at least signature length")

	signature := payload[len(payload)-SignatureLength:]
	canonical := payload[:len(payload)-SignatureLength]

	require.Len(h.t, signature, SignatureLength, "signature should be 65 bytes")
	require.NotEmpty(h.t, canonical, "canonical should not be empty")
	require.Greater(h.t, len(canonical), MinCanonicalLength, "canonical should have substantial data")
}

func TestListAttestations(t *testing.T) {
	const testActionName = "test_list_action"
	addrs := NewTestAddresses()

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "ATTESTATION03_ListAttestations",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:       addrs.Owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform.Deployer = addrs.Owner.Bytes()
				helper := NewAttestationTestHelper(t, ctx, platform)

				require.NoError(t, helper.SetupTestAction(testActionName, TestActionIDList))

				t.Run("Empty", func(t *testing.T) {
					testListEmpty(helper)
				})

				t.Run("NoFilter", func(t *testing.T) {
					testListNoFilter(helper, testActionName, addrs)
				})

				t.Run("FilterByRequester", func(t *testing.T) {
					testListFilterByRequester(helper, addrs)
				})

				t.Run("FilterByRequestTxID", func(t *testing.T) {
					testListFilterByRequestTxID(helper, testActionName, addrs)
				})

				t.Run("Pagination", func(t *testing.T) {
					testListPagination(helper, testActionName, addrs)
				})

				t.Run("MaxLimit", func(t *testing.T) {
					testListMaxLimit(helper)
				})

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}

func testListEmpty(h *AttestationTestHelper) {
	count := h.CountRows("list_attestations", []any{nil, nil, 10, 0, nil})
	require.Equal(h.t, 0, count, "should return no results when empty")
}

func testListNoFilter(h *AttestationTestHelper, actionName string, addrs *TestAddresses) {
	h.CreateAttestationForRequester(actionName, addrs.Requester1, 1)
	h.CreateAttestationForRequester(actionName, addrs.Requester1, 2)
	h.CreateAttestationForRequester(actionName, addrs.Requester2, 3)

	count := 0
	h.CallAction("list_attestations", []any{nil, nil, 100, 0, nil}, func(row *common.Row) error {
		count++
		require.Len(h.t, row.Values, 8, "should return 8 columns: request_tx_id, attestation_hash, requester, data_provider, stream_id, created_height, signed_height, encrypt_sig")

		// Verify data_provider and stream_id are present
		dataProvider := row.Values[3].(string)
		streamID := row.Values[4].(string)
		require.NotEmpty(h.t, dataProvider, "data_provider should not be empty")
		require.NotEmpty(h.t, streamID, "stream_id should not be empty")
		require.Equal(h.t, TestDataProviderHex, dataProvider, "data_provider should match test value")
		require.Equal(h.t, TestStreamID, streamID, "stream_id should match test value")
		return nil
	})
	require.Equal(h.t, 3, count, "should return all 3 attestations")
}

func testListFilterByRequester(h *AttestationTestHelper, addrs *TestAddresses) {
	count := 0
	h.CallAction("list_attestations", []any{addrs.Requester1.Bytes(), nil, 100, 0, nil}, func(row *common.Row) error {
		count++
		requester := row.Values[2].([]byte)
		require.Equal(h.t, addrs.Requester1.Bytes(), requester, "requester should match filter")
		return nil
	})
	require.Equal(h.t, 2, count, "should return only requester1's attestations")
}

func testListFilterByRequestTxID(h *AttestationTestHelper, actionName string, addrs *TestAddresses) {
	// Create multiple attestations
	requestTxID1, hash1 := h.RequestAttestation(actionName, 500)
	requestTxID2, _ := h.RequestAttestation(actionName, 501)
	h.RequestAttestation(actionName, 502)

	// Filter by specific request_tx_id
	count := 0
	var returnedTxID string
	var returnedHash []byte
	h.CallAction("list_attestations", []any{nil, requestTxID1, nil, nil, nil}, func(row *common.Row) error {
		count++
		returnedTxID = row.Values[0].(string)
		returnedHash = row.Values[1].([]byte)
		return nil
	})

	require.Equal(h.t, 1, count, "should return exactly 1 result for specific request_tx_id")
	require.Equal(h.t, requestTxID1, returnedTxID, "returned tx_id should match filter")
	require.Equal(h.t, hash1, returnedHash, "returned hash should match the attestation")

	// Verify different request_tx_id returns different result
	count = 0
	h.CallAction("list_attestations", []any{nil, requestTxID2, nil, nil, nil}, func(row *common.Row) error {
		count++
		returnedTxID = row.Values[0].(string)
		return nil
	})
	require.Equal(h.t, 1, count, "should return exactly 1 result for second request_tx_id")
	require.Equal(h.t, requestTxID2, returnedTxID, "returned tx_id should match second filter")

	// Verify non-existent request_tx_id returns no results
	count = h.CountRows("list_attestations", []any{nil, InvalidTxID, nil, nil, nil})
	require.Equal(h.t, 0, count, "should return 0 results for non-existent request_tx_id")
}

func testListPagination(h *AttestationTestHelper, actionName string, addrs *TestAddresses) {
	for i := 0; i < 5; i++ {
		h.CreateAttestationForRequester(actionName, addrs.Requester1, int64(100+i))
	}

	// First page
	count := h.CountRows("list_attestations", []any{addrs.Requester1.Bytes(), nil, 3, 0, nil})
	require.Equal(h.t, 3, count, "first page should return 3 results")

	// Second page
	count = h.CountRows("list_attestations", []any{addrs.Requester1.Bytes(), nil, 3, 3, nil})
	require.Equal(h.t, 3, count, "second page should return 3 results")
}

func testListMaxLimit(h *AttestationTestHelper) {
	count := h.CountRows("list_attestations", []any{nil, nil, 99999, 0, nil})
	require.Greater(h.t, count, 0, "should return results with large limit")
}
