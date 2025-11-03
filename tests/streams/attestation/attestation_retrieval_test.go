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
	count := h.CountRows("list_attestations", []any{nil, 10, 0, nil})
	require.Equal(h.t, 0, count, "should return no results when empty")
}

func testListNoFilter(h *AttestationTestHelper, actionName string, addrs *TestAddresses) {
	h.CreateAttestationForRequester(actionName, addrs.Requester1, 1)
	h.CreateAttestationForRequester(actionName, addrs.Requester1, 2)
	h.CreateAttestationForRequester(actionName, addrs.Requester2, 3)

	count := 0
	h.CallAction("list_attestations", []any{nil, 100, 0, nil}, func(row *common.Row) error {
		count++
		require.Len(h.t, row.Values, 6, "should return 6 columns")
		return nil
	})
	require.Equal(h.t, 3, count, "should return all 3 attestations")
}

func testListFilterByRequester(h *AttestationTestHelper, addrs *TestAddresses) {
	count := 0
	h.CallAction("list_attestations", []any{addrs.Requester1.Bytes(), 100, 0, nil}, func(row *common.Row) error {
		count++
		requester := row.Values[2].([]byte)
		require.Equal(h.t, addrs.Requester1.Bytes(), requester, "requester should match filter")
		return nil
	})
	require.Equal(h.t, 2, count, "should return only requester1's attestations")
}

func testListPagination(h *AttestationTestHelper, actionName string, addrs *TestAddresses) {
	for i := 0; i < 5; i++ {
		h.CreateAttestationForRequester(actionName, addrs.Requester1, int64(100+i))
	}

	// First page
	count := h.CountRows("list_attestations", []any{addrs.Requester1.Bytes(), 3, 0, nil})
	require.Equal(h.t, 3, count, "first page should return 3 results")

	// Second page
	count = h.CountRows("list_attestations", []any{addrs.Requester1.Bytes(), 3, 3, nil})
	require.Equal(h.t, 3, count, "second page should return 3 results")
}

func testListMaxLimit(h *AttestationTestHelper) {
	count := h.CountRows("list_attestations", []any{nil, 99999, 0, nil})
	require.Greater(h.t, count, 0, "should return results with large limit")
}
