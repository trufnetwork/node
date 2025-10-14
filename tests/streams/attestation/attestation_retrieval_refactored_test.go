//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

func TestGetSignedAttestationRefactored(t *testing.T) {
	const testActionName = "test_get_signed_action"
	addrs := NewTestAddresses()

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "ATTESTATION02_GetSignedAttestation_Refactored",
		SeedScripts: migrations.GetSeedScriptPaths(),
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

func TestListAttestationsRefactored(t *testing.T) {
	const testActionName = "test_list_action"
	addrs := NewTestAddresses()

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "ATTESTATION03_ListAttestations_Refactored",
		SeedScripts: migrations.GetSeedScriptPaths(),
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
