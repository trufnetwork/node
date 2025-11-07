//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// TestMaxFeeValidation verifies that max_fee parameter works correctly
func TestMaxFeeValidation(t *testing.T) {
	const testActionName = "test_max_fee_action"
	addrs := NewTestAddresses()

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ATTESTATION_MAX_FEE_Validation",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          addrs.Owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				platform.Deployer = addrs.Owner.Bytes()
				helper := NewAttestationTestHelper(t, ctx, platform)

				require.NoError(t, helper.SetupTestAction(testActionName, TestActionIDRequest))

				t.Run("MaxFeeNull_Success", func(t *testing.T) {
					testMaxFeeNull(t, helper, testActionName)
				})

				t.Run("MaxFeeZero_Success", func(t *testing.T) {
					testMaxFeeZero(t, helper, testActionName)
				})

				t.Run("MaxFeeExactly40TRUF_Success", func(t *testing.T) {
					testMaxFeeExactly40TRUF(t, helper, testActionName)
				})

				t.Run("MaxFeeAbove40TRUF_Success", func(t *testing.T) {
					testMaxFeeAbove40TRUF(t, helper, testActionName)
				})

				t.Run("MaxFeeBelow40TRUF_Fails", func(t *testing.T) {
					testMaxFeeBelow40TRUF(t, helper, testActionName)
				})

				t.Run("MaxFeeVeryLarge_Success", func(t *testing.T) {
					testMaxFeeVeryLarge(t, helper, testActionName)
				})

				t.Run("MaxFeeNegative_Success", func(t *testing.T) {
					testMaxFeeNegative(t, helper, testActionName)
				})

				return nil
			},
		},
	}, testutils.GetTestOptionsWithCache())
}

// testMaxFeeNull verifies that NULL max_fee allows attestation (no limit)
func testMaxFeeNull(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(42)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	var requestTxID string
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		nil, // max_fee = NULL (no limit)
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.Nil(t, res.Error, "NULL max_fee should allow attestation")
	require.NotEmpty(t, requestTxID, "should return request_tx_id")
}

// testMaxFeeZero verifies that zero max_fee allows attestation (treated as no limit)
func testMaxFeeZero(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(43)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	maxFee := types.MustParseDecimalExplicit("0", 78, 0) // max_fee = 0 (treated as no limit per validation logic)

	var requestTxID string
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		maxFee,
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.Nil(t, res.Error, "Zero max_fee should be ignored (treated as no limit)")
	require.NotEmpty(t, requestTxID, "should return request_tx_id")
}

// testMaxFeeExactly40TRUF verifies that max_fee of exactly 40 TRUF succeeds
func testMaxFeeExactly40TRUF(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(44)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	maxFee := types.MustParseDecimalExplicit("40000000000000000000", 78, 0) // Exactly 40 TRUF in wei

	var requestTxID string
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		maxFee,
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.Nil(t, res.Error, "max_fee of exactly 40 TRUF should succeed")
	require.NotEmpty(t, requestTxID, "should return request_tx_id")
}

// testMaxFeeAbove40TRUF verifies that max_fee above 40 TRUF succeeds
func testMaxFeeAbove40TRUF(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(45)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	maxFee := types.MustParseDecimalExplicit("50000000000000000000", 78, 0) // 50 TRUF in wei

	var requestTxID string
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		maxFee,
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.Nil(t, res.Error, "max_fee above 40 TRUF should succeed")
	require.NotEmpty(t, requestTxID, "should return request_tx_id")
}

// testMaxFeeBelow40TRUF verifies that max_fee below 40 TRUF fails
func testMaxFeeBelow40TRUF(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(46)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	maxFee := types.MustParseDecimalExplicit("30000000000000000000", 78, 0) // 30 TRUF in wei

	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		maxFee,
	}, func(row *common.Row) error {
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.NotNil(t, res.Error, "max_fee below 40 TRUF should fail")
	require.Contains(t, res.Error.Error(), "exceeds caller max_fee limit",
		"error should indicate fee exceeds max_fee")
}

// testMaxFeeVeryLarge verifies that very large max_fee succeeds (tests NUMERIC(78,0) capacity)
func testMaxFeeVeryLarge(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(47)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	// Use a very large number that wouldn't fit in INT8 (max ~9.2 TRUF)
	// This is 1 billion TRUF in wei
	maxFee := types.MustParseDecimalExplicit("1000000000000000000000000000", 78, 0) // 1 billion TRUF in wei

	var requestTxID string
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		maxFee,
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.Nil(t, res.Error, "very large max_fee should succeed (NUMERIC(78,0) should handle it)")
	require.NotEmpty(t, requestTxID, "should return request_tx_id")
}

// testMaxFeeNegative verifies that negative max_fee is treated as no limit
func testMaxFeeNegative(t *testing.T, h *AttestationTestHelper, actionName string) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{int64(48)})
	require.NoError(t, err, "encode action args")

	engineCtx := h.NewEngineContext()

	maxFee := types.MustParseDecimalExplicit("-1000000000000000000", 78, 0) // -1 TRUF in wei

	var requestTxID string
	res, err := h.platform.Engine.Call(engineCtx, h.platform.DB, "", "request_attestation", []any{
		TestDataProviderHex,
		TestStreamID,
		actionName,
		argsBytes,
		false,
		maxFee,
	}, func(row *common.Row) error {
		requestTxID = row.Values[0].(string)
		return nil
	})

	require.NoError(t, err, "call should not error at engine level")
	require.Nil(t, res.Error, "negative max_fee should be treated as no limit")
	require.NotEmpty(t, requestTxID, "should return request_tx_id")
}
