//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestAttestationDateRangeValidation(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ATTESTATION_DATE_RANGE_Validation",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testAllDateRangeValidations(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testAllDateRangeValidations(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// === Setup ===
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()
		streamID := "st000000000000000000000000000000"

		// Grant roles and create stream
		err := setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", systemAdmin.Address())
		require.NoError(t, err)

		err = setup.CreateDataProvider(ctx, platform, systemAdmin.Address())
		require.NoError(t, err)

		streamLocator := types.StreamLocator{
			StreamId:     util.GenerateStreamId(streamID),
			DataProvider: systemAdmin,
		}
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type:    setup.ContractTypePrimitive,
			Locator: streamLocator,
		})
		require.NoError(t, err)

		// Initialize ERC20 and give balance for attestation fees
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveAttestationBalance(ctx, platform, systemAdmin.Address(), "1000000000000000000000")
		require.NoError(t, err)

		// Insert multiple data points for high/low tests
		err = insertTestDataPoint(ctx, platform, &systemAdmin, streamID, 1000000, "75.000000000000000000")
		require.NoError(t, err)
		err = insertTestDataPoint(ctx, platform, &systemAdmin, streamID, 1000100, "120.000000000000000000")
		require.NoError(t, err)
		err = insertTestDataPoint(ctx, platform, &systemAdmin, streamID, 1000200, "30.000000000000000000")
		require.NoError(t, err)

		// =====================================================================
		// Group 1: Actions 1-3 are BLOCKED from attestation
		// =====================================================================

		// === Test 1: get_record is blocked ===
		t.Log("Test 1: get_record is blocked from attestation")
		from30 := int64(1000000)
		to30 := int64(1000000 + 30*24*60*60)
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", from30, to30)
		require.Error(t, err, "get_record should be blocked")
		require.Contains(t, err.Error(), "not allowed for attestation")

		// === Test 2: get_index is blocked ===
		t.Log("Test 2: get_index is blocked from attestation")
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_index", from30, to30)
		require.Error(t, err, "get_index should be blocked")
		require.Contains(t, err.Error(), "not allowed for attestation")

		// === Test 3: get_change_over_time is blocked ===
		t.Log("Test 3: get_change_over_time is blocked from attestation")
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_change_over_time", from30, to30)
		require.Error(t, err, "get_change_over_time should be blocked")
		require.Contains(t, err.Error(), "not allowed for attestation")

		// === Test 4: get_record with null dates is also blocked ===
		t.Log("Test 4: get_record with null dates is still blocked")
		nullArgs := []any{
			systemAdmin.Address(), streamID,
			nil, nil,   // from, to both nil
			nil, false, // frozen_at, use_cache
		}
		argsBytes, err := tn_utils.EncodeActionArgs(nullArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", argsBytes)
		require.Error(t, err, "get_record should be blocked even with null dates")
		require.Contains(t, err.Error(), "not allowed for attestation")

		// =====================================================================
		// Group 2: Actions 4-5 pass (single-point, LIMIT 1)
		// =====================================================================

		// === Test 5: get_last_record (action_id=4) passes ===
		t.Log("Test 5: get_last_record should pass (single-point query)")
		lastRecordArgs := []any{
			systemAdmin.Address(), streamID,
			nil,   // before
			nil,   // frozen_at
			false, // use_cache
		}
		argsBytes, err = tn_utils.EncodeActionArgs(lastRecordArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_last_record", argsBytes)
		require.NoError(t, err, "get_last_record should pass")

		// =====================================================================
		// Group 3: Actions 6-9 pass (binary, single boolean)
		// =====================================================================

		// === Test 6: binary action (price_above_threshold) passes ===
		t.Log("Test 6: binary action should pass")
		thresholdVal, err := kwilTypes.ParseDecimal("50.000000000000000000")
		require.NoError(t, err)
		thresholdVal.SetPrecisionAndScale(36, 18)
		binaryArgs := []any{
			systemAdmin.Address(), streamID,
			int64(1000000),  // timestamp
			thresholdVal,    // threshold
			nil,             // frozen_at
		}
		argsBytes, err = tn_utils.EncodeActionArgs(binaryArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "price_above_threshold", argsBytes)
		require.NoError(t, err, "binary action should pass")

		// =====================================================================
		// Group 4: Actions 10-11 date range validation
		// =====================================================================

		// === Test 7: get_high_value with valid 30-day range succeeds ===
		t.Log("Test 7: get_high_value with valid range should succeed")
		highArgs := []any{
			systemAdmin.Address(), streamID,
			int64(900000), int64(1100000), // ~1.2 day range
			nil, // frozen_at
		}
		argsBytes, err = tn_utils.EncodeActionArgs(highArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_high_value", argsBytes)
		require.NoError(t, err, "get_high_value with valid range should succeed")

		// === Test 8: get_low_value with valid range succeeds ===
		t.Log("Test 8: get_low_value with valid range should succeed")
		lowArgs := []any{
			systemAdmin.Address(), streamID,
			int64(900000), int64(1100000),
			nil,
		}
		argsBytes, err = tn_utils.EncodeActionArgs(lowArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_low_value", argsBytes)
		require.NoError(t, err, "get_low_value with valid range should succeed")

		// === Test 9: get_high_value with 180-day range fails ===
		t.Log("Test 9: get_high_value with 180-day range should fail")
		highLongArgs := []any{
			systemAdmin.Address(), streamID,
			int64(1000000), int64(1000000 + 180*24*60*60),
			nil,
		}
		argsBytes, err = tn_utils.EncodeActionArgs(highLongArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_high_value", argsBytes)
		require.Error(t, err, "get_high_value with 180-day range should fail")
		require.Contains(t, err.Error(), "exceeds maximum")

		// === Test 10: get_high_value with from > to fails ===
		t.Log("Test 10: get_high_value with from > to should fail")
		highInvertedArgs := []any{
			systemAdmin.Address(), streamID,
			int64(2000000), int64(1000000),
			nil,
		}
		argsBytes, err = tn_utils.EncodeActionArgs(highInvertedArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_high_value", argsBytes)
		require.Error(t, err, "inverted range should fail")
		require.Contains(t, err.Error(), "must be less than or equal")

		// === Test 11: get_high_value with nil from/to fails ===
		t.Log("Test 11: get_high_value with nil dates should fail")
		highNilArgs := []any{
			systemAdmin.Address(), streamID,
			nil, nil,
			nil,
		}
		argsBytes, err = tn_utils.EncodeActionArgs(highNilArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_high_value", argsBytes)
		require.Error(t, err, "get_high_value with nil dates should fail")
		require.Contains(t, err.Error(), "require both")

		// === Test 12: get_high_value with exactly 90 days succeeds ===
		t.Log("Test 12: get_high_value with exactly 90-day range should succeed")
		high90Args := []any{
			systemAdmin.Address(), streamID,
			int64(1000000), int64(1000000 + 90*24*60*60),
			nil,
		}
		argsBytes, err = tn_utils.EncodeActionArgs(high90Args)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_high_value", argsBytes)
		require.NoError(t, err, "exactly 90-day range should succeed")

		t.Log("All attestation restriction tests passed")
		return nil
	}
}
