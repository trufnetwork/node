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

		// Insert a data point for binary action test
		err = insertTestDataPoint(ctx, platform, &systemAdmin, streamID, 1000000, "75.000000000000000000")
		require.NoError(t, err)

		// === Test 1: 30-day range should succeed ===
		t.Log("Test 1: get_record with 30-day range should succeed")
		from30 := int64(1000000)
		to30 := int64(1000000 + 30*24*60*60)
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", from30, to30)
		require.NoError(t, err, "30-day range should succeed")

		// === Test 2: 180-day range should fail ===
		t.Log("Test 2: get_record with 180-day range should fail")
		from180 := int64(1000000)
		to180 := int64(1000000 + 180*24*60*60)
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", from180, to180)
		require.Error(t, err, "180-day range should fail")
		require.Contains(t, err.Error(), "exceeds maximum", "should mention exceeding maximum")

		// === Test 3: Both from and to nil should succeed (latest record, safe) ===
		t.Log("Test 3: get_record with both null dates should succeed")
		nullArgs := []any{
			systemAdmin.Address(), streamID,
			nil, nil,   // from, to both nil
			nil, false, // frozen_at, use_cache
		}
		argsBytes, err := tn_utils.EncodeActionArgs(nullArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", argsBytes)
		require.NoError(t, err, "both null dates should succeed (returns latest record)")

		// === Test 4: Only one date param should fail (unbounded range) ===
		t.Log("Test 4: get_record with only 'to' should fail")
		oneNullArgs := []any{
			systemAdmin.Address(), streamID,
			nil, int64(99999), // from=nil (unbounded), to=specified
			nil, false,
		}
		argsBytes, err = tn_utils.EncodeActionArgs(oneNullArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", argsBytes)
		require.Error(t, err, "one null date should fail")
		require.Contains(t, err.Error(), "must specify both", "should require both from and to")

		// === Test 5: Exactly 90 days should succeed (boundary) ===
		t.Log("Test 5: get_record with exactly 90-day range should succeed")
		from90 := int64(1000000)
		to90 := int64(1000000 + 90*24*60*60)
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", from90, to90)
		require.NoError(t, err, "exactly 90-day range should succeed")

		// === Test 6: 91 days should fail (just over boundary) ===
		t.Log("Test 6: get_record with 91-day range should fail")
		from91 := int64(1000000)
		to91 := int64(1000000 + 91*24*60*60)
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", from91, to91)
		require.Error(t, err, "91-day range should fail")
		require.Contains(t, err.Error(), "exceeds maximum")

		// === Test 7: Binary action (price_above_threshold) skips validation ===
		t.Log("Test 7: binary action should skip date range validation")
		thresholdVal, err := kwilTypes.ParseDecimal("50.000000000000000000")
		require.NoError(t, err)
		thresholdVal.SetPrecisionAndScale(36, 18)
		binaryArgs := []any{
			systemAdmin.Address(), streamID,
			int64(1000000),  // timestamp
			thresholdVal,    // threshold as NUMERIC(36,18)
			nil,             // frozen_at
		}
		argsBytes, err = tn_utils.EncodeActionArgs(binaryArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "price_above_threshold", argsBytes)
		require.NoError(t, err, "binary action should skip date range validation")

		// === Test 8: get_index with 180-day range should also fail ===
		t.Log("Test 8: get_index with 180-day range should fail")
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_index", from180, to180)
		require.Error(t, err, "get_index with 180-day range should fail")
		require.Contains(t, err.Error(), "exceeds maximum")

		// === Test 8b: get_change_over_time with 180-day range should also fail ===
		t.Log("Test 8b: get_change_over_time with 180-day range should fail")
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_change_over_time", from180, to180)
		require.Error(t, err, "get_change_over_time with 180-day range should fail")
		require.Contains(t, err.Error(), "exceeds maximum")

		// === Test 9: get_last_record (action_id=4) should skip validation ===
		// Signature: get_last_record($data_provider TEXT, $stream_id TEXT, $before INT8, $frozen_at INT8, $use_cache BOOL)
		t.Log("Test 9: get_last_record should skip date range validation")
		lastRecordArgs := []any{
			systemAdmin.Address(), streamID,
			nil,   // before
			nil,   // frozen_at
			false, // use_cache
		}
		argsBytes, err = tn_utils.EncodeActionArgs(lastRecordArgs)
		require.NoError(t, err)
		err = requestAttestationWithArgsBytes(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_last_record", argsBytes)
		require.NoError(t, err, "get_last_record should skip date range validation")

		// === Test 10: from == to (single-point query, dateRange=0) should succeed ===
		t.Log("Test 10: get_record with from == to should succeed")
		samePoint := int64(1000000)
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", samePoint, samePoint)
		require.NoError(t, err, "single-point query (from == to) should succeed")

		// === Test 11: from > to (negative range) should fail ===
		t.Log("Test 11: get_record with from > to should fail")
		err = requestAttestationWithTimeRange(ctx, platform, &systemAdmin, systemAdmin.Address(), streamID, "get_record", int64(2000000), int64(1000000))
		require.Error(t, err, "negative range (from > to) should fail")
		require.Contains(t, err.Error(), "must be less than or equal", "should reject inverted range")

		t.Log("All date range validation tests passed")
		return nil
	}
}
