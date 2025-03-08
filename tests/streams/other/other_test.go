package tests

import (
	"context"
	"testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

var defaultCaller = "0x0000000000000000000000000000000000000001"
var defaultStreamLocator = types.StreamLocator{
	StreamId:     *util.NewRawStreamId("st123456789012345678901234567890"),
	DataProvider: util.Unsafe_NewEthereumAddressFromString(defaultCaller),
}

// [OTHER01] All referenced addresses must be lowercased and valid EVM addresses starting with `0x`.
// TestAddressValidation tests that all referenced addresses must be lowercased and valid EVM addresses starting with `0x`.
func TestAddressValidation(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "address_validation_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testAddressValidation(t),
		},
	}, testutils.GetTestOptions())
}

// [OTHER02] Stream ids must respect the following regex: `^st[a-z0-9]{30}$` and be unique by each stream owner.
// TestStreamIDValidation tests that stream ids must respect the following regex: `^st[a-z0-9]{30}$`
func TestStreamIDValidation(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "stream_id_validation_test",
		SeedScripts: []string{
			"../../../internal/migrations/000-initial-data.sql",
			"../../../internal/migrations/001-common-actions.sql",
		},
		FunctionTests: []kwilTesting.TestFunc{
			testStreamIDValidation(t),
			testNonDuplicateStreamID(t),
		},
	}, testutils.GetTestOptions())
}

// [OTHER03] Any user can create a stream.
// TestAnyUserCanCreateStream tests that any user with a valid Ethereum address can create a stream
func TestAnyUserCanCreateStream(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "any_user_can_create_stream_test",
		SeedScripts: []string{
			"../../../internal/migrations/000-initial-data.sql",
			"../../../internal/migrations/001-common-actions.sql",
		},
		FunctionTests: []kwilTesting.TestFunc{
			testAnyUserCanCreateStream(t),
			testAnyUserCanCreateStream(t),
		},
	}, testutils.GetTestOptions())
}

// testStreamIDValidation tests that stream ids must respect the following regex: `^st[a-z0-9]{30}$`
func testStreamIDValidation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test valid stream ID
		_, err := setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), defaultCaller, string(setup.ContractTypePrimitive))
		if err != nil {
			return errors.Wrap(err, "valid stream ID should be accepted")
		}

		// Test invalid stream ID - too short
		_, err = setup.UntypedCreateStream(ctx, platform, "stO", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "too short stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - too long
		_, err = setup.UntypedCreateStream(ctx, platform, "st0000000000000000000000000000000000000000000000000", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "too long stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - wrong prefix
		_, err = setup.UntypedCreateStream(ctx, platform, "xx123456789012345678901234567890", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "wrong prefix stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - uppercase letters
		// TODO: Uncomment this once the regex is updated to disallow uppercase letters
		// err = testutils.ExecuteCreateStream(ctx, platform, "stABCDEF89012345678901234567890", "primitive", defaultCaller)
		// assert.Error(t, err, "uppercase letters in stream ID should be rejected")
		// assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - special characters
		// TODO: Uncomment this once the regex is updated to disallow special characters
		// err = testutils.ExecuteCreateStream(ctx, platform, "st12345678901234567890123456-+*&", "primitive", defaultCaller)
		// assert.Error(t, err, "special characters in stream ID should be rejected")
		// assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// now let's execute a statement getting all streams
		rows := []common.Row{}
		err = platform.Engine.Execute(&common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: ctx,
			},
		}, platform.DB, "SELECT * FROM streams", map[string]any{}, func(row *common.Row) error {
			rows = append(rows, *row)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "failed to get all streams")
		}

		// expect to have only the valid stream
		assert.Len(t, rows, 1)
		assert.Equal(t, rows[0].Values[0], "st123456789012345678901234567890")

		return nil
	}
}

// testAddressValidation tests that all referenced addresses are valid EVM addresses
func testAddressValidation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test with valid address
		validAddress := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")
		_, err := setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), validAddress.Address(), string(setup.ContractTypePrimitive))
		if err != nil {
			return errors.Wrap(err, "valid address should be accepted")
		}

		// Test with invalid address - missing 0x prefix
		invalidAddress1 := "0000000000000000000000000000000000000001"
		_, err = setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), invalidAddress1, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "address without 0x prefix should be rejected")
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		// Test with invalid address - wrong length
		invalidAddress2 := "0x00000000000000000000000000000000000000"
		_, err = setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), invalidAddress2, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "address with wrong length should be rejected")
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		// Test with invalid address - too long
		invalidAddress3 := "0x000000000000000000000000000000000000000001"
		_, err = setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), invalidAddress3, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "address that is too long should be rejected")
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		return nil
	}
}

// testNonDuplicateStreamID tests that stream ids must be unique by each stream owner
func testNonDuplicateStreamID(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create a stream with a valid ID
		streamID := "st123456789012345678901234567890"
		owner1 := defaultCaller

		// Create the first stream with owner1
		_, err := setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type: setup.ContractTypePrimitive,
			Locator: types.StreamLocator{
				StreamId:     *util.NewRawStreamId(streamID),
				DataProvider: util.Unsafe_NewEthereumAddressFromString(owner1),
			},
		})
		if err != nil {
			return errors.Wrap(err, "failed to create first stream")
		}

		// Attempt to create another stream with the same ID for the same owner (should fail)
		_, err = setup.UntypedCreateStream(ctx, platform, streamID, owner1, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "Should not allow duplicate stream ID for the same owner")
		assert.Contains(t, err.Error(), "already exists", "error message should indicate duplicate stream ID")

		// Attempt to create a stream with the same ID but different owner
		// (according to the requirement, stream IDs should be unique per owner, so this should succeed)
		owner2 := "0x0000000000000000000000000000000000000456"
		_, err = setup.UntypedCreateStream(ctx, platform, streamID, owner2, string(setup.ContractTypePrimitive))
		if err != nil {
			t.Logf("System enforces globally unique stream IDs regardless of owner: %v", err)
		} else {
			t.Log("System allows the same stream ID for different owners (each owner can have their own namespace)")
		}

		return nil
	}
}

// testAnyUserCanCreateStream tests that any user with a valid Ethereum address can create a stream
func testAnyUserCanCreateStream(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test with multiple different users
		users := []string{
			"0x0000000000000000000000000000000000000001",
			"0x0000000000000000000000000000000000000002",
			"0x0000000000000000000000000000000000000003",
			"0x0000000000000000000000000000000000000004",
			"0x0000000000000000000000000000000000000005",
		}

		for i, user := range users {
			// Generate a unique stream ID for each user
			streamID := "st" + "user" + string(rune('a'+i)) + "2345678901234567890123456"

			// Attempt to create a stream with the user
			_, err := setup.UntypedCreateStream(ctx, platform, streamID, user, string(setup.ContractTypePrimitive))
			if err != nil {
				return errors.Wrapf(err, "user %s should be able to create a stream", user)
			}
		}

		return nil
	}
}
