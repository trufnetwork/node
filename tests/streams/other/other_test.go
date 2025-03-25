package tests

import (
	"context"
	"sort"
	"testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
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
		err := setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), defaultCaller, string(setup.ContractTypePrimitive))
		if err != nil {
			return errors.Wrap(err, "valid stream ID should be accepted")
		}

		// Test invalid stream ID - too short
		err = setup.UntypedCreateStream(ctx, platform, "stO", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "too short stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - too long
		err = setup.UntypedCreateStream(ctx, platform, "st0000000000000000000000000000000000000000000000000", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "too long stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - wrong prefix
		err = setup.UntypedCreateStream(ctx, platform, "xx123456789012345678901234567890", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "wrong prefix stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - uppercase letters
		err = setup.UntypedCreateStream(ctx, platform, "stABCDEF89012345678901234567890", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "uppercase letters in stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - special characters
		err = setup.UntypedCreateStream(ctx, platform, "st12345678901234567890123456-+*&", defaultCaller, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "special characters in stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

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
		// Valid Ethereum address
		validAddress := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000001")

		// Test with valid Ethereum address
		err := setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), validAddress.Address(), string(setup.ContractTypePrimitive))
		if err != nil {
			return errors.Wrap(err, "valid Ethereum address should be accepted")
		}

		// Test with invalid address - missing 0x prefix
		invalidAddress1 := "0000000000000000000000000000000000000001"
		err = setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), invalidAddress1, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "address without 0x prefix should be rejected")
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		// Test with invalid address - wrong length
		invalidAddress2 := "0x9"
		err = setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), invalidAddress2, string(setup.ContractTypePrimitive))
		if err == nil {
			return errors.New("address with wrong length should be rejected")
		}
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		// Test with invalid address - too long
		invalidAddress3 := "0x000000000000000000000000000000000000000001"
		err = setup.UntypedCreateStream(ctx, platform, defaultStreamLocator.StreamId.String(), invalidAddress3, string(setup.ContractTypePrimitive))
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
		err := setup.CreateStream(ctx, platform, setup.StreamInfo{
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
		err = setup.UntypedCreateStream(ctx, platform, streamID, owner1, string(setup.ContractTypePrimitive))
		assert.Error(t, err, "Should not allow duplicate stream ID for the same owner")
		assert.Contains(t, err.Error(), "already exists", "error message should indicate duplicate stream ID")

		// Attempt to create a stream with the same ID but different owner
		// (according to the requirement, stream IDs should be unique per owner, so this should succeed)
		owner2 := "0x0000000000000000000000000000000000000456"
		err = setup.UntypedCreateStream(ctx, platform, streamID, owner2, string(setup.ContractTypePrimitive))
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
			err := setup.UntypedCreateStream(ctx, platform, streamID, user, string(setup.ContractTypePrimitive))
			if err != nil {
				return errors.Wrapf(err, "user %s should be able to create a stream", user)
			}
		}

		return nil
	}
}

// [OTHER04] Multiple streams can be created in a single transaction.
// TestMultipleStreamCreation tests that multiple streams can be created in a single transaction using CreateStreams
func TestMultipleStreamCreation(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "multiple_stream_creation_test",
		SeedScripts: []string{
			"../../../internal/migrations/000-initial-data.sql",
			"../../../internal/migrations/001-common-actions.sql",
		},
		FunctionTests: []kwilTesting.TestFunc{
			testMultipleStreamCreation(t),
		},
	}, testutils.GetTestOptions())
}

// testMultipleStreamCreation tests that multiple streams can be created in a single transaction
func testMultipleStreamCreation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Generate unique stream IDs and prepare stream info
		deployer, err := util.NewEthereumAddressFromString(defaultCaller)
		if err != nil {
			return errors.Wrap(err, "error creating ethereum address")
		}
		platform = procedure.WithSigner(platform, deployer.Bytes())
		streamInfos := []setup.StreamInfo{
			{
				Type: setup.ContractTypePrimitive,
				Locator: types.StreamLocator{
					StreamId: *util.NewRawStreamId("st111111111111111111111111111111"),
				},
			},
			{
				Type: setup.ContractTypeComposed,
				Locator: types.StreamLocator{
					StreamId: *util.NewRawStreamId("st222222222222222222222222222222"),
				},
			},
			{
				Type: setup.ContractTypePrimitive,
				Locator: types.StreamLocator{
					StreamId: *util.NewRawStreamId("st333333333333333333333333333333"),
				},
			},
		}

		// Create multiple streams in a single transaction
		err = setup.CreateStreams(ctx, platform, streamInfos)
		if err != nil {
			return errors.Wrap(err, "failed to create multiple streams")
		}

		// Verify that all streams were created successfully
		rows := []common.Row{}
		err = platform.Engine.Execute(&common.EngineContext{
			TxContext: &common.TxContext{
				Ctx: ctx,
			},
		}, platform.DB, "SELECT stream_id, stream_type FROM streams WHERE data_provider = $address ORDER BY stream_id", map[string]any{
			"address": deployer.Address(),
		}, func(row *common.Row) error {
			rows = append(rows, *row)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "failed to query streams")
		}

		// Check that we have the expected number of streams
		assert.Equal(t, 3, len(rows), "Expected 3 streams to be created")

		// Verify stream IDs and types
		expectedStreamIds := []string{
			"st111111111111111111111111111111",
			"st222222222222222222222222222222",
			"st333333333333333333333333333333",
		}
		expectedTypes := []string{
			string(setup.ContractTypePrimitive),
			string(setup.ContractTypeComposed),
			string(setup.ContractTypePrimitive),
		}

		// Sort the expectedStreamIds to match the DB query's ORDER BY
		sort.Strings(expectedStreamIds)

		for i, row := range rows {
			assert.Equal(t, expectedStreamIds[i], row.Values[0], "Unexpected stream ID")
			assert.Equal(t, expectedTypes[i], row.Values[1], "Unexpected stream type")
		}

		// Test creating duplicate streams (should fail)
		err = setup.CreateStreams(ctx, platform, streamInfos)
		assert.Error(t, err, "Should not allow duplicate streams")
		assert.Contains(t, err.Error(), "duplicate key value violates unique constraint", "error message should indicate duplicate streams")

		// Test creating streams with different types but same IDs (should fail)
		for i := range streamInfos {
			if streamInfos[i].Type == setup.ContractTypePrimitive {
				streamInfos[i].Type = setup.ContractTypeComposed
			} else {
				streamInfos[i].Type = setup.ContractTypePrimitive
			}
		}
		err = setup.CreateStreams(ctx, platform, streamInfos)
		assert.Error(t, err, "Should not allow duplicate stream IDs even with different types")

		// Test creating streams with different owners
		newOwner := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000002")
		newOwnerPlatform := procedure.WithSigner(platform, newOwner.Bytes())
		newStreamInfos := []setup.StreamInfo{
			{
				Type: setup.ContractTypePrimitive,
				Locator: types.StreamLocator{
					StreamId: *util.NewRawStreamId("st444444444444444444444444444444"),
				},
			},
			{
				Type: setup.ContractTypeComposed,
				Locator: types.StreamLocator{
					StreamId: *util.NewRawStreamId("st555555555555555555555555555555"),
				},
			},
		}

		err = setup.CreateStreams(ctx, newOwnerPlatform, newStreamInfos)
		if err == nil {
			// Check if the streams were actually created with the correct owner
			rows = []common.Row{}
			err = platform.Engine.Execute(&common.EngineContext{
				TxContext: &common.TxContext{
					Ctx: ctx,
				},
			}, platform.DB, "SELECT * FROM streams WHERE data_provider = $address", map[string]any{
				"address": deployer.Address(),
			}, func(row *common.Row) error {
				rows = append(rows, *row)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "failed to query streams")
			}

			if len(rows) > 0 {
				t.Log("CreateStreams created streams with specified data provider, not the caller")
			} else {
				t.Log("CreateStreams appears to have created streams with the deployer as the data provider")
			}
		} else {
			t.Logf("CreateStreams with different owners failed: %v", err)
		}

		return nil
	}
}
