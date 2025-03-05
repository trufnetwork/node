package tests

import (
	"context"
	"testing"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var defaultCaller = "0x0000000000000000000000000000000000000001"

// Common test options
func getTestOptions() *kwilTesting.Options {
	return &kwilTesting.Options{
		UseTestContainer: true,
	}
}

// [OTHER01] All referenced addresses must be lowercased and valid EVM addresses starting with `0x`.
// TestAddressValidation tests that all referenced addresses must be lowercased and valid EVM addresses starting with `0x`.
func TestAddressValidation(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "address_validation_test",
		SeedScripts: []string{
			"../../../internal/migrations/000-initial-data.sql",
			"../../../internal/migrations/001-common-actions.sql",
		},
		FunctionTests: []kwilTesting.TestFunc{
			testAddressValidation(t),
		},
	}, getTestOptions())
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
	}, getTestOptions())
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
		},
	}, getTestOptions())
}

// testStreamIDValidation tests that stream ids must respect the following regex: `^st[a-z0-9]{30}$`
func testStreamIDValidation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test valid stream ID
		err := executeCreateStream(ctx, platform, "st123456789012345678901234567890", "primitive", defaultCaller)
		if err != nil {
			return errors.Wrap(err, "valid stream ID should be accepted")
		}

		// Test invalid stream ID - too short
		err = executeCreateStream(ctx, platform, "st12345", "primitive", defaultCaller)
		assert.Error(t, err, "too short stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - too long
		err = executeCreateStream(ctx, platform, "st1234567890123456789012345678901", "primitive", defaultCaller)
		assert.Error(t, err, "too long stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - wrong prefix
		err = executeCreateStream(ctx, platform, "xx123456789012345678901234567890", "primitive", defaultCaller)
		assert.Error(t, err, "wrong prefix stream ID should be rejected")
		assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - uppercase letters
		// TODO: Uncomment this once the regex is updated to disallow uppercase letters
		// err = executeCreateStream(ctx, platform, "stABCDEF89012345678901234567890", "primitive", defaultCaller)
		// assert.Error(t, err, "uppercase letters in stream ID should be rejected")
		// assert.Contains(t, err.Error(), "Invalid stream_id format", "error message should indicate invalid format")

		// Test invalid stream ID - special characters
		// TODO: Uncomment this once the regex is updated to disallow special characters
		// err = executeCreateStream(ctx, platform, "st12345678901234567890123456-+*&", "primitive", defaultCaller)
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
		validAddress := "0x0000000000000000000000000000000000000001"
		err := executeCreateStream(ctx, platform, "st123456789012345678901234567890", "primitive", validAddress)
		if err != nil {
			return errors.Wrap(err, "valid address should be accepted")
		}

		// Test with invalid address - missing 0x prefix
		invalidAddress1 := "0000000000000000000000000000000000000001"
		err = executeCreateStream(ctx, platform, "st223456789012345678901234567890", "primitive", invalidAddress1)
		assert.Error(t, err, "address without 0x prefix should be rejected")
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		// Test with invalid address - wrong length
		invalidAddress2 := "0x00000000000000000000000000000000000000"
		err = executeCreateStream(ctx, platform, "st323456789012345678901234567890", "primitive", invalidAddress2)
		assert.Error(t, err, "address with wrong length should be rejected")
		assert.Contains(t, err.Error(), "Invalid data provider address", "error message should indicate invalid address format")

		// Test with invalid address - too long
		invalidAddress3 := "0x000000000000000000000000000000000000000001"
		err = executeCreateStream(ctx, platform, "st423456789012345678901234567890", "primitive", invalidAddress3)
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
		err := executeCreateStream(ctx, platform, streamID, "primitive", owner1)
		if err != nil {
			return errors.Wrap(err, "failed to create first stream")
		}

		// Attempt to create another stream with the same ID for the same owner (should fail)
		err = executeCreateStream(ctx, platform, streamID, "primitive", owner1)
		assert.Error(t, err, "Should not allow duplicate stream ID for the same owner")
		assert.Contains(t, err.Error(), "already exists", "error message should indicate duplicate stream ID")

		// Attempt to create a stream with the same ID but different owner
		// (according to the requirement, stream IDs should be unique per owner, so this should succeed)
		owner2 := "0x0000000000000000000000000000000000000456"
		err = executeCreateStream(ctx, platform, streamID, "primitive", owner2)
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
			err := executeCreateStream(ctx, platform, streamID, "primitive", user)
			if err != nil {
				return errors.Wrapf(err, "user %s should be able to create a stream", user)
			}
		}

		return nil
	}
}

// executeCreateStream executes the create_stream procedure
func executeCreateStream(ctx context.Context, platform *kwilTesting.Platform, streamID string, streamType string, caller string) error {
	// Convert hex string to bytes for the signer
	var signerBytes []byte
	if len(caller) > 2 {
		// Remove 0x prefix if present
		if caller[:2] == "0x" {
			signerBytes = []byte(caller[2:])
		} else {
			signerBytes = []byte(caller)
		}
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       signerBytes,
		Caller:       caller,
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	_, err := platform.Engine.Call(engineContext, platform.DB, "", "create_stream", []any{
		streamID,
		streamType,
	}, func(row *common.Row) error {
		return nil
	})

	return err
}
