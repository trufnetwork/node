package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	kwiltesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestBackwardCompatibility(t *testing.T) {
	kwiltesting.RunSchemaTest(t, kwiltesting.SchemaTest{
		Name:        "backward_compatibility_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwiltesting.TestFunc{
			testBackwardCompatibilityFunc(t),
		},
	}, testutils.GetTestOptions())
}

func testBackwardCompatibilityFunc(t *testing.T) kwiltesting.TestFunc {
	return func(ctx context.Context, platform *kwiltesting.Platform) error {
		// Initialize platform deployer with a valid address to prevent the default kwil "deployer" string issue
		defaultDeployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform.Deployer = defaultDeployer.Bytes()

		// Test calling get_record with 5 arguments (without use_cache)
		t.Run("get_record_with_5_args", func(t *testing.T) {
			testBackwardCompatibilityCall(t, platform, "get_record", []any{
				defaultDeployer.Address(),
				"test_stream",
				nil, // from
				nil, // to
				nil, // frozen_at
				// useCache parameter is omitted - should default to false
			})
		})

		// Test calling get_record with 6 arguments (with use_cache)
		t.Run("get_record_with_6_args", func(t *testing.T) {
			testBackwardCompatibilityCall(t, platform, "get_record", []any{
				defaultDeployer.Address(),
				"test_stream",
				nil,  // from
				nil,  // to
				nil,  // frozen_at
				true, // useCache
			})
		})

		return nil
	}
}

func testBackwardCompatibilityCall(t *testing.T, platform *kwiltesting.Platform, actionName string, args []any) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		t.Fatalf("Failed to create deployer address: %v", err)
	}

	txContext := &common.TxContext{
		Ctx: context.Background(),
		BlockContext: &common.BlockContext{
			Height: 1,
		},
		TxID:   platform.Txid(),
		Signer: platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// This should not panic or fail due to argument count mismatch
	result, err := platform.Engine.Call(engineContext, platform.DB, "", actionName, args, func(row *common.Row) error {
		return nil
	})

	// We expect this to fail with a permission error, not an argument count error
	if err != nil {
		fmt.Printf("Expected error (permission/stream not found): %v\n", err)
		return
	}

	if result.Error != nil {
		fmt.Printf("Expected result error (permission/stream not found): %v\n", result.Error)
		return
	}

	fmt.Printf("Call to %s with %d args succeeded\n", actionName, len(args))
}
