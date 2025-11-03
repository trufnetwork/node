//go:build kwiltest

package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// Common deterministic values used across ERC20 bridge tests
const (
	TestChain          = "sepolia"
	TestExtensionAlias = "erc20_bridge_test"
	TestEscrowA        = "0x1111111111111111111111111111111111111111"
	TestEscrowB        = "0x2222222222222222222222222222222222222222"
	TestERC20          = "0x2222222222222222222222222222222222222222"
	TestUserA          = "0xabc0000000000000000000000000000000000001"
	TestUserB          = "0xabc0000000000000000000000000000000000002"
	TestUserC          = "0xabc0000000000000000000000000000000000003"
	TestUserD          = "0xabc0000000000000000000000000000000000004"
	TestAmount1        = "1000000000000000000" // 1.0 tokens
	TestAmount2        = "2000000000000000000" // 2.0 tokens
)

// seedAndRun is a helper that handles test execution with proper isolation
func seedAndRun(t TestingT, name string, fn kwilTesting.TestFunc) {
	seedStatements := migrations.GetSeedScriptStatements()

	// Wrap the test function to add singleton reset and cleanup
	wrappedFn := func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Run the actual test inside a transaction for rollback isolation
		tx, err := platform.DB.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback(ctx)

		txPlatform := &kwilTesting.Platform{
			Engine:   platform.Engine,
			DB:       tx,
			Deployer: platform.Deployer,
			Logger:   platform.Logger,
		}

		return fn(ctx, txPlatform)
	}

	testutils.RunSchemaTest(t.(*testing.T), kwilTesting.SchemaTest{
		Name:           name,
		SeedStatements: seedStatements,
		FunctionTests:  []kwilTesting.TestFunc{wrappedFn},
	}, &testutils.Options{Options: testutils.GetTestOptions()})
}

// engCtx creates a standard EngineContext for testing
func engCtx(ctx context.Context, platform *kwilTesting.Platform, caller string, height int64, overrideAuthz bool) *common.EngineContext {
	// Generate a leader public key for fee collection
	// This is required for actions that transfer fees to @leader_sender
	_, leaderPubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		panic(fmt.Sprintf("failed to generate leader key: %v", err))
	}
	leaderPub := leaderPubGeneric.(*crypto.Secp256k1PublicKey)

	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:           ctx,
			BlockContext:  &common.BlockContext{Height: height, Proposer: leaderPub},
			Signer:        platform.Deployer,
			Caller:        caller,
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		},
		OverrideAuthz: overrideAuthz,
	}
}

// TestingT interface for test functions (matches testutils)
type TestingT interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Cleanup(func())
}
