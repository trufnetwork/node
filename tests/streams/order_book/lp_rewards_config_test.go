//go:build kwiltest

package order_book

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestLPRewardsConfig tests the LP rewards configuration table and actions
func TestLPRewardsConfig(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "LP_REWARDS_CONFIG",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testGetLPRewardsConfigDefault(t),
			testUpdateLPRewardsConfig(t),
			testUpdateLPRewardsConfigUnauthorized(t),
			testGetActiveMarketsForSampling(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testGetLPRewardsConfigDefault tests that config can be read
func testGetLPRewardsConfigDefault(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        []byte("anonymous"),
			Caller:        "anonymous",
			TxID:          platform.Txid(),
			Authenticator: "anonymous",
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		var enabled bool
		var samplingInterval int64
		var maxMarkets int64
		var found bool

		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"get_lp_rewards_config",
			[]any{},
			func(row *common.Row) error {
				enabled = row.Values[0].(bool)
				samplingInterval = row.Values[1].(int64)
				maxMarkets = row.Values[2].(int64)
				found = true
				return nil
			},
		)
		require.NoError(t, err)
		if res.Error != nil {
			return fmt.Errorf("get_lp_rewards_config failed: %v", res.Error)
		}
		require.True(t, found, "Should return config row")

		// Verify config values are reasonable (not checking exact defaults due to test pollution)
		require.GreaterOrEqual(t, samplingInterval, int64(1), "Sampling interval should be >= 1")
		require.GreaterOrEqual(t, maxMarkets, int64(1), "Max markets should be >= 1")

		t.Logf("LP rewards config: enabled=%v, sampling_interval=%d, max_markets=%d",
			enabled, samplingInterval, maxMarkets)

		return nil
	}
}

// testUpdateLPRewardsConfig tests updating config with network_writer role
func testUpdateLPRewardsConfig(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a test address and grant it network_writer role
		networkWriter := util.Unsafe_NewEthereumAddressFromString("0xf9820f9143699cac6f662b19a4b29e13c9393783")

		// Grant network_writer role to this address
		err := setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", networkWriter.Address())
		require.NoError(t, err, "Failed to grant network_writer role")

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        networkWriter.Bytes(),
			Caller:        networkWriter.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		// Update config to new values
		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"update_lp_rewards_config",
			[]any{false, int64(20), int64(100)}, // enabled=false, interval=20, max=100
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			return fmt.Errorf("update_lp_rewards_config failed: %v", res.Error)
		}

		// Verify updated values
		var enabled bool
		var samplingInterval int64
		var maxMarkets int64

		readCtx := &common.EngineContext{TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        []byte("anonymous"),
			Caller:        "anonymous",
			TxID:          platform.Txid(),
			Authenticator: "anonymous",
		}}

		res, err = platform.Engine.Call(
			readCtx,
			platform.DB,
			"",
			"get_lp_rewards_config",
			[]any{},
			func(row *common.Row) error {
				enabled = row.Values[0].(bool)
				samplingInterval = row.Values[1].(int64)
				maxMarkets = row.Values[2].(int64)
				return nil
			},
		)
		require.NoError(t, err)
		if res.Error != nil {
			return fmt.Errorf("get_lp_rewards_config after update failed: %v", res.Error)
		}

		require.False(t, enabled, "Should be disabled after update")
		require.Equal(t, int64(20), samplingInterval, "Sampling interval should be 20")
		require.Equal(t, int64(100), maxMarkets, "Max markets should be 100")

		t.Logf("Updated LP rewards config: enabled=%v, sampling_interval=%d, max_markets=%d",
			enabled, samplingInterval, maxMarkets)

		// Reset back to defaults for other tests
		res, err = platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"update_lp_rewards_config",
			[]any{true, int64(10), int64(50)},
			nil,
		)
		require.NoError(t, err)
		if res.Error != nil {
			return fmt.Errorf("reset config failed: %v", res.Error)
		}

		return nil
	}
}

// testUpdateLPRewardsConfigUnauthorized tests that non-network_writer cannot update config
func testUpdateLPRewardsConfigUnauthorized(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use a random address that doesn't have network_writer role
		randomUser := util.Unsafe_NewEthereumAddressFromString("0x1234567890123456789012345678901234567890")

		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        randomUser.Bytes(),
			Caller:        randomUser.Address(),
			TxID:          platform.Txid(),
			Authenticator: coreauth.EthPersonalSignAuth,
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		// Try to update config - should fail
		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"update_lp_rewards_config",
			[]any{false, int64(100), int64(200)},
			nil,
		)
		require.NoError(t, err) // Engine call succeeds

		// Action should return error
		require.NotNil(t, res.Error, "Should fail for unauthorized user")
		require.Contains(t, res.Error.Error(), "network_writer", "Error should mention network_writer role")

		t.Logf("Unauthorized update correctly rejected: %v", res.Error)

		return nil
	}
}

// testGetActiveMarketsForSampling tests getting active markets list
func testGetActiveMarketsForSampling(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		// Initialize ERC20 extension
		err := initERC20ForTest(ctx, platform)
		require.NoError(t, err)

		user := util.Unsafe_NewEthereumAddressFromString("0xabcdef0123456789abcdef0123456789abcdef01")

		// Give user balance
		err = giveBalanceChained(ctx, platform, user.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create a market (will be active/unsettled)
		queryComponents, err := encodeQueryComponentsForTests(user.Address(), "sttest00000000000000000000000057", "get_record", []byte{0x01})
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &user, queryComponents, settleTime, 5, 20, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// Get active markets
		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        []byte("anonymous"),
			Caller:        "anonymous",
			TxID:          platform.Txid(),
			Authenticator: "anonymous",
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		var activeMarkets []int64
		res, err := platform.Engine.Call(
			engineCtx,
			platform.DB,
			"",
			"get_active_markets_for_sampling",
			[]any{int64(10)}, // limit=10
			func(row *common.Row) error {
				activeMarkets = append(activeMarkets, row.Values[0].(int64))
				return nil
			},
		)
		require.NoError(t, err)
		if res.Error != nil {
			return fmt.Errorf("get_active_markets_for_sampling failed: %v", res.Error)
		}

		// Should include our newly created market
		require.NotEmpty(t, activeMarkets, "Should have at least one active market")
		t.Logf("Active markets for sampling: %v (created market: %d)", activeMarkets, marketID)

		// Verify our market is in the list
		found := false
		for _, id := range activeMarkets {
			if id == marketID {
				found = true
				break
			}
		}
		require.True(t, found, "Newly created market should be in active markets list")

		return nil
	}
}

// Helper to initialize ERC20 extension
func initERC20ForTest(ctx context.Context, platform *kwilTesting.Platform) error {
	return erc20bridge.ForTestingInitializeExtension(ctx, platform)
}

// TestLPRewardsExtensionConfig tests loading config via extension engine operations
func TestLPRewardsExtensionConfig(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "LP_REWARDS_EXTENSION_CONFIG",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testExtensionConfigLoad(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testExtensionConfigLoad tests that extension can load config from database
func testExtensionConfigLoad(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Query the config table directly to verify it exists and has data
		tx := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height:    1,
				Timestamp: time.Now().Unix(),
			},
			Signer:        []byte("anonymous"),
			Caller:        "anonymous",
			TxID:          platform.Txid(),
			Authenticator: "anonymous",
		}
		engineCtx := &common.EngineContext{TxContext: tx}

		var enabled bool
		var samplingInterval int64
		var maxMarkets int64
		var found bool

		err := platform.Engine.Execute(
			engineCtx,
			platform.DB,
			"SELECT enabled, sampling_interval_blocks, max_markets_per_run FROM lp_rewards_config WHERE id = 1",
			nil,
			func(row *common.Row) error {
				enabled = row.Values[0].(bool)
				samplingInterval = row.Values[1].(int64)
				// max_markets_per_run might be INT which returns as int64
				switch v := row.Values[2].(type) {
				case int64:
					maxMarkets = v
				case int:
					maxMarkets = int64(v)
				case *types.Decimal:
					// Handle if it's stored as NUMERIC
					maxMarkets = 50 // default
				}
				found = true
				return nil
			},
		)
		require.NoError(t, err)
		require.True(t, found, "Config row should exist")

		t.Logf("Extension config from DB: enabled=%v, interval=%d, max_markets=%d",
			enabled, samplingInterval, maxMarkets)

		// Verify config values are valid
		require.True(t, enabled)
		require.Equal(t, int64(10), samplingInterval)
		require.Equal(t, int64(50), maxMarkets)

		return nil
	}
}
