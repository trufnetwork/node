//go:build kwiltest

package order_book

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestLPTracking is the main test suite for LP eligibility tracking (Issue 9A)
func TestLPTracking(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_09A_LPTracking",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			// Category A: LP Qualification Tests
			testLPQualificationWithinSpread(t),
			testLPQualificationOutsideSpread(t),
			testLPQualificationBelowMinSize(t),
			testLPQualificationExactBoundary(t),

			// Category B: LP Volume Accumulation Tests
			testLPMultipleOrdersAccumulate(t),
			testLPSameUserMultipleMarkets(t),

			// Category C: LP Stats Query Tests
			testGetLPStatsEmpty(t),
			testGetLPStatsSingleLP(t),
			testGetLPStatsMultipleLPs(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// =============================================================================
// Category A: LP Qualification Tests
// =============================================================================

// testLPQualificationWithinSpread tests that users qualify as LPs when spread is tight
func testLPQualificationWithinSpread(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

		// Setup: Initialize ERC20 extension
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give user balance (200 TRUF = 2 for market creation + buffer for orders)
		err = giveBalanceChained(ctx, platform, userAddr.Address(), "200000000000000000000")
		require.NoError(t, err)

		// Create market with default parameters (max_spread=5, min_order_size=20)
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Will it rain tomorrow?", 24, 5, 20)
		require.NoError(t, err)

		// Place qualifying split order: 50 shares @ 52/48 (distance=2, within spread of 5)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 52, 50)
		require.NoError(t, err)

		// Verify LP was tracked
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 1, "Should have 1 LP tracked")

		lp := lps[0]
		require.Equal(t, userAddr.Bytes(), lp.WalletAddress, "LP wallet address should match")
		require.Equal(t, int64(50), lp.SplitOrderAmount, "LP volume should be 50")
		require.Equal(t, int16(52), lp.LastOrderTruePrice, "True price should be 52")
		require.Equal(t, int16(48), lp.LastOrderFalsePrice, "False price should be 48")
		require.Equal(t, 1, lp.TotalQualifiedOrders, "Should have 1 qualified order")

		return nil
	}
}

// testLPQualificationOutsideSpread tests that users don't qualify when spread is too wide
func testLPQualificationOutsideSpread(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "200000000000000000000") // 200 TRUF
		require.NoError(t, err)

		// Create market with default parameters (max_spread=5)
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Will it rain?", 24, 5, 20)
		require.NoError(t, err)

		// Place non-qualifying split order: 50 shares @ 60/40 (distance=10, exceeds spread of 5)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 60, 50)
		require.NoError(t, err)

		// Verify LP was NOT tracked
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 0, "Should have 0 LPs tracked (spread too wide)")

		return nil
	}
}

// testLPQualificationBelowMinSize tests that users don't qualify when amount is too small
func testLPQualificationBelowMinSize(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "200000000000000000000") // 200 TRUF
		require.NoError(t, err)

		// Create market with default parameters (min_order_size=20)
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Will it rain?", 24, 5, 20)
		require.NoError(t, err)

		// Place non-qualifying split order: 15 shares @ 52/48 (spread OK, but amount < 20)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 52, 15)
		require.NoError(t, err)

		// Verify LP was NOT tracked
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 0, "Should have 0 LPs tracked (amount too small)")

		return nil
	}
}

// testLPQualificationExactBoundary tests edge case where spread equals max_spread exactly
func testLPQualificationExactBoundary(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "110000000000000000000") // 110 TRUF (2 for market + 100 for order + buffer)
		require.NoError(t, err)

		// Create market with default parameters (max_spread=5)
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Will it rain?", 24, 5, 20)
		require.NoError(t, err)

		// Place split order at exact boundary: 100 shares @ 55/45 (distance=5, equals max_spread)
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 55, 100)
		require.NoError(t, err)

		// Verify LP WAS tracked (boundary is inclusive: <=)
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 1, "Should have 1 LP tracked (boundary is inclusive)")

		lp := lps[0]
		require.Equal(t, int64(100), lp.SplitOrderAmount, "LP volume should be 100")

		return nil
	}
}

// =============================================================================
// Category B: LP Volume Accumulation Tests
// =============================================================================

// testLPMultipleOrdersAccumulate tests that multiple qualifying orders accumulate volume
func testLPMultipleOrdersAccumulate(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x5555555555555555555555555555555555555555")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create market
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Will it rain?", 24, 5, 20)
		require.NoError(t, err)

		// Place first qualifying order: 50 shares @ 52/48
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 52, 50)
		require.NoError(t, err)

		// Verify first order tracked
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 1)
		require.Equal(t, int64(50), lps[0].SplitOrderAmount, "First order: 50")
		require.Equal(t, 1, lps[0].TotalQualifiedOrders)

		// Place second qualifying order: 30 shares @ 53/47
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 53, 30)
		require.NoError(t, err)

		// Verify volume accumulated
		lps, err = getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 1)
		require.Equal(t, int64(80), lps[0].SplitOrderAmount, "Total volume: 50+30=80")
		require.Equal(t, 2, lps[0].TotalQualifiedOrders, "Should have 2 qualified orders")

		// Verify last order prices updated
		require.Equal(t, int16(53), lps[0].LastOrderTruePrice, "Last true price should be 53")
		require.Equal(t, int16(47), lps[0].LastOrderFalsePrice, "Last false price should be 47")

		// Place third qualifying order: 70 shares @ 54/46
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 54, 70)
		require.NoError(t, err)

		// Verify final accumulation
		lps, err = getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 1)
		require.Equal(t, int64(150), lps[0].SplitOrderAmount, "Total volume: 50+30+70=150")
		require.Equal(t, 3, lps[0].TotalQualifiedOrders)
		require.Equal(t, int16(54), lps[0].LastOrderTruePrice)

		return nil
	}
}

// testLPSameUserMultipleMarkets tests that same user tracked separately per market
func testLPSameUserMultipleMarkets(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x6666666666666666666666666666666666666666")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Create first market
		queryID1, err := createTestMarket(ctx, platform, &userAddr, "Market 1", 24, 5, 20)
		require.NoError(t, err)

		// Create second market
		queryID2, err := createTestMarket(ctx, platform, &userAddr, "Market 2", 24, 5, 20)
		require.NoError(t, err)

		// Place order on market 1: 100 shares
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID1, 52, 100)
		require.NoError(t, err)

		// Place order on market 2: 200 shares
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID2, 53, 200)
		require.NoError(t, err)

		// Verify market 1 LP stats
		lps1, err := getLPStats(ctx, platform, queryID1)
		require.NoError(t, err)
		require.Len(t, lps1, 1)
		require.Equal(t, int64(100), lps1[0].SplitOrderAmount, "Market 1 volume: 100")

		// Verify market 2 LP stats (separate tracking)
		lps2, err := getLPStats(ctx, platform, queryID2)
		require.NoError(t, err)
		require.Len(t, lps2, 1)
		require.Equal(t, int64(200), lps2[0].SplitOrderAmount, "Market 2 volume: 200")

		return nil
	}
}

// =============================================================================
// Category C: LP Stats Query Tests
// =============================================================================

// testGetLPStatsEmpty tests query when no LPs exist
func testGetLPStatsEmpty(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x7777777777777777777777777777777777777777")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "200000000000000000000") // 200 TRUF
		require.NoError(t, err)

		// Create market
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Market with no LPs", 24, 5, 20)
		require.NoError(t, err)

		// Query LP stats (should be empty)
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 0, "Should have no LPs")

		return nil
	}
}

// testGetLPStatsSingleLP tests query with one LP
func testGetLPStatsSingleLP(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		userAddr := util.Unsafe_NewEthereumAddressFromString("0x8888888888888888888888888888888888888888")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		err = giveBalanceChained(ctx, platform, userAddr.Address(), "200000000000000000000") // 200 TRUF
		require.NoError(t, err)

		// Create market and place qualifying order
		queryID, err := createTestMarket(ctx, platform, &userAddr, "Single LP market", 24, 5, 20)
		require.NoError(t, err)

		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 52, 100)
		require.NoError(t, err)

		// Query LP stats
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 1)

		lp := lps[0]
		require.Equal(t, userAddr.Bytes(), lp.WalletAddress)
		require.Equal(t, int64(100), lp.SplitOrderAmount)
		require.Equal(t, int16(52), lp.LastOrderTruePrice)
		require.Equal(t, int16(48), lp.LastOrderFalsePrice)
		require.Equal(t, 1, lp.TotalQualifiedOrders)
		require.Greater(t, lp.FirstQualifiedAt, int64(0), "Timestamp should be set")
		require.Equal(t, lp.FirstQualifiedAt, lp.LastQualifiedAt, "First order: timestamps should match")

		return nil
	}
}

// testGetLPStatsMultipleLPs tests query with multiple LPs, sorted by volume
func testGetLPStatsMultipleLPs(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker for this test (each test is independent)
		balancePointCounter = 100
		lastBalancePoint = nil

		alice := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
		bob := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		charlie := util.Unsafe_NewEthereumAddressFromString("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

		// Setup
		err := erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Give all users balance (MUST use chained helper for multi-user)
		// Alice: 2 TRUF market creation + 100 shares = 102+ TRUF
		err = giveBalanceChained(ctx, platform, alice.Address(), "200000000000000000000")
		require.NoError(t, err)

		// Bob: 200 shares = 200+ TRUF
		err = giveBalanceChained(ctx, platform, bob.Address(), "250000000000000000000")
		require.NoError(t, err)

		// Charlie: 700 shares = 700+ TRUF
		err = giveBalanceChained(ctx, platform, charlie.Address(), "750000000000000000000")
		require.NoError(t, err)

		// Alice creates market
		queryID, err := createTestMarket(ctx, platform, &alice, "Multi-LP market", 24, 5, 20)
		require.NoError(t, err)

		// Alice places qualifying order: 100 shares
		err = callPlaceSplitLimitOrder(ctx, platform, &alice, queryID, 52, 100)
		require.NoError(t, err)

		// Bob places qualifying order: 200 shares
		err = callPlaceSplitLimitOrder(ctx, platform, &bob, queryID, 53, 200)
		require.NoError(t, err)

		// Charlie places qualifying order: 700 shares
		err = callPlaceSplitLimitOrder(ctx, platform, &charlie, queryID, 51, 700)
		require.NoError(t, err)

		// Query LP stats (should be sorted by volume DESC)
		lps, err := getLPStats(ctx, platform, queryID)
		require.NoError(t, err)
		require.Len(t, lps, 3, "Should have 3 LPs")

		// Verify sorting (highest volume first)
		require.Equal(t, charlie.Bytes(), lps[0].WalletAddress, "Charlie should be first (700)")
		require.Equal(t, int64(700), lps[0].SplitOrderAmount)

		require.Equal(t, bob.Bytes(), lps[1].WalletAddress, "Bob should be second (200)")
		require.Equal(t, int64(200), lps[1].SplitOrderAmount)

		require.Equal(t, alice.Bytes(), lps[2].WalletAddress, "Alice should be third (100)")
		require.Equal(t, int64(100), lps[2].SplitOrderAmount)

		return nil
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

// LPRecord represents a liquidity provider record from get_lp_stats
type LPRecord struct {
	ParticipantID        int
	WalletAddress        []byte
	SplitOrderAmount     int64
	TotalQualifiedOrders int
	FirstQualifiedAt     int64
	LastQualifiedAt      int64
	LastOrderTruePrice   int16
	LastOrderFalsePrice  int16
}

// getLPStats queries liquidity provider stats for a market using get_lp_stats action
func getLPStats(ctx context.Context, platform *kwilTesting.Platform, queryID int) ([]LPRecord, error) {
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	var lps []LPRecord
	_, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"",
		"get_lp_stats",
		[]any{queryID},
		func(row *common.Row) error {
			lp := LPRecord{
				ParticipantID:        int(row.Values[0].(int64)),      // INT → int64
				WalletAddress:        row.Values[1].([]byte),          // BYTEA
				SplitOrderAmount:     row.Values[2].(int64),           // INT8
				TotalQualifiedOrders: int(row.Values[3].(int64)),      // INT → int64
				FirstQualifiedAt:     row.Values[4].(int64),           // INT8
				LastQualifiedAt:      row.Values[5].(int64),           // INT8
				LastOrderTruePrice:   int16(row.Values[6].(int64)),    // INT → int64
				LastOrderFalsePrice:  int16(row.Values[7].(int64)),    // INT → int64
			}
			lps = append(lps, lp)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return lps, nil
}

// createTestMarket is a helper to create a market with default or custom parameters
func createTestMarket(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, queryText string, hoursUntilSettle int64, maxSpread int64, minOrderSize int64) (int, error) {
	queryHash := sha256.Sum256([]byte(queryText))
	settleTime := time.Now().Add(time.Duration(hoursUntilSettle) * time.Hour).Unix()

	var queryID int
	err := callCreateMarket(ctx, platform, signer, queryHash[:], settleTime, maxSpread, minOrderSize, func(row *common.Row) error {
		queryID = int(row.Values[0].(int64))
		return nil
	})
	return queryID, err
}
