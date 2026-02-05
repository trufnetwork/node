//go:build kwiltest

package order_book

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"

	attestationTests "github.com/trufnetwork/node/tests/streams/attestation"
)

// TestSettlementPayouts tests that process_settlement() correctly pays winners
func TestSettlementPayouts(t *testing.T) {
	owner := util.Unsafe_NewEthereumAddressFromString("0x1111111111111111111111111111111111111111")

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_08_SettlementPayouts",
		SeedStatements: migrations.GetSeedScriptStatements(),
		Owner:          owner.Address(),
		FunctionTests: []kwilTesting.TestFunc{
			testWinnerReceivesFullPayout(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// testWinnerReceivesFullPayout verifies that a winner receives 100% payout (no fee)
// Scenario: User creates 100 YES shares, market settles as YES, user receives full 100 USDC
// Note: Settlement is zero-sum - losers fund winners, no redemption fee charged
func testWinnerReceivesFullPayout(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Use valid Ethereum address as user
		userAddr := util.Unsafe_NewEthereumAddressFromString("0x2222222222222222222222222222222222222222")
		platform.Deployer = userAddr.Bytes()

		// Setup attestation helper (for signing)
		helper := attestationTests.NewAttestationTestHelper(t, ctx, platform)

		// Create data provider FIRST
		err := setup.CreateDataProvider(ctx, platform, userAddr.Address())
		require.NoError(t, err)

		// Give user initial balance (injects to DB) - injects BOTH TRUF and USDC
		err = giveBalance(ctx, platform, userAddr.Address(), "500000000000000000000")
		require.NoError(t, err)

		// Initialize ERC20 extension AFTER balance (loads DB instances to singleton)
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Get USDC balance before market operations (payouts are in USDC)
		balanceBefore, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		t.Logf("User USDC balance before: %s", balanceBefore.String())

		// Use simple stream ID (exactly 32 characters)
		streamID := "stpayouttest00000000000000000000" // Exactly 32 chars
		dataProvider := userAddr.Address()

		// CRITICAL: create_stream + insert_records + get_record must share the SAME
		// engine context to ensure inserted data is visible within the transaction
		engineCtx := helper.NewEngineContext()

		// Create primitive stream
		createRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_stream",
			[]any{streamID, "primitive"},
			nil)
		require.NoError(t, err)
		if createRes.Error != nil {
			t.Fatalf("create_stream error: %v", createRes.Error)
		}
		t.Logf("✓ Created stream: %s", streamID)

		// Insert outcome data directly using the SAME engineCtx
		eventTime := int64(1000)

		// Create decimal value (1.0 = YES outcome)
		valueStr := "1.000000000000000000" // 1.0 with 18 decimal places
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(valueStr, 36, 18)
		require.NoError(t, err)
		t.Logf("✓ Parsed decimal value: %v", valueDecimal)

		// Insert using the same engineCtx so data is visible to subsequent calls
		insertRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "insert_records",
			[]any{
				[]string{dataProvider},
				[]string{streamID},
				[]int64{eventTime},
				[]*kwilTypes.Decimal{valueDecimal},
			},
			nil)
		require.NoError(t, err)
		if insertRes.Error != nil {
			t.Fatalf("insert_records error: %v", insertRes.Error)
		}
		t.Logf("✓ Inserted record: provider=%s, stream=%s, time=%d, value=%s",
			dataProvider, streamID, eventTime, valueStr)

		// Verify data was inserted by querying directly (reuse same context)
		var foundData bool
		getRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "get_record",
			[]any{
				dataProvider,
				streamID,
				int64(500),
				int64(1500),
				nil,
				false,
			},
			func(row *common.Row) error {
				t.Logf("Found data: event_time=%v, value=%v", row.Values[0], row.Values[1])
				foundData = true
				return nil
			})
		require.NoError(t, err)
		if getRes.Error != nil {
			t.Fatalf("get_record error: %v", getRes.Error)
		}
		require.True(t, foundData, "Data should be found in stream")

		// Request attestation for get_record
		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			dataProvider,
			streamID,
			int64(500),
			int64(1500),
			nil,
			false,
		})
		require.NoError(t, err)

		var requestTxID string
		var attestationHash []byte
		engineCtx = helper.NewEngineContext()
		res, err := platform.Engine.Call(engineCtx, platform.DB, "", "request_attestation",
			[]any{
				dataProvider,
				streamID,
				"get_record",
				argsBytes,
				false,
				nil,
			},
			func(row *common.Row) error {
				requestTxID = row.Values[0].(string)
				attestationHash = append([]byte(nil), row.Values[1].([]byte)...)
				return nil
			})
		require.NoError(t, err)
		if res.Error != nil {
			t.Logf("request_attestation error: %v", res.Error)
			require.NoError(t, res.Error, "request_attestation failed")
		}
		require.NotEmpty(t, requestTxID)
		require.NotEmpty(t, attestationHash)
		t.Logf("Created attestation: txID=%s, hash=%x", requestTxID, attestationHash)

		// Sign the attestation
		helper.SignAttestation(requestTxID)

		// Encode query components for create_market (must match attestation args!)
		queryComponents, err := encodeQueryComponentsForTests(
			dataProvider, streamID, "get_record", argsBytes)
		require.NoError(t, err)

		// Create market using query_components
		settleTime := int64(100) // Future timestamp
		maxSpread := int64(5)
		minOrderSize := int64(1)
		var queryID int

		// Use timestamp 50 for market creation (before settleTime)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 50
		createMarketRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "create_market",
			[]any{testExtensionName, queryComponents, settleTime, maxSpread, minOrderSize},
			func(row *common.Row) error {
				queryID = int(row.Values[0].(int64))
				return nil
			})
		require.NoError(t, err)
		if createMarketRes.Error != nil {
			t.Logf("create_market error: %v", createMarketRes.Error)
			require.NoError(t, createMarketRes.Error)
		}
		require.Greater(t, queryID, 0, "queryID should be positive")
		t.Logf("Created market ID: %d", queryID)

		// User places split limit order to create 100 YES shares
		// Cost: 100 USDC locked in vault
		err = callPlaceSplitLimitOrder(ctx, platform, &userAddr, queryID, 60, 100)
		require.NoError(t, err)

		// Verify user has 100 YES holdings
		positions, err := getPositions(ctx, platform, queryID)
		require.NoError(t, err)
		var yesHoldings *Position
		for i := range positions {
			if positions[i].Outcome && positions[i].Price == 0 {
				yesHoldings = &positions[i]
				break
			}
		}
		require.NotNil(t, yesHoldings, "User should have YES holdings")
		require.Equal(t, int64(100), yesHoldings.Amount)
		t.Logf("User has %d YES holdings before settlement", yesHoldings.Amount)

		// Settle the market with timestamp 200 (after settleTime)
		engineCtx = helper.NewEngineContext()
		engineCtx.TxContext.BlockContext.Timestamp = 200
		settleRes, err := platform.Engine.Call(engineCtx, platform.DB, "", "settle_market",
			[]any{queryID},
			nil)
		require.NoError(t, err)
		if settleRes.Error != nil {
			t.Logf("settle_market error: %v", settleRes.Error)
		}
		require.Nil(t, settleRes.Error, "settle_market should succeed")

		// Verify market is settled
		engineCtx = helper.NewEngineContext()
		var settled bool
		var winningOutcome *bool
		err = platform.Engine.Execute(engineCtx, platform.DB,
			`SELECT settled, winning_outcome FROM ob_queries WHERE id = $id`,
			map[string]any{"id": queryID},
			func(row *common.Row) error {
				settled = row.Values[0].(bool)
				if row.Values[1] != nil {
					outcome := row.Values[1].(bool)
					winningOutcome = &outcome
				}
				return nil
			})
		require.NoError(t, err)
		require.True(t, settled, "market should be marked as settled")
		require.NotNil(t, winningOutcome, "winning_outcome should be set")
		require.True(t, *winningOutcome, "outcome should be TRUE (YES) since value was 1.0")

		// NEW: Verify all positions are deleted (payout processing)
		positionsAfter, err := getPositions(ctx, platform, queryID)
		require.NoError(t, err)
		require.Empty(t, positionsAfter, "All positions should be deleted after settlement")
		t.Logf("✓ All positions cleared after settlement")

		// Verify user received 98% payout (2% redemption fee for LP rewards)
		// Expected: 100 shares × $0.98 = 98 USDC (per Latest.md: 2% fee to LPs)
		balanceAfter, err := getUSDCBalance(ctx, platform, userAddr.Address())
		require.NoError(t, err)
		t.Logf("User USDC balance after: %s", balanceAfter.String())

		// Net USDC change: -100 USDC (locked) + 98 USDC (payout after 2% fee) = -2 USDC
		// Per Latest.md: "2% rewards commission taken from every settlement transaction"
		// This fee is distributed to Liquidity Providers based on sampled rewards
		// Note: Market creation fee (2 TRUF) is separate from USDC
		netChange := new(big.Int).Sub(balanceAfter, balanceBefore)
		// Expected: -2 USDC (2% of 100 USDC = 2 USDC fee)
		expectedFee := new(big.Int).Mul(big.NewInt(2), big.NewInt(1e18)) // 2 USDC in wei
		expectedNetChange := new(big.Int).Neg(expectedFee)               // -2 USDC
		require.Equal(t, expectedNetChange.String(), netChange.String(),
			"USDC net change should be -2 USDC (2% redemption fee for LP rewards)")
		t.Logf("✓ Net USDC balance change: %s (2%% fee = 2 USDC)", netChange.String())

		return nil
	}
}
