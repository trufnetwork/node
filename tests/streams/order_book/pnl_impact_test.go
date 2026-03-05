//go:build kwiltest

package order_book

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
	"github.com/trufnetwork/kwil-db/core/types"
)

type NetImpact struct {
	ID               int
	TxHash           []byte
	QueryID          int
	ParticipantID    int
	Outcome          bool
	SharesChange     int64
	CollateralChange *big.Int
	IsNegative       bool
	Timestamp        int64
}

// TestPnLImpact verifies that the ob_net_impacts table is correctly populated
func TestPnLImpact(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "ORDER_BOOK_PnLImpact",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testPnLImpactTrading(t),
			testPnLImpactSettlement(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testPnLImpactTrading(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		userA := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
		userB := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

		// 1. Setup: Give balances (both TRUF and USDC) FIRST
		err := InjectDualBalance(ctx, platform, userA.Address(), "100000000000000000000") // 100 TRUF/USDC
		require.NoError(t, err)

		err = InjectDualBalance(ctx, platform, userB.Address(), "100000000000000000000") // 100 TRUF/USDC
		require.NoError(t, err)

		// Initialize ERC20 extension AFTER injection so it sees the initial points
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// 2. Create Market
		queryComponents, err := encodeQueryComponentsForTests(userA.Address(), "st_pnl_test_000000000000000000001", "get_record", nil)
		require.NoError(t, err)
		settleTime := time.Now().Add(24 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userA, queryComponents, settleTime, 5, 1, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// 3. User A places Buy Order: 10 shares @ $0.60
		// Collateral: 10 * 60 * 10^16 = 6 * 10^18
		err = callPlaceBuyOrder(ctx, platform, &userA, int(marketID), true, 60, 10)
		require.NoError(t, err)

		// Verify User A impact: Collateral -6 TRUF, Shares 0 (not filled yet)
		impacts, err := getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, impacts, 1, "should have 1 impact record")
		require.Equal(t, int64(0), impacts[0].SharesChange)
		require.Equal(t, toWei("-6").String(), impacts[0].CollateralChange.String())

		// 4. User B places Split Order: 10 pairs @ $0.60
		// Mint 10 pairs (locks 10 TRUF) -> holds 10 YES, sells 10 NO @ 0.40
		err = callPlaceSplitLimitOrder(ctx, platform, &userB, int(marketID), 60, 10)
		require.NoError(t, err)
		
		// User B now has 10 YES holdings. Let's sell them to User A.
		err = callPlaceSellOrder(ctx, platform, &userB, int(marketID), true, 60, 10)
		require.NoError(t, err)

		// Verify impacts after match
		impacts, err = getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, impacts, 3)
		
		// Check User B split order impact (index 1)
		require.Equal(t, int64(10), impacts[1].SharesChange)
		require.Equal(t, toWei("-10").String(), impacts[1].CollateralChange.String())
		
		// Check User B sell order impact (index 2)
		require.Equal(t, int64(-10), impacts[2].SharesChange)
		require.Equal(t, toWei("6").String(), impacts[2].CollateralChange.String())

		// 5. Test Cancel Order
		err = callPlaceBuyOrder(ctx, platform, &userA, int(marketID), true, 50, 10)
		require.NoError(t, err)
		
		err = callCancelOrder(ctx, platform, &userA, int(marketID), true, -50)
		require.NoError(t, err)
		
		impacts, err = getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, impacts, 5)
		
		// Last impact should be the cancel refund (+5 TRUF)
		lastImpact := impacts[len(impacts)-1]
		require.Equal(t, int64(0), lastImpact.SharesChange)
		require.Equal(t, toWei("5").String(), lastImpact.CollateralChange.String())

		return nil
	}
}

func testPnLImpactSettlement(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Reset balance point tracker
		lastBalancePoint = nil
		lastTrufBalancePoint = nil

		userA := util.Unsafe_NewEthereumAddressFromString("0xCCCCCCC333333333333333333333333333333333")
		
		// Setup FIRST: Inject balance
		err := InjectDualBalance(ctx, platform, userA.Address(), "100000000000000000000")
		require.NoError(t, err)

		// Initialize extension
		err = erc20bridge.ForTestingInitializeExtension(ctx, platform)
		require.NoError(t, err)

		// Create market
		queryComponents, err := encodeQueryComponentsForTests(userA.Address(), "st_pnl_settle_000000000000000000001", "get_record", nil)
		require.NoError(t, err)
		settleTime := time.Now().Add(1 * time.Hour).Unix()

		var marketID int64
		err = callCreateMarket(ctx, platform, &userA, queryComponents, settleTime, 5, 1, func(row *common.Row) error {
			marketID = row.Values[0].(int64)
			return nil
		})
		require.NoError(t, err)

		// User A buys 10 shares of YES @ $1.00 (via split order for simplicity)
		err = callPlaceSplitLimitOrder(ctx, platform, &userA, int(marketID), 50, 10)
		require.NoError(t, err)

		// Settle market with YES winning
		err = callProcessSettlement(t, ctx, platform, int(marketID), true)
		require.NoError(t, err)

		// Verify settlement impact
		impacts, err := getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		
		require.Len(t, impacts, 2)
		// Index 1 is settlement payout
		require.Equal(t, toWei("9.8").String(), impacts[1].CollateralChange.String())

		return nil
	}
}

// ===== HELPERS =====

func getNetImpacts(ctx context.Context, platform *kwilTesting.Platform, marketID int) ([]NetImpact, error) {
	tx := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		TxID:         platform.Txid(),
	}
	engineCtx := &common.EngineContext{TxContext: tx}

	var impacts []NetImpact
	err := platform.Engine.Execute(
		engineCtx,
		platform.DB,
		"SELECT id, tx_hash, query_id, participant_id, outcome, shares_change, collateral_change, is_negative, timestamp FROM ob_net_impacts WHERE query_id = $query_id ORDER BY id ASC",
		map[string]any{"$query_id": marketID},
		func(row *common.Row) error {
			mag := row.Values[6].(*types.Decimal).BigInt()
			isNeg := row.Values[7].(bool)
			
			colChange := new(big.Int).Set(mag)
			if isNeg {
				colChange.Neg(colChange)
			}

			imp := NetImpact{
				ID:               int(row.Values[0].(int64)),
				TxHash:           row.Values[1].([]byte),
				QueryID:          int(row.Values[2].(int64)),
				ParticipantID:    int(row.Values[3].(int64)),
				Outcome:          row.Values[4].(bool),
				SharesChange:     row.Values[5].(int64),
				CollateralChange: colChange,
				IsNegative:       isNeg,
				Timestamp:        row.Values[8].(int64),
			}
			impacts = append(impacts, imp)
			return nil
		},
	)

	return impacts, err
}

func callProcessSettlement(t require.TestingT, ctx context.Context, platform *kwilTesting.Platform, marketID int, winningOutcome bool) error {
	pub := NewTestProposerPub(t)

	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:    1,
			Timestamp: time.Now().Unix(),
			Proposer:  pub,
		},
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx, OverrideAuthz: true}

	_, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"",
		"process_settlement",
		[]any{marketID, winningOutcome},
		nil,
	)
	return err
}
