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
		// Mint 10 pairs (locks 10 TRUF) -> holds 10 YES, holds 10 NO (listed)
		// Collateral is split 50/50 between outcomes (-5 each)
		err = callPlaceSplitLimitOrder(ctx, platform, &userB, int(marketID), 60, 10)
		require.NoError(t, err)
		
		impacts, err = getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, impacts, 3)
		
		// Index 1 & 2 are User B's split order impacts (YES and NO)
		var userBSplitYes *NetImpact
		var userBSplitNo *NetImpact
		for i := 1; i < 3; i++ {
			if impacts[i].Outcome {
				userBSplitYes = &impacts[i]
			} else {
				userBSplitNo = &impacts[i]
			}
		}
		require.NotNil(t, userBSplitYes)
		require.NotNil(t, userBSplitNo)
		require.Equal(t, int64(10), userBSplitYes.SharesChange)
		require.Equal(t, toWei("-5").String(), userBSplitYes.CollateralChange.String())
		require.Equal(t, int64(10), userBSplitNo.SharesChange)
		require.Equal(t, toWei("-5").String(), userBSplitNo.CollateralChange.String())

		// 5. User B sells YES holdings to User A
		err = callPlaceSellOrder(ctx, platform, &userB, int(marketID), true, 60, 10)
		require.NoError(t, err)

		// Verify impacts after match
		impacts, err = getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, impacts, 5)
		
		// Index 3 & 4 should be the match impacts for Seller (B) and Buyer (A)
		var matchSeller *NetImpact
		var matchBuyer *NetImpact
		for i := 3; i < 5; i++ {
			if impacts[i].SharesChange < 0 {
				matchSeller = &impacts[i]
			} else {
				matchBuyer = &impacts[i]
			}
		}
		
		require.NotNil(t, matchSeller, "match seller impact not found")
		require.NotNil(t, matchBuyer, "match buyer impact not found")
		
		// Seller (B) lost 10 YES shares, gained 6 TRUF
		require.Equal(t, int64(-10), matchSeller.SharesChange)
		require.Equal(t, toWei("6").String(), matchSeller.CollateralChange.String())
		
		// Buyer (A) gained 10 YES shares, 0 collateral change
		require.Equal(t, int64(10), matchBuyer.SharesChange)
		require.Equal(t, toWei("0").String(), matchBuyer.CollateralChange.String())

		// 6. Test Cancel Order
		err = callPlaceBuyOrder(ctx, platform, &userA, int(marketID), true, 50, 10)
		require.NoError(t, err)
		
		err = callCancelOrder(ctx, platform, &userA, int(marketID), true, -50)
		require.NoError(t, err)
		
		impacts, err = getNetImpacts(ctx, platform, int(marketID))
		require.NoError(t, err)
		require.Len(t, impacts, 7)
		
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
		
		// Expected impacts:
		// 1. Initial split (NO): +10 shares, -5 TRUF (split collateral)
		// 2. Initial split (YES): +10 shares, -5 TRUF (split collateral)
		// 3. Settlement payout (YES): +9.8 TRUF
		// 4. Validator reward (YES): +0.025 TRUF
		require.Len(t, impacts, 4)
		
		// Verify split collateral impacts
		var splitYes, splitNo *NetImpact
		for i := 0; i < 2; i++ {
			if impacts[i].Outcome {
				splitYes = &impacts[i]
			} else {
				splitNo = &impacts[i]
			}
		}
		require.Equal(t, toWei("-5").String(), splitYes.CollateralChange.String())
		require.Equal(t, toWei("-5").String(), splitNo.CollateralChange.String())

		// Find payout impact
		var settlementPayout *NetImpact
		var validatorReward *NetImpact
		for _, imp := range impacts {
			if imp.SharesChange == 0 && !imp.IsNegative {
				if imp.CollateralChange.String() == toWei("9.8").String() {
					settlementPayout = &imp
				} else if imp.CollateralChange.String() == "25000000000000000" {
					validatorReward = &imp
				}
			}
		}
		
		require.NotNil(t, settlementPayout, "settlement payout impact not found")
		require.Equal(t, true, settlementPayout.Outcome, "payout should be recorded against winning outcome")
		
		require.NotNil(t, validatorReward, "validator reward impact not found")
		require.Equal(t, true, validatorReward.Outcome, "validator reward should be recorded against winning outcome")

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

	res, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"",
		"process_settlement",
		[]any{marketID, winningOutcome},
		nil,
	)
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}
