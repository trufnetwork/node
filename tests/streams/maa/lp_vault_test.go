//go:build kwiltest

package maa

import (
	"context"
	"encoding/hex"
	"testing"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/sdk-go/core/util"
)

// LP VAULT OPERATOR — the first end-to-end MAA example (0MainGoal.md Example 1).
//
// A liquidity provider (the unrestricted owner) funds an agent wallet (the MAA); a delegated
// trading bot (the restricted agent) runs an order-book strategy AS the MAA, limited to an
// allow-list of order-book actions; the owner monitors the bot's activity and withdraws at any
// time, paying the agent its agreed commission — and the agent provably can NEVER move the
// funds out. This file drives that whole lifecycle against the REAL order book.
//
// It builds on the merged MAA stack: the 048 rule store, the 049 commission/withdraw actions,
// the new 050 by-wallet event getter, and the kwil-db pieces the route integrates — @caller
// rewritten to the MAA (reproduced here by callAsMAA, as route_e2e_test.go does) and the erc20
// MAARestricted token boundary that blocks every fund-exit for the restricted role while
// leaving collateral lock/unlock open so allow-listed trading survives.
//
// The bot's allow-list is the four real order-book trading actions in the default ("main")
// namespace; create_market is deliberately NOT allow-listed (market provisioning is not a
// liquidity-maintenance task — though its leader-paid fee would pass the boundary's write-fee
// carve-out, see data_agent_test.go) and settle_market is left to the network. The market here
// is created by an ordinary market-maker account, not the agent.

// lpActions is the canonical liquidity-vault-operator allow-list: place + amend + cancel on the
// order book, nothing that can move funds out.
var (
	lpNamespaces  = []string{"main", "main", "main", "main"}
	lpActions     = []string{"place_buy_order", "place_sell_order", "place_split_limit_order", "cancel_order"}
	lpBodyHashes  = [][]byte{nil, nil, nil, nil}
	lpSettleTime  = int64(2000000000) // far future; well past any block timestamp used below
	lpMarketBlock = int64(1000)       // a fixed block timestamp before lpSettleTime (trading open)
)

// Independent ordered-sync points for the LP-vault funding (distinct from withdraw_test's
// point=1 single-deposit path) so several wallets can be funded on the same bridge in one test.
var (
	lpUSDCPoint int64 = 7000
	lpUSDCPrev  *int64
	lpTRUFPoint int64 = 8000
	lpTRUFPrev  *int64
)

func TestMAALPVault(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_LPVault",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testLPVaultLifecycle(t),
			testLPVaultBotFillIsAudited(t),
			testLPVaultAgentCannotDrain(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// ---------------------------------------------------------------------------
// helpers (LP-vault specific; the order-book trading helpers live in package
// order_book's _test.go files and cannot be imported, so the minimal ones are
// replicated here)
// ---------------------------------------------------------------------------

// lpResetFunding clears the ordered-sync cursor and the per-bridge chaining pointers; call once
// at the start of each function test (each gets a fresh DB).
func lpResetFunding() {
	orderedsync.ForTestingReset()
	lpUSDCPrev = nil
	lpTRUFPrev = nil
}

// lpFundUSDC credits `to` on the hoodi_tt2 (USDC) collateral bridge, chaining to the previous
// USDC deposit so multiple wallets fund correctly under ordered-sync.
func lpFundUSDC(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, to, amount string) {
	t.Helper()
	lpUSDCPoint++
	p := lpUSDCPoint
	require.NoError(t, testerc20.InjectERC20Transfer(ctx, platform, wdChain, wdEscrow, wdERC20, to, to, amount, p, lpUSDCPrev))
	lpUSDCPrev = &p
}

// lpFundTRUF credits `to` on the hoodi_tt (TRUF) bridge — the market-creation fee is always
// charged in TRUF, independent of the collateral bridge.
func lpFundTRUF(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, to, amount string) {
	t.Helper()
	lpTRUFPoint++
	p := lpTRUFPoint
	require.NoError(t, testerc20.InjectERC20Transfer(ctx, platform, wdChain, wdTRUFEscrow, wdTRUFERC20, to, to, amount, p, lpTRUFPrev))
	lpTRUFPrev = &p
}

// lpCallAs invokes an action as an ordinary signer with an eth-personal-sign authenticator —
// the order-book actions resolve the caller through it. (callAs omits the authenticator, which
// is fine for the maa_* actions but not for the order book.)
func lpCallAs(ctx context.Context, platform *kwilTesting.Platform, caller util.EthereumAddress, action string, args []any) error {
	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: 1, Timestamp: lpMarketBlock},
		Signer:        caller.Bytes(),
		Caller:        caller.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "", action, args, func(*common.Row) error { return nil })
	if err != nil {
		return err
	}
	return res.Error
}

// createLPRule registers the liquidity-vault allow-list rule, signed by the restricted agent.
func createLPRule(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, restricted util.EthereumAddress, feeBps int64, salt []byte) []byte {
	t.Helper()
	var ruleID []byte
	err := callAs(ctx, platform, restricted, "maa_create_rule", []any{
		salt, "bps", feeBps, dec(t, "0"),
		lpNamespaces, lpActions, lpBodyHashes,
	}, func(row *common.Row) error {
		ruleID = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})
	require.NoError(t, err, "maa_create_rule (LP allow-list) should succeed")
	require.Len(t, ruleID, 32)
	return ruleID
}

// encodeLPQueryComponents builds the ABI-encoded (address,bytes32,string,bytes) query
// components create_market hashes. The stream need not exist — create_market only hashes these
// for the market id and never resolves them (settlement, which would, is out of scope here).
func encodeLPQueryComponents(dataProvider, streamID, actionID string) ([]byte, error) {
	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		dataProvider, streamID, int64(0), int64(999999999), nil, false,
	})
	if err != nil {
		return nil, err
	}
	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, err
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, err
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, err
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, err
	}
	abiArgs := gethAbi.Arguments{
		{Type: addressType, Name: "data_provider"},
		{Type: bytes32Type, Name: "stream_id"},
		{Type: stringType, Name: "action_id"},
		{Type: bytesType, Name: "args"},
	}
	var streamIDBytes [32]byte
	copy(streamIDBytes[:], []byte(streamID))
	return abiArgs.Pack(gethCommon.HexToAddress(dataProvider), streamIDBytes, actionID, argsBytes)
}

// createOBMarket creates a tradable order-book market on the hoodi_tt2 bridge, signed by an
// ordinary market-maker account (NOT the agent). It needs a block proposer (the fee goes to the
// leader) and TRUF balance for the 2-TRUF creation fee.
func createOBMarket(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, creator util.EthereumAddress, streamID string) int {
	t.Helper()
	_, proposerPub, err := crypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	qc, err := encodeLPQueryComponents(creator.Address(), streamID, "get_last_record")
	require.NoError(t, err)

	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: 1, Timestamp: lpMarketBlock, Proposer: proposerPub},
		Signer:        creator.Bytes(),
		Caller:        creator.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	var marketID int
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "", "create_market",
		[]any{wdBridge, qc, lpSettleTime, int64(5), int64(1)},
		func(row *common.Row) error { marketID = int(row.Values[0].(int64)); return nil })
	require.NoError(t, err)
	require.NoError(t, res.Error, "create_market should succeed")
	require.Greater(t, marketID, 0)
	return marketID
}

// lpEvent is a single order-event row from the 050 by-wallet getter.
type lpEvent struct {
	eventType    string
	outcome      bool
	price        int64
	amount       int64
	counterparty string // "" when none
}

// orderEventsByWallet reads a wallet's order-event history via the new public getter (the LP's
// monitoring surface). The reader address is irrelevant — it is a PUBLIC VIEW.
func orderEventsByWallet(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, reader util.EthereumAddress, walletHex string, marketID int) []lpEvent {
	t.Helper()
	var out []lpEvent
	require.NoError(t, callAs(ctx, platform, reader, "get_order_events_by_wallet",
		[]any{walletHex, int64(marketID), int64(100), int64(0)},
		func(row *common.Row) error {
			cp := ""
			if row.Values[7] != nil {
				cp = row.Values[7].(string)
			}
			out = append(out, lpEvent{
				eventType:    row.Values[2].(string),
				outcome:      row.Values[3].(bool),
				price:        row.Values[4].(int64),
				amount:       row.Values[5].(int64),
				counterparty: cp,
			})
			return nil
		}))
	return out
}

func eventTypes(evts []lpEvent) []string {
	out := make([]string, len(evts))
	for i, e := range evts {
		out[i] = e.eventType
	}
	return out
}

// lpPosition is one ob_positions row with its owner wallet resolved.
type lpPosition struct {
	outcome bool
	price   int64
	amount  int64
	wallet  string
}

// positionsForMarket reads every position in a market with the owning wallet joined in.
func positionsForMarket(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, marketID int) []lpPosition {
	t.Helper()
	tx := &common.TxContext{Ctx: ctx, BlockContext: &common.BlockContext{Height: 1}, TxID: platform.Txid()}
	var out []lpPosition
	require.NoError(t, platform.Engine.Execute(&common.EngineContext{TxContext: tx}, platform.DB,
		`SELECT p.outcome, p.price, p.amount, '0x' || encode(part.wallet_address, 'hex')
		 FROM ob_positions p JOIN ob_participants part ON p.participant_id = part.id
		 WHERE p.query_id = $qid
		 ORDER BY p.outcome, p.price`,
		map[string]any{"$qid": int64(marketID)},
		func(row *common.Row) error {
			out = append(out, lpPosition{
				outcome: row.Values[0].(bool),
				price:   row.Values[1].(int64),
				amount:  row.Values[2].(int64),
				wallet:  row.Values[3].(string),
			})
			return nil
		}))
	return out
}

// countWithdrawEvents returns the number of WITHDRAW rows in the MAA audit log for a rule.
func countWithdrawEvents(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, reader util.EthereumAddress, ruleID []byte) int {
	t.Helper()
	n := 0
	require.NoError(t, callAs(ctx, platform, reader, "maa_get_events", []any{ruleID, int64(100), int64(0)},
		func(row *common.Row) error {
			if row.Values[2].(string) == "WITHDRAW" {
				n++
			}
			return nil
		}))
	return n
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// testLPVaultLifecycle: the full happy path with the owner keeping complete control of the
// funds. The bot places a real order as the MAA (locking collateral); the owner monitors it,
// withdraws the free portion (commission to the agent), cancels the open order to free the rest,
// and withdraws everything — proving "withdraw at any time" returns ALL the LP's funds.
func testLPVaultLifecycle(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		marketMaker := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		lpResetFunding()

		// Market maker funds the 2-TRUF creation fee and opens a tradable market.
		lpFundTRUF(t, ctx, platform, marketMaker.Address(), "10000000000000000000") // 10 TRUF
		marketID := createOBMarket(t, ctx, platform, marketMaker, "lpvault-lifecycle")

		// Agent registers the LP allow-list rule; the LP joins it and funds the MAA with 100 USDC.
		ruleID := createLPRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		lpFundUSDC(t, ctx, platform, maaAddr.Address(), "100000000000000000000") // 100 USDC
		require.Equal(t, "100000000000000000000", balance(t, ctx, platform, maaAddr.Address()))

		// 1) THE BOT TRADES. The restricted agent places a buy order AS the MAA. lock() escrows
		//    40 USDC of collateral (100 shares × $0.40); this is the novel property — a restricted
		//    agent can lock collateral to trade, even though it can never move funds out.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, true /* restricted bot */, "place_buy_order",
			[]any{int64(marketID), true /* YES */, int64(40), int64(100)}),
			"the restricted bot must be able to place an allow-listed order as the MAA")
		require.Equal(t, "60000000000000000000", balance(t, ctx, platform, maaAddr.Address()),
			"placing the order must lock 40 USDC of collateral, leaving 60 free")

		// 2) THE LP MONITORS the bot via the new by-wallet getter.
		require.Equal(t, []string{"buy_placed"},
			eventTypes(orderEventsByWallet(t, ctx, platform, unrestricted, maaAddr.Address(), marketID)),
			"the owner can see the bot's placed order on the agent wallet")

		// 3) THE OWNER WITHDRAWS the free portion (40 USDC), paying the 2.5% commission. This is the
		//    route's owner-exit privilege (MAARestricted=false): commission -> agent, payout -> owner.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, false /* unrestricted owner */, "maa_withdraw",
			[]any{wdBridge, dec(t, "40000000000000000000")}),
			"the owner must be able to withdraw the free balance any time")
		require.Equal(t, "20000000000000000000", balance(t, ctx, platform, maaAddr.Address()), "60 free - 40 withdrawn = 20 free")
		require.Equal(t, "1000000000000000000", balance(t, ctx, platform, restrictedHex), "agent earns 2.5% of 40 = 1 USDC")
		require.Equal(t, "39000000000000000000", balance(t, ctx, platform, unrestrictedHex), "owner receives the 39 USDC remainder")

		// 4) THE OWNER UNWINDS the open order to reclaim its locked collateral. cancel_order unlocks
		//    the 40 USDC back to the MAA itself (@caller), restoring withdrawable balance.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, false /* owner */, "cancel_order",
			[]any{int64(marketID), true /* YES */, int64(-40) /* buy price is negative */}),
			"the owner can cancel the bot's open order")
		require.Equal(t, "60000000000000000000", balance(t, ctx, platform, maaAddr.Address()),
			"cancel unlocks the 40 collateral back to the MAA: 20 + 40 = 60 free")
		require.Equal(t, []string{"buy_placed", "cancelled"},
			eventTypes(orderEventsByWallet(t, ctx, platform, unrestricted, maaAddr.Address(), marketID)),
			"the audit trail now shows the placement and the cancel")

		// 5) THE OWNER WITHDRAWS THE REST — the LP gets ALL its funds out, paying commission again.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, false /* owner */, "maa_withdraw",
			[]any{wdBridge, dec(t, "60000000000000000000")}))
		require.Equal(t, "0", balance(t, ctx, platform, maaAddr.Address()), "the agent wallet is fully drained")
		require.Equal(t, "2500000000000000000", balance(t, ctx, platform, restrictedHex), "agent total commission: 1 + 1.5 = 2.5 USDC")
		require.Equal(t, "97500000000000000000", balance(t, ctx, platform, unrestrictedHex), "owner recovered everything net of commission: 39 + 58.5 = 97.5 USDC")

		// The two exits are recorded for audit.
		require.Equal(t, 2, countWithdrawEvents(t, ctx, platform, unrestricted, ruleID), "both withdrawals are audited")
		return nil
	}
}

// testLPVaultBotFillIsAudited: the bot's order actually fills against a counterparty (a mint
// match), and the fill is visible to the owner via the by-wallet getter — "monitor the
// activities of the liquidity bot" with a real trade, not just a resting order.
func testLPVaultBotFillIsAudited(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		marketMaker := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		counterparty := util.Unsafe_NewEthereumAddressFromString("0x4444444444444444444444444444444444444444")
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		lpResetFunding()

		lpFundTRUF(t, ctx, platform, marketMaker.Address(), "10000000000000000000")
		marketID := createOBMarket(t, ctx, platform, marketMaker, "lpvault-fill")

		ruleID := createLPRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		lpFundUSDC(t, ctx, platform, maaAddr.Address(), "100000000000000000000")      // bot vault: 100 USDC
		lpFundUSDC(t, ctx, platform, counterparty.Address(), "100000000000000000000") // counterparty: 100 USDC

		// The bot rests a buy YES @ $0.40 as the MAA.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, true /* restricted bot */, "place_buy_order",
			[]any{int64(marketID), true /* YES */, int64(40), int64(100)}))

		// A counterparty buys NO @ $0.60. YES@40 + NO@60 = 100 -> the matching engine mints the pair:
		// the bot receives 100 YES shares, the counterparty 100 NO shares.
		require.NoError(t, lpCallAs(ctx, platform, counterparty, "place_buy_order",
			[]any{int64(marketID), false /* NO */, int64(60), int64(100)}),
			"counterparty's complementary buy should mint-match the bot's order")

		// The bot now holds 100 YES shares (a filled position), keyed to the MAA wallet. The LP
		// rule's allow-list differs from golden-vector A, so this MAA address is not goldenMAAHex;
		// compare against the actual derived address (SQL emits lowercase hex).
		maaWallet := "0x" + hex.EncodeToString(maa)
		var botYES int64
		for _, p := range positionsForMarket(t, ctx, platform, marketID) {
			if p.wallet == maaWallet && p.outcome && p.price == 0 {
				botYES = p.amount
			}
		}
		require.Equal(t, int64(100), botYES, "the bot's order filled into 100 YES holdings on the MAA")

		// The owner sees the placement AND the fill via the by-wallet getter, with the counterparty.
		evts := orderEventsByWallet(t, ctx, platform, unrestricted, maaAddr.Address(), marketID)
		require.Equal(t, []string{"buy_placed", "mint_fill"}, eventTypes(evts), "monitoring shows the bot's trade history")
		require.Equal(t, counterparty.Address(), evts[1].counterparty, "the fill records the counterparty wallet")
		require.True(t, evts[1].outcome, "the bot's fill is on the YES side")
		return nil
	}
}

// testLPVaultAgentCannotDrain: the safety promise. The restricted bot can run its allow-listed
// strategy, but every attempt to move funds OUT of the MAA is blocked at the erc20 token
// boundary — no commission is skimmed, nothing leaves, the LP's custody is intact.
func testLPVaultAgentCannotDrain(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		marketMaker := util.Unsafe_NewEthereumAddressFromString("0x3333333333333333333333333333333333333333")
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		lpResetFunding()

		lpFundTRUF(t, ctx, platform, marketMaker.Address(), "10000000000000000000")
		marketID := createOBMarket(t, ctx, platform, marketMaker, "lpvault-nodrain")

		ruleID := createLPRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		lpFundUSDC(t, ctx, platform, maaAddr.Address(), "100000000000000000000")
		_ = ruleID // rule audit is covered by testLPVaultLifecycle; this test focuses on the boundary

		// Sanity: the agent CAN do its job — an allow-listed order locks collateral and succeeds.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, true /* restricted */, "place_buy_order",
			[]any{int64(marketID), true, int64(40), int64(100)}))
		require.Equal(t, "60000000000000000000", balance(t, ctx, platform, maaAddr.Address()))

		// But the agent CANNOT withdraw — the commission transfer leg hits the restricted boundary.
		err := callAsMAA(ctx, platform, maaAddr, true /* restricted */, "maa_withdraw",
			[]any{wdBridge, dec(t, "10000000000000000000")})
		require.Error(t, err, "a restricted agent must not be able to withdraw")
		require.ErrorContains(t, err, "restricted agent (MAA) execution",
			"the withdrawal must be stopped at the erc20 token boundary")

		// Nor bridge funds off to L1.
		require.Error(t, callAsMAA(ctx, platform, maaAddr, true /* restricted */, "maa_bridge_out",
			[]any{wdBridge, dec(t, "10000000000000000000"), nil}),
			"a restricted agent must not be able to bridge funds out")

		// Nothing moved: the free balance is unchanged and the agent skimmed no commission.
		require.Equal(t, "60000000000000000000", balance(t, ctx, platform, maaAddr.Address()), "blocked exits move nothing")
		require.Equal(t, "0", balance(t, ctx, platform, restrictedHex), "no commission was paid on the blocked attempts")
		return nil
	}
}
