//go:build kwiltest

package maa

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/feefund"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// DATA PROVISION AGENT — the second end-to-end MAA example (0MainGoal.md Example 2).
//
// An AI agent (the restricted key) creates new indexes and provides regular data to them,
// running entirely AS its agent wallet (the MAA). The owner (the unrestricted key) "fills up"
// the MAA with TRUF; the agent's job costs TRUF — create_streams charges 100 TRUF per stream
// and insert_records a flat 1 TRUF once the wallet is enrolled in system:fee_required — and
// both fees are caller-keyed bridge transfers, so with @caller rewritten to the MAA they
// debit the MAA's OWN escrow with zero new fee code. The owner stays secure that the agent
// can never exfiltrate the funds: the erc20 MAARestricted boundary blocks every fund exit
// for the restricted role, carving out ONLY the protocol write-fee — a transfer whose
// recipient is the block leader's sender address (@leader_sender), consensus-determined and
// never agent-chosen.
//
// The agent's allow-list is exactly the two data-provision actions in the default ("main")
// namespace. Unlike the LP-vault example (whose fee-paying create_market had to stay OFF the
// allow-list before the leader-fee carve-out existed), fee-paying actions are the POINT here.

var (
	daNamespaces = []string{"main", "main"}
	daActions    = []string{"create_streams", "insert_records"}
	daBodyHashes = [][]byte{nil, nil}
)

// Independent ordered-sync points for the data-agent funding (the lp_vault tests use
// 7000/8000; withdraw tests low single digits).
var (
	daTRUFPoint int64 = 9000
	daTRUFPrev  *int64
)

const (
	daInsertFee = "1000000000000000000" // flat 1 TRUF per insert_records tx (003, fee_required-gated)
)

func TestMAADataAgent(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_DataAgent",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testDataAgentLifecycle(t),
			testDataAgentInsufficientEscrow(t),
			testDataAgentCannotExfiltrate(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// daResetFunding clears the ordered-sync cursor and the TRUF deposit chain; call once at the
// start of each function test (each gets a fresh DB).
func daResetFunding() {
	orderedsync.ForTestingReset()
	daTRUFPrev = nil
}

// daFundTRUF credits `to` on the hoodi_tt (TRUF) bridge, chaining to the previous deposit so
// several wallets can be funded in one test.
func daFundTRUF(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, to, amount string) {
	t.Helper()
	daTRUFPoint++
	p := daTRUFPoint
	require.NoError(t, testerc20.InjectERC20Transfer(ctx, platform, wdChain, wdTRUFEscrow, wdTRUFERC20, to, to, amount, p, daTRUFPrev))
	daTRUFPrev = &p
}

// daLeader generates the block proposer for a function test and returns its key with the
// eth address @leader_sender resolves to — the write-fee recipient.
func daLeader(t *testing.T) (*crypto.Secp256k1PublicKey, string) {
	t.Helper()
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)
	addr := util.Unsafe_NewEthereumAddressFromString("0x" + hex.EncodeToString(crypto.EthereumAddressFromPubKey(pub)))
	return pub, addr.Address()
}

// daCallAsMAAIn is callAsMAA with a block proposer in context: the fee-charging actions
// resolve @leader_sender from it (NULL proposer makes them ERROR before the fee transfer).
// Everything else reproduces the maa_exec route's inner call — @caller rewritten to the MAA,
// the signer's role carried as TxContext.MAARestricted, and the signer's authenticator.
func daCallAsMAAIn(ctx context.Context, platform *kwilTesting.Platform, maa util.EthereumAddress, restricted bool, leader *crypto.Secp256k1PublicKey, namespace, action string, args []any) error {
	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: 1, Proposer: leader},
		Signer:        maa.Bytes(),
		Caller:        maa.Address(),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
		MAARestricted: restricted,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, namespace, action, args, func(*common.Row) error { return nil })
	if err != nil {
		return err
	}
	return res.Error
}

// daCallAsMAA is daCallAsMAAIn against the default ("main") namespace.
func daCallAsMAA(ctx context.Context, platform *kwilTesting.Platform, maa util.EthereumAddress, restricted bool, leader *crypto.Secp256k1PublicKey, action string, args []any) error {
	return daCallAsMAAIn(ctx, platform, maa, restricted, leader, "", action, args)
}

// createDARule registers the data-provision allow-list rule, signed by the restricted agent.
func createDARule(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, restricted util.EthereumAddress, feeBps int64, salt []byte) []byte {
	t.Helper()
	var ruleID []byte
	err := callAs(ctx, platform, restricted, "maa_create_rule", []any{
		salt, "bps", feeBps, dec(t, "0"),
		daNamespaces, daActions, daBodyHashes,
	}, func(row *common.Row) error {
		ruleID = append([]byte(nil), row.Values[0].([]byte)...)
		return nil
	})
	require.NoError(t, err, "maa_create_rule (data-provision allow-list) should succeed")
	require.Len(t, ruleID, 32)
	return ruleID
}

// dec36 parses a NUMERIC(36,18) literal — the primitive-record value type.
func dec36(t *testing.T, s string) *kwilTypes.Decimal {
	t.Helper()
	d, err := kwilTypes.ParseDecimalExplicit(s, 36, 18)
	require.NoError(t, err)
	return d
}

// daRecord is one (event_time, value) row read back from a stream.
type daRecord struct {
	eventTime int64
	value     string
}

// readRecords reads a stream's records via the public get_record query — the owner's (or
// anyone's) view of the data the agent provided. The explicit [0, 10^9] range matters: NULL
// from/to means "latest record only".
func readRecords(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, reader util.EthereumAddress, dataProvider, streamID string) []daRecord {
	t.Helper()
	var out []daRecord
	require.NoError(t, callAs(ctx, platform, reader, "get_record",
		[]any{dataProvider, streamID, int64(0), int64(1000000000), nil, false},
		func(row *common.Row) error {
			out = append(out, daRecord{
				eventTime: row.Values[0].(int64),
				value:     row.Values[1].(*kwilTypes.Decimal).String(),
			})
			return nil
		}))
	return out
}

// streamOwner returns the data_provider recorded for a stream ("" when the stream is absent).
func streamOwner(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, streamID string) string {
	t.Helper()
	tx := &common.TxContext{Ctx: ctx, BlockContext: &common.BlockContext{Height: 1}, TxID: platform.Txid()}
	owner := ""
	require.NoError(t, platform.Engine.Execute(&common.EngineContext{TxContext: tx}, platform.DB,
		`SELECT data_provider FROM streams WHERE stream_id = $sid`,
		map[string]any{"$sid": streamID},
		func(row *common.Row) error {
			owner = row.Values[0].(string)
			return nil
		}))
	return owner
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// testDataAgentLifecycle: the full happy path. The agent creates an index and provides data
// to it AS the MAA, every fee coming out of the MAA's TRUF escrow (100 TRUF per stream;
// 1 TRUF per insert once the wallet is enrolled in system:fee_required) and landing with the
// block leader; the owner reads the provided data back and withdraws the remaining escrow at
// any time, paying the agent its commission.
func testDataAgentLifecycle(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		daResetFunding()
		leader, leaderAddr := daLeader(t)

		// Agent registers the data-provision rule; the owner joins and fills the MAA with
		// 250 TRUF — the agent's working budget for fees, never the agent's to take.
		ruleID := createDARule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		daFundTRUF(t, ctx, platform, maaAddr.Address(), "250000000000000000000") // 250 TRUF
		require.Equal(t, "250000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()))

		// 1) THE AGENT CREATES AN INDEX. create_streams runs AS the MAA: the 100-TRUF
		//    per-stream fee debits the MAA's escrow and lands with the block leader — the
		//    one fund movement the restricted boundary permits. The MAA is auto-onboarded
		//    as a data provider and owns the stream.
		streamID := "stdataagent000000000000000000001"
		require.NoError(t, daCallAsMAA(ctx, platform, maaAddr, true /* restricted agent */, leader,
			"create_streams", []any{[]string{streamID}, []string{"primitive"}}),
			"the restricted agent must be able to create a stream as the MAA")
		require.Equal(t, "150000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()),
			"the 100-TRUF stream fee must come out of the MAA escrow")
		require.Equal(t, feefund.StreamCreationFeeWei, balanceTRUF(t, ctx, platform, leaderAddr),
			"the stream fee must land with the block leader")
		require.Equal(t, maaAddr.Address(), streamOwner(t, ctx, platform, streamID),
			"the stream belongs to the agent wallet, not the agent's own key")

		// 2) THE AGENT PROVIDES DATA. Before fee_required enrollment the insert is free —
		//    today's phased-rollout default.
		require.NoError(t, daCallAsMAA(ctx, platform, maaAddr, true /* agent */, leader,
			"insert_records", []any{
				[]string{maaAddr.Address()}, []string{streamID}, []int64{100}, []*kwilTypes.Decimal{dec36(t, "42.5")},
			}),
			"the agent must be able to insert records into its wallet's stream")
		require.Equal(t, "150000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()),
			"un-enrolled wallets pay no insert fee (phased rollout)")

		// 3) Once the wallet is enrolled in system:fee_required, the flat 1-TRUF write fee
		//    applies — and it too comes out of the MAA escrow.
		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "fee_required", maaAddr.Address()))
		require.NoError(t, daCallAsMAA(ctx, platform, maaAddr, true /* agent */, leader,
			"insert_records", []any{
				[]string{maaAddr.Address()}, []string{streamID}, []int64{200}, []*kwilTypes.Decimal{dec36(t, "43.75")},
			}),
			"the enrolled agent must still be able to insert, paying the write fee from escrow")
		require.Equal(t, "149000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()),
			"the 1-TRUF write fee must come out of the MAA escrow")
		require.Equal(t, "101000000000000000000", balanceTRUF(t, ctx, platform, leaderAddr),
			"the write fee must land with the block leader: 100 + 1")

		// 4) THE DATA IS PROVIDED: anyone (here the owner) reads the records back.
		records := readRecords(t, ctx, platform, unrestricted, maaAddr.Address(), streamID)
		require.Equal(t, []daRecord{
			{eventTime: 100, value: "42.500000000000000000"},
			{eventTime: 200, value: "43.750000000000000000"},
		}, records, "the owner sees the data the agent provided")

		// 5) THE OWNER WITHDRAWS the un-spent escrow at any time, paying the 2.5%
		//    commission: 149 × 2.5% = 3.725 TRUF to the agent, the rest to the owner.
		require.NoError(t, callAsMAA(ctx, platform, maaAddr, false /* unrestricted owner */, "maa_withdraw",
			[]any{wdTRUFBridge, dec(t, "149000000000000000000")}),
			"the owner must be able to withdraw the remaining escrow any time")
		require.Equal(t, "0", balanceTRUF(t, ctx, platform, maaAddr.Address()), "the agent wallet is drained")
		require.Equal(t, "3725000000000000000", balanceTRUF(t, ctx, platform, restrictedHex), "agent earns 2.5% of 149 = 3.725 TRUF")
		require.Equal(t, "145275000000000000000", balanceTRUF(t, ctx, platform, unrestrictedHex), "owner recovers the 145.275 TRUF remainder")
		require.Equal(t, 1, countWithdrawEvents(t, ctx, platform, unrestricted, ruleID), "the exit is audited")
		return nil
	}
}

// testDataAgentInsufficientEscrow: the escrow-paid-fee path fails closed. With less than the
// per-stream fee on the wallet, the agent's create_streams reverts before any state is
// written — no stream, no partial fee, nothing for the leader.
func testDataAgentInsufficientEscrow(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		daResetFunding()
		leader, leaderAddr := daLeader(t)

		ruleID := createDARule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		daFundTRUF(t, ctx, platform, maaAddr.Address(), "50000000000000000000") // 50 TRUF < the 100-TRUF fee

		streamID := "stdataagent000000000000000000002"
		err := daCallAsMAA(ctx, platform, maaAddr, true /* agent */, leader,
			"create_streams", []any{[]string{streamID}, []string{"primitive"}})
		require.Error(t, err, "create_streams must revert when the escrow cannot cover the fee")
		require.ErrorContains(t, err, "Insufficient balance for stream creation")

		require.Equal(t, "50000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()), "the failed create must charge nothing")
		require.Equal(t, "0", balanceTRUF(t, ctx, platform, leaderAddr), "the leader must receive nothing")
		require.Equal(t, "", streamOwner(t, ctx, platform, streamID), "no stream row may exist")
		return nil
	}
}

// testDataAgentCannotExfiltrate: the safety promise the owner relies on when filling up the
// wallet. The agent can do its (fee-paying) job, but every path that would move escrow to an
// agent-chosen destination is blocked at the erc20 token boundary — the leader-fee carve-out
// does not open a single exfiltration route.
func testDataAgentCannotExfiltrate(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		daResetFunding()
		leader, _ := daLeader(t)

		ruleID := createDARule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		daFundTRUF(t, ctx, platform, maaAddr.Address(), "250000000000000000000")
		_ = ruleID // rule audit is covered by testDataAgentLifecycle

		// Sanity: the agent CAN do its job — the fee-paying create succeeds.
		require.NoError(t, daCallAsMAA(ctx, platform, maaAddr, true /* agent */, leader,
			"create_streams", []any{[]string{"stdataagent000000000000000000003"}, []string{"primitive"}}))
		require.Equal(t, "150000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()))

		// But the agent CANNOT withdraw — the commission transfer leg targets the agent,
		// not the leader, so the boundary rejects it.
		err := daCallAsMAA(ctx, platform, maaAddr, true /* agent */, leader, "maa_withdraw",
			[]any{wdTRUFBridge, dec(t, "10000000000000000000")})
		require.Error(t, err, "a restricted agent must not be able to withdraw")
		require.ErrorContains(t, err, "restricted agent (MAA) execution")

		// Nor bridge escrow off to L1 — the carve-out is transfer-to-leader only.
		require.Error(t, daCallAsMAA(ctx, platform, maaAddr, true /* agent */, leader, "maa_bridge_out",
			[]any{wdTRUFBridge, dec(t, "10000000000000000000"), nil}),
			"a restricted agent must not be able to bridge funds out")

		// Nor move tokens to an address of its choosing (its own key here) via the bridge's
		// raw transfer — the exact primitive the fee uses, with a non-leader recipient.
		err = daCallAsMAAIn(ctx, platform, maaAddr, true /* agent */, leader, "hoodi_tt", "transfer",
			[]any{restrictedHex, dec(t, "10000000000000000000")})
		require.Error(t, err, "a restricted agent must not be able to transfer to itself")
		require.ErrorContains(t, err, "restricted agent (MAA) execution")

		// Nothing moved, no commission skimmed.
		require.Equal(t, "150000000000000000000", balanceTRUF(t, ctx, platform, maaAddr.Address()), "blocked exits move nothing")
		require.Equal(t, "0", balanceTRUF(t, ctx, platform, restrictedHex), "no commission was paid on the blocked attempts")
		return nil
	}
}
