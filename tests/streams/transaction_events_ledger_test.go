//go:build kwiltest

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	testerc20 "github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	sdkTypes "github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const (
	ledgerChain          = "sepolia"
	ledgerEscrow         = "0x502430eD0BbE0f230215870c9C2853e126eE5Ae3"
	ledgerERC20          = "0x2222222222222222222222222222222222222222"
	ledgerExtensionAlias = "sepolia_bridge"

	feeOneTRUF       = "1000000000000000000"
	feeTwoTRUF       = "2000000000000000000"
	feeFourTRUF      = "4000000000000000000"
	feeFortyTRUF     = "40000000000000000000"
	transferAmount   = "5000000000000000000"
	withdrawAmount   = "10000000000000000000"
	initialUserFunds = "200000000000000000000"
)

var ledgerPointCounter int64 = 20000

func TestTransactionEventsLedger(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "LEDGER_FEE01_TransactionEventsLedger",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			runTransactionEventsLedgerScenario(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func runTransactionEventsLedgerScenario(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		systemAdmin := util.Unsafe_NewEthereumAddressFromString("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
		platform.Deployer = systemAdmin.Bytes()

		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", systemAdmin.Address()))
		require.NoError(t, setup.CreateDataProvider(ctx, platform, systemAdmin.Address()))

		err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, ledgerChain, ledgerEscrow, ledgerERC20, 18, 60, ledgerExtensionAlias)
		if err != nil {
			if !strings.Contains(err.Error(), "alias \"sepolia_bridge\" already exists") {
				require.NoError(t, err, "failed to seed ERC-20 bridge instance")
			}
			require.NoError(t, erc20shim.ForTestingInitializeExtension(ctx, platform), "failed to reinitialize ERC-20 bridge extension")
		}
		require.NoError(t, erc20shim.ForTestingInitializeExtension(ctx, platform))

		actorVal := util.Unsafe_NewEthereumAddressFromString("0x9999999999999999999999999999999999999999")
		actor := &actorVal
		require.NoError(t, setup.CreateDataProviderWithoutRole(ctx, platform, actor.Address()))
		require.NoError(t, ledgerGiveBalance(ctx, platform, actor.Address(), initialUserFunds))

		receiverVal := util.Unsafe_NewEthereumAddressFromString("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
		receiver := &receiverVal
		bonusRecipientVal := util.Unsafe_NewEthereumAddressFromString("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		bonusRecipientLower := strings.ToLower(bonusRecipientVal.Address())

		attestationStream := util.GenerateStreamId("ledger_attestation_stream")
		require.NoError(t, setup.CreateStream(ctx, platform, setup.StreamInfo{
			Type: setup.ContractTypePrimitive,
			Locator: sdkTypes.StreamLocator{
				StreamId:     attestationStream,
				DataProvider: systemAdmin,
			},
		}))

		userLower := strings.ToLower(actor.Address())
		height := int64(5)

		primitiveStream := util.GenerateStreamId("ledger_primitive_stream")
		composedStream := util.GenerateStreamId("ledger_composed_stream")

		createLeaderPub, createLeaderAddr := newLeader(t)
		createTx, err := callActionWithLeader(ctx, platform, actor, createLeaderPub, height, "create_streams", []any{
			[]string{primitiveStream.String(), composedStream.String()},
			[]string{"primitive", "composed"},
		})
		require.NoError(t, err)
		height++

		insertLeaderPub, insertLeaderAddr := newLeader(t)
		insertValue, err := kwilTypes.ParseDecimalExplicit("10.5", 36, 18)
		require.NoError(t, err)
		insertTx, err := callActionWithLeader(ctx, platform, actor, insertLeaderPub, height, "insert_records", []any{
			[]string{userLower},
			[]string{primitiveStream.String()},
			[]int64{1000},
			[]*kwilTypes.Decimal{insertValue},
		})
		require.NoError(t, err)
		height++

		tableCheck, err := platform.DB.Execute(ctx,
			`SELECT table_schema::TEXT 
			 FROM information_schema.tables 
			 WHERE table_name = 'transaction_event_distributions'`,
		)
		require.NoError(t, err)
		require.NotEmpty(t, tableCheck.Rows, "transaction_event_distributions table missing")

		schemaName, ok := tableCheck.Rows[0][0].(string)
		require.True(t, ok, "expected schema name as string")
		t.Logf("ledger distribution table schema: %s", schemaName)

		updateSQL := fmt.Sprintf(`
			UPDATE %s.transaction_event_distributions
			 SET amount = $1::NUMERIC(78, 0)
			 WHERE tx_id = $2 AND sequence = 1`, schemaName)
		_, err = platform.DB.Execute(ctx,
			updateSQL,
			feeOneTRUF,
			insertTx,
		)
		require.NoError(t, err)

		insertSQL := fmt.Sprintf(`
			INSERT INTO %s.transaction_event_distributions (tx_id, sequence, recipient, amount, note)
			 VALUES ($1, $2, $3, $4::NUMERIC(78, 0), NULL)`, schemaName)
		_, err = platform.DB.Execute(ctx,
			insertSQL,
			insertTx,
			2,
			bonusRecipientLower,
			feeOneTRUF,
		)
		require.NoError(t, err)

		taxLeaderPub, taxLeaderAddr := newLeader(t)
		weight, err := kwilTypes.ParseDecimalExplicit("1.0", 36, 18)
		require.NoError(t, err)
		taxTx, err := callActionWithLeader(ctx, platform, actor, taxLeaderPub, height, "insert_taxonomy", []any{
			userLower,
			composedStream.String(),
			[]string{userLower},
			[]string{primitiveStream.String()},
			[]*kwilTypes.Decimal{weight},
			nil,
		})
		require.NoError(t, err)
		height++

		transferLeaderPub, transferLeaderAddr := newLeader(t)
		transferTx, err := callActionWithLeader(ctx, platform, actor, transferLeaderPub, height, "sepolia_transfer", []any{
			receiver.Address(),
			transferAmount,
		})
		require.NoError(t, err)
		height++

		withdrawLeaderPub, withdrawLeaderAddr := newLeader(t)
		withdrawTx, err := callActionWithLeader(ctx, platform, actor, withdrawLeaderPub, height, "sepolia_bridge_tokens", []any{
			actor.Address(),
			withdrawAmount,
		})
		require.NoError(t, err)
		height++

		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", actor.Address()))

		argsBytes, err := tn_utils.EncodeActionArgs([]any{
			strings.ToLower(systemAdmin.Address()),
			attestationStream.String(),
			int64(0),
			int64(999),
			int64(99999),
			false,
		})
		require.NoError(t, err)

		attLeaderPub, attLeaderAddr := newLeader(t)
		attTx, err := callActionWithLeader(ctx, platform, actor, attLeaderPub, height, "request_attestation", []any{
			strings.ToLower(systemAdmin.Address()),
			attestationStream.String(),
			"get_record",
			argsBytes,
			false,
			nil,
		})
		require.NoError(t, err)

		assertNoMetadata := func(meta metadataMap) {
			require.Empty(t, meta, "expected no metadata for ledger event")
		}

		expected := map[string]ledgerExpectation{
			createTx: {
				method:       "deployStream",
				fee:          feeFourTRUF,
				feeRecipient: createLeaderAddr,
				feeDistributions: []string{
					buildDistribution(createLeaderAddr, feeFourTRUF),
				},
				assertMetadata: assertNoMetadata,
			},
			insertTx: {
				method:       "insertRecords",
				fee:          feeTwoTRUF,
				feeRecipient: insertLeaderAddr,
				feeDistributions: []string{
					buildDistribution(insertLeaderAddr, feeOneTRUF),
					buildDistribution(bonusRecipientLower, feeOneTRUF),
				},
				assertMetadata: assertNoMetadata,
			},
			taxTx: {
				method:       "setTaxonomies",
				fee:          feeTwoTRUF,
				feeRecipient: taxLeaderAddr,
				feeDistributions: []string{
					buildDistribution(taxLeaderAddr, feeTwoTRUF),
				},
				assertMetadata: assertNoMetadata,
			},
			transferTx: {
				method:       "transferTN",
				fee:          feeOneTRUF,
				feeRecipient: transferLeaderAddr,
				feeDistributions: []string{
					buildDistribution(transferLeaderAddr, feeOneTRUF),
				},
				assertMetadata: assertNoMetadata,
			},
			withdrawTx: {
				method:       "withdrawTN",
				fee:          feeFortyTRUF,
				feeRecipient: withdrawLeaderAddr,
				feeDistributions: []string{
					buildDistribution(withdrawLeaderAddr, feeFortyTRUF),
				},
				assertMetadata: assertNoMetadata,
			},
			attTx: {
				method:       "requestAttestation",
				fee:          feeFortyTRUF,
				feeRecipient: attLeaderAddr,
				feeDistributions: []string{
					buildDistribution(attLeaderAddr, feeFortyTRUF),
				},
				assertMetadata: assertNoMetadata,
			},
		}

		buildExpectedCounts := func() (map[string]map[string]int, int) {
			counts := make(map[string]map[string]int, len(expected))
			total := 0
			for txID, exp := range expected {
				distList := exp.feeDistributions
				if len(distList) == 0 {
					distList = []string{""}
				}
				counts[txID] = make(map[string]int, len(distList))
				for _, dist := range distList {
					counts[txID][dist]++
					total++
				}
			}
			return counts, total
		}

		assertLedgerRows := func(rows []ledgerRow) {
			counts, total := buildExpectedCounts()
			require.Len(t, rows, total)
			seen := make(map[string]map[string]int, len(expected))
			for txID := range expected {
				seen[txID] = make(map[string]int)
			}
			for _, row := range rows {
				exp, ok := expected[row.TxID]
				require.True(t, ok, "unexpected tx in results: %s", row.TxID)
				require.Equal(t, exp.method, row.Method)
				require.Equal(t, userLower, row.Caller)
				require.Equal(t, exp.fee, row.FeeAmount)
				require.Equal(t, exp.feeRecipient, row.FeeRecipient)
				pair := buildDistribution(row.DistributionRecipient, row.DistributionAmount)
				require.Contains(t, counts[row.TxID], pair, "unexpected distribution %s for tx %s", pair, row.TxID)
				seen[row.TxID][pair]++
				t.Logf("metadata for %s (%s): raw=%s parsed=%v", row.TxID, row.Method, row.RawMetadata, row.Metadata)
				exp.assertMetadata(row.Metadata)
			}
			for txID, expectedCounts := range counts {
				for dist, want := range expectedCounts {
					require.Equal(t, want, seen[txID][dist], "distribution %s for tx %s missing or duplicated", dist, txID)
				}
			}
		}

		paidRows, err := fetchTransactionFees(ctx, platform, actor.Address(), userLower, "paid")
		require.NoError(t, err)
		assertLedgerRows(paidRows)

		bothRows, err := fetchTransactionFees(ctx, platform, actor.Address(), userLower, "both")
		require.NoError(t, err)
		assertLedgerRows(bothRows)

		withdrawReceivedRows, err := fetchTransactionFees(ctx, platform, actor.Address(), withdrawLeaderAddr, "received")
		require.NoError(t, err)
		require.Len(t, withdrawReceivedRows, 1)
		require.Equal(t, withdrawTx, withdrawReceivedRows[0].TxID)
		require.Equal(t, withdrawLeaderAddr, withdrawReceivedRows[0].FeeRecipient)
		require.Equal(t, withdrawLeaderAddr, withdrawReceivedRows[0].DistributionRecipient)
		require.Equal(t, feeFortyTRUF, withdrawReceivedRows[0].DistributionAmount)
		require.Equal(t, userLower, withdrawReceivedRows[0].Caller)

		insertLeaderReceivedRows, err := fetchTransactionFees(ctx, platform, actor.Address(), insertLeaderAddr, "received")
		require.NoError(t, err)
		require.Len(t, insertLeaderReceivedRows, 1)
		require.Equal(t, insertTx, insertLeaderReceivedRows[0].TxID)
		require.Equal(t, insertLeaderAddr, insertLeaderReceivedRows[0].FeeRecipient)
		require.Equal(t, insertLeaderAddr, insertLeaderReceivedRows[0].DistributionRecipient)
		require.Equal(t, feeOneTRUF, insertLeaderReceivedRows[0].DistributionAmount)

		bonusReceivedRows, err := fetchTransactionFees(ctx, platform, actor.Address(), bonusRecipientLower, "received")
		require.NoError(t, err)
		require.Len(t, bonusReceivedRows, 1)
		require.Equal(t, insertTx, bonusReceivedRows[0].TxID)
		require.Equal(t, insertLeaderAddr, bonusReceivedRows[0].FeeRecipient)
		require.Equal(t, bonusRecipientLower, bonusReceivedRows[0].DistributionRecipient)
		require.Equal(t, feeOneTRUF, bonusReceivedRows[0].DistributionAmount)

		lastTxRows, err := fetchLastTransactions(ctx, platform, actor.Address(), userLower, int64(len(expected)))
		require.NoError(t, err)
		assertLedgerRows(lastTxRows)

		withdrawHistoryRows, err := fetchLastTransactions(ctx, platform, actor.Address(), withdrawLeaderAddr, 10)
		require.NoError(t, err)
		require.NotEmpty(t, withdrawHistoryRows)
		require.Equal(t, withdrawTx, withdrawHistoryRows[0].TxID)

		bonusHistoryRows, err := fetchLastTransactions(ctx, platform, actor.Address(), bonusRecipientLower, 10)
		require.NoError(t, err)
		require.NotEmpty(t, bonusHistoryRows)
		foundBonus := false
		for _, row := range bonusHistoryRows {
			if row.DistributionRecipient == bonusRecipientLower {
				require.Equal(t, insertTx, row.TxID)
				require.Equal(t, feeOneTRUF, row.DistributionAmount)
				foundBonus = true
				break
			}
		}
		require.True(t, foundBonus, "expected to find distribution row for bonus recipient")

		legacyRows, err := fetchLegacyLastTransactions(ctx, platform, actor.Address(), userLower, int64(len(expected)))
		require.NoError(t, err)
		require.NotEmpty(t, legacyRows)

		methodSet := make(map[string]struct{})
		for _, exp := range expected {
			methodSet[exp.method] = struct{}{}
		}

		for _, row := range legacyRows {
			require.Greater(t, row.CreatedAt, int64(0))
			_, ok := methodSet[row.Method]
			require.True(t, ok, "unexpected method in legacy view: %s", row.Method)
		}

		for txID, exp := range expected {
			eventRow, err := fetchTransactionEvent(ctx, platform, actor.Address(), txID)
			require.NoError(t, err, "failed to fetch transaction event for %s", txID)
			require.Equal(t, exp.method, eventRow.Method)
			require.Equal(t, exp.fee, eventRow.FeeAmount)
			require.Equal(t, exp.feeRecipient, eventRow.FeeRecipient)
			expectedAggregate := strings.Join(exp.feeDistributions, ",")
			require.Equal(t, expectedAggregate, eventRow.FeeDistributions)
			exp.assertMetadata(eventRow.Metadata)
		}

		return nil
	}
}

type ledgerExpectation struct {
	method           string
	fee              string
	feeRecipient     string
	feeDistributions []string
	assertMetadata   func(meta metadataMap)
}

type legacyRow struct {
	CreatedAt int64
	Method    string
}

type ledgerRow struct {
	TxID                  string
	CreatedAt             int64
	Method                string
	Caller                string
	FeeAmount             string
	FeeRecipient          string
	Metadata              metadataMap
	FeeDistributions      string
	DistributionSequence  int
	DistributionRecipient string
	DistributionAmount    string
	RawMetadata           string
}

func newLeader(t *testing.T) (*crypto.Secp256k1PublicKey, string) {
	t.Helper()
	_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	pub := pubGeneric.(*crypto.Secp256k1PublicKey)
	return pub, addressFromPub(pub)
}

func addressFromPub(pub *crypto.Secp256k1PublicKey) string {
	return fmt.Sprintf("0x%x", crypto.EthereumAddressFromPubKey(pub))
}

func callActionWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer *util.EthereumAddress, leaderPub *crypto.Secp256k1PublicKey, height int64, action string, args []any) (string, error) {
	txID := strings.ToLower(platform.Txid())
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   height,
			Proposer: leaderPub,
		},
		Signer:        signer.Bytes(),
		Caller:        signer.Address(),
		TxID:          txID,
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", action, args, func(row *common.Row) error { return nil })
	if err != nil {
		return "", err
	}
	if res != nil && res.Error != nil {
		return "", res.Error
	}
	return "0x" + txID, nil
}

func fetchTransactionFees(ctx context.Context, platform *kwilTesting.Platform, caller string, wallet string, mode string) ([]ledgerRow, error) {
	rows := make([]ledgerRow, 0, 8)
	args := []any{strings.ToLower(wallet), mode, 20, 0}
	err := callView(ctx, platform, caller, "list_transaction_fees", args, func(row *common.Row) error {
		feeRecipient := ""
		if row.Values[5] != nil {
			feeRecipient = strings.ToLower(row.Values[5].(string))
		}

		rawMetadata := stringOrEmpty(row.Values[6])
		meta := parseMetadata(row.Values[6])

		seq := int(row.Values[7].(int64))
		distRecipient := ""
		if row.Values[8] != nil {
			distRecipient = strings.ToLower(row.Values[8].(string))
		}

		rows = append(rows, ledgerRow{
			TxID:                  row.Values[0].(string),
			CreatedAt:             row.Values[1].(int64),
			Method:                row.Values[2].(string),
			Caller:                strings.ToLower(row.Values[3].(string)),
			FeeAmount:             decimalToString(row.Values[4]),
			FeeRecipient:          feeRecipient,
			Metadata:              meta,
			DistributionSequence:  seq,
			DistributionRecipient: distRecipient,
			DistributionAmount:    decimalToString(row.Values[9]),
			RawMetadata:           rawMetadata,
		})
		return nil
	})
	return rows, err
}

func fetchLastTransactions(ctx context.Context, platform *kwilTesting.Platform, caller string, address string, limit int64) ([]ledgerRow, error) {
	rows := make([]ledgerRow, 0, 8)
	addr := strings.ToLower(address)
	args := []any{addr, limit}
	err := callView(ctx, platform, caller, "get_last_transactions_v2", args, func(row *common.Row) error {
		feeRecipient := ""
		if row.Values[5] != nil {
			feeRecipient = strings.ToLower(row.Values[5].(string))
		}

		rawMetadata := stringOrEmpty(row.Values[6])
		rows = append(rows, ledgerRow{
			TxID:                  row.Values[0].(string),
			CreatedAt:             row.Values[1].(int64),
			Method:                row.Values[2].(string),
			Caller:                strings.ToLower(row.Values[3].(string)),
			FeeAmount:             decimalToString(row.Values[4]),
			FeeRecipient:          feeRecipient,
			Metadata:              parseMetadata(row.Values[6]),
			DistributionSequence:  int(row.Values[7].(int64)),
			DistributionRecipient: strings.ToLower(stringOrEmpty(row.Values[8])),
			DistributionAmount:    decimalToString(row.Values[9]),
			RawMetadata:           rawMetadata,
		})
		return nil
	})
	return rows, err
}

func fetchLegacyLastTransactions(ctx context.Context, platform *kwilTesting.Platform, caller string, dataProvider string, limit int64) ([]legacyRow, error) {
	rows := make([]legacyRow, 0, 8)
	args := []any{dataProvider, limit}
	err := callView(ctx, platform, caller, "get_last_transactions_v1", args, func(row *common.Row) error {
		rows = append(rows, legacyRow{
			CreatedAt: row.Values[0].(int64),
			Method:    row.Values[1].(string),
		})
		return nil
	})
	return rows, err
}

func fetchTransactionEvent(ctx context.Context, platform *kwilTesting.Platform, caller string, txID string) (ledgerRow, error) {
	var result ledgerRow
	err := callView(ctx, platform, caller, "get_transaction_event", []any{txID}, func(row *common.Row) error {
		feeRecipient := ""
		if row.Values[5] != nil {
			feeRecipient = strings.ToLower(row.Values[5].(string))
		}

		rawMetadata := stringOrEmpty(row.Values[6])
		result = ledgerRow{
			TxID:                  row.Values[0].(string),
			Method:                row.Values[2].(string),
			Caller:                strings.ToLower(row.Values[3].(string)),
			FeeAmount:             decimalToString(row.Values[4]),
			FeeRecipient:          feeRecipient,
			Metadata:              parseMetadata(row.Values[6]),
			FeeDistributions:      stringOrEmpty(row.Values[7]),
			DistributionSequence:  0,
			DistributionRecipient: "",
			DistributionAmount:    "",
			RawMetadata:           rawMetadata,
		}
		return nil
	})
	return result, err
}

func callView(ctx context.Context, platform *kwilTesting.Platform, caller string, action string, args []any, fn func(*common.Row) error) error {
	address, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return err
	}
	tx := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 100,
		},
		Signer:        address.Bytes(),
		Caller:        strings.ToLower(caller),
		TxID:          platform.Txid(),
		Authenticator: coreauth.EthPersonalSignAuth,
	}
	engineCtx := &common.EngineContext{TxContext: tx}
	res, err := platform.Engine.Call(engineCtx, platform.DB, "", action, args, fn)
	if err != nil {
		return err
	}
	if res != nil && res.Error != nil {
		return res.Error
	}
	return nil
}

func decimalToString(val any) string {
	if val == nil {
		return ""
	}
	dec, ok := val.(*kwilTypes.Decimal)
	if !ok {
		panic(fmt.Sprintf("expected *types.Decimal, got %T", val))
	}
	return dec.String()
}

type metadataMap map[string]any

func parseMetadata(val any) metadataMap {
	if val == nil {
		return metadataMap{}
	}

	switch v := val.(type) {
	case string:
		if v == "" {
			return metadataMap{}
		}
		var m metadataMap
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			panic(fmt.Sprintf("failed to parse metadata %q: %v", v, err))
		}
		return m
	default:
		panic(fmt.Sprintf("unexpected metadata type %T", v))
	}
}

func (m metadataMap) String(key string) string {
	val, ok := m[key]
	if !ok {
		return ""
	}
	str, ok := val.(string)
	if !ok {
		panic(fmt.Sprintf("metadata %s is not string: %T", key, val))
	}
	return strings.ToLower(str)
}

func (m metadataMap) Bool(key string) bool {
	val, ok := m[key]
	if !ok {
		return false
	}
	b, ok := val.(bool)
	if !ok {
		panic(fmt.Sprintf("metadata %s is not bool: %T", key, val))
	}
	return b
}

func (m metadataMap) Int(key string) int64 {
	val, ok := m[key]
	if !ok {
		return 0
	}
	switch num := val.(type) {
	case float64:
		return int64(num)
	case json.Number:
		i, err := num.Int64()
		if err != nil {
			panic(fmt.Sprintf("metadata %s invalid json number: %v", key, err))
		}
		return i
	default:
		panic(fmt.Sprintf("metadata %s is not numeric: %T", key, val))
	}
}

func (m metadataMap) StringSlice(key string) []string {
	val, ok := m[key]
	if !ok {
		return nil
	}
	arr, ok := val.([]any)
	if !ok {
		panic(fmt.Sprintf("metadata %s is not array: %T", key, val))
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		str, ok := v.(string)
		if !ok {
			panic(fmt.Sprintf("metadata array %s contains non-string %T", key, v))
		}
		out = append(out, strings.ToLower(str))
	}
	return out
}

func (m metadataMap) Optional(key string) any {
	val, ok := m[key]
	if !ok {
		return nil
	}
	if val == nil {
		return nil
	}
	return val
}

func stringOrEmpty(val any) string {
	if val == nil {
		return ""
	}
	return val.(string)
}

func buildDistribution(recipient, amount string) string {
	if amount == "" || recipient == "" {
		return ""
	}
	return strings.ToLower(recipient) + ":" + amount
}

func ledgerGiveBalance(ctx context.Context, platform *kwilTesting.Platform, wallet string, amount string) error {
	ledgerPointCounter++
	return testerc20.InjectERC20Transfer(
		ctx,
		platform,
		ledgerChain,
		ledgerEscrow,
		ledgerERC20,
		wallet,
		wallet,
		amount,
		ledgerPointCounter,
		nil,
	)
}
