package tn_attestation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	nodesql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestComputeAttestationHash(t *testing.T) {
	const (
		version   = uint8(1)
		algorithm = uint8(0)
		height    = uint64(99)
		actionID  = uint16(7)
	)
	dataProvider := bytes.Repeat([]byte{0x11}, 20)
	streamID := bytes.Repeat([]byte{0x22}, 32)
	args := []byte{0x01, 0x02}
	result := []byte{0x03, 0x04}

	canonical := BuildCanonicalPayload(version, algorithm, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	expected := sha256.Sum256(buildHashMaterial(version, algorithm, dataProvider, streamID, actionID, args))
	actual := computeAttestationHash(payload)
	assert.Equal(t, expected, actual)

	payload.raw = nil
	actual = computeAttestationHash(payload)
	assert.Equal(t, expected, actual)
}

func TestGoldenPayloadFixtureMatches(t *testing.T) {
	const goldenPrivateKeyHex = "0000000000000000000000000000000000000000000000000000000000000001"

	type goldenArgs struct {
		DataProvider  string      `json:"data_provider"`
		StreamID      string      `json:"stream_id"`
		StartTime     int64       `json:"start_time"`
		EndTime       int64       `json:"end_time"`
		PendingFilter interface{} `json:"pending_filter"`
		UseCache      bool        `json:"use_cache"`
	}

	type goldenResult struct {
		Timestamps []int64  `json:"timestamps"`
		Values     []string `json:"values"`
	}

	type goldenFixture struct {
		CanonicalHex string       `json:"canonical_hex"`
		SignatureHex string       `json:"signature_hex"`
		PayloadHex   string       `json:"payload_hex"`
		DataProvider string       `json:"data_provider"`
		StreamID     string       `json:"stream_id"`
		BlockHeight  uint64       `json:"block_height"`
		ActionID     uint16       `json:"action_id"`
		Args         goldenArgs   `json:"args"`
		Result       goldenResult `json:"result"`
	}

	_, filename, _, _ := runtime.Caller(0)
	fixturePath := filepath.Join(filepath.Dir(filename), "testdata", "attestation_golden.json")
	fixtureBytes, err := os.ReadFile(fixturePath)
	require.NoError(t, err, "read golden fixture")

	var fx goldenFixture
	require.NoError(t, json.Unmarshal(fixtureBytes, &fx), "parse golden fixture")

	argsBytes, err := tn_utils.EncodeActionArgs([]any{
		fx.Args.DataProvider,
		fx.Args.StreamID,
		fx.Args.StartTime,
		fx.Args.EndTime,
		fx.Args.PendingFilter,
		fx.Args.UseCache,
	})
	require.NoError(t, err, "encode golden args")

	rows := make([]*common.Row, len(fx.Result.Timestamps))
	for i := range fx.Result.Timestamps {
		dec := ktypes.MustParseDecimalExplicit(fx.Result.Values[i], 36, 18)
		rows[i] = &common.Row{Values: []any{fx.Result.Timestamps[i], dec}}
	}

	resultCanonical, err := tn_utils.EncodeQueryResultCanonical(rows)
	require.NoError(t, err, "encode result canonical")
	resultPayload, err := tn_utils.EncodeDataPointsABI(resultCanonical)
	require.NoError(t, err, "encode ABI payload")

	providerAddr := util.Unsafe_NewEthereumAddressFromString(fx.DataProvider)
	computedCanonical := BuildCanonicalPayload(
		1,
		0,
		fx.BlockHeight,
		providerAddr.Bytes(),
		[]byte(fx.StreamID),
		fx.ActionID,
		argsBytes,
		resultPayload,
	)

	computedCanonicalHex := hex.EncodeToString(computedCanonical)
	require.Equal(t, strings.ToLower(fx.CanonicalHex), computedCanonicalHex, "canonical payload mismatch")

	parsed, err := ParseCanonicalPayload(computedCanonical)
	require.NoError(t, err)
	digest := parsed.SigningDigest()

	privKey, err := kwilcrypto.Secp256k1PrivateKeyFromHex(goldenPrivateKeyHex)
	require.NoError(t, err)
	signer, err := NewValidatorSigner(privKey)
	require.NoError(t, err)
	signature, err := signer.SignDigest(digest[:])
	require.NoError(t, err)

	signatureHex := hex.EncodeToString(signature)
	require.Equal(t, strings.ToLower(fx.SignatureHex), signatureHex, "signature mismatch")

	computedPayloadHex := hex.EncodeToString(append(append([]byte(nil), computedCanonical...), signature...))
	require.Equal(t, strings.ToLower(fx.PayloadHex), computedPayloadHex, "payload mismatch")
}

func TestPrepareSigningWork(t *testing.T) {
	t.Cleanup(ResetValidatorSignerForTesting)

	privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	require.NoError(t, InitializeValidatorSigner(privateKey))

	version := uint8(1)
	algo := uint8(0)
	height := uint64(77)
	actionID := uint16(5)
	dataProvider := bytes.Repeat([]byte{0x33}, 20)
	streamID := bytes.Repeat([]byte{0x44}, 32)
	args := []byte{0x01, 0x02}
	result := []byte{0xAA}

	canonical := BuildCanonicalPayload(version, algo, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	hash := computeAttestationHash(payload)

	engine := &stubEngine{
		rows: []*common.Row{
			{
				Values: []any{
					"0xprepare",
					hash[:],
					[]byte("requester"),
					canonical,
					int64(123),
				},
			},
		},
	}

	ext := &signerExtension{
		logger:             log.DiscardLogger,
		scanIntervalBlocks: 100,
	}
	ext.setApp(&common.App{
		Engine: engine,
		DB:     stubDB{},
	})

	prepared, err := ext.prepareSigningWork(context.Background(), hex.EncodeToString(hash[:]))
	require.NoError(t, err)
	require.Len(t, prepared, 1)

	ps := prepared[0]
	assert.Equal(t, hash[:], ps.Hash)
	assert.Equal(t, payload, ps.Payload)
	assert.Equal(t, int64(123), ps.CreatedHeight)
	assert.Len(t, ps.Signature, 65)
}

func TestSubmitSignature(t *testing.T) {
	t.Cleanup(ResetValidatorSignerForTesting)

	privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	require.NoError(t, InitializeValidatorSigner(privateKey))

	version := uint8(1)
	algo := uint8(0)
	height := uint64(77)
	actionID := uint16(5)
	dataProvider := bytes.Repeat([]byte{0x55}, 20)
	streamID := bytes.Repeat([]byte{0x66}, 32)
	args := []byte{0x01, 0x02}
	result := []byte{0xAA}

	canonical := BuildCanonicalPayload(version, algo, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	hash := computeAttestationHash(payload)

	engine := &stubEngine{
		rows: []*common.Row{
			{
				Values: []any{
					"0xsubmit",
					hash[:],
					[]byte("requester"),
					canonical,
					int64(123),
				},
			},
		},
	}

	service := &common.Service{
		Logger:        log.DiscardLogger,
		GenesisConfig: &config.GenesisConfig{ChainID: "test-chain"},
		LocalConfig:   &config.Config{},
	}

	accounts := &stubAccounts{
		acct: &ktypes.Account{Nonce: 7},
	}

	broadcaster := &recordingBroadcaster{}

	ext := &signerExtension{
		logger:             log.DiscardLogger,
		scanIntervalBlocks: 100,
	}
	ext.setService(service)
	ext.setApp(&common.App{
		Engine:   engine,
		DB:       stubDB{},
		Accounts: accounts,
		Service:  service,
	})
	ext.setNodeSigner(auth.GetNodeSigner(privateKey))
	ext.setBroadcaster(broadcaster)

	prepared, err := ext.prepareSigningWork(context.Background(), hex.EncodeToString(hash[:]))
	require.NoError(t, err)
	require.Len(t, prepared, 1)

	err = ext.submitSignature(context.Background(), prepared[0])
	require.NoError(t, err)

	assert.Equal(t, 1, broadcaster.calls)
	require.NotNil(t, broadcaster.lastTx)
	var decoded ktypes.ActionExecution
	require.NoError(t, decoded.UnmarshalBinary(broadcaster.lastTx.Body.Payload))
	assert.Equal(t, "main", decoded.Namespace)
	assert.Equal(t, "sign_attestation", decoded.Action)
	require.Len(t, decoded.Arguments, 1)
	require.Len(t, decoded.Arguments[0], 2)

	requestTxID := decodeStringArg(t, decoded.Arguments[0][0], "request_tx_id")
	assert.Equal(t, prepared[0].RequestTxID, requestTxID)

	signatureBytes := decodeBytesArg(t, decoded.Arguments[0][1], "signature")
	assert.Len(t, signatureBytes, 65)
}

func decodeBytesArg(t *testing.T, arg *ktypes.EncodedValue, fieldName string) []byte {
	t.Helper()
	val, err := arg.Decode()
	require.NoError(t, err)
	switch typed := val.(type) {
	case []byte:
		return typed
	case *[]byte:
		require.NotNil(t, typed, "%s argument pointer was nil", fieldName)
		return *typed
	default:
		t.Fatalf("unexpected %s argument type %T", fieldName, val)
		return nil
	}
}

func decodeInt64Arg(t *testing.T, arg *ktypes.EncodedValue, fieldName string) int64 {
	t.Helper()
	val, err := arg.Decode()
	require.NoError(t, err)
	switch typed := val.(type) {
	case int64:
		return typed
	case *int64:
		require.NotNil(t, typed, "%s argument pointer was nil", fieldName)
		return *typed
	default:
		t.Fatalf("unexpected %s argument type %T", fieldName, val)
		return 0
	}
}

func decodeStringArg(t *testing.T, arg *ktypes.EncodedValue, fieldName string) string {
	t.Helper()
	val, err := arg.Decode()
	require.NoError(t, err)
	switch typed := val.(type) {
	case string:
		return typed
	case *string:
		require.NotNil(t, typed, "%s argument pointer was nil", fieldName)
		return *typed
	default:
		t.Fatalf("unexpected %s argument type %T", fieldName, val)
		return ""
	}
}

func buildHashMaterial(version, algo uint8, dataProvider, streamID []byte, actionID uint16, args []byte) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(version)
	buf.WriteByte(algo)
	buf.Write(lengthPrefixBigEndian(dataProvider))
	buf.Write(lengthPrefixBigEndian(streamID))

	var actionBytes [2]byte
	binary.BigEndian.PutUint16(actionBytes[:], actionID)
	buf.Write(actionBytes[:])
	buf.Write(lengthPrefixBigEndian(args))

	return buf.Bytes()
}

func TestFetchPendingHashes(t *testing.T) {
	ext := &signerExtension{
		logger:             log.DiscardLogger,
		scanIntervalBlocks: 100,
		scanBatchLimit:     5,
	}

	engine := &stubEngine{
		hashRows: []*common.Row{
			{Values: []any{"aaa"}},
			{Values: []any{"bbb"}},
			{Values: []any{"ccc"}},
		},
	}

	ext.setApp(&common.App{
		Engine: engine,
		DB:     stubDB{},
	})

	hashes, err := ext.fetchPendingHashes(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"aaa", "bbb"}, hashes)

	hashes, err = ext.fetchPendingHashes(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"aaa", "bbb", "ccc"}, hashes)
}

type stubEngine struct {
	rows     []*common.Row
	hashRows []*common.Row
}

func (s *stubEngine) Call(ctx *common.EngineContext, db nodesql.DB, namespace, action string, args []any, resultFn func(*common.Row) error) (*common.CallResult, error) {
	panic("not implemented")
}

func (s *stubEngine) CallWithoutEngineCtx(ctx context.Context, db nodesql.DB, namespace, action string, args []any, resultFn func(*common.Row) error) (*common.CallResult, error) {
	panic("not implemented")
}

func (s *stubEngine) Execute(ctx *common.EngineContext, db nodesql.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	panic("not implemented")
}

func (s *stubEngine) ExecuteWithoutEngineCtx(ctx context.Context, db nodesql.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	rows := s.rows
	if params != nil {
		if limit, ok := params["limit"]; ok {
			rows = s.hashRows
			if n := toInt(limit); n >= 0 && n < len(rows) {
				rows = rows[:n]
			}
		}
	} else if strings.Contains(statement, "GROUP BY attestation_hash") {
		// Fallback for callers that forget to pass params.
		rows = s.hashRows
	}
	for _, row := range rows {
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

type stubDB struct{}

func (stubDB) Execute(ctx context.Context, stmt string, args ...any) (*nodesql.ResultSet, error) {
	return nil, fmt.Errorf("not implemented")
}

func (stubDB) BeginTx(ctx context.Context) (nodesql.Tx, error) {
	return nil, fmt.Errorf("not implemented")
}

func toInt(v any) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case uint:
		return int(val)
	case uint32:
		return int(val)
	case uint64:
		return int(val)
	default:
		return -1
	}
}

type stubAccounts struct {
	acct *ktypes.Account
	err  error
}

func (s *stubAccounts) Credit(ctx context.Context, tx nodesql.Executor, account *ktypes.AccountID, balance *big.Int) error {
	return nil
}

func (s *stubAccounts) Transfer(ctx context.Context, tx nodesql.TxMaker, from, to *ktypes.AccountID, amt *big.Int) error {
	return nil
}

func (s *stubAccounts) GetAccount(ctx context.Context, tx nodesql.Executor, account *ktypes.AccountID) (*ktypes.Account, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.acct != nil {
		return s.acct, nil
	}
	return nil, fmt.Errorf("account not found")
}

func (s *stubAccounts) ApplySpend(ctx context.Context, tx nodesql.Executor, account *ktypes.AccountID, amount *big.Int, nonce int64) error {
	return nil
}

type recordingBroadcaster struct {
	calls  int
	lastTx *ktypes.Transaction
}

func (b *recordingBroadcaster) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	b.calls++
	b.lastTx = tx
	return ktypes.Hash{}, &ktypes.TxResult{Code: uint32(ktypes.CodeOk)}, nil
}
