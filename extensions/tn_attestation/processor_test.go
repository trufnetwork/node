package tn_attestation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
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
)

func TestComputeAttestationHash(t *testing.T) {
	const (
		version   = uint8(1)
		algorithm = uint8(1)
		height    = uint64(99)
		actionID  = uint16(7)
	)
	dataProvider := []byte("provider")
	streamID := []byte("stream")
	args := []byte{0x01, 0x02}
	result := []byte{0x03, 0x04}

	canonical := buildCanonical(version, algorithm, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	t.Run("hashes canonical bytes when raw present", func(t *testing.T) {
		expected := sha256.Sum256(canonical)
		actual := computeAttestationHash(payload)
		assert.Equal(t, expected, actual)
	})

	t.Run("re-encodes when raw missing", func(t *testing.T) {
		payload.raw = nil
		expected := sha256.Sum256(canonical)
		actual := computeAttestationHash(payload)
		assert.Equal(t, expected, actual)
	})
}

func TestPrepareSigningWork(t *testing.T) {
	t.Cleanup(ResetValidatorSignerForTesting)

	privateKey, _, err := kwilcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	require.NoError(t, InitializeValidatorSigner(privateKey))

	version := uint8(1)
	algo := uint8(1)
	height := uint64(77)
	actionID := uint16(5)
	dataProvider := []byte("provider-1")
	streamID := []byte("stream-abc")
	args := []byte{0x01, 0x02}
	result := []byte{0xAA}

	canonical := buildCanonical(version, algo, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	hash := computeAttestationHash(payload)

	engine := &stubEngine{
		rows: []*common.Row{
			{
				Values: []any{
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
	algo := uint8(1)
	height := uint64(77)
	actionID := uint16(5)
	dataProvider := []byte("provider-1")
	streamID := []byte("stream-abc")
	args := []byte{0x01, 0x02}
	result := []byte{0xAA}

	canonical := buildCanonical(version, algo, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	hash := computeAttestationHash(payload)

	engine := &stubEngine{
		rows: []*common.Row{
			{
				Values: []any{
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
	require.Len(t, decoded.Arguments[0], 4)

	hashBytes := decodeBytesArg(t, decoded.Arguments[0][0], "hash")
	assert.Equal(t, hash[:], hashBytes)

	requesterBytes := decodeBytesArg(t, decoded.Arguments[0][1], "requester")
	assert.Equal(t, []byte("requester"), requesterBytes)

	createdHeight := decodeInt64Arg(t, decoded.Arguments[0][2], "created_height")
	assert.Equal(t, int64(123), createdHeight)

	signatureBytes := decodeBytesArg(t, decoded.Arguments[0][3], "signature")
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
