package tn_attestation

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	kcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

func TestSigningWorkflowIntegration(t *testing.T) {
	t.Helper()

	t.Run("QueuePath", func(t *testing.T) {
		runSigningIntegration(t, true)
	})

	t.Run("FallbackPath", func(t *testing.T) {
		runSigningIntegration(t, false)
	})
}

func runSigningIntegration(t *testing.T, useQueue bool) {
	t.Helper()

	resetIntegrationState()

	privateKey, publicKey, err := kcrypto.GenerateSecp256k1Key(nil)
	require.NoError(t, err)
	require.NoError(t, InitializeValidatorSigner(privateKey))
	defer ResetValidatorSignerForTesting()

	// Canonical payload mirrors the SQL construction to exercise the full pipeline.
	version := uint8(1)
	algo := uint8(1)
	height := uint64(77)
	actionID := uint16(5)
	dataProvider := []byte("provider-queue-flow")
	streamID := []byte("stream-queue-flow")
	args := []byte{0x01, 0x02}
	result := []byte{0x03}

	canonical := buildCanonicalPayload(version, algo, height, dataProvider, streamID, actionID, args, result)
	payload, err := ParseCanonicalPayload(canonical)
	require.NoError(t, err)

	hash := computeAttestationHash(payload)
	hashHex := hex.EncodeToString(hash[:])
	requester := []byte("requester-1")
	requestTxID := "0xqueuepath"

	engine := &integrationEngineStub{
		rows: []*common.Row{
			{
				Values: []any{
					requestTxID,
					hash[:],
					requester,
					canonical,
					int64(123),
				},
			},
		},
		hashRows: []*common.Row{
			{Values: []any{hashHex}},
		},
	}

	ext := getExtension()
	ext.logger = log.DiscardLogger
	ext.service = &common.Service{
		Logger:   log.DiscardLogger,
		Identity: publicKey.Bytes(),
		GenesisConfig: &config.GenesisConfig{
			ChainID: "integration-test-chain",
		},
	}
	ext.engine = engine
	ext.db = integrationDBStub{}
	ext.accounts = &signerAccountsStub{}
	ext.scanIntervalBlocks = 1
	ext.scanBatchLimit = 10
	ext.nodeSigner = auth.GetNodeSigner(privateKey)

	broadcaster := &captureBroadcaster{}
	ext.broadcaster = broadcaster

	queue := GetAttestationQueue()
	queue.Clear()
	if useQueue {
		queue.Enqueue(hashHex)
	}

	ctx := context.Background()
	records, err := ext.fetchUnsignedAttestations(ctx, hash[:])
	require.NoError(t, err)
	require.Truef(t, engine.served, "engine.ExecuteWithoutEngineCtx was not invoked (statement=%s)", engine.lastStmt)
	require.Lenf(t, records, 1, "fetchUnsignedAttestations returned no rows (statement=%s)", engine.lastStmt)

	prepared, err := ext.prepareSigningWork(ctx, hashHex)
	require.NoError(t, err)
	require.Len(t, prepared, 1, "expected signing work to be prepared")
	require.Equal(t, hashHex, prepared[0].HashHex)
	require.Equal(t, int64(123), prepared[0].CreatedHeight)
	require.Equal(t, requestTxID, prepared[0].RequestTxID)

	if useQueue {
		ext.processAttestationHashes(ctx, []string{hashHex})
	} else {
		hashes, err := ext.fetchPendingHashes(ctx, 10)
		require.NoError(t, err)
		require.Equal(t, []string{hashHex}, hashes)
		ext.processAttestationHashes(ctx, hashes)
	}

	require.Equal(t, 1, broadcaster.calls, "expected single broadcast")
	require.NoError(t, broadcaster.lastErr)
	require.Len(t, broadcaster.requestTxIDs, 1)
	require.Equal(t, requestTxID, broadcaster.requestTxIDs[0])
	require.Len(t, broadcaster.signatures, 1)
	require.Len(t, broadcaster.signatures[0], 65, "expected 65-byte signature")
}

func resetIntegrationState() {
	extensionOnce = sync.Once{}
	SetExtension(nil)
	queueOnce = sync.Once{}
	attestationQueueSingleton = nil
}

type captureBroadcaster struct {
	requestTxIDs []string
	signatures   [][]byte
	calls        int
	lastErr      error
}

func (b *captureBroadcaster) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	b.calls++

	payload := new(ktypes.ActionExecution)
	if err := payload.UnmarshalBinary(tx.Body.Payload); err != nil {
		b.lastErr = err
		return ktypes.Hash{}, nil, err
	}

	if len(payload.Arguments) == 0 || len(payload.Arguments[0]) != 2 {
		err := fmt.Errorf("unexpected argument shape")
		b.lastErr = err
		return ktypes.Hash{}, nil, err
	}

	requestVal, err := payload.Arguments[0][0].Decode()
	if err != nil {
		b.lastErr = err
		return ktypes.Hash{}, nil, err
	}
	var requestTxID string
	switch typed := requestVal.(type) {
	case string:
		requestTxID = typed
	case *string:
		if typed == nil {
			err := fmt.Errorf("request_tx_id argument was null")
			b.lastErr = err
			return ktypes.Hash{}, nil, err
		}
		requestTxID = *typed
	default:
		err := fmt.Errorf("request_tx_id argument type %T", requestVal)
		b.lastErr = err
		return ktypes.Hash{}, nil, err
	}
	b.requestTxIDs = append(b.requestTxIDs, requestTxID)

	sigVal, err := payload.Arguments[0][1].Decode()
	if err != nil {
		b.lastErr = err
		return ktypes.Hash{}, nil, err
	}
	var sigBytes []byte
	switch typed := sigVal.(type) {
	case []byte:
		sigBytes = typed
	case *[]byte:
		if typed == nil {
			err := fmt.Errorf("signature argument was null")
			b.lastErr = err
			return ktypes.Hash{}, nil, err
		}
		sigBytes = *typed
	default:
		err := fmt.Errorf("signature argument type %T", sigVal)
		b.lastErr = err
		return ktypes.Hash{}, nil, err
	}
	b.signatures = append(b.signatures, bytes.Clone(sigBytes))

	return ktypes.Hash{}, &ktypes.TxResult{Code: uint32(ktypes.CodeOk)}, nil
}

type integrationEngineStub struct {
	rows     []*common.Row
	hashRows []*common.Row
	lastStmt string
	served   bool
}

func (s *integrationEngineStub) Call(*common.EngineContext, sql.DB, string, string, []any, func(*common.Row) error) (*common.CallResult, error) {
	panic("Call not implemented")
}

func (s *integrationEngineStub) CallWithoutEngineCtx(context.Context, sql.DB, string, string, []any, func(*common.Row) error) (*common.CallResult, error) {
	panic("CallWithoutEngineCtx not implemented")
}

func (s *integrationEngineStub) Execute(*common.EngineContext, sql.DB, string, map[string]any, func(*common.Row) error) error {
	panic("Execute not implemented")
}

func (s *integrationEngineStub) ExecuteWithoutEngineCtx(ctx context.Context, db sql.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	s.lastStmt = statement
	if strings.Contains(statement, "GROUP BY attestation_hash") {
		for _, row := range s.hashRows {
			if err := fn(row); err != nil {
				return err
			}
		}
		return nil
	}
	for _, row := range s.rows {
		s.served = true
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

type integrationDBStub struct{}

func (integrationDBStub) Execute(context.Context, string, ...any) (*sql.ResultSet, error) {
	return nil, nil
}

func (integrationDBStub) BeginTx(context.Context) (sql.Tx, error) {
	return nil, fmt.Errorf("transactions not supported in stub")
}

type signerAccountsStub struct{}

func (signerAccountsStub) Credit(context.Context, sql.Executor, *ktypes.AccountID, *big.Int) error {
	return nil
}

func (signerAccountsStub) Transfer(context.Context, sql.TxMaker, *ktypes.AccountID, *ktypes.AccountID, *big.Int) error {
	return nil
}

func (signerAccountsStub) GetAccount(context.Context, sql.Executor, *ktypes.AccountID) (*ktypes.Account, error) {
	return nil, fmt.Errorf("not found")
}

func (signerAccountsStub) ApplySpend(context.Context, sql.Executor, *ktypes.AccountID, *big.Int, int64) error {
	return nil
}

func buildCanonicalPayload(version, algo uint8, blockHeight uint64, dataProvider, streamID []byte, actionID uint16, args, result []byte) []byte {
	versionBytes := []byte{version}
	algoBytes := []byte{algo}

	heightBytes := make([]byte, 8)
	binaryBigEndianPutUint64(heightBytes, blockHeight)

	actionBytes := make([]byte, 2)
	binaryBigEndianPutUint16(actionBytes, actionID)

	segments := [][]byte{
		versionBytes,
		algoBytes,
		heightBytes,
		lengthPrefixLittleEndian(dataProvider),
		lengthPrefixLittleEndian(streamID),
		actionBytes,
		lengthPrefixLittleEndian(args),
		lengthPrefixLittleEndian(result),
	}

	var buf bytes.Buffer
	for _, seg := range segments {
		buf.Write(seg)
	}
	return buf.Bytes()
}

func lengthPrefixLittleEndian(data []byte) []byte {
	if data == nil {
		data = []byte{}
	}
	prefixed := make([]byte, 4+len(data))
	binaryLittleEndianPutUint32(prefixed[:4], uint32(len(data)))
	copy(prefixed[4:], data)
	return prefixed
}

func binaryBigEndianPutUint64(b []byte, v uint64) {
	_ = b[7]
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func binaryBigEndianPutUint16(b []byte, v uint16) {
	_ = b[1]
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func binaryLittleEndianPutUint32(b []byte, v uint32) {
	_ = b[3]
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
