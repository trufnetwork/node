package stream_storage_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	kwilcrypto "github.com/kwilteam/kwil-db/core/crypto"
	"github.com/kwilteam/kwil-db/core/crypto/auth"
	"github.com/kwilteam/kwil-db/core/types/transactions"
	"github.com/trufnetwork/sdk-go/core/tnclient"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
	"golang.org/x/sync/errgroup"
)

// streamInfo holds information about a deployed stream
// It assumes the constants TestPrivateKey, TestKwilProvider and workers are defined in the package

type streamInfo struct {
	name          string
	streamId      util.StreamId
	streamLocator types.StreamLocator
}

// txInfo holds information about a transaction

type txInfo struct {
	hash     transactions.TxHash
	streamId string
}

// streamManager handles stream operations

type streamManager struct {
	client *tnclient.Client
	t      *testing.T
}

// newStreamManager creates a new stream manager using the global TestPrivateKey and TestKwilProvider constants
func newStreamManager(ctx context.Context, t *testing.T) (*streamManager, error) {
	pk, err := kwilcrypto.Secp256k1PrivateKeyFromHex(TestPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	client, err := tnclient.NewClient(
		ctx,
		TestKwilProvider,
		tnclient.WithSigner(&auth.EthPersonalSigner{Key: *pk}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create TN client: %w", err)
	}

	return &streamManager{client: client, t: t}, nil
}

// retryIfNonceError tries the operation up to 5 times if a nonce-related error is detected.
func (sm *streamManager) retryIfNonceError(ctx context.Context, operation func() (transactions.TxHash, error)) (transactions.TxHash, error) {
	const maxAttempts = 5
	var lastErr error

	for attempts := 1; attempts <= maxAttempts; attempts++ {
		txHash, err := operation()
		if err == nil {
			return txHash, nil
		}

		if strings.Contains(strings.ToLower(err.Error()), "nonce") {
			lastErr = err
			sm.t.Logf("Nonce error detected (attempt %d/%d): %v", attempts, maxAttempts, err)
			time.Sleep(1 * time.Second)
			continue
		}
		return txHash, err
	}
	return transactions.TxHash(""), fmt.Errorf("operation failed after %d attempts. Last error: %w", maxAttempts, lastErr)
}

// submitAndWaitForTxs is a generic helper to reduce duplication in deploy, initialize, and destroy steps.
func (sm *streamManager) submitAndWaitForTxs(ctx context.Context, items []string, submitFunc func(string) (transactions.TxHash, error)) error {
	txInfos := make([]txInfo, 0, len(items))

	for _, item := range items {
		txHash, err := sm.retryIfNonceError(ctx, func() (transactions.TxHash, error) {
			return submitFunc(item)
		})
		if err != nil {
			return fmt.Errorf("failed to submit tx for item %s: %w", item, err)
		}
		sm.t.Logf("Submitted TX for %s, hash: %s", item, txHash)
		txInfos = append(txInfos, txInfo{hash: txHash, streamId: item})
	}

	eg, ctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, workers)
	for _, tx := range txInfos {
		tx := tx
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() { <-sem }()
			_, err := sm.client.WaitForTx(ctx, tx.hash, time.Second)
			if err != nil {
				return fmt.Errorf("tx %s for %s not mined: %w", tx.hash, tx.streamId, err)
			}
			sm.t.Logf("TX mined for %s, hash: %s", tx.streamId, tx.hash)
			return nil
		})
	}
	return eg.Wait()
}

// deployStreams deploys streams using the generic submitAndWaitForTxs helper.
func (sm *streamManager) deployStreams(ctx context.Context, count int) ([]streamInfo, error) {
	sm.t.Logf("Deploying %d streams", count)
	streamNames := make([]string, count)
	for i := 0; i < count; i++ {
		streamNames[i] = fmt.Sprintf("stream-%d", i)
	}

	err := sm.submitAndWaitForTxs(ctx, streamNames, func(s string) (transactions.TxHash, error) {
		streamId := util.GenerateStreamId(s)
		return sm.client.DeployStream(ctx, streamId, types.StreamTypePrimitiveUnix)
	})
	if err != nil {
		return nil, err
	}

	streams := make([]streamInfo, count)
	for i, name := range streamNames {
		sid := util.GenerateStreamId(name)
		streams[i] = streamInfo{
			name:          name,
			streamId:      sid,
			streamLocator: sm.client.OwnStreamLocator(sid),
		}
	}
	return streams, nil
}

// initializeStreams initializes streams using the submitAndWaitForTxs helper.
func (sm *streamManager) initializeStreams(ctx context.Context, streams []streamInfo) error {
	sm.t.Logf("Initializing %d streams", len(streams))
	names := make([]string, len(streams))
	for i, s := range streams {
		names[i] = s.name
	}
	return sm.submitAndWaitForTxs(ctx, names, func(s string) (transactions.TxHash, error) {
		sid := util.GenerateStreamId(s)
		stream, err := sm.client.LoadPrimitiveStream(sm.client.OwnStreamLocator(sid))
		if err != nil {
			return transactions.TxHash(""), err
		}
		return stream.InitializeStream(ctx)
	})
}

// destroyStreams destroys streams using the submitAndWaitForTxs helper.
func (sm *streamManager) destroyStreams(ctx context.Context, count int) error {
	sm.t.Logf("Destroying %d streams", count)
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("stream-%d", i)
	}
	return sm.submitAndWaitForTxs(ctx, names, func(s string) (transactions.TxHash, error) {
		sid := util.GenerateStreamId(s)
		return sm.client.DestroyStream(ctx, sid)
	})
}
