package tn_attestation

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
)

type fakeTxQueryClient struct {
	mu        sync.Mutex
	responses []*ktypes.TxQueryResponse
	errs      []error
	expected  int
	calls     int
	done      chan struct{}
}

func newFakeTxQueryClient(resps []*ktypes.TxQueryResponse, errs []error, expected int) *fakeTxQueryClient {
	if expected == 0 {
		expected = len(resps)
		if len(errs) > expected {
			expected = len(errs)
		}
		if expected == 0 {
			expected = 1
		}
	}
	return &fakeTxQueryClient{
		responses: resps,
		errs:      errs,
		expected:  expected,
		done:      make(chan struct{}),
	}
}

func (f *fakeTxQueryClient) TxQuery(ctx context.Context, txHash ktypes.Hash) (*ktypes.TxQueryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var resp *ktypes.TxQueryResponse
	if len(f.responses) > 0 {
		resp = f.responses[0]
		f.responses = f.responses[1:]
	}

	var err error
	if len(f.errs) > 0 {
		err = f.errs[0]
		f.errs = f.errs[1:]
	}

	f.calls++
	if f.calls >= f.expected {
		select {
		case <-f.done:
		default:
			close(f.done)
		}
	}

	return resp, err
}

func (f *fakeTxQueryClient) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestStatusWorkerProcessesSuccess(t *testing.T) {
	origDelays := statusRetryDelays
	delays := make([]time.Duration, statusMaxAttempts)
	for i := range delays {
		delays[i] = time.Millisecond
	}
	statusRetryDelays = delays
	defer func() { statusRetryDelays = origDelays }()

	ext := &signerExtension{
		logger: log.DiscardLogger,
	}

	client := newFakeTxQueryClient([]*ktypes.TxQueryResponse{
		{Height: 10, Result: &ktypes.TxResult{Code: uint32(ktypes.CodeOk)}},
	}, nil, 1)

	ext.setTxQueryClient(client)
	ext.startStatusWorker()
	ext.enqueueStatusCheck(ktypes.Hash{}, "success-attestation", []byte("requester"))

	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("transaction status worker did not complete in time")
	}

	require.Equal(t, 1, client.Calls())
	close(ext.statusQueue)
}

func TestStatusWorkerRetriesOnFailure(t *testing.T) {
	origDelays := statusRetryDelays
	delays := make([]time.Duration, statusMaxAttempts)
	for i := range delays {
		delays[i] = time.Millisecond
	}
	statusRetryDelays = delays
	defer func() { statusRetryDelays = origDelays }()

	ext := &signerExtension{
		logger: log.DiscardLogger,
	}

	errs := make([]error, statusMaxAttempts)
	for i := range errs {
		errs[i] = fmt.Errorf("tx not found")
	}
	client := newFakeTxQueryClient(nil, errs, statusMaxAttempts)

	ext.setTxQueryClient(client)
	ext.startStatusWorker()
	ext.enqueueStatusCheck(ktypes.Hash{}, "fail-attestation", []byte("requester"))

	select {
	case <-client.done:
	case <-time.After(2 * time.Second):
		t.Fatal("transaction status worker did not exhaust retries in time")
	}

	require.Equal(t, statusMaxAttempts, client.Calls())
	close(ext.statusQueue)
}
