// This file implements the attestation signing worker and async transaction monitoring.
//
// Transaction status checking runs in a dedicated background goroutine to prevent blocking
// EndBlock processing. The worker polls for confirmation over ~2 minutes, logging outcomes
// for operational visibility without impacting consensus performance.
package tn_attestation

import (
	"context"
	"fmt"
	"strings"
	"time"

	ktypes "github.com/trufnetwork/kwil-db/core/types"
)

const (
	statusMaxAttempts   = 12
	statusWorkerTimeout = 2 * time.Minute
)

var statusRetryDelays = []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second}

// txStatusWork queues a broadcast transaction for async status monitoring.
// Avoids blocking consensus by deferring potentially slow RPC queries to a worker goroutine.
type txStatusWork struct {
	hash            ktypes.Hash // Transaction hash from broadcast
	attestationHash string      // Original attestation hash for log correlation
	requester       []byte      // Requester address for audit trail
}

// processAttestationHashes iterates through every dequeued hash, prepares the
// canonical payload(s) for signing, and submits signatures back through
// consensus. All failures are logged and do not abort the remainder of the
// batch so we can make steady progress even when an individual record is
// problematic.
func (e *signerExtension) processAttestationHashes(ctx context.Context, hashes []string) {
	if len(hashes) == 0 {
		return
	}

	e.mu.RLock()
	override := e.processOverride
	e.mu.RUnlock()
	if override != nil {
		override(ctx, hashes)
		return
	}

	logger := e.Logger()
	for _, hashHex := range hashes {
		prepared, err := e.prepareSigningWork(ctx, hashHex)
		if err != nil {
			logger.Error("tn_attestation: failed to prepare signing payload", "hash", hashHex, "error", err)
			continue
		}
		if len(prepared) == 0 {
			logger.Debug("tn_attestation: no unsigned rows for hash", "hash", hashHex)
			continue
		}
		for _, item := range prepared {
			if err := e.submitSignature(ctx, item); err != nil {
				logger.Error("tn_attestation: submit signature failed", "hash", hashHex, "requester", fmt.Sprintf("%x", item.Requester), "error", err)
				continue
			}
			logger.Debug("tn_attestation: attestation signed", "hash", hashHex, "requester", fmt.Sprintf("%x", item.Requester))
		}
	}
}

func (e *signerExtension) submitSignature(ctx context.Context, item *PreparedSignature) error {
	if item == nil {
		return fmt.Errorf("prepared signature is nil")
	}
	if len(item.Requester) == 0 {
		return fmt.Errorf("requester not available for attestation hash %s", item.HashHex)
	}

	service := e.Service()
	if service == nil || service.GenesisConfig == nil {
		return fmt.Errorf("service or genesis config not available")
	}
	if service.GenesisConfig.ChainID == "" {
		return fmt.Errorf("chain id not configured")
	}

	broadcaster := e.Broadcaster()
	if broadcaster == nil {
		return fmt.Errorf("transaction broadcaster unavailable")
	}

	signer := e.NodeSigner()
	if signer == nil {
		return fmt.Errorf("node signer not initialised")
	}

	accountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return fmt.Errorf("derive account id: %w", err)
	}

	accounts := e.Accounts()
	if accounts == nil {
		return fmt.Errorf("accounts subsystem unavailable")
	}

	db := e.DB()
	if db == nil {
		return fmt.Errorf("database handle unavailable")
	}

	account, err := accounts.GetAccount(ctx, db, accountID)
	var nonce uint64 = 1
	if err != nil {
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "not found") && !strings.Contains(msg, "no rows") {
			return fmt.Errorf("get account: %w", err)
		}
	} else {
		nonce = uint64(account.Nonce + 1)
	}

	hashArg, err := ktypes.EncodeValue(item.Hash)
	if err != nil {
		return fmt.Errorf("encode hash argument: %w", err)
	}
	requesterArg, err := ktypes.EncodeValue(item.Requester)
	if err != nil {
		return fmt.Errorf("encode requester argument: %w", err)
	}
	heightArg, err := ktypes.EncodeValue(item.CreatedHeight)
	if err != nil {
		return fmt.Errorf("encode created_height argument: %w", err)
	}
	signatureArg, err := ktypes.EncodeValue(item.Signature)
	if err != nil {
		return fmt.Errorf("encode signature argument: %w", err)
	}

	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "sign_attestation",
		Arguments: [][]*ktypes.EncodedValue{{
			hashArg, requesterArg, heightArg, signatureArg,
		}},
	}

	tx, err := ktypes.CreateNodeTransaction(payload, service.GenesisConfig.ChainID, nonce)
	if err != nil {
		return fmt.Errorf("create tx: %w", err)
	}
	if err := tx.Sign(signer); err != nil {
		return fmt.Errorf("sign tx: %w", err)
	}

	txHash, _, err := broadcaster.BroadcastTx(ctx, tx, 0) // Use BroadcastWaitAccept to avoid blocking consensus
	if err != nil {
		return fmt.Errorf("broadcast tx: %w", err)
	}

	logger := e.Logger()
	logger.Info("tn_attestation: signature broadcast",
		"hash", item.HashHex,
		"tx_hash", txHash,
		"requester", fmt.Sprintf("%x", item.Requester))

	// Queue asynchronous transaction status check for logging
	e.enqueueStatusCheck(txHash, item.HashHex, item.Requester)

	return nil
}

// startStatusWorker initializes the background goroutine that monitors transaction status.
// Uses sync.Once to prevent goroutine leaks across leader transitions. The 128-entry
// buffer provides headroom during burst signing without blocking EndBlock.
func (e *signerExtension) startStatusWorker() {
	e.statusOnce.Do(func() {
		if e.statusQueue == nil {
			e.statusQueue = make(chan txStatusWork, 128)
		}
		go e.runStatusWorker()
	})
}

// enqueueStatusCheck queues a transaction for async status monitoring.
// Drops entries if the queue is full to avoid blocking the signing workflow.
func (e *signerExtension) enqueueStatusCheck(txHash ktypes.Hash, attestationHash string, requester []byte) {
	if e.getTxQueryClient() == nil {
		return
	}
	if e.statusQueue == nil {
		e.startStatusWorker()
	}
	work := txStatusWork{
		hash:            txHash,
		attestationHash: attestationHash,
		requester:       append([]byte(nil), requester...),
	}
	select {
	case e.statusQueue <- work:
	default:
		e.Logger().Warn("tn_attestation: transaction status queue full, dropping entry",
			"hash", attestationHash,
			"tx_hash", txHash)
	}
}

// runStatusWorker consumes queued transactions and monitors each for confirmation.
// Runs for the lifetime of the extension process, surviving leader transitions.
func (e *signerExtension) runStatusWorker() {
	for work := range e.statusQueue {
		ctx, cancel := context.WithTimeout(context.Background(), statusWorkerTimeout)
		e.monitorTransaction(ctx, work)
		cancel()
	}
}

// monitorTransaction polls for transaction confirmation with exponential backoff.
// Queries up to 12 times over ~2 minutes (2s, 5s, then 10s intervals) to handle
// network delays and block production variance. Logs final outcome for observability.
func (e *signerExtension) monitorTransaction(ctx context.Context, work txStatusWork) {
	client := e.getTxQueryClient()
	if client == nil {
		return
	}

	delays := make([]time.Duration, len(statusRetryDelays))
	copy(delays, statusRetryDelays)
	if len(delays) < statusMaxAttempts {
		extra := make([]time.Duration, statusMaxAttempts-len(delays))
		for i := range extra {
			extra[i] = 10 * time.Second
		}
		delays = append(delays, extra...)
	}

	logger := e.Logger()

	for attempt, delay := range delays {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				logger.Warn("tn_attestation: transaction status check cancelled",
					"hash", work.attestationHash,
					"tx_hash", work.hash)
				return
			case <-time.After(delay):
			}
		}

		resp, err := client.TxQuery(ctx, work.hash)
		if err != nil {
			if attempt == len(delays)-1 {
				logger.Warn("tn_attestation: transaction status unknown",
					"hash", work.attestationHash,
					"tx_hash", work.hash,
					"attempt", attempt+1,
					"requester", fmt.Sprintf("%x", work.requester),
					"error", err)
			}
			continue
		}

		if resp.Height <= 0 {
			continue
		}

		if resp.Result != nil && resp.Result.Code == uint32(ktypes.CodeOk) {
			logger.Info("tn_attestation: transaction confirmed",
				"hash", work.attestationHash,
				"tx_hash", work.hash,
				"height", resp.Height,
				"requester", fmt.Sprintf("%x", work.requester))
		} else {
			code := uint32(0)
			logMsg := "transaction result missing"
			if resp.Result != nil {
				code = resp.Result.Code
				logMsg = resp.Result.Log
			}
			logger.Error("tn_attestation: transaction failed",
				"hash", work.attestationHash,
				"tx_hash", work.hash,
				"height", resp.Height,
				"code", code,
				"log", logMsg,
				"requester", fmt.Sprintf("%x", work.requester))
		}
		return
	}
}
