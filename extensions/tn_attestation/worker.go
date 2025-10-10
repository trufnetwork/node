package tn_attestation

import (
	"context"
	"fmt"
	"strings"

	ktypes "github.com/trufnetwork/kwil-db/core/types"
)

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

	// Async transaction status check for logging
	go e.checkTransactionStatus(context.Background(), txHash, item.HashHex, item.Requester)

	return nil
}
