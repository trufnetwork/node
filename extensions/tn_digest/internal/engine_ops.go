package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// DigestTxResult represents the parsed result from an auto_digest transaction
type DigestTxResult struct {
	ProcessedDays    int
	TotalDeletedRows int
	HasMoreToDelete  bool
}

// EngineOperations wraps engine calls needed by the digest extension.
type EngineOperations struct {
	engine   common.Engine
	logger   log.Logger
	db       sql.DB
	accounts common.Accounts
}

func NewEngineOperations(engine common.Engine, db sql.DB, accounts common.Accounts, logger log.Logger) *EngineOperations {
	return &EngineOperations{engine: engine, db: db, accounts: accounts, logger: logger.New("ops")}
}

// LoadDigestConfig reads the single-row digest configuration.
// Returns (enabled, schedule). If table/row missing, returns false, "" and no error.
func (e *EngineOperations) LoadDigestConfig(ctx context.Context) (bool, string, error) {
	var (
		enabled  bool
		schedule string
		found    bool
	)
	// Read using engine without engine ctx (owner-level read)
	err := e.engine.ExecuteWithoutEngineCtx(ctx, e.db,
		`SELECT enabled, digest_schedule FROM main.digest_config WHERE id = 1`, nil,
		func(row *common.Row) error {
			if len(row.Values) >= 2 {
				if v, ok := row.Values[0].(bool); ok {
					enabled = v
				}
				if s, ok := row.Values[1].(string); ok {
					schedule = s
				}
				found = true
			}
			return nil
		})
	if err != nil {
		msg := err.Error()
		// tolerate missing table; everything else should surface to caller
		if strings.Contains(msg, "digest_config") && (strings.Contains(msg, "does not exist") || strings.Contains(msg, "undefined") || strings.Contains(msg, "not found")) {
			e.logger.Info("digest_config table not found; using defaults")
			return false, "", nil
		}
		return false, "", err
	}
	if !found {
		return false, "", nil
	}
	return enabled, schedule, nil
}

// BuildAndBroadcastAutoDigestTx builds an ActionExecution tx for auto_digest and broadcasts it using the node signer
func (e *EngineOperations) BuildAndBroadcastAutoDigestTx(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error)) error {
	// Get the signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return fmt.Errorf("failed to get signer account: %w", err)
	}

	// Get account information using the accounts service directly on database
	// DB interface embeds Executor, so we can use it directly
	account, err := e.accounts.GetAccount(ctx, e.db, signerAccountID)
	var nextNonce uint64
	if err != nil {
		// Account doesn't exist yet - use nonce 1 for first transaction
		e.logger.Info("DEBUG: Account not found, using nonce 1 for first transaction", "account", fmt.Sprintf("%x", signerAccountID.Identifier), "error", err)
		nextNonce = uint64(1)
	} else {
		// Account exists - use next nonce
		nextNonce = uint64(account.Nonce + 1)
		e.logger.Info("DEBUG: Account found, using next nonce", "account", fmt.Sprintf("%x", signerAccountID.Identifier), "currentNonce", account.Nonce, "nextNonce", nextNonce, "balance", account.Balance)
	}

	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "auto_digest",
		Arguments: nil,
	}

	// Create transaction with proper nonce
	tx, err := ktypes.CreateNodeTransaction(payload, chainID, nextNonce)
	if err != nil {
		return fmt.Errorf("create tx: %w", err)
	}
	if err := tx.Sign(signer); err != nil {
		return fmt.Errorf("sign tx: %w", err)
	}
	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		return fmt.Errorf("broadcast tx: %w", err)
	}

	// Check transaction result code before parsing logs
	if txResult.Code != uint32(ktypes.CodeOk) {
		return fmt.Errorf("transaction failed with code %d (expected %d): %s",
			txResult.Code, uint32(ktypes.CodeOk), txResult.Log)
	}

	// Parse the digest result from the transaction log
	result, err := parseDigestResultFromTxLog(txResult.Log)
	if err != nil {
		return fmt.Errorf("failed to parse digest result: %w", err)
	}

	e.logger.Info("auto_digest completed",
		"processed_days", result.ProcessedDays,
		"deleted_rows", result.TotalDeletedRows,
		"has_more", result.HasMoreToDelete,
		"tx_hash", hash.String())

	return nil
}

// BroadcastAutoDigestWithArgsAndParse builds an ActionExecution tx for auto_digest with args and broadcasts it,
// then parses the result from the transaction log
func (e *EngineOperations) BroadcastAutoDigestWithArgsAndParse(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	deleteCap, expectedRecords, preserveDays int,
) (*DigestTxResult, error) {
	// Get the signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return nil, fmt.Errorf("failed to get signer account: %w", err)
	}

	// Get account information using the accounts service directly on database
	account, err := e.accounts.GetAccount(ctx, e.db, signerAccountID)
	var nextNonce uint64
	if err != nil {
		// Only treat “not found” / “no rows” as missing-account; fail fast on any other error
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "not found") && !strings.Contains(msg, "no rows") {
			return nil, fmt.Errorf("get account: %w", err)
		}
		e.logger.Info(
			"Account not found, using nonce 1 for first transaction",
			"account", fmt.Sprintf("%x", signerAccountID.Identifier),
		)
		nextNonce = 1
	} else {
		// Account exists - use next nonce
		nextNonce = uint64(account.Nonce + 1)
		e.logger.Info(
			"Account found, using next nonce",
			"account", fmt.Sprintf("%x", signerAccountID.Identifier),
			"currentNonce", account.Nonce,
			"nextNonce", nextNonce,
			"balance", account.Balance,
		)
	}

	// Encode arguments
	deleteCapArg, err := ktypes.EncodeValue(int64(deleteCap))
	if err != nil {
		return nil, fmt.Errorf("encode deleteCap: %w", err)
	}
	expectedRecordsArg, err := ktypes.EncodeValue(int64(expectedRecords))
	if err != nil {
		return nil, fmt.Errorf("encode expectedRecords: %w", err)
	}
	preserveDaysArg, err := ktypes.EncodeValue(int64(preserveDays))
	if err != nil {
		return nil, fmt.Errorf("encode preserveDays: %w", err)
	}

	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "auto_digest",
		Arguments: [][]*ktypes.EncodedValue{{
			deleteCapArg, expectedRecordsArg, preserveDaysArg,
		}},
	}

	// Create transaction with proper nonce
	tx, err := ktypes.CreateNodeTransaction(payload, chainID, nextNonce)
	if err != nil {
		return nil, fmt.Errorf("create tx: %w", err)
	}
	if err := tx.Sign(signer); err != nil {
		return nil, fmt.Errorf("sign tx: %w", err)
	}

	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		return nil, fmt.Errorf("broadcast tx: %w", err)
	}

	// Check transaction result code before parsing logs
	if txResult.Code != uint32(ktypes.CodeOk) {
		return nil, fmt.Errorf("transaction failed with code %d (expected %d): %s",
			txResult.Code, uint32(ktypes.CodeOk), txResult.Log)
	}

	// Parse the digest result from the transaction log
	result, err := parseDigestResultFromTxLog(txResult.Log)
	if err != nil {
		return nil, fmt.Errorf("failed to parse digest result: %w", err)
	}

	e.logger.Info("auto_digest with args completed",
		"processed_days", result.ProcessedDays,
		"deleted_rows", result.TotalDeletedRows,
		"has_more", result.HasMoreToDelete,
		"tx_hash", hash.String(),
		"delete_cap", deleteCap,
		"expected_records", expectedRecords,
		"preserve_days", preserveDays)

	return result, nil
}

// BroadcastAutoDigestWithArgsAndRetry wraps broadcast with simple retry logic.
// On ANY error, it waits and refetches a fresh nonce from the database before retrying.
// This handles ALL error scenarios: timeouts, concurrent transactions, nonce collisions, etc.
func (e *EngineOperations) BroadcastAutoDigestWithArgsAndRetry(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	deleteCap, expectedRecords, preserveDays int,
	maxRetries int,
) (*DigestTxResult, error) {
	var lastErr error
	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			e.logger.Warn("Retrying auto_digest broadcast with fresh nonce",
				"attempt", attempt,
				"max_retries", maxRetries,
				"backoff", backoff,
				"last_error", lastErr)

			// Wait before retry (allows pending tx to resolve and prevents rapid retries)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				// Continue to retry
			}

			// Exponential backoff with max cap (applied after wait for next retry)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		// ALWAYS fetch fresh nonce from database on each attempt
		// This automatically handles: timeouts, concurrent transactions, nonce collisions, etc.
		result, hash, err := e.broadcastAutoDigestWithFreshNonce(
			ctx, chainID, signer, broadcaster,
			deleteCap, expectedRecords, preserveDays,
		)

		if err == nil {
			// Success!
			return result, nil
		}

		// On ANY error, retry with fresh nonce after backoff
		lastErr = err
		e.logger.Warn("Broadcast failed, will retry with fresh nonce",
			"attempt", attempt,
			"tx_hash", hash.String(),
			"error", err)
	}

	return nil, fmt.Errorf("max retries (%d) exceeded: %w", maxRetries, lastErr)
}

// broadcastAutoDigestWithFreshNonce always fetches a fresh nonce from the database before broadcasting.
// This ensures we don't use stale nonces even if other transactions have been submitted.
func (e *EngineOperations) broadcastAutoDigestWithFreshNonce(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	deleteCap, expectedRecords, preserveDays int,
) (*DigestTxResult, ktypes.Hash, error) {
	// Get the signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return nil, ktypes.Hash{}, fmt.Errorf("get signer account: %w", err)
	}

	// ALWAYS query database for current nonce (fresh state)
	account, err := e.accounts.GetAccount(ctx, e.db, signerAccountID)
	var nextNonce uint64
	if err != nil {
		// Only treat "not found" / "no rows" as missing-account
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "not found") && !strings.Contains(msg, "no rows") {
			return nil, ktypes.Hash{}, fmt.Errorf("get account: %w", err)
		}
		nextNonce = 1
		e.logger.Info("Account not found, using nonce 1",
			"account", fmt.Sprintf("%x", signerAccountID.Identifier))
	} else {
		// Account exists - use next nonce
		nextNonce = uint64(account.Nonce + 1)
		e.logger.Info("Fresh nonce from database",
			"account", fmt.Sprintf("%x", signerAccountID.Identifier),
			"db_nonce", account.Nonce,
			"next_nonce", nextNonce,
			"balance", account.Balance)
	}

	// Encode arguments
	deleteCapArg, err := ktypes.EncodeValue(int64(deleteCap))
	if err != nil {
		return nil, ktypes.Hash{}, fmt.Errorf("encode deleteCap: %w", err)
	}
	expectedRecordsArg, err := ktypes.EncodeValue(int64(expectedRecords))
	if err != nil {
		return nil, ktypes.Hash{}, fmt.Errorf("encode expectedRecords: %w", err)
	}
	preserveDaysArg, err := ktypes.EncodeValue(int64(preserveDays))
	if err != nil {
		return nil, ktypes.Hash{}, fmt.Errorf("encode preserveDays: %w", err)
	}

	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "auto_digest",
		Arguments: [][]*ktypes.EncodedValue{{
			deleteCapArg, expectedRecordsArg, preserveDaysArg,
		}},
	}

	// Create transaction with fresh nonce
	tx, err := ktypes.CreateNodeTransaction(payload, chainID, nextNonce)
	if err != nil {
		return nil, ktypes.Hash{}, fmt.Errorf("create tx: %w", err)
	}
	if err := tx.Sign(signer); err != nil {
		return nil, ktypes.Hash{}, fmt.Errorf("sign tx: %w", err)
	}

	// Broadcast
	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		// Return error but also return hash for logging purposes
		return nil, hash, err
	}

	// Check transaction result code before parsing logs
	if txResult.Code != uint32(ktypes.CodeOk) {
		return nil, hash, fmt.Errorf("transaction failed with code %d (expected %d): %s",
			txResult.Code, uint32(ktypes.CodeOk), txResult.Log)
	}

	// Parse the digest result from the transaction log
	result, err := parseDigestResultFromTxLog(txResult.Log)
	if err != nil {
		return nil, hash, fmt.Errorf("parse digest result: %w", err)
	}

	e.logger.Info("auto_digest with args completed",
		"processed_days", result.ProcessedDays,
		"deleted_rows", result.TotalDeletedRows,
		"has_more", result.HasMoreToDelete,
		"tx_hash", hash.String(),
		"nonce", nextNonce,
		"delete_cap", deleteCap,
		"expected_records", expectedRecords,
		"preserve_days", preserveDays)

	return result, hash, nil
}

// parseDigestResultFromTxLog parses the digest result from transaction log output
// The digest action outputs log entries like: "auto_digest:{...json...}"
func parseDigestResultFromTxLog(logOutput string) (*DigestTxResult, error) {
	if logOutput == "" {
		return nil, fmt.Errorf("empty log output")
	}

	// Split log into lines and look for auto_digest entries
	lines := strings.Split(logOutput, "\n")
	var digestJSON string

	// Find the last auto_digest line
	for _, line := range lines {
		if strings.Contains(line, "auto_digest:") {
			// Extract JSON part after "auto_digest:"
			parts := strings.SplitN(line, "auto_digest:", 2)
			if len(parts) == 2 {
				digestJSON = strings.TrimSpace(parts[1])
			}
		}
	}

	if digestJSON == "" {
		return nil, fmt.Errorf("no auto_digest log entry found")
	}

	// Remove surrounding quotes if present (in case the JSON is quoted in the log)
	digestJSON = strings.Trim(digestJSON, `"`)

	// Parse JSON properly
	var jsonResult struct {
		ProcessedDays    int  `json:"processed_days"`
		TotalDeletedRows int  `json:"total_deleted_rows"`
		HasMoreToDelete  bool `json:"has_more_to_delete"`
	}

	if err := json.Unmarshal([]byte(digestJSON), &jsonResult); err != nil {
		return nil, fmt.Errorf("failed to parse digest JSON: %w", err)
	}

	result := &DigestTxResult{
		ProcessedDays:    jsonResult.ProcessedDays,
		TotalDeletedRows: jsonResult.TotalDeletedRows,
		HasMoreToDelete:  jsonResult.HasMoreToDelete,
	}

	return result, nil
}
