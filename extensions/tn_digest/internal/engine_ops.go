package internal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
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
		// Account doesn't exist yet - use nonce 1 for first transaction
		e.logger.Info("DEBUG: Account not found, using nonce 1 for first transaction", "account", fmt.Sprintf("%x", signerAccountID.Identifier), "error", err)
		nextNonce = uint64(1)
	} else {
		// Account exists - use next nonce
		nextNonce = uint64(account.Nonce + 1)
		e.logger.Info("DEBUG: Account found, using next nonce", "account", fmt.Sprintf("%x", signerAccountID.Identifier), "currentNonce", account.Nonce, "nextNonce", nextNonce, "balance", account.Balance)
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

	// Parse the JSON (simple parsing since we know the expected structure)
	result := &DigestTxResult{}

	// Remove surrounding quotes if present
	digestJSON = strings.Trim(digestJSON, `"`)

	// Extract processed_days
	if start := strings.Index(digestJSON, `"processed_days":`); start != -1 {
		start += len(`"processed_days":`)
		if end := strings.Index(digestJSON[start:], ","); end != -1 {
			if val, err := strconv.Atoi(digestJSON[start : start+end]); err == nil {
				result.ProcessedDays = val
			}
		} else if end := strings.Index(digestJSON[start:], "}"); end != -1 {
			if val, err := strconv.Atoi(digestJSON[start : start+end]); err == nil {
				result.ProcessedDays = val
			}
		}
	}

	// Extract total_deleted_rows
	if start := strings.Index(digestJSON, `"total_deleted_rows":`); start != -1 {
		start += len(`"total_deleted_rows":`)
		if end := strings.Index(digestJSON[start:], ","); end != -1 {
			if val, err := strconv.Atoi(digestJSON[start : start+end]); err == nil {
				result.TotalDeletedRows = val
			}
		} else if end := strings.Index(digestJSON[start:], "}"); end != -1 {
			if val, err := strconv.Atoi(digestJSON[start : start+end]); err == nil {
				result.TotalDeletedRows = val
			}
		}
	}

	// Extract has_more_to_delete
	if start := strings.Index(digestJSON, `"has_more_to_delete":`); start != -1 {
		start += len(`"has_more_to_delete":`)
		if end := strings.Index(digestJSON[start:], ","); end != -1 {
			result.HasMoreToDelete = digestJSON[start:start+end] == "true"
		} else if end := strings.Index(digestJSON[start:], "}"); end != -1 {
			result.HasMoreToDelete = digestJSON[start:start+end] == "true"
		}
	}

	return result, nil
}
