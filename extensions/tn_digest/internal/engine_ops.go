package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
)

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
	if _, _, err := broadcaster(ctx, tx, 0); err != nil {
		return fmt.Errorf("broadcast tx: %w", err)
	}
	return nil
}
