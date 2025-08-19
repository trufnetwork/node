package internal

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	sql "github.com/trufnetwork/kwil-db/node/types/sql"
)

// EngineOperations wraps engine calls needed by the digest extension.
type EngineOperations struct {
	engine common.Engine
	logger log.Logger
	db     sql.DB
}

func NewEngineOperations(engine common.Engine, db sql.DB, logger log.Logger) *EngineOperations {
	return &EngineOperations{engine: engine, db: db, logger: logger.New("ops")}
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
		`SELECT enabled, digest_schedule FROM digest_config WHERE id = 1`, nil,
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
		// tolerate missing table; extension will use defaults
		e.logger.Warn("failed to read digest_config; using defaults", "error", err)
		return false, "", nil
	}
	if !found {
		return false, "", nil
	}
	return enabled, schedule, nil
}

// EnsureDefaultDigestConfig inserts a default disabled row if none exists, using owner exec.
func (e *EngineOperations) EnsureDefaultDigestConfig(ctx context.Context) error {
	stmt := `INSERT INTO digest_config (id, enabled, digest_schedule, updated_at_height)
	         SELECT 1, false, '0 */6 * * *', 0
	         WHERE NOT EXISTS (SELECT 1 FROM digest_config WHERE id = 1)`
	if err := e.engine.ExecuteWithoutEngineCtx(ctx, e.db, stmt, nil, nil); err != nil {
		// Ignore errors (e.g., table missing); will be retried on next start or after migrations
		e.logger.Debug("ensure default digest_config failed (ignored)", "error", err)
	}
	return nil
}

// BuildAndBroadcastAutoDigestTx builds an ActionExecution tx for auto_digest and broadcasts it using the node signer
func (e *EngineOperations) BuildAndBroadcastAutoDigestTx(ctx context.Context, chainID string, signer auth.Signer, broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error)) error {
	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "auto_digest",
		Arguments: nil,
	}
	// Nonce handling: for now rely on node to correct via mempool replacement if needed. Use 0.
	tx, err := ktypes.CreateNodeTransaction(payload, chainID, 0)
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
