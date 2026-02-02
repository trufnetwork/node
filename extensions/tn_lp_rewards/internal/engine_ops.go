package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// EngineOperations wraps engine calls needed by the LP rewards extension
type EngineOperations struct {
	engine   common.Engine
	logger   log.Logger
	db       sql.DB
	dbPool   sql.DelayedReadTxMaker
	accounts common.Accounts
}

// NewEngineOperations creates a new EngineOperations instance
func NewEngineOperations(engine common.Engine, db sql.DB, dbPool sql.DelayedReadTxMaker, accounts common.Accounts, logger log.Logger) *EngineOperations {
	return &EngineOperations{
		engine:   engine,
		db:       db,
		dbPool:   dbPool,
		accounts: accounts,
		logger:   logger.New("lp_rewards_ops"),
	}
}

// isAccountNotFoundError checks if the error indicates an account was not found
func isAccountNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no rows")
}

// getFreshReadTx returns a fresh database connection for read operations
func (e *EngineOperations) getFreshReadTx(ctx context.Context) (sql.DB, func(), error) {
	if e.dbPool != nil {
		readTx := e.dbPool.BeginDelayedReadTx()
		cleanup := func() {
			readTx.Rollback(ctx)
		}
		return readTx, cleanup, nil
	}
	// Fallback to stored db connection
	if e.db == nil {
		return nil, func() {}, fmt.Errorf("no database connection available (both dbPool and db are nil)")
	}
	e.logger.Warn("dbPool is nil, falling back to stored db connection (may be stale)")
	return e.db, func() {}, nil
}

// LoadLPRewardsConfig reads the LP rewards configuration
// Returns (enabled, samplingIntervalBlocks, maxMarketsPerRun, error)
func (e *EngineOperations) LoadLPRewardsConfig(ctx context.Context) (bool, int, int, error) {
	var (
		enabled    bool = true
		interval   int  = 10
		maxMarkets int  = 50
		found      bool
	)

	db, cleanup, err := e.getFreshReadTx(ctx)
	if err != nil {
		return true, 10, 50, fmt.Errorf("get fresh read tx: %w", err)
	}
	defer cleanup()

	err = e.engine.ExecuteWithoutEngineCtx(ctx, db,
		`SELECT enabled, sampling_interval_blocks, max_markets_per_run
		 FROM main.lp_rewards_config WHERE id = 1`, nil,
		func(row *common.Row) error {
			if len(row.Values) >= 3 {
				if v, ok := row.Values[0].(bool); ok {
					enabled = v
				}
				if v, ok := row.Values[1].(int); ok {
					interval = v
				} else if v64, ok := row.Values[1].(int64); ok {
					interval = int(v64)
				}
				if v, ok := row.Values[2].(int); ok {
					maxMarkets = v
				} else if v64, ok := row.Values[2].(int64); ok {
					maxMarkets = int(v64)
				}
				found = true
			}
			return nil
		})

	if err != nil {
		// Tolerate missing table
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "lp_rewards_config") &&
			(strings.Contains(msg, "does not exist") ||
				strings.Contains(msg, "no such table") ||
				strings.Contains(msg, "undefined table") ||
				strings.Contains(msg, "not found")) {
			e.logger.Info("lp_rewards_config table not found; using defaults")
			return true, 10, 50, nil
		}
		return true, 10, 50, err
	}
	if !found {
		return true, 10, 50, nil
	}
	return enabled, interval, maxMarkets, nil
}

// GetActiveMarkets returns active (unsettled) market IDs
func (e *EngineOperations) GetActiveMarkets(ctx context.Context, limit int) ([]int, error) {
	var markets []int

	db, cleanup, err := e.getFreshReadTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("get fresh read tx: %w", err)
	}
	defer cleanup()

	query := `
		SELECT id FROM ob_queries
		WHERE settled = false
		ORDER BY id ASC
		LIMIT $limit
	`

	err = e.engine.ExecuteWithoutEngineCtx(ctx, db, query,
		map[string]any{"limit": int64(limit)},
		func(row *common.Row) error {
			if len(row.Values) >= 1 {
				switch v := row.Values[0].(type) {
				case int:
					markets = append(markets, v)
				case int32:
					markets = append(markets, int(v))
				case int64:
					markets = append(markets, int(v))
				default:
					return fmt.Errorf("unexpected type for id: %T", v)
				}
			}
			return nil
		})

	if err != nil {
		// Tolerate missing table (migrations not run yet)
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "ob_queries") &&
			(strings.Contains(msg, "does not exist") ||
				strings.Contains(msg, "no such table")) {
			return nil, nil
		}
		return nil, fmt.Errorf("query active markets: %w", err)
	}

	return markets, nil
}

// BroadcastSampleLPRewards broadcasts a sample_lp_rewards transaction
func (e *EngineOperations) BroadcastSampleLPRewards(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	queryID int,
	blockHeight int64,
) error {
	// Get signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return fmt.Errorf("get signer account: %w", err)
	}

	// Fetch fresh nonce from database
	var nextNonce uint64
	if e.dbPool != nil {
		readTx := e.dbPool.BeginDelayedReadTx()
		defer readTx.Rollback(ctx)

		account, err := e.accounts.GetAccount(ctx, readTx, signerAccountID)
		if err != nil {
			if !isAccountNotFoundError(err) {
				return fmt.Errorf("get account: %w", err)
			}
			nextNonce = 1
		} else {
			nextNonce = uint64(account.Nonce + 1)
		}
	} else {
		account, err := e.accounts.GetAccount(ctx, e.db, signerAccountID)
		if err != nil {
			if !isAccountNotFoundError(err) {
				return fmt.Errorf("get account: %w", err)
			}
			nextNonce = 1
		} else {
			nextNonce = uint64(account.Nonce + 1)
		}
	}

	// Encode arguments for sample_lp_rewards action
	// Parameters: $query_id INT, $block INT8
	queryIDArg, err := ktypes.EncodeValue(int64(queryID))
	if err != nil {
		return fmt.Errorf("encode query_id: %w", err)
	}
	blockArg, err := ktypes.EncodeValue(blockHeight)
	if err != nil {
		return fmt.Errorf("encode block: %w", err)
	}

	// Build ActionExecution payload
	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "sample_lp_rewards",
		Arguments: [][]*ktypes.EncodedValue{{queryIDArg, blockArg}},
	}

	// Create transaction
	tx, err := ktypes.CreateNodeTransaction(payload, chainID, nextNonce)
	if err != nil {
		return fmt.Errorf("create tx: %w", err)
	}

	// Sign transaction
	if err := tx.Sign(signer); err != nil {
		return fmt.Errorf("sign tx: %w", err)
	}

	// Broadcast with sync mode = 1 (wait for commit) to ensure nonce increments properly
	// before broadcasting next transaction
	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		return fmt.Errorf("broadcast tx: %w", err)
	}

	// Check immediate result (may not have error yet in async mode)
	if txResult != nil && txResult.Code != uint32(ktypes.CodeOk) {
		return fmt.Errorf("transaction failed with code %d: %s",
			txResult.Code, txResult.Log)
	}

	e.logger.Debug("sample_lp_rewards transaction broadcast",
		"query_id", queryID,
		"block", blockHeight,
		"tx_hash", hash.String(),
		"nonce", nextNonce)

	return nil
}
