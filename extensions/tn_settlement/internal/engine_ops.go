package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	gethAbi "github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// QueryComponents holds decoded ABI-encoded query components from a market
type QueryComponents struct {
	DataProvider string
	StreamID     string
	ActionName   string
	ArgsBytes    []byte
}

// EngineOperations wraps engine calls needed by the settlement extension
type EngineOperations struct {
	engine   common.Engine
	logger   log.Logger
	db       sql.DB
	dbPool   sql.DelayedReadTxMaker // For fresh read transactions in background jobs
	accounts common.Accounts
}

// UnsettledMarket represents a market that is ready for settlement
type UnsettledMarket struct {
	ID         int    // query_id
	Hash       []byte // attestation hash
	SettleTime int64  // Unix timestamp
}

func NewEngineOperations(engine common.Engine, db sql.DB, dbPool sql.DelayedReadTxMaker, accounts common.Accounts, logger log.Logger) *EngineOperations {
	return &EngineOperations{
		engine:   engine,
		db:       db,
		dbPool:   dbPool,
		accounts: accounts,
		logger:   logger.New("settlement_ops"),
	}
}

// isAccountNotFoundError checks if the error indicates an account was not found.
// TODO: Replace with typed error from accounts package if available (e.g., accounts.ErrNotFound)
func isAccountNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no rows")
}

// LoadSettlementConfig reads the single-row settlement configuration
// Returns (enabled, schedule, maxMarketsPerRun, retryAttempts)
// If table/row missing, returns false, "", 10, 3 and no error
func (e *EngineOperations) LoadSettlementConfig(ctx context.Context) (bool, string, int, int, error) {
	var (
		enabled    bool
		schedule   string
		maxMarkets int
		retries    int
		found      bool
	)

	// Read using engine without engine ctx (owner-level read)
	err := e.engine.ExecuteWithoutEngineCtx(ctx, e.db,
		`SELECT enabled, settlement_schedule, max_markets_per_run, retry_attempts
		 FROM main.settlement_config WHERE id = 1`, nil,
		func(row *common.Row) error {
			if len(row.Values) >= 4 {
				if v, ok := row.Values[0].(bool); ok {
					enabled = v
				}
				if s, ok := row.Values[1].(string); ok {
					schedule = s
				}
				if m, ok := row.Values[2].(int); ok {
					maxMarkets = m
				} else if m64, ok := row.Values[2].(int64); ok {
					maxMarkets = int(m64)
				}
				if r, ok := row.Values[3].(int); ok {
					retries = r
				} else if r64, ok := row.Values[3].(int64); ok {
					retries = int(r64)
				}
				found = true
			}
			return nil
		})

	if err != nil {
		// tolerate missing table; everything else should surface to caller
		// TODO: Use typed error from engine API if available
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "settlement_config") &&
			(strings.Contains(msg, "does not exist") ||
				strings.Contains(msg, "no such table") ||
				strings.Contains(msg, "undefined table") ||
				strings.Contains(msg, "not found")) {
			e.logger.Info("settlement_config table not found; using defaults")
			return false, "", 10, 3, nil
		}
		return false, "", 10, 3, err
	}
	if !found {
		return false, "", 10, 3, nil
	}
	return enabled, schedule, maxMarkets, retries, nil
}

// FindUnsettledMarkets queries for markets past settle_time that haven't been settled yet
// Uses the current Unix timestamp to determine which markets are ready
func (e *EngineOperations) FindUnsettledMarkets(ctx context.Context, limit int) ([]*UnsettledMarket, error) {
	var markets []*UnsettledMarket

	// Get current Unix timestamp for comparison
	currentTime := time.Now().Unix()

	// Query: SELECT id, hash, settle_time FROM ob_queries
	//        WHERE settled = FALSE AND settle_time <= $current_time
	//        ORDER BY settle_time ASC LIMIT $limit
	query := `
		SELECT id, hash, settle_time
		FROM ob_queries
		WHERE settled = false AND settle_time <= $current_time
		ORDER BY settle_time ASC
		LIMIT $limit
	`

	err := e.engine.ExecuteWithoutEngineCtx(ctx, e.db, query,
		map[string]any{
			"current_time": currentTime,
			"limit":        int64(limit),
		},
		func(row *common.Row) error {
			if len(row.Values) >= 3 {
				// Extract values with type assertions
				var id int
				var hash []byte
				var settleTime int64

				// Handle id (can be int32 or int64)
				switch v := row.Values[0].(type) {
				case int:
					id = v
				case int32:
					id = int(v)
				case int64:
					id = int(v)
				default:
					return fmt.Errorf("unexpected type for id: %T", v)
				}

				// Handle hash
				var ok bool
				hash, ok = row.Values[1].([]byte)
				if !ok {
					return fmt.Errorf("unexpected type for hash: %T", row.Values[1])
				}

				// Handle settle_time
				switch v := row.Values[2].(type) {
				case int64:
					settleTime = v
				case int:
					settleTime = int64(v)
				default:
					return fmt.Errorf("unexpected type for settle_time: %T", v)
				}

				market := &UnsettledMarket{
					ID:         id,
					Hash:       hash,
					SettleTime: settleTime,
				}
				markets = append(markets, market)
			}
			return nil
		})

	if err != nil {
		return nil, fmt.Errorf("query unsettled markets: %w", err)
	}

	return markets, nil
}

// AttestationExists checks if a signed attestation exists for the given hash
func (e *EngineOperations) AttestationExists(ctx context.Context, marketHash []byte) (bool, error) {
	var exists bool

	query := `
		SELECT 1 FROM attestations
		WHERE attestation_hash = $hash AND signature IS NOT NULL
		LIMIT 1
	`

	err := e.engine.ExecuteWithoutEngineCtx(ctx, e.db, query,
		map[string]any{"hash": marketHash},
		func(row *common.Row) error {
			exists = true
			return nil
		})

	if err != nil {
		return false, fmt.Errorf("check attestation: %w", err)
	}

	return exists, nil
}

// BroadcastSettleMarketWithRetry broadcasts settle_market transaction with retry logic
func (e *EngineOperations) BroadcastSettleMarketWithRetry(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	queryID int,
	maxRetries int,
) error {
	var lastErr error
	backoff := 2 * time.Second
	maxBackoff := 30 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			e.logger.Warn("retrying settle_market broadcast",
				"attempt", attempt,
				"query_id", queryID,
				"backoff", backoff,
				"last_error", lastErr)

			// Wait before retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			// Exponential backoff
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		// Broadcast with fresh nonce
		hash, err := e.broadcastSettleMarketWithFreshNonce(
			ctx, chainID, signer, broadcaster, queryID,
		)

		if err == nil {
			e.logger.Info("settle_market broadcast succeeded",
				"query_id", queryID,
				"tx_hash", hash.String())
			return nil
		}

		lastErr = err
		e.logger.Warn("settle_market broadcast failed",
			"attempt", attempt,
			"query_id", queryID,
			"tx_hash", hash.String(),
			"error", err)
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", maxRetries, lastErr)
}

// broadcastSettleMarketWithFreshNonce builds and broadcasts settle_market with fresh nonce
func (e *EngineOperations) broadcastSettleMarketWithFreshNonce(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	queryID int,
) (ktypes.Hash, error) {
	// Get signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return ktypes.Hash{}, fmt.Errorf("get signer account: %w", err)
	}

	// Fetch fresh nonce from database using a fresh read transaction
	var nextNonce uint64
	if e.dbPool != nil {
		readTx := e.dbPool.BeginDelayedReadTx()
		defer readTx.Rollback(ctx)

		account, err := e.accounts.GetAccount(ctx, readTx, signerAccountID)
		if err != nil {
			if !isAccountNotFoundError(err) {
				return ktypes.Hash{}, fmt.Errorf("get account: %w", err)
			}
			nextNonce = 1
			e.logger.Info("account not found, using nonce 1",
				"account", fmt.Sprintf("%x", signerAccountID.Identifier))
		} else {
			nextNonce = uint64(account.Nonce + 1)
			e.logger.Info("fresh nonce from database",
				"account", fmt.Sprintf("%x", signerAccountID.Identifier),
				"db_nonce", account.Nonce,
				"next_nonce", nextNonce)
		}
	} else {
		// Fallback to stored db (may fail if tx is closed)
		account, err := e.accounts.GetAccount(ctx, e.db, signerAccountID)
		if err != nil {
			if !isAccountNotFoundError(err) {
				return ktypes.Hash{}, fmt.Errorf("get account: %w", err)
			}
			nextNonce = 1
			e.logger.Info("account not found, using nonce 1",
				"account", fmt.Sprintf("%x", signerAccountID.Identifier))
		} else {
			nextNonce = uint64(account.Nonce + 1)
			e.logger.Info("fresh nonce from database",
				"account", fmt.Sprintf("%x", signerAccountID.Identifier),
				"db_nonce", account.Nonce,
				"next_nonce", nextNonce)
		}
	}

	// Encode query_id argument
	queryIDArg, err := ktypes.EncodeValue(int64(queryID))
	if err != nil {
		return ktypes.Hash{}, fmt.Errorf("encode query_id: %w", err)
	}

	// Build ActionExecution payload
	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "settle_market",
		Arguments: [][]*ktypes.EncodedValue{{queryIDArg}},
	}

	// Create transaction
	tx, err := ktypes.CreateNodeTransaction(payload, chainID, nextNonce)
	if err != nil {
		return ktypes.Hash{}, fmt.Errorf("create tx: %w", err)
	}

	// Sign transaction
	if err := tx.Sign(signer); err != nil {
		return ktypes.Hash{}, fmt.Errorf("sign tx: %w", err)
	}

	// Broadcast (sync mode = WaitCommit)
	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		return hash, fmt.Errorf("broadcast tx: %w", err)
	}

	// Check transaction result
	if txResult.Code != uint32(ktypes.CodeOk) {
		return hash, fmt.Errorf("transaction failed with code %d: %s",
			txResult.Code, txResult.Log)
	}

	e.logger.Info("settle_market transaction succeeded",
		"query_id", queryID,
		"tx_hash", hash.String(),
		"nonce", nextNonce)

	return hash, nil
}

// GetMarketQueryComponents fetches and decodes query_components for a market
func (e *EngineOperations) GetMarketQueryComponents(ctx context.Context, queryID int) (*QueryComponents, error) {
	var queryComponentsBytes []byte
	var foundRow bool
	var foundData bool

	err := e.engine.ExecuteWithoutEngineCtx(ctx, e.db,
		`SELECT query_components FROM ob_queries WHERE id = $query_id`,
		map[string]any{"query_id": int64(queryID)},
		func(row *common.Row) error {
			if len(row.Values) >= 1 {
				foundRow = true
				if row.Values[0] == nil {
					return fmt.Errorf("query_components is NULL for query_id=%d", queryID)
				}
				bytes, ok := row.Values[0].([]byte)
				if !ok {
					return fmt.Errorf("unexpected query_components type: %T", row.Values[0])
				}
				queryComponentsBytes = bytes
				foundData = true
			}
			return nil
		})

	if err != nil {
		return nil, fmt.Errorf("fetch query_components: %w", err)
	}
	if !foundRow {
		return nil, fmt.Errorf("market not found: query_id=%d", queryID)
	}
	if !foundData {
		return nil, fmt.Errorf("query_components missing or invalid for query_id=%d", queryID)
	}

	return decodeQueryComponents(queryComponentsBytes)
}

// decodeQueryComponents decodes ABI-encoded query components (address, bytes32, string, bytes)
func decodeQueryComponents(data []byte) (*QueryComponents, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("query_components is empty")
	}

	addressType, err := gethAbi.NewType("address", "", nil)
	if err != nil {
		return nil, fmt.Errorf("create address type: %w", err)
	}
	bytes32Type, err := gethAbi.NewType("bytes32", "", nil)
	if err != nil {
		return nil, fmt.Errorf("create bytes32 type: %w", err)
	}
	stringType, err := gethAbi.NewType("string", "", nil)
	if err != nil {
		return nil, fmt.Errorf("create string type: %w", err)
	}
	bytesType, err := gethAbi.NewType("bytes", "", nil)
	if err != nil {
		return nil, fmt.Errorf("create bytes type: %w", err)
	}

	args := gethAbi.Arguments{
		{Type: addressType},
		{Type: bytes32Type},
		{Type: stringType},
		{Type: bytesType},
	}

	decoded, err := args.Unpack(data)
	if err != nil {
		return nil, fmt.Errorf("unpack query_components: %w", err)
	}

	if len(decoded) != 4 {
		return nil, fmt.Errorf("expected 4 components, got %d", len(decoded))
	}

	// Extract data provider address
	dataProvider, ok := decoded[0].(gethCommon.Address)
	if !ok {
		return nil, fmt.Errorf("invalid data_provider type: %T", decoded[0])
	}

	// Extract stream ID (bytes32 -> string, trim null padding)
	streamIDBytes, ok := decoded[1].([32]byte)
	if !ok {
		return nil, fmt.Errorf("invalid stream_id type: %T", decoded[1])
	}
	streamID := strings.TrimRight(string(streamIDBytes[:]), "\x00")

	// Extract action name
	actionName, ok := decoded[2].(string)
	if !ok {
		return nil, fmt.Errorf("invalid action_name type: %T", decoded[2])
	}

	// Extract args bytes
	argsBytes, ok := decoded[3].([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid args_bytes type: %T", decoded[3])
	}

	return &QueryComponents{
		DataProvider: strings.ToLower(dataProvider.Hex()),
		StreamID:     streamID,
		ActionName:   actionName,
		ArgsBytes:    argsBytes,
	}, nil
}

// RequestAttestationForMarket broadcasts a request_attestation transaction for a market
func (e *EngineOperations) RequestAttestationForMarket(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	market *UnsettledMarket,
) error {
	// Get query components from market
	components, err := e.GetMarketQueryComponents(ctx, market.ID)
	if err != nil {
		return fmt.Errorf("get query components: %w", err)
	}

	// Get signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return fmt.Errorf("get signer account: %w", err)
	}

	// Fetch fresh nonce from database using a fresh read transaction
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
		// Fallback to stored db (may fail if tx is closed)
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

	// Encode arguments for request_attestation action
	// Parameters: data_provider TEXT, stream_id TEXT, action_name TEXT, args_bytes BYTEA, encrypt_sig BOOL, max_fee NUMERIC
	dataProviderArg, err := ktypes.EncodeValue(components.DataProvider)
	if err != nil {
		return fmt.Errorf("encode data_provider: %w", err)
	}
	streamIDArg, err := ktypes.EncodeValue(components.StreamID)
	if err != nil {
		return fmt.Errorf("encode stream_id: %w", err)
	}
	actionNameArg, err := ktypes.EncodeValue(components.ActionName)
	if err != nil {
		return fmt.Errorf("encode action_name: %w", err)
	}
	argsBytesArg, err := ktypes.EncodeValue(components.ArgsBytes)
	if err != nil {
		return fmt.Errorf("encode args_bytes: %w", err)
	}
	encryptSigArg, err := ktypes.EncodeValue(false)
	if err != nil {
		return fmt.Errorf("encode encrypt_sig: %w", err)
	}
	// max_fee is NULL for network_writer role (exempt from fees)
	maxFeeArg, err := ktypes.EncodeValue(nil)
	if err != nil {
		return fmt.Errorf("encode max_fee: %w", err)
	}

	// Build ActionExecution payload
	payload := &ktypes.ActionExecution{
		Namespace: "main",
		Action:    "request_attestation",
		Arguments: [][]*ktypes.EncodedValue{{
			dataProviderArg,
			streamIDArg,
			actionNameArg,
			argsBytesArg,
			encryptSigArg,
			maxFeeArg,
		}},
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

	// Broadcast (sync mode = WaitCommit)
	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		return fmt.Errorf("broadcast tx: %w", err)
	}

	// Check transaction result
	if txResult.Code != uint32(ktypes.CodeOk) {
		return fmt.Errorf("transaction failed with code %d: %s", txResult.Code, txResult.Log)
	}

	e.logger.Info("request_attestation broadcast succeeded",
		"query_id", market.ID,
		"tx_hash", hash.String(),
		"data_provider", components.DataProvider,
		"stream_id", components.StreamID,
		"action_name", components.ActionName)

	return nil
}
