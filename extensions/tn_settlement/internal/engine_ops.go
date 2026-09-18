package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	readDB   sql.DB                 // Independent read handle for poll reads; bypasses the engine interpreter lock
	accounts common.Accounts

	// nonceMu guards the settlement cycle's nonce counter below.
	nonceMu sync.Mutex
	// cycleNonce is the nonce for this cycle's next transaction, and cycleNonceSet
	// says whether it has been seeded yet. A cycle's request_attestation
	// transactions are broadcast accept-only, so the account's committed nonce
	// stays behind until they are in a block and cannot be read again for the
	// next one. Counting locally is what lets a whole ladder reach the mempool
	// together. See BeginCycle for the rules that keep the counter honest.
	cycleNonce    uint64
	cycleNonceSet bool
}

// BeginCycle discards the nonce left over from the previous settlement cycle so
// the next transaction re-seeds from committed account state.
//
// The counter is deliberately short-lived. Three subsystems sign with the node
// key — tn_settlement, tn_digest and tn_attestation — so any counter that
// outlives the work it was seeded for goes stale as soon as one of the others
// lands a transaction, and every later broadcast is then rejected for an invalid
// nonce. Seeding once per cycle bounds that to the cycle that was running.
func (e *EngineOperations) BeginCycle() {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()
	e.cycleNonce = 0
	e.cycleNonceSet = false
}

// nextCycleNonce returns the nonce to use for the next transaction of this
// cycle, seeding from committed account state on first use.
func (e *EngineOperations) nextCycleNonce(ctx context.Context, accountID *ktypes.AccountID) (uint64, error) {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()

	if e.cycleNonceSet {
		return e.cycleNonce, nil
	}

	nonce, err := e.committedNonce(ctx, accountID)
	if err != nil {
		return 0, err
	}
	e.cycleNonce, e.cycleNonceSet = nonce, true
	return nonce, nil
}

// advanceCycleNonce consumes the current nonce. Call it only after a broadcast
// the mempool accepted: a rejected transaction never consumed its nonce, and
// advancing past it would leave a gap that rejects everything after it.
func (e *EngineOperations) advanceCycleNonce() {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()
	if e.cycleNonceSet {
		e.cycleNonce++
	}
}

// resetCycleNonce drops the counter so the next transaction re-seeds from
// committed state. Call it when a broadcast failed and the counter can no longer
// be trusted — most of all on an invalid-nonce rejection, which means another
// subsystem signing with the node key got there first.
func (e *EngineOperations) resetCycleNonce() {
	e.nonceMu.Lock()
	defer e.nonceMu.Unlock()
	e.cycleNonce = 0
	e.cycleNonceSet = false
}

// committedNonce reads the account's committed nonce and returns the next one to
// use. Callers hold nonceMu.
func (e *EngineOperations) committedNonce(ctx context.Context, accountID *ktypes.AccountID) (uint64, error) {
	db := e.db
	if e.dbPool != nil {
		// A fresh read transaction, so a background job never reads through a
		// handle another caller may have closed.
		readTx := e.dbPool.BeginDelayedReadTx()
		defer readTx.Rollback(ctx)
		db = readTx
	}

	account, err := e.accounts.GetAccount(ctx, db, accountID)
	if err != nil {
		if !isAccountNotFoundError(err) {
			return 0, fmt.Errorf("get account: %w", err)
		}
		// Never transacted, so the first nonce is 1.
		e.logger.Info("account not found, using nonce 1",
			"account", fmt.Sprintf("%x", accountID.Identifier))
		return 1, nil
	}

	nonce := uint64(account.Nonce + 1)
	e.logger.Info("seeded settlement cycle nonce from committed state",
		"account", fmt.Sprintf("%x", accountID.Identifier),
		"db_nonce", account.Nonce,
		"next_nonce", nonce)
	return nonce, nil
}

// UnsettledMarket represents a market that is ready for settlement
type UnsettledMarket struct {
	ID         int    // query_id
	Hash       []byte // attestation hash
	SettleTime int64  // Unix timestamp
}

// NewEngineOperations builds the settlement engine-ops wrapper.
//
// readDB is an independent read handle (a *ReadPool in production) used for all
// poll-time SELECTs. It runs plain SQL and does NOT go through the engine
// interpreter, so a settlement read can never hold the interpreter lock while
// waiting on a Postgres table lock — the deadlock this fixes. In tests readDB
// may be the platform tx so reads observe the test's uncommitted writes.
func NewEngineOperations(engine common.Engine, db sql.DB, dbPool sql.DelayedReadTxMaker, readDB sql.DB, accounts common.Accounts, logger log.Logger) *EngineOperations {
	return &EngineOperations{
		engine:   engine,
		db:       db,
		dbPool:   dbPool,
		readDB:   readDB,
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

// toInt64 coerces a decoded SQL integer value (INT/INT8/etc. decode to int64)
// to an int64. It returns false for NULLs or unexpected types.
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	default:
		return 0, false
	}
}

// LoadSettlementConfig reads the single-row settlement configuration
// Returns (enabled, schedule, maxMarketsPerRun, retryAttempts)
// If table/row missing, returns false, "", 10, 3 and no error.
//
// The read runs on the independent readDB handle (plain SQL, no engine
// interpreter) so it cannot deadlock against in-block DDL. This method is
// invoked both from the background scheduler and, periodically, from within
// EndBlock; the readDB's bounded lock_timeout ensures the EndBlock call fails
// fast (surfacing an error to the caller's retry path) rather than hanging.
func (e *EngineOperations) LoadSettlementConfig(ctx context.Context) (bool, string, int, int, error) {
	if e.readDB == nil {
		return false, "", 10, 3, fmt.Errorf("settlement read handle not initialized")
	}

	rs, err := e.readDB.Execute(ctx,
		`SELECT enabled, settlement_schedule, max_markets_per_run, retry_attempts
		 FROM main.settlement_config WHERE id = 1`)
	if err != nil {
		// tolerate missing table; everything else should surface to caller
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

	if len(rs.Rows) == 0 || len(rs.Rows[0]) < 4 {
		return false, "", 10, 3, nil
	}

	row := rs.Rows[0]
	enabled, _ := row[0].(bool)
	schedule, _ := row[1].(string)
	maxMarkets := 10
	if n, ok := toInt64(row[2]); ok {
		maxMarkets = int(n)
	}
	retries := 3
	if n, ok := toInt64(row[3]); ok {
		retries = int(n)
	}
	return enabled, schedule, maxMarkets, retries, nil
}

// FindUnsettledMarkets queries for markets past settle_time that haven't been settled yet
// Uses the current Unix timestamp to determine which markets are ready.
//
// Reads run on the independent readDB handle (plain SQL, positional params,
// main-schema-qualified) rather than through the engine interpreter.
func (e *EngineOperations) FindUnsettledMarkets(ctx context.Context, limit int) ([]*UnsettledMarket, error) {
	if e.readDB == nil {
		return nil, fmt.Errorf("settlement read handle not initialized")
	}

	// Get current Unix timestamp for comparison
	currentTime := time.Now().Unix()

	rs, err := e.readDB.Execute(ctx,
		`SELECT id, hash, settle_time
		 FROM main.ob_queries
		 WHERE settled = false AND settle_time <= $1
		 ORDER BY settle_time ASC
		 LIMIT $2`,
		currentTime, int64(limit))
	if err != nil {
		return nil, fmt.Errorf("query unsettled markets: %w", err)
	}

	markets := make([]*UnsettledMarket, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if len(row) < 3 {
			continue
		}

		idVal, ok := toInt64(row[0])
		if !ok {
			return nil, fmt.Errorf("unexpected type for id: %T", row[0])
		}
		hash, ok := row[1].([]byte)
		if !ok {
			return nil, fmt.Errorf("unexpected type for hash: %T", row[1])
		}
		settleTime, ok := toInt64(row[2])
		if !ok {
			return nil, fmt.Errorf("unexpected type for settle_time: %T", row[2])
		}

		markets = append(markets, &UnsettledMarket{
			ID:         int(idVal),
			Hash:       hash,
			SettleTime: settleTime,
		})
	}

	return markets, nil
}

// CaptureStatus is what the settlement path knows about a market's captured query
// result: whether one has been taken at all, and whether a validator has signed it.
//
// The two are separate questions and answering only the second is what produces a
// second capture. A request_attestation transaction writes its row immediately and
// the signature arrives in a later block — usually the next one, but the tail
// reaches past two hundred. A market whose capture is signed but not yet visible
// as signed has still been captured, and asking for another one gives it a second
// result taken at a different height against different chain state.
type CaptureStatus struct {
	// Captured is true once a request_attestation row exists, signed or not.
	Captured bool
	// Signed is true once one of those rows carries a validator signature, which
	// is the point settle_market can use it.
	Signed bool
	// CapturedAt is the height of the earliest capture, or 0 when there is none.
	// It is the only handle an operator has on how long a capture has been
	// waiting, so it is logged when a market is held back.
	CapturedAt int64
}

// CaptureStatusFor reports what has been captured for a market.
//
// Reads run on the independent readDB handle rather than the engine interpreter.
func (e *EngineOperations) CaptureStatusFor(ctx context.Context, marketHash []byte) (CaptureStatus, error) {
	if e.readDB == nil {
		return CaptureStatus{}, fmt.Errorf("settlement read handle not initialized")
	}

	// One row always comes back: with no captures the aggregates are NULL, which
	// decodes to the zero CaptureStatus.
	rs, err := e.readDB.Execute(ctx,
		`SELECT bool_or(signature IS NOT NULL) AS signed,
		        min(created_height)           AS captured_at
		   FROM main.attestations
		  WHERE attestation_hash = $1`,
		marketHash)
	if err != nil {
		return CaptureStatus{}, fmt.Errorf("check attestation: %w", err)
	}
	if len(rs.Rows) == 0 || len(rs.Rows[0]) < 2 {
		return CaptureStatus{}, nil
	}

	row := rs.Rows[0]
	capturedAt, captured := toInt64(row[1])
	signed, _ := row[0].(bool)

	return CaptureStatus{
		Captured:   captured,
		Signed:     signed,
		CapturedAt: capturedAt,
	}, nil
}

// AttestationExists checks if a signed attestation exists for the given hash.
//
// Reads run on the independent readDB handle rather than the engine interpreter.
func (e *EngineOperations) AttestationExists(ctx context.Context, marketHash []byte) (bool, error) {
	status, err := e.CaptureStatusFor(ctx, marketHash)
	if err != nil {
		return false, err
	}
	return status.Signed, nil
}

// BroadcastSettleMarketWithRetry broadcasts settle_market transaction with retry logic.
// Uses exponential backoff since external systems may use the same wallet (nonce conflicts).
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
				"is_nonce_error", isNonceError(lastErr),
				"last_error", lastErr)

			// Wait before retry with context cancellation support
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
			"is_nonce_error", isNonceError(err),
			"error", err)

		// A permanent (deterministic) failure recurs identically on every retry,
		// so stop now instead of burning more nonces and committing more failed
		// txs to blocks. Returning the wrapped sentinel lets the scheduler
		// quarantine the market and flag it for manual intervention.
		if isPermanentSettleError(err) {
			e.logger.Error("settle_market permanently failed; not retrying (needs manual intervention)",
				"query_id", queryID,
				"error", err)
			return fmt.Errorf("%w (query_id=%d): %w", ErrPermanentSettleFailure, queryID, err)
		}
	}

	return fmt.Errorf("settle_market failed after %d retries: %w", maxRetries, lastErr)
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

	// Take the cycle's nonce. This cycle's request_attestation transactions may
	// still be in the mempool, in which case the account's committed nonce is
	// behind by however many of them there are and reading it here would build a
	// transaction the mempool rejects.
	nextNonce, err := e.nextCycleNonce(ctx, signerAccountID)
	if err != nil {
		return ktypes.Hash{}, err
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

	// Broadcast (sync mode = WaitCommit). settle_market keeps waiting for the
	// commit because isPermanentSettleError classifies a quarantine decision from
	// the execution result, which only a committed transaction has.
	hash, txResult, err := broadcaster(ctx, tx, 1)
	if err != nil {
		// The counter can no longer be trusted: the transaction may or may not
		// have consumed its nonce. Re-seed from committed state next time, which
		// is accurate because this path waits for the commit.
		e.resetCycleNonce()
		return hash, fmt.Errorf("broadcast tx: %w", err)
	}

	// Check transaction result. A reverting transaction is still in a block and
	// still consumed its nonce, so re-seed rather than reuse the counter.
	if txResult.Code != uint32(ktypes.CodeOk) {
		e.resetCycleNonce()
		return hash, fmt.Errorf("transaction failed with code %d: %s",
			txResult.Code, txResult.Log)
	}

	e.advanceCycleNonce()

	e.logger.Info("settle_market transaction succeeded",
		"query_id", queryID,
		"tx_hash", hash.String(),
		"nonce", nextNonce)

	return hash, nil
}

// GetMarketQueryComponents fetches and decodes query_components for a market.
//
// Reads run on the independent readDB handle rather than the engine interpreter.
func (e *EngineOperations) GetMarketQueryComponents(ctx context.Context, queryID int) (*QueryComponents, error) {
	if e.readDB == nil {
		return nil, fmt.Errorf("settlement read handle not initialized")
	}

	rs, err := e.readDB.Execute(ctx,
		`SELECT query_components FROM main.ob_queries WHERE id = $1`,
		int64(queryID))
	if err != nil {
		return nil, fmt.Errorf("fetch query_components: %w", err)
	}

	if len(rs.Rows) == 0 || len(rs.Rows[0]) < 1 {
		return nil, fmt.Errorf("market not found: query_id=%d", queryID)
	}

	raw := rs.Rows[0][0]
	if raw == nil {
		return nil, fmt.Errorf("query_components is NULL for query_id=%d", queryID)
	}
	queryComponentsBytes, ok := raw.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected query_components type: %T", raw)
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

// isNonceError checks if the error is related to nonce conflicts
func isNonceError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "nonce") || strings.Contains(msg, "invalid nonce")
}

// ErrPermanentSettleFailure marks a settle_market failure that will recur
// identically on every retry. A market's attestation is signed and immutable, so
// a deterministic parse/decode failure of it can never succeed by re-broadcasting
// — the retry only burns nonces and commits another failed tx to a block. The
// scheduler detects this with errors.Is and quarantines the market for manual
// intervention (re-attestation or admin_force_settle_market) instead of retrying.
var ErrPermanentSettleFailure = errors.New("permanent settle_market failure")

// permanentSettleErrorSignatures are lowercased substrings that identify a
// settle_market revert caused by an unparseable or malformed attestation. These
// come from tn_utils.parse_attestation_boolean / parseBinaryActionResult
// (extensions/tn_utils/precompiles.go) and are deterministic on-chain action
// errors: the signed attestation cannot change, so the same parse fails forever.
// The list is deliberately narrow — only errors that are provably permanent — so
// a transient or unfamiliar failure keeps the existing retry behavior rather than
// being mistakenly quarantined.
var permanentSettleErrorSignatures = []string{
	// Binary-action payload wrong width, e.g. an empty 128-byte result on a binary
	// market whose data landed after the attestation was captured (the market-368
	// case): "binary action result must be 32 bytes (abi-encoded bool), got 128".
	"binary action result must be",
	"abi-encoded bool",
	"failed to decode boolean abi result",
	"expected 1 value from boolean decode",
	"decoded value is not boolean",
	// Numeric-action payload (action_id 1-5): parse_attestation_boolean routes a
	// numeric-settled market to parseNumericActionResult, which fails
	// deterministically on an empty or malformed immutable payload — the same
	// late-arriving-data failure mode as market 368, for numeric markets.
	"result payload contains no values",
	"failed to decode abi result payload",
	"expected 2 arrays (timestamps, values)",
	"values must be []*big.int",
	// Malformed / empty canonical result — an immutable attestation that can never
	// be parsed.
	"invalid result_canonical",
	"result_canonical cannot be empty",
	"unsupported action_id",
}

// isPermanentSettleError reports whether a settle_market broadcast error is a
// deterministic, non-recoverable failure that must not be retried. Nonce and
// network/broadcast errors mean the tx never executed deterministically, so they
// are explicitly excluded and left to the retry path.
func isPermanentSettleError(err error) bool {
	if err == nil {
		return false
	}
	if isNonceError(err) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, sig := range permanentSettleErrorSignatures {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// RequestAttestationForMarket broadcasts a request_attestation transaction for a
// market, capturing the query result at whatever block the transaction lands in.
//
// It does not retry. A market whose books settle together must have every one of
// its captures in the same block, and there is no way to wait between two of them
// without splitting the ladder across blocks — which is the whole defect. A
// failure here ends the cycle's capture pass; the next poll five minutes later
// re-requests whatever is still missing, together.
func (e *EngineOperations) RequestAttestationForMarket(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	market *UnsettledMarket,
) error {
	components, err := e.GetMarketQueryComponents(ctx, market.ID)
	if err != nil {
		return fmt.Errorf("get query components: %w", err)
	}

	if err := e.broadcastRequestAttestation(ctx, chainID, signer, broadcaster, market, components); err != nil {
		e.logger.Warn("request_attestation broadcast failed",
			"query_id", market.ID,
			"is_nonce_error", isNonceError(err),
			"error", err)
		return err
	}
	return nil
}

// broadcastRequestAttestation builds and broadcasts one request_attestation
// against the settlement cycle's nonce counter.
func (e *EngineOperations) broadcastRequestAttestation(
	ctx context.Context,
	chainID string,
	signer auth.Signer,
	broadcaster func(context.Context, *ktypes.Transaction, uint8) (ktypes.Hash, *ktypes.TxResult, error),
	market *UnsettledMarket,
	components *QueryComponents,
) error {
	// Get signer account ID
	signerAccountID, err := ktypes.GetSignerAccount(signer)
	if err != nil {
		return fmt.Errorf("get signer account: %w", err)
	}

	// Take the cycle's nonce rather than reading committed state. The previous
	// capture of this cycle is still in the mempool, so the committed nonce is
	// behind it and a transaction built from it would be rejected.
	nextNonce, err := e.nextCycleNonce(ctx, signerAccountID)
	if err != nil {
		return err
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

	// Broadcast accept-only (sync mode = WaitAccept). Waiting for the commit is
	// what limited a cycle to one capture per block: the next transaction could
	// not be built until this one was in a block, so every book of a ladder
	// landed in a different block and captured a different value.
	//
	// The returned result is nil by definition — the transaction is in the
	// mempool, not in a block — so there is nothing to check here. Whether the
	// action executed is answered by AttestationExists on the next poll: a
	// request that reverted leaves no attestation row and is simply requested
	// again, so no market can settle on a transaction that did not execute.
	hash, _, err := broadcaster(ctx, tx, 0)
	if err != nil {
		// Rejected, so the nonce was not consumed. Drop the counter rather than
		// advance it: the caller abandons the rest of this cycle's captures and
		// the next cycle re-seeds from committed state.
		e.resetCycleNonce()
		return fmt.Errorf("broadcast tx: %w", err)
	}

	e.advanceCycleNonce()

	e.logger.Info("request_attestation accepted into the mempool",
		"query_id", market.ID,
		"tx_hash", hash.String(),
		"data_provider", components.DataProvider,
		"stream_id", components.StreamID,
		"action_name", components.ActionName,
		"nonce", nextNonce)

	return nil
}
