package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
)

// An action's return value is not visible to a transaction's broadcaster, so the
// sweep's counters reach the scheduler only through the NOTICE the action emits.
// Parsing it is the whole interface between the two.

const pruneNotice = `auto_prune_duplicates:{"swept_streams":100,"deleted_event_times":42,"deleted_rows":57,"has_more_to_delete":true}`

func TestParsePruneResultFromTxLog_ReadsTheSweepCounters(t *testing.T) {
	res, err := parsePruneResultFromTxLog("INFO something\nNOTICE: " + pruneNotice + "\nother")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.SweptStreams != 100 {
		t.Fatalf("swept_streams: want 100, got %d", res.SweptStreams)
	}
	if res.DeletedEventTimes != 42 {
		t.Fatalf("deleted_event_times: want 42, got %d", res.DeletedEventTimes)
	}
	if res.DeletedRows != 57 {
		t.Fatalf("deleted_rows: want 57, got %d", res.DeletedRows)
	}
	if !res.HasMoreToDelete {
		t.Fatalf("has_more_to_delete: want true, got false")
	}
}

// has_more_to_delete false is what ends a drain, so reading it wrong would either
// spin the loop to its run budget every firing or stop a sweep on its first batch.
func TestParsePruneResultFromTxLog_ReadsTheEndOfAPass(t *testing.T) {
	res, err := parsePruneResultFromTxLog(`auto_prune_duplicates:{"swept_streams":7,"deleted_event_times":0,"deleted_rows":0,"has_more_to_delete":false}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.HasMoreToDelete {
		t.Fatalf("has_more_to_delete: want false, got true")
	}
	if res.SweptStreams != 7 {
		t.Fatalf("swept_streams: want 7, got %d", res.SweptStreams)
	}
}

// A transaction that committed without the notice means the action did not run the
// way this code assumes. Treating that as a zero-valued success would report a
// finished pass and silently stop the drain.
func TestParsePruneResultFromTxLog_NoEntry(t *testing.T) {
	if _, err := parsePruneResultFromTxLog("INFO: nothing relevant here\nNOTICE: auto_digest:{}"); err == nil {
		t.Fatal("expected an error for a log with no auto_prune_duplicates entry")
	}
}

// The digest marker is not a prefix of this one, but both parsers scan the same
// log, so it is worth pinning that neither reads the other's line.
func TestParsePruneResultFromTxLog_IgnoresTheDigestNotice(t *testing.T) {
	log := "NOTICE: auto_digest:{\"processed_days\":2,\"total_deleted_rows\":500,\"has_more_to_delete\":false}\nNOTICE: " + pruneNotice
	res, err := parsePruneResultFromTxLog(log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.DeletedRows != 57 {
		t.Fatalf("read the digest line: deleted_rows want 57, got %d", res.DeletedRows)
	}

	digestRes, err := parseDigestResultFromTxLog(log)
	if err != nil {
		t.Fatalf("unexpected error reading the digest notice: %v", err)
	}
	if digestRes.TotalDeletedRows != 500 {
		t.Fatalf("read the prune line: total_deleted_rows want 500, got %d", digestRes.TotalDeletedRows)
	}
}

type prunePathBroadcaster struct {
	attempts  int
	failUntil int
	// action and argCount record what the last transaction actually asked for.
	action   string
	argCount int
}

func (m *prunePathBroadcaster) broadcast(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	m.attempts++

	if payload := new(ktypes.ActionExecution); payload.UnmarshalBinary(tx.Body.Payload) == nil {
		m.action = payload.Action
		if len(payload.Arguments) == 1 {
			m.argCount = len(payload.Arguments[0])
		}
	}

	result := &ktypes.TxResult{Code: uint32(ktypes.CodeOk), Log: pruneNotice}
	if m.attempts <= m.failUntil {
		return ktypes.Hash{}, result, errors.New("network error")
	}
	return ktypes.Hash{1, 2, 3}, result, nil
}

func newPruneSigner(t *testing.T) auth.Signer {
	t.Helper()
	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return auth.GetNodeSigner(priv)
}

// retention_days is deliberately not passed. It is the third parameter and it
// defaults to NULL, which makes the action read retention from
// duplicate_prune_config -- so an operator can change it with a signed exec-sql
// instead of a binary release. Sending two arguments is what keeps that true.
func TestBroadcastAutoPruneDuplicates_LeavesRetentionToTheConfig(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &prunePathBroadcaster{}
	ops := &EngineOperations{logger: log.New(), accounts: accounts}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := ops.BroadcastAutoPruneDuplicatesWithRetry(
		ctx, "test-chain", newPruneSigner(t), broadcaster.broadcast, 100000, 100, 3,
	)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if result.DeletedRows != 57 {
		t.Fatalf("deleted_rows: want 57, got %d", result.DeletedRows)
	}
	if broadcaster.action != "auto_prune_duplicates" {
		t.Fatalf("action: want auto_prune_duplicates, got %q", broadcaster.action)
	}
	if broadcaster.argCount != 2 {
		t.Fatalf("argument count: want 2 so retention_days keeps its NULL default, got %d", broadcaster.argCount)
	}
	if broadcaster.attempts != 1 {
		t.Fatalf("attempts: want 1, got %d", broadcaster.attempts)
	}
}

// Each retry refetches the nonce rather than reusing the one that just lost, which
// is what makes a collision with the digest drain recoverable rather than fatal.
func TestBroadcastAutoPruneDuplicates_RefetchesTheNonceOnRetry(t *testing.T) {
	accounts := &mockAccounts{}
	// One failure, not two: the claim is that a retry refetches, and the backoff
	// before the second attempt is a real five seconds of test time.
	broadcaster := &prunePathBroadcaster{failUntil: 1}
	ops := &EngineOperations{logger: log.New(), accounts: accounts}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := ops.BroadcastAutoPruneDuplicatesWithRetry(
		ctx, "test-chain", newPruneSigner(t), broadcaster.broadcast, 100000, 100, 3,
	); err != nil {
		t.Fatalf("expected success after a retry, got %v", err)
	}
	if broadcaster.attempts != 2 {
		t.Fatalf("attempts: want 2, got %d", broadcaster.attempts)
	}
	if accounts.nonceCalls != 2 {
		t.Fatalf("nonce fetches: want one per attempt (2), got %d", accounts.nonceCalls)
	}
}

// A drain that outlives its leadership has to stop rather than keep broadcasting.
func TestBroadcastAutoPruneDuplicates_StopsOnContextCancellation(t *testing.T) {
	accounts := &mockAccounts{}
	broadcaster := &prunePathBroadcaster{failUntil: 100}
	ops := &EngineOperations{logger: log.New(), accounts: accounts}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := ops.BroadcastAutoPruneDuplicatesWithRetry(
		ctx, "test-chain", newPruneSigner(t), broadcaster.broadcast, 100000, 100, 5,
	); err == nil {
		t.Fatal("expected an error once the context was canceled")
	}
	if broadcaster.attempts > 2 {
		t.Fatalf("kept broadcasting past cancellation: %d attempts", broadcaster.attempts)
	}
}
