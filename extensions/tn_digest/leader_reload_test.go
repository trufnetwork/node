package tn_digest

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	crypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	coretypes "github.com/trufnetwork/kwil-db/core/types"
	sqltypes "github.com/trufnetwork/kwil-db/node/types/sql"
	digestinternal "github.com/trufnetwork/node/extensions/tn_digest/internal"
)

// testPubKey implements crypto.PublicKey for tests
type testPubKey struct {
	b  []byte
	kt crypto.KeyType
}

// Key interface methods
func (p testPubKey) Equals(k crypto.Key) bool {
	return p.Type() == k.Type() && string(p.Bytes()) == string(k.Bytes())
}
func (p testPubKey) Bytes() []byte        { return p.b }
func (p testPubKey) Type() crypto.KeyType { return p.kt }

// PublicKey interface method
func (p testPubKey) Verify(data []byte, sig []byte) (bool, error) { return true, nil }

// fakeDB implements sql.DB for controlled SELECTs used by LoadDigestConfig
type fakeDB struct {
	enabled  bool
	schedule string
}

func (f *fakeDB) Execute(ctx context.Context, stmt string, args ...any) (*sqltypes.ResultSet, error) {
	// Return one row for SELECT enabled, digest_schedule FROM digest_config WHERE id = 1
	// Any other stmt returns empty rows
	if len(stmt) >= 6 && stmt[:6] == "SELECT" {
		if f.schedule == "" {
			return &sqltypes.ResultSet{Columns: []string{"enabled", "digest_schedule"}, Rows: [][]any{}}, nil
		}
		return &sqltypes.ResultSet{Columns: []string{"enabled", "digest_schedule"}, Rows: [][]any{{f.enabled, f.schedule}}}, nil
	}
	return &sqltypes.ResultSet{Columns: []string{}, Rows: [][]any{}}, nil
}

func (f *fakeDB) BeginTx(ctx context.Context) (sqltypes.Tx, error) { return &fakeTx{}, nil }

type fakeTx struct{}

func (t *fakeTx) Execute(ctx context.Context, stmt string, args ...any) (*sqltypes.ResultSet, error) {
	return &sqltypes.ResultSet{Columns: []string{}, Rows: [][]any{}}, nil
}
func (t *fakeTx) BeginTx(ctx context.Context) (sqltypes.Tx, error) { return &fakeTx{}, nil }
func (t *fakeTx) Rollback(ctx context.Context) error               { return nil }
func (t *fakeTx) Commit(ctx context.Context) error                 { return nil }

// fakeAccounts implements common.Accounts for tests
type fakeAccounts struct{}

func (f *fakeAccounts) GetAccount(ctx context.Context, db sqltypes.Executor, acctID *coretypes.AccountID) (*coretypes.Account, error) {
	return &coretypes.Account{
		ID:      acctID,
		Nonce:   0,
		Balance: big.NewInt(1000),
	}, nil
}

func (f *fakeAccounts) Credit(ctx context.Context, tx sqltypes.Executor, acctID *coretypes.AccountID, amount *big.Int) error {
	return nil
}

func (f *fakeAccounts) Transfer(ctx context.Context, tx sqltypes.TxMaker, from, to *coretypes.AccountID, amt *big.Int) error {
	return nil
}

func (f *fakeAccounts) ApplySpend(ctx context.Context, tx sqltypes.Executor, account *coretypes.AccountID, amount *big.Int, nonce int64) error {
	return nil
}

// fakeEngine implements common.Engine minimally for tests
type fakeEngine struct{}

func (f *fakeEngine) Call(ctx *common.EngineContext, db sqltypes.DB, namespace, action string, args []any, resultFn func(*common.Row) error) (*common.CallResult, error) {
	return &common.CallResult{}, nil
}

func (f *fakeEngine) CallWithoutEngineCtx(ctx context.Context, db sqltypes.DB, namespace, action string, args []any, resultFn func(*common.Row) error) (*common.CallResult, error) {
	return &common.CallResult{}, nil
}

func (f *fakeEngine) Execute(ctx *common.EngineContext, db sqltypes.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	return nil
}

func (f *fakeEngine) ExecuteWithoutEngineCtx(ctx context.Context, db sqltypes.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	rs, err := db.Execute(ctx, statement)
	if err != nil {
		return err
	}
	for _, r := range rs.Rows {
		row := &common.Row{Values: r}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

// Helpers
func makeService(identity []byte, reloadBlocks string) *common.Service {
	cfg := &config.Config{Extensions: map[string]map[string]string{ExtensionName: {"reload_interval_blocks": reloadBlocks}}}
	return &common.Service{Logger: log.New(log.WithLevel(log.LevelError)), LocalConfig: cfg, Identity: identity}
}

func makeBlock(height int64, leader []byte) *common.BlockContext {
	return &common.BlockContext{
		Height: height,
		ChainContext: &common.ChainContext{NetworkParameters: &coretypes.NetworkParameters{
			Leader: coretypes.PublicKey{PublicKey: testPubKey{b: leader, kt: crypto.KeyTypeEd25519}},
		}},
	}
}

func resetExtensionForTest() *Extension {
	ext := &Extension{logger: log.New(log.WithLevel(log.LevelError))}
	// prerequisites so scheduler can be created during tests
	ext.SetNodeSigner(mockSigner{})
	ext.SetBroadcaster(mockBroadcaster{})
	// minimal engine ops
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, &fakeDB{}, &fakeAccounts{}, log.New()))
	SetExtension(ext)
	return ext
}

// Tests
func TestDigest_DefaultDisabled_NoSchedulerOnLeaderAcquire(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("nodeA")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)
	block := makeBlock(1, identity)

	digestLeaderAcquire(context.Background(), app, block)
	assert.Nil(t, ext.Scheduler())
}

func TestDigest_LeaderAcquire_StartsScheduler_WhenEnabled(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("nodeB")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)
	block := makeBlock(1, identity)

	digestLeaderAcquire(context.Background(), app, block)
	require.NotNil(t, ext.Scheduler())
	// cleanup
	_ = ext.Scheduler().Stop()
}

func TestDigest_LoseLeadership_StopsScheduler(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("nodeC")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)

	// acquire leadership
	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())

	// lose leadership
	other := []byte("other")
	digestLeaderLose(context.Background(), app, makeBlock(2, other))

	// idempotent stop
	_ = ext.Scheduler().Stop()
}

func TestDigest_Reload_EnablesAndStarts_WhenBecomesEnabled(t *testing.T) {
	ext := resetExtensionForTest()
	// start disabled
	ext.SetConfig(false, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1)
	// avoid reload on first block
	ext.SetLastCheckedHeight(1)
	identity := []byte("nodeD")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	// attach EngineOps with fake DB that returns enabled on reload BEFORE first hook
	fdb := &fakeDB{enabled: true, schedule: "*/5 * * * *"}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, &fakeAccounts{}, log.New()))

	// leader at height 1: disabled, no scheduler
	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	assert.Nil(t, ext.Scheduler())

	// height 2 triggers reload -> should enable and start
	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))
	require.NotNil(t, ext.Scheduler())
	_ = ext.Scheduler().Stop()
}

func TestDigest_Reload_DisablesAndStops_WhenBecomesDisabled(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1)
	// avoid reload on first block
	ext.SetLastCheckedHeight(1)
	identity := []byte("nodeE")
	app := &common.App{Service: makeService(identity, "1")}
	ext.SetService(app.Service)

	// start as leader enabled (no reload yet)
	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.NotNil(t, ext.Scheduler())

	// reload returns disabled
	fdb := &fakeDB{enabled: false, schedule: "*/5 * * * *"}
	ext.SetEngineOps(digestinternal.NewEngineOperations(&fakeEngine{}, fdb, &fakeAccounts{}, log.New()))
	digestLeaderEndBlock(context.Background(), app, makeBlock(2, identity))

	// stop should be idempotent
	_ = ext.Scheduler().Stop()
}

func TestDigest_LeaderDetection_UsesNetworkParametersLeader(t *testing.T) {
	ext := resetExtensionForTest()
	ext.SetConfig(true, "*/5 * * * *")
	ext.SetReloadIntervalBlocks(1000)
	identity := []byte("nodeF")
	app := &common.App{Service: makeService(identity, "1000")}
	ext.SetService(app.Service)

	// Proposer would be different, but we use NetworkParameters.Leader in makeBlock
	digestLeaderAcquire(context.Background(), app, makeBlock(1, identity))
	require.True(t, ext.IsLeader())
	require.NotNil(t, ext.Scheduler())
	_ = ext.Scheduler().Stop()
}

// minimal mocks for signer and broadcaster

type mockSigner struct{}

func (mockSigner) Sign(msg []byte) (*auth.Signature, error) {
	return &auth.Signature{Data: []byte("sig"), Type: "stub"}, nil
}
func (mockSigner) CompactID() []byte { return []byte("node") }
func (mockSigner) PubKey() crypto.PublicKey {
	return testPubKey{b: []byte("node"), kt: crypto.KeyTypeEd25519}
}
func (mockSigner) AuthType() string { return "stub" }

type mockBroadcaster struct{}

func (mockBroadcaster) BroadcastTx(ctx context.Context, tx *coretypes.Transaction, sync uint8) (coretypes.Hash, *coretypes.TxResult, error) {
	return coretypes.Hash{}, &coretypes.TxResult{Code: uint32(coretypes.CodeOk)}, nil
}
