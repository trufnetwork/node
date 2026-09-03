package scheduler

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_digest/internal"
)

// The digest drain and the duplicate prune drain share one slot. Both ship with
// the same six-hourly default, so on most firings they want to run at the same
// instant; without the slot they would fetch the same nonce and one would lose.

func newSlotScheduler() *DigestScheduler {
	return NewDigestScheduler(NewDigestSchedulerParams{Logger: log.New(log.WithLevel(log.LevelError))})
}

func TestDrainSlot_SecondWaiterBlocksUntilTheFirstReleases(t *testing.T) {
	s := newSlotScheduler()
	ctx := context.Background()

	if !s.acquireDrainSlot(ctx) {
		t.Fatal("first acquire should succeed")
	}

	got := make(chan bool, 1)
	go func() { got <- s.acquireDrainSlot(ctx) }()

	select {
	case <-got:
		t.Fatal("second acquire returned while the first drain still held the slot")
	case <-time.After(50 * time.Millisecond):
	}

	s.releaseDrainSlot()

	select {
	case ok := <-got:
		if !ok {
			t.Fatal("second acquire should have succeeded once the slot was released")
		}
	case <-time.After(time.Second):
		t.Fatal("second acquire never returned after the slot was released")
	}
	s.releaseDrainSlot()
}

// A drain waiting behind the other one still has to give up when the node loses
// leadership. Waiting is the right behaviour, waiting forever is not.
func TestDrainSlot_WaiterGivesUpWhenItsContextIsDone(t *testing.T) {
	s := newSlotScheduler()
	if !s.acquireDrainSlot(context.Background()) {
		t.Fatal("first acquire should succeed")
	}
	defer s.releaseDrainSlot()

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan bool, 1)
	go func() { got <- s.acquireDrainSlot(ctx) }()
	cancel()

	select {
	case ok := <-got:
		if ok {
			t.Fatal("a canceled waiter should not report that it took the slot")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter never returned")
	}
}

// Releasing a slot nobody holds is what a drain that gave up on its context does
// on the way out, so it has to be a no-op rather than a block.
func TestDrainSlot_ReleaseWithoutHoldingIsANoOp(t *testing.T) {
	s := newSlotScheduler()
	s.releaseDrainSlot()

	if !s.acquireDrainSlot(context.Background()) {
		t.Fatal("the slot should still be free after a spurious release")
	}
	s.releaseDrainSlot()
}

// A scheduler built as a struct literal rather than through the constructor has a
// nil slot. Selecting on a nil channel blocks forever, so the acquire has to make
// one rather than trust the constructor.
func TestDrainSlot_InitialisesWhenTheSchedulerWasBuiltByHand(t *testing.T) {
	s := &DigestScheduler{logger: log.New(log.WithLevel(log.LevelError))}

	done := make(chan bool, 1)
	go func() { done <- s.acquireDrainSlot(context.Background()) }()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("acquire on a hand-built scheduler should succeed")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire on a hand-built scheduler blocked")
	}
	s.releaseDrainSlot()
}

// gocron's Stop waits for a running job to return, and a sweep waiting on the slot
// returns only when its context is done. So the cancel has to come first, and
// neither call may hold the mutex the job takes on entry. Getting that order wrong
// hangs the node's leadership transition for as long as the other drain runs,
// which is up to a hundred minutes.
//
// The sweep has to be a real cron job for this to reproduce: it is gocron's own
// wait on its running jobs that turns the wrong order into a deadlock.
func TestStopPrune_ReleasesASweepWaitingForTheSlot(t *testing.T) {
	s := NewDigestScheduler(NewDigestSchedulerParams{
		Logger:    log.New(log.WithLevel(log.LevelError)),
		Service:   &common.Service{GenesisConfig: &config.GenesisConfig{ChainID: "test-chain"}},
		EngineOps: internal.NewEngineOperations(nil, nil, nil, nil, log.New(log.WithLevel(log.LevelError))),
		Signer:    testSigner(),
		Tx:        stubBroadcaster{},
	})

	// The digest drain holds the slot, so the sweep will block on it.
	if !s.acquireDrainSlot(context.Background()) {
		t.Fatal("could not take the slot for the digest drain")
	}
	defer s.releaseDrainSlot()

	// Every second, so the job is running well before the stop.
	if err := s.StartPrune(context.Background(), "* * * * * *"); err != nil {
		t.Fatalf("StartPrune: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	stopped := make(chan error, 1)
	go func() { stopped <- s.StopPrune() }()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("StopPrune blocked behind a sweep that was waiting for the drain slot")
	}
}

// RunPruneOnce broadcasts from the same signer account as the scheduled drains, so
// letting it run alongside one means both read the same nonce and one transaction
// loses. It refuses instead of waiting, and the error says which of the two it was.
func TestRunPruneOnce_RefusesWhileADrainHoldsTheSlot(t *testing.T) {
	s := newPruneTestScheduler()

	if !s.acquireDrainSlot(context.Background()) {
		t.Fatal("could not take the slot for the drain")
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.RunPruneOnce(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("RunPruneOnce should have refused while a drain held the slot")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunPruneOnce waited for the slot instead of refusing")
	}

	// And it leaves the slot where it found it, so the drain still owns it.
	s.releaseDrainSlot()
	if !s.tryAcquireDrainSlot() {
		t.Fatal("the refused call consumed or corrupted the slot")
	}
	s.releaseDrainSlot()
}

// The other half of the same claim: refusing is about contention, not a permanent
// state, so a free slot lets the one-off through and hands it back afterwards.
func TestRunPruneOnce_RunsAndReleasesWhenTheSlotIsFree(t *testing.T) {
	s := newPruneTestScheduler()

	if _, err := s.RunPruneOnce(context.Background()); err != nil {
		t.Fatalf("RunPruneOnce with a free slot: %v", err)
	}
	if !s.tryAcquireDrainSlot() {
		t.Fatal("RunPruneOnce did not release the slot")
	}
	s.releaseDrainSlot()
}

// newPruneTestScheduler builds a scheduler whose dependencies are present but inert,
// which is enough for the entry points that only broadcast.
func newPruneTestScheduler() *DigestScheduler {
	return NewDigestScheduler(NewDigestSchedulerParams{
		Logger:    log.New(log.WithLevel(log.LevelError)),
		Service:   &common.Service{GenesisConfig: &config.GenesisConfig{ChainID: "test-chain"}},
		EngineOps: internal.NewEngineOperations(nil, nil, nil, stubAccounts{}, log.New(log.WithLevel(log.LevelError))),
		Signer:    testSigner(),
		Tx:        stubBroadcaster{},
	})
}

type stubAccounts struct{}

func (stubAccounts) GetAccount(ctx context.Context, db sql.Executor, id *ktypes.AccountID) (*ktypes.Account, error) {
	return &ktypes.Account{ID: id, Nonce: 0, Balance: big.NewInt(1000)}, nil
}
func (stubAccounts) Credit(ctx context.Context, db sql.Executor, id *ktypes.AccountID, amt *big.Int) error {
	return nil
}
func (stubAccounts) Transfer(ctx context.Context, db sql.TxMaker, from, to *ktypes.AccountID, amt *big.Int) error {
	return nil
}
func (stubAccounts) ApplySpend(ctx context.Context, db sql.Executor, id *ktypes.AccountID, amt *big.Int, nonce int64) error {
	return nil
}

// testSigner is a real key rather than a stub. A stub with a nil PubKey panics
// inside GetSignerAccount, which the slot tests never reach but the one-off
// entry points do.
func testSigner() auth.Signer {
	priv, _, err := crypto.GenerateSecp256k1Key(nil)
	if err != nil {
		panic(err)
	}
	return auth.GetNodeSigner(priv)
}

type stubBroadcaster struct{}

func (stubBroadcaster) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	return ktypes.Hash{}, &ktypes.TxResult{
		Code: uint32(ktypes.CodeOk),
		Log:  `auto_prune_duplicates:{"swept_streams":1,"deleted_event_times":0,"deleted_rows":0,"has_more_to_delete":false}`,
	}, nil
}
