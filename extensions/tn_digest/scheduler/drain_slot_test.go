package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/log"
	ktypes "github.com/trufnetwork/kwil-db/core/types"
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
		Signer:    stubSigner{},
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

type stubSigner struct{}

func (stubSigner) Sign(msg []byte) (*auth.Signature, error) {
	return &auth.Signature{Data: []byte("sig"), Type: "stub"}, nil
}
func (stubSigner) CompactID() []byte        { return []byte("node") }
func (stubSigner) PubKey() crypto.PublicKey { return nil }
func (stubSigner) AuthType() string         { return "stub" }

type stubBroadcaster struct{}

func (stubBroadcaster) BroadcastTx(ctx context.Context, tx *ktypes.Transaction, sync uint8) (ktypes.Hash, *ktypes.TxResult, error) {
	return ktypes.Hash{}, &ktypes.TxResult{Code: uint32(ktypes.CodeOk)}, nil
}
