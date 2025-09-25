package leaderwatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/log"
	coretypes "github.com/trufnetwork/kwil-db/core/types"
)

func makeBlock(height int64, leader []byte) *common.BlockContext {
	pk := &fakePubKey{b: leader}
	return &common.BlockContext{
		Height: height,
		ChainContext: &common.ChainContext{
			NetworkParameters: &coretypes.NetworkParameters{
				Leader: coretypes.PublicKey{PublicKey: pk},
			},
		},
	}
}

type fakePubKey struct {
	b []byte
}

func (f *fakePubKey) Equals(other crypto.Key) bool {
	if other == nil {
		return false
	}
	return string(f.Bytes()) == string(other.Bytes()) && f.Type() == other.Type()
}
func (f *fakePubKey) Bytes() []byte                                { return f.b }
func (f *fakePubKey) Type() crypto.KeyType                         { return crypto.KeyTypeEd25519 }
func (f *fakePubKey) Verify(data []byte, sig []byte) (bool, error) { return true, nil }

func TestLeaderwatch_CallbackOrderingAndTransitions(t *testing.T) {
	ResetForTest()
	app := &common.App{Service: &common.Service{Identity: []byte("nodeA"), Logger: log.New()}}
	ctx := context.Background()

	var events []string

	err := Register("first", Callbacks{
		OnAcquire: func(ctx context.Context, app *common.App, block *common.BlockContext) {
			events = append(events, "first_acquire")
		},
		OnLose: func(ctx context.Context, app *common.App, block *common.BlockContext) {
			events = append(events, "first_lose")
		},
		OnEndBlock: func(ctx context.Context, app *common.App, block *common.BlockContext) {
			events = append(events, "first_end")
		},
	})
	require.NoError(t, err)

	err = Register("second", Callbacks{
		OnAcquire: func(ctx context.Context, app *common.App, block *common.BlockContext) {
			events = append(events, "second_acquire")
		},
		OnLose: func(ctx context.Context, app *common.App, block *common.BlockContext) {
			events = append(events, "second_lose")
		},
		OnEndBlock: func(ctx context.Context, app *common.App, block *common.BlockContext) {
			events = append(events, "second_end")
		},
	})
	require.NoError(t, err)

	// become leader
	require.NoError(t, endBlockHook(ctx, app, makeBlock(1, []byte("nodeA"))))
	require.Equal(t, []string{
		"first_acquire", "first_end",
		"second_acquire", "second_end",
	}, events)

	// stay leader - only end callbacks fire
	events = nil
	require.NoError(t, endBlockHook(ctx, app, makeBlock(2, []byte("nodeA"))))
	require.Equal(t, []string{"first_end", "second_end"}, events)

	// lose leadership
	events = nil
	require.NoError(t, endBlockHook(ctx, app, makeBlock(3, []byte("nodeB"))))
	require.Equal(t, []string{
		"first_lose", "first_end",
		"second_lose", "second_end",
	}, events)
}

func TestLeaderwatch_DuplicateRegistrationFails(t *testing.T) {
	ResetForTest()
	require.NoError(t, Register("dup", Callbacks{}))
	err := Register("dup", Callbacks{})
	require.Error(t, err)
}

func TestLeaderwatch_ResetClearsState(t *testing.T) {
	ResetForTest()
	require.NoError(t, Register("one", Callbacks{}))
	ResetForTest()
	require.NoError(t, Register("one", Callbacks{}))
}
