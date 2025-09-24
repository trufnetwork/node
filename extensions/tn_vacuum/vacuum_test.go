package tn_vacuum

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
)

type fakeTrigger struct {
	started  int
	stopped  int
	endCalls int
	lastCfg  TriggerConfig
}

type stubMechanism struct {
	prepared bool
}

func (s *stubMechanism) Name() string { return "stub" }
func (s *stubMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	s.prepared = true
	return nil
}
func (s *stubMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	return &RunReport{Mechanism: s.Name(), Status: "ok"}, nil
}
func (s *stubMechanism) Close(ctx context.Context) error { return nil }

func (f *fakeTrigger) Configure(ctx context.Context, cfg TriggerConfig, fire func(context.Context, FireOpts) error) error {
	f.lastCfg = cfg
	return nil
}
func (f *fakeTrigger) Start(ctx context.Context) error {
	f.started++
	return nil
}
func (f *fakeTrigger) Stop(ctx context.Context) error {
	f.stopped++
	return nil
}
func (f *fakeTrigger) OnLeaderChange(ctx context.Context, isLeader bool) error { return nil }
func (f *fakeTrigger) OnEndBlock(ctx context.Context, block *common.BlockContext) error {
	f.endCalls++
	return nil
}
func (f *fakeTrigger) Kind() string { return "fake" }

func TestLeaderCallbacksRespectEnabledFlag(t *testing.T) {
	ctx := context.Background()
	ResetForTest()
	ext := GetExtension()
	setMechanismFactoryForTest(func() Mechanism { return &stubMechanism{} })
	defer resetMechanismFactory()

	fake := &fakeTrigger{}
	mech := newMechanism()
	require.NoError(t, mech.Prepare(ctx, MechanismDeps{Logger: log.New()}))

	ext.mu.Lock()
	ext.logger = log.New()
	ext.config = Config{Enabled: false, Trigger: TriggerConfig{Kind: TriggerManual}}
	ext.trigger = fake
	ext.mechanism = mech
	ext.reloadIntervalBlocks = defaultReloadBlocks
	ext.mu.Unlock()

	leaderAcquire(ctx, nil, nil)
	require.Equal(t, 0, fake.started, "should not start when disabled")

	ext.mu.Lock()
	ext.config.Enabled = true
	ext.mu.Unlock()

	leaderAcquire(ctx, nil, nil)
	require.Equal(t, 1, fake.started)

	leaderLose(ctx, nil, nil)
	require.Equal(t, 1, fake.stopped)
}

func TestLeaderEndBlockTriggersReload(t *testing.T) {
	ctx := context.Background()
	ResetForTest()
	ext := GetExtension()
	setMechanismFactoryForTest(func() Mechanism { return &stubMechanism{} })
	defer resetMechanismFactory()

	fake := &fakeTrigger{}
	mech := newMechanism()
	require.NoError(t, mech.Prepare(ctx, MechanismDeps{Logger: log.New()}))

	ext.mu.Lock()
	ext.logger = log.New()
	ext.config = Config{Enabled: true, Trigger: TriggerConfig{Kind: TriggerManual}, ReloadIntervalBlocks: 3}
	ext.trigger = fake
	ext.mechanism = mech
	ext.reloadIntervalBlocks = 3
	ext.lastConfigHeight = 1
	ext.mu.Unlock()

	// end block before reload threshold -> only end callback
	leaderEndBlock(ctx, nil, &common.BlockContext{Height: 2})
	require.Equal(t, 1, fake.endCalls)
	require.Equal(t, TriggerManual, ext.Config().Trigger.Kind)

	// prepare service config to swap trigger kind on reload
	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				"enabled":        "true",
				"trigger":        TriggerBlockInterval,
				"block_interval": "5",
			},
		}},
	}
	app := &common.App{Service: svc}

	leaderEndBlock(ctx, app, &common.BlockContext{Height: 5})
	require.GreaterOrEqual(t, ext.LastConfigHeight(), int64(5))
	require.Equal(t, 2, fake.endCalls) // old trigger still sees callback before reload
	require.Equal(t, TriggerBlockInterval, ext.Config().Trigger.Kind)
	require.NotEqual(t, fake, ext.Trigger())
}

func TestPgRepackMechanismRequiresBinary(t *testing.T) {
	ctx := context.Background()
	resetMechanismFactory()
	mech := newMechanism()
	pr, ok := mech.(*pgRepackMechanism)
	require.True(t, ok, "mechanism should be pgRepackMechanism")

	oldPath := os.Getenv("PATH")
	require.NoError(t, os.Setenv("PATH", ""))
	defer os.Setenv("PATH", oldPath)

	err := mech.Prepare(ctx, MechanismDeps{Logger: log.New()})
	require.ErrorIs(t, err, ErrPgRepackUnavailable)

	_, runErr := mech.Run(ctx, RunRequest{Reason: "test"})
	require.ErrorIs(t, runErr, ErrPgRepackUnavailable)

	require.NoError(t, pr.Close(ctx))
}
