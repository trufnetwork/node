package tn_vacuum

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
)

type stubMechanism struct {
	prepared int
	runs     []RunRequest
	closeCnt int
}

func (s *stubMechanism) Name() string { return "stub" }

func (s *stubMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	s.prepared++
	return nil
}

func (s *stubMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	s.runs = append(s.runs, req)
	return &RunReport{Mechanism: s.Name(), Status: "ok"}, nil
}

func (s *stubMechanism) Close(ctx context.Context) error {
	s.closeCnt++
	return nil
}

type failingMechanism struct{}

func (f *failingMechanism) Name() string { return "fail" }
func (f *failingMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	return errors.New("prepare failed")
}
func (f *failingMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	return nil, errors.New("should not run")
}
func (f *failingMechanism) Close(ctx context.Context) error { return nil }

func TestConfigureDisabledSkipsMechanism(t *testing.T) {
	ctx := context.Background()
	ResetForTest()
	ext := GetExtension()
	ext.setLogger(log.New())

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	require.NoError(t, ext.configure(ctx, Config{Enabled: false, BlockInterval: 5}))
	require.Equal(t, 0, stub.prepared)
}

func TestEngineReadyPreparesMechanism(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				"enabled":        "true",
				"block_interval": "3",
			},
		}},
	}

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, stub.prepared)

	block := &common.BlockContext{Height: 1}
	require.NoError(t, endBlockHook(ctx, app, block))
	require.Len(t, stub.runs, 1)
	require.Equal(t, "block_interval:1", stub.runs[0].Reason)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 2}))
	require.Len(t, stub.runs, 1)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 4}))
	require.Len(t, stub.runs, 2)
}

func TestConfigureFailureLeavesMechanismNil(t *testing.T) {
	ctx := context.Background()
	ResetForTest()
	ext := GetExtension()
	ext.setLogger(log.New())

	setMechanismFactoryForTest(func() Mechanism { return &failingMechanism{} })
	defer resetMechanismFactory()

	err := ext.configure(ctx, Config{Enabled: true, BlockInterval: 10})
	require.Error(t, err)

	ext.mu.RLock()
	defer ext.mu.RUnlock()
	require.Nil(t, ext.mechanism)
}
