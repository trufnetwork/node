package tn_vacuum

import (
	"context"
	"errors"
	"testing"
	"time"

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
	return &RunReport{
		Mechanism:       s.Name(),
		Status:          StatusOK,
		Duration:        100 * time.Millisecond,
		TablesProcessed: 5,
	}, nil
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

type nilReportMechanism struct{}

func (n *nilReportMechanism) Name() string { return "nil_report" }
func (n *nilReportMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	return nil
}
func (n *nilReportMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	return nil, nil
}
func (n *nilReportMechanism) Close(ctx context.Context) error { return nil }

type errorRunMechanism struct{}

func (e *errorRunMechanism) Name() string { return "error_run" }
func (e *errorRunMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	return nil
}
func (e *errorRunMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	return nil, errors.New("run failed")
}
func (e *errorRunMechanism) Close(ctx context.Context) error { return nil }

type stubStateStore struct {
	ensureCount int
	loadCount   int
	saveCount   int
	loadState   runState
	loadOK      bool
	loadErr     error
	saveErr     error
	lastSaved   runState
}

func (s *stubStateStore) Ensure(ctx context.Context) error {
	s.ensureCount++
	return nil
}

func (s *stubStateStore) Load(ctx context.Context) (runState, bool, error) {
	s.loadCount++
	if s.loadErr != nil {
		return runState{}, false, s.loadErr
	}
	return s.loadState, s.loadOK, nil
}

func (s *stubStateStore) Save(ctx context.Context, state runState) error {
	s.saveCount++
	s.lastSaved = state
	if s.saveErr != nil {
		return s.saveErr
	}
	return nil
}

func (s *stubStateStore) Close() {}

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
				"enabled":             "true",
				"block_interval":      "3",
				ConfigKeyPgRepackJobs: "2",
			},
		}},
	}

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, stub.prepared)

	block := &common.BlockContext{Height: 1}
	require.NoError(t, endBlockHook(ctx, app, block))
	waitForRunCount(t, stub, 1)
	require.Equal(t, "block_interval:1", stub.runs[0].Reason)
	require.Equal(t, 2, stub.runs[0].PgRepackJobs)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 2}))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, stub.runs, 1)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 4}))
	waitForRunCount(t, stub, 2)
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

func TestRunReportEnhancement(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				ConfigKeyEnabled:       "true",
				ConfigKeyBlockInterval: "1",
			},
		}},
	}

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))

	block := &common.BlockContext{Height: 1}
	require.NoError(t, endBlockHook(ctx, app, block))

	waitForRunCount(t, stub, 1)

	// Verify the stub returns enhanced report data
	ext := GetExtension()
	runner := ext.runner
	require.NotNil(t, runner)

	report, err := stub.Run(ctx, RunRequest{Reason: "test"})
	require.NoError(t, err)
	require.NotNil(t, report)
	require.Equal(t, "stub", report.Mechanism)
	require.Equal(t, StatusOK, report.Status)
	require.Equal(t, 100*time.Millisecond, report.Duration)
	require.Equal(t, 5, report.TablesProcessed)
}

func TestVacuumSkippedMetrics(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				ConfigKeyEnabled:       "true",
				ConfigKeyBlockInterval: "10",
			},
		}},
	}

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))

	// First run at height 1
	block := &common.BlockContext{Height: 1}
	require.NoError(t, endBlockHook(ctx, app, block))
	waitForRunCount(t, stub, 1)

	// Should be skipped at height 5 (interval not met)
	block = &common.BlockContext{Height: 5}
	require.NoError(t, endBlockHook(ctx, app, block))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, stub.runs, 1, "should not run - interval not met")

	// Should run at height 11 (interval met)
	block = &common.BlockContext{Height: 11}
	require.NoError(t, endBlockHook(ctx, app, block))
	waitForRunCount(t, stub, 2)
}

func TestEngineReadyLoadsPersistedState(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	store := &stubStateStore{
		loadState: runState{LastRunHeight: 12, LastRunAt: time.Unix(50, 0)},
		loadOK:    true,
	}

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				ConfigKeyEnabled:       "true",
				ConfigKeyBlockInterval: "5",
			},
		}},
	}

	app := &common.App{Service: svc}

	ext := GetExtension()
	ext.setLogger(log.New())
	ext.setStateStore(store)

	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, store.ensureCount)
	require.Equal(t, 1, store.loadCount)

	ext.mu.RLock()
	require.Equal(t, int64(12), ext.state.LastRunHeight)
	ext.mu.RUnlock()

	metricsStub := &stubMetricsRecorder{}
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 14}))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, stub.runs, 0, "should not run before interval is met")

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 18}))
	waitForRunCount(t, stub, 1)
	waitForCondition(t, time.Second, func() bool { return store.saveCount == 1 })
	require.Equal(t, int64(18), metricsStub.lastHeight)
}

func TestSuccessfulRunPersistsState(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	store := &stubStateStore{}

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				ConfigKeyEnabled:       "true",
				ConfigKeyBlockInterval: "1",
			},
		}},
	}

	app := &common.App{Service: svc}

	ext := GetExtension()
	ext.setLogger(log.New())
	ext.setStateStore(store)

	now := time.Unix(100, 0)
	ext.setNowFunc(func() time.Time { return now })

	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, store.ensureCount)

	metricsStub := &stubMetricsRecorder{}
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 5}))
	waitForRunCount(t, stub, 1)
	waitForCondition(t, time.Second, func() bool { return store.saveCount == 1 })
	require.Equal(t, int64(5), store.lastSaved.LastRunHeight)
	require.Equal(t, now.UTC(), store.lastSaved.LastRunAt)
	require.Equal(t, 1, metricsStub.completeCount)
	require.Equal(t, int64(5), metricsStub.lastHeight)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 5}))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, store.saveCount, "duplicate height should not persist again")
}

func TestRunnerHandlesNilReport(t *testing.T) {
	ctx := context.Background()
	runner := &Runner{logger: log.New()}
	metricsStub := &stubMetricsRecorder{}

	require.NoError(t, runner.Execute(ctx, RunnerArgs{
		Mechanism: &nilReportMechanism{},
		Logger:    log.New(),
		Reason:    "test",
		Metrics:   metricsStub,
	}))

	require.Equal(t, 1, metricsStub.startCount)
	require.Equal(t, 1, metricsStub.completeCount)
	require.Zero(t, metricsStub.lastDuration)
	require.Zero(t, metricsStub.lastTables)
	require.Equal(t, "nil_report", metricsStub.lastMechanism)
}

func TestMaybeRunRecordsErrorOnce(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	setMechanismFactoryForTest(func() Mechanism { return &errorRunMechanism{} })
	defer resetMechanismFactory()

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{Extensions: map[string]map[string]string{
			ExtensionName: {
				ConfigKeyEnabled:       "true",
				ConfigKeyBlockInterval: "1",
			},
		}},
	}

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))

	metricsStub := &stubMetricsRecorder{}
	ext := GetExtension()
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 1}))
	waitForCondition(t, time.Second, func() bool { return metricsStub.errorCount == 1 })
	require.Equal(t, 1, metricsStub.startCount)
	require.Equal(t, "error_run", metricsStub.lastErrorMechanism)
}

func TestEnqueueRunBusy(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	ext := GetExtension()
	ext.setLogger(log.New())
	ext.mu.Lock()
	ext.runQueue = make(chan runRequest, 1)
	ext.runInProgress = true
	ext.mu.Unlock()

	req := runRequest{height: 1, reason: "test"}
	require.False(t, ext.enqueueRun(ctx, req))
}

type stubMetricsRecorder struct {
	startCount         int
	completeCount      int
	errorCount         int
	skippedCount       int
	lastDuration       time.Duration
	lastTables         int
	lastMechanism      string
	lastErrorType      string
	lastErrorMechanism string
	lastSkipReason     string
	lastHeight         int64
}

func (s *stubMetricsRecorder) RecordVacuumStart(ctx context.Context, mechanism string) {
	s.startCount++
	s.lastMechanism = mechanism
}

func (s *stubMetricsRecorder) RecordVacuumComplete(ctx context.Context, mechanism string, duration time.Duration, tablesProcessed int) {
	s.completeCount++
	s.lastMechanism = mechanism
	s.lastDuration = duration
	s.lastTables = tablesProcessed
}

func (s *stubMetricsRecorder) RecordVacuumError(ctx context.Context, mechanism string, errType string) {
	s.errorCount++
	s.lastErrorMechanism = mechanism
	s.lastErrorType = errType
}

func (s *stubMetricsRecorder) RecordVacuumSkipped(ctx context.Context, reason string) {
	s.skippedCount++
	s.lastSkipReason = reason
}

func (s *stubMetricsRecorder) RecordLastRunHeight(ctx context.Context, height int64) {
	s.lastHeight = height
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fn() {
		return
	}
	t.Fatalf("condition not met within %s", timeout)
}

func waitForRunCount(t *testing.T, stub *stubMechanism, count int) {
	waitForCondition(t, time.Second, func() bool {
		return len(stub.runs) >= count
	})
}
