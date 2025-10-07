package tn_vacuum

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
)

type stubMechanism struct {
	mu       sync.RWMutex
	prepared int
	runs     []RunRequest
	closeCnt int
}

func (s *stubMechanism) Name() string { return "stub" }

func (s *stubMechanism) Prepare(ctx context.Context, deps MechanismDeps) error {
	s.mu.Lock()
	s.prepared++
	s.mu.Unlock()
	return nil
}

func (s *stubMechanism) Run(ctx context.Context, req RunRequest) (*RunReport, error) {
	s.mu.Lock()
	s.runs = append(s.runs, req)
	s.mu.Unlock()
	return &RunReport{
		Mechanism:       s.Name(),
		Status:          StatusOK,
		Duration:        100 * time.Millisecond,
		TablesProcessed: 5,
	}, nil
}

func (s *stubMechanism) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closeCnt++
	s.mu.Unlock()
	return nil
}

func (s *stubMechanism) preparedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prepared
}

func (s *stubMechanism) runCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.runs)
}

func (s *stubMechanism) runAt(i int) (RunRequest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.runs) {
		return RunRequest{}, false
	}
	return s.runs[i], true
}

func (s *stubMechanism) runsSnapshot() []RunRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copyRuns := make([]RunRequest, len(s.runs))
	copy(copyRuns, s.runs)
	return copyRuns
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
	mu          sync.RWMutex
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
	s.mu.Lock()
	s.ensureCount++
	s.mu.Unlock()
	return nil
}

func (s *stubStateStore) Load(ctx context.Context) (runState, bool, error) {
	s.mu.Lock()
	s.loadCount++
	err := s.loadErr
	state := s.loadState
	ok := s.loadOK
	s.mu.Unlock()
	if err != nil {
		return runState{}, false, err
	}
	return state, ok, nil
}

func (s *stubStateStore) Save(ctx context.Context, state runState) error {
	s.mu.Lock()
	s.saveCount++
	s.lastSaved = state
	err := s.saveErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (s *stubStateStore) Close() {}

func (s *stubStateStore) ensureCountValue() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ensureCount
}

func (s *stubStateStore) loadCountValue() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadCount
}

func (s *stubStateStore) saveCountValue() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.saveCount
}

func (s *stubStateStore) lastSavedState() runState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastSaved
}

func TestConfigureDisabledSkipsMechanism(t *testing.T) {
	ctx := context.Background()
	ResetForTest()
	ext := GetExtension()
	ext.setLogger(log.New())

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	require.NoError(t, ext.configure(ctx, Config{Enabled: false, BlockInterval: 5}))
	require.Equal(t, 0, stub.preparedCount())
}

func TestEngineReadyPreparesMechanism(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	stub := &stubMechanism{}
	setMechanismFactoryForTest(func() Mechanism { return stub })
	defer resetMechanismFactory()

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{
			DB: config.DBConfig{DBName: "kwild_test"},
			Extensions: map[string]map[string]string{
				ExtensionName: {
					"enabled":             "true",
					"block_interval":      "3",
					ConfigKeyPgRepackJobs: "2",
				},
			},
		},
	}

	ext := GetExtension()
	ext.setStateStore(&stubStateStore{})

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, stub.preparedCount())

	block := &common.BlockContext{Height: 1}
	require.NoError(t, endBlockHook(ctx, app, block))
	waitForRunCount(t, stub, 1)
	firstRun, ok := stub.runAt(0)
	require.True(t, ok)
	require.Equal(t, "block_interval:1", firstRun.Reason)
	require.Equal(t, 2, firstRun.PgRepackJobs)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 2}))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, stub.runsSnapshot(), 1)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 4}))
	waitForRunCount(t, stub, 2)
}

func TestConfigureFailureLeavesMechanismNil(t *testing.T) {
	ctx := context.Background()
	ResetForTest()
	ext := GetExtension()
	ext.setLogger(log.New())
	ext.setService(&common.Service{LocalConfig: &config.Config{DB: config.DBConfig{DBName: "kwild_test"}}})

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
		LocalConfig: &config.Config{
			DB: config.DBConfig{DBName: "kwild_test"},
			Extensions: map[string]map[string]string{
				ExtensionName: {
					ConfigKeyEnabled:       "true",
					ConfigKeyBlockInterval: "1",
				},
			},
		},
	}

	ext := GetExtension()
	ext.setStateStore(&stubStateStore{})

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))

	block := &common.BlockContext{Height: 1}
	require.NoError(t, endBlockHook(ctx, app, block))

	waitForRunCount(t, stub, 1)

	// Verify the stub returns enhanced report data
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
		LocalConfig: &config.Config{
			DB: config.DBConfig{DBName: "kwild_test"},
			Extensions: map[string]map[string]string{
				ExtensionName: {
					ConfigKeyEnabled:       "true",
					ConfigKeyBlockInterval: "10",
				},
			},
		},
	}

	ext := GetExtension()
	ext.setStateStore(&stubStateStore{})

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
	require.Len(t, stub.runsSnapshot(), 1, "should not run - interval not met")

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
		LocalConfig: &config.Config{
			DB: config.DBConfig{DBName: "kwild_test"},
			Extensions: map[string]map[string]string{
				ExtensionName: {
					ConfigKeyEnabled:       "true",
					ConfigKeyBlockInterval: "5",
				},
			},
		},
	}

	app := &common.App{Service: svc}

	ext := GetExtension()
	ext.setLogger(log.New())
	ext.setStateStore(store)

	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, store.ensureCountValue())
	require.Equal(t, 1, store.loadCountValue())

	ext.mu.RLock()
	require.Equal(t, int64(12), ext.state.LastRunHeight)
	ext.mu.RUnlock()

	metricsStub := &stubMetricsRecorder{}
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 14}))
	time.Sleep(50 * time.Millisecond)
	require.Len(t, stub.runsSnapshot(), 0, "should not run before interval is met")

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 18}))
	waitForRunCount(t, stub, 1)
	waitForCondition(t, time.Second, func() bool { return store.saveCountValue() == 1 })
	require.Equal(t, int64(18), metricsStub.snapshot().lastHeight)
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
		LocalConfig: &config.Config{
			DB: config.DBConfig{DBName: "kwild_test"},
			Extensions: map[string]map[string]string{
				ExtensionName: {
					ConfigKeyEnabled:       "true",
					ConfigKeyBlockInterval: "1",
				},
			},
		},
	}

	app := &common.App{Service: svc}

	ext := GetExtension()
	ext.setLogger(log.New())
	ext.setStateStore(store)

	now := time.Unix(100, 0)
	ext.setNowFunc(func() time.Time { return now })

	require.NoError(t, engineReadyHook(ctx, app))
	require.Equal(t, 1, store.ensureCountValue())

	metricsStub := &stubMetricsRecorder{}
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 5}))
	waitForRunCount(t, stub, 1)
	waitForCondition(t, time.Second, func() bool { return store.saveCountValue() == 1 })
	lastState := store.lastSavedState()
	require.Equal(t, int64(5), lastState.LastRunHeight)
	require.Equal(t, now.UTC(), lastState.LastRunAt)
	snap := metricsStub.snapshot()
	require.Equal(t, 1, snap.completeCount)
	require.Equal(t, int64(5), snap.lastHeight)

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 5}))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, store.saveCountValue(), "duplicate height should not persist again")
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

	snapshot := metricsStub.snapshot()
	require.Equal(t, 1, snapshot.startCount)
	require.Equal(t, 1, snapshot.completeCount)
	require.Zero(t, snapshot.lastDuration)
	require.Zero(t, snapshot.lastTables)
	require.Equal(t, "nil_report", snapshot.lastMechanism)
}

func TestMaybeRunRecordsErrorOnce(t *testing.T) {
	ctx := context.Background()
	ResetForTest()

	setMechanismFactoryForTest(func() Mechanism { return &errorRunMechanism{} })
	defer resetMechanismFactory()

	svc := &common.Service{
		Logger: log.New(),
		LocalConfig: &config.Config{
			DB: config.DBConfig{DBName: "kwild_test"},
			Extensions: map[string]map[string]string{
				ExtensionName: {
					ConfigKeyEnabled:       "true",
					ConfigKeyBlockInterval: "1",
				},
			},
		},
	}

	ext := GetExtension()
	ext.setStateStore(&stubStateStore{})

	app := &common.App{Service: svc}
	require.NoError(t, engineReadyHook(ctx, app))

	metricsStub := &stubMetricsRecorder{}
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	require.NoError(t, endBlockHook(ctx, app, &common.BlockContext{Height: 1}))
	waitForCondition(t, time.Second, func() bool { return metricsStub.snapshot().errorCount == 1 })
	errorSnapshot := metricsStub.snapshot()
	require.Equal(t, 1, errorSnapshot.startCount)
	require.Equal(t, "error_run", errorSnapshot.lastErrorMechanism)
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
	mu                 sync.RWMutex
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
	s.mu.Lock()
	s.startCount++
	s.lastMechanism = mechanism
	s.mu.Unlock()
}

func (s *stubMetricsRecorder) RecordVacuumComplete(ctx context.Context, mechanism string, duration time.Duration, tablesProcessed int) {
	s.mu.Lock()
	s.completeCount++
	s.lastMechanism = mechanism
	s.lastDuration = duration
	s.lastTables = tablesProcessed
	s.mu.Unlock()
}

func (s *stubMetricsRecorder) RecordVacuumError(ctx context.Context, mechanism string, errType string) {
	s.mu.Lock()
	s.errorCount++
	s.lastErrorMechanism = mechanism
	s.lastErrorType = errType
	s.mu.Unlock()
}

func (s *stubMetricsRecorder) RecordVacuumSkipped(ctx context.Context, reason string) {
	s.mu.Lock()
	s.skippedCount++
	s.lastSkipReason = reason
	s.mu.Unlock()
}

func (s *stubMetricsRecorder) RecordLastRunHeight(ctx context.Context, height int64) {
	s.mu.Lock()
	s.lastHeight = height
	s.mu.Unlock()
}

func (s *stubMetricsRecorder) snapshot() stubMetricsSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return stubMetricsSnapshot{
		startCount:         s.startCount,
		completeCount:      s.completeCount,
		errorCount:         s.errorCount,
		skipCount:          s.skippedCount,
		lastDuration:       s.lastDuration,
		lastTables:         s.lastTables,
		lastMechanism:      s.lastMechanism,
		lastErrorMechanism: s.lastErrorMechanism,
		lastSkipReason:     s.lastSkipReason,
		lastHeight:         s.lastHeight,
	}
}

type stubMetricsSnapshot struct {
	startCount         int
	completeCount      int
	errorCount         int
	skipCount          int
	lastDuration       time.Duration
	lastTables         int
	lastMechanism      string
	lastErrorMechanism string
	lastSkipReason     string
	lastHeight         int64
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
		return stub.runCount() >= count
	})
}

// stubNodeStatus implements common.NodeStatusProvider for testing
type stubNodeStatus struct {
	mu      sync.RWMutex
	syncing bool
}

func (s *stubNodeStatus) IsSyncing() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncing
}

func (s *stubNodeStatus) setSyncing(syncing bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncing = syncing
}

// TestVacuumSkipsDuringCatchup verifies that vacuum operations are skipped
// when the node is in catch-up mode (syncing/block sync).
func TestVacuumSkipsDuringCatchup(t *testing.T) {
	ResetForTest()
	ext := GetExtension()

	// Create stub node status
	nodeStatus := &stubNodeStatus{}

	// Setup test service and mechanism
	svc := &common.Service{
		Logger: log.New(log.WithLevel(log.LevelInfo)),
		LocalConfig: &config.Config{
			DB: config.DBConfig{
				Host:   "localhost",
				Port:   "5432",
				User:   "kwild",
				Pass:   "kwild",
				DBName: "kwild",
			},
		},
		NodeStatus: nodeStatus,
	}

	stub := &stubMechanism{}
	ext.setService(svc)
	ext.setLogger(svc.Logger)
	ext.setStateStore(&stubStateStore{loadOK: false})

	// Manually set up the extension without calling configure
	// to avoid pg_repack dependency
	ext.mu.Lock()
	ext.config = Config{
		Enabled:       true,
		BlockInterval: 100,
	}
	ext.mechanism = stub
	ext.runner = &Runner{logger: svc.Logger}
	ext.mu.Unlock()

	metricsStub := &stubMetricsRecorder{}
	ext.mu.Lock()
	ext.metrics = metricsStub
	ext.mu.Unlock()

	app := &common.App{
		Service: svc,
	}

	// Test 1: Vacuum should be skipped when node is syncing
	nodeStatus.setSyncing(true)
	blockCtx := &common.BlockContext{
		Height:    1000,
		Timestamp: time.Now().Unix(),
	}

	err := endBlockHook(context.Background(), app, blockCtx)
	require.NoError(t, err)

	// Verify no vacuum was queued
	require.Equal(t, 0, stub.runCount(), "vacuum should not run during sync")

	// Verify skip metric was recorded
	snapshot := metricsStub.snapshot()
	require.Equal(t, 1, snapshot.skipCount, "skip metric should be recorded")
	require.Equal(t, "node_syncing", snapshot.lastSkipReason)

	// Test 2: Vacuum should run when node is not syncing after interval
	nodeStatus.setSyncing(false)
	blockCtx.Height = 1101 // Beyond interval of 100

	err = endBlockHook(context.Background(), app, blockCtx)
	require.NoError(t, err)

	// Wait for vacuum to be queued and processed
	waitForRunCount(t, stub, 1)
	require.Equal(t, 1, stub.runCount(), "vacuum should run when not syncing")

	// Test 3: Multiple blocks during sync should all be skipped
	nodeStatus.setSyncing(true)
	for i := int64(1102); i <= 1300; i++ {
		blockCtx.Height = i
		err = endBlockHook(context.Background(), app, blockCtx)
		require.NoError(t, err)
	}

	// Still only 1 run (the one from test 2)
	require.Equal(t, 1, stub.runCount(), "vacuum should not run during extended catch-up")

	snapshot = metricsStub.snapshot()
	require.Greater(t, snapshot.skipCount, 1, "multiple skips should be recorded")
}

// TestVacuumResumesAfterCatchup verifies that vacuum resumes normal operation
// after catch-up is complete.
func TestVacuumResumesAfterCatchup(t *testing.T) {
	ResetForTest()
	ext := GetExtension()

	// Create stub node status
	nodeStatus := &stubNodeStatus{}
	nodeStatus.setSyncing(true) // Start in syncing state

	svc := &common.Service{
		Logger: log.New(log.WithLevel(log.LevelInfo)),
		LocalConfig: &config.Config{
			DB: config.DBConfig{
				Host:   "localhost",
				Port:   "5432",
				User:   "kwild",
				Pass:   "kwild",
				DBName: "kwild",
			},
		},
		NodeStatus: nodeStatus,
	}

	stub := &stubMechanism{}
	ext.setService(svc)
	ext.setLogger(svc.Logger)
	ext.setStateStore(&stubStateStore{loadOK: false})

	// Manually set up the extension without calling configure
	// to avoid pg_repack dependency
	ext.mu.Lock()
	ext.config = Config{
		Enabled:       true,
		BlockInterval: 50,
	}
	ext.mechanism = stub
	ext.runner = &Runner{logger: svc.Logger}
	ext.mu.Unlock()

	app := &common.App{
		Service: svc,
	}

	// Simulate sync period
	ctx := context.Background()
	for i := int64(1); i <= 100; i++ {
		blockCtx := &common.BlockContext{
			Height:    i,
			Timestamp: time.Now().Unix(),
		}
		err := endBlockHook(ctx, app, blockCtx)
		require.NoError(t, err)
	}

	// No vacuum should have run during sync
	require.Equal(t, 0, stub.runCount())

	// Sync complete, resume normal operation
	nodeStatus.setSyncing(false)
	blockCtx := &common.BlockContext{
		Height:    151, // Beyond interval
		Timestamp: time.Now().Unix(),
	}
	err := endBlockHook(ctx, app, blockCtx)
	require.NoError(t, err)

	// Vacuum should now run
	waitForRunCount(t, stub, 1)
	require.Equal(t, 1, stub.runCount())

	// Continue normal operation
	blockCtx.Height = 201
	err = endBlockHook(ctx, app, blockCtx)
	require.NoError(t, err)

	waitForRunCount(t, stub, 2)
	require.Equal(t, 2, stub.runCount())
}
