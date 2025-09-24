package leaderwatch

import (
	"context"
	"fmt"
	"sync"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/extensions/hooks"
)

type Callbacks struct {
	OnAcquire  func(ctx context.Context, app *common.App, block *common.BlockContext)
	OnLose     func(ctx context.Context, app *common.App, block *common.BlockContext)
	OnEndBlock func(ctx context.Context, app *common.App, block *common.BlockContext)
}

type watcher struct {
	callbacks Callbacks
	isLeader  bool
}

type extension struct {
	mu       sync.RWMutex
	logger   log.Logger
	service  *common.Service
	watchers map[string]*watcher
	order    []string
}

var (
	extOnce sync.Once
	extInst *extension
)

func getExtension() *extension {
	extOnce.Do(func() {
		extInst = &extension{
			logger:   log.New(log.WithLevel(log.LevelInfo)).New(ExtensionName),
			watchers: make(map[string]*watcher),
		}
	})
	return extInst
}

func InitializeExtension() {
	if err := hooks.RegisterEngineReadyHook(ExtensionName+"_engine_ready", engineReadyHook); err != nil {
		panic(fmt.Sprintf("failed to register %s engine ready hook: %v", ExtensionName, err))
	}
	if err := hooks.RegisterEndBlockHook(ExtensionName+"_end_block", endBlockHook); err != nil {
		panic(fmt.Sprintf("failed to register %s end block hook: %v", ExtensionName, err))
	}
}

func engineReadyHook(ctx context.Context, app *common.App) error {
	ext := getExtension()
	if app != nil && app.Service != nil {
		ext.mu.Lock()
		ext.logger = app.Service.Logger.New(ExtensionName)
		ext.service = app.Service
		ext.mu.Unlock()
	}
	return nil
}

func endBlockHook(ctx context.Context, app *common.App, block *common.BlockContext) error {
	ext := getExtension()

	isLeader := determineLeader(app, block)

	ext.mu.Lock()
	var svc *common.Service
	var logger log.Logger
	if app != nil {
		svc = app.Service
	}
	if svc != nil {
		logger = svc.Logger.New(ExtensionName)
	} else {
		logger = ext.logger
	}
	ext.service = svc
	ext.logger = logger

	updates := make([]struct {
		callbacks Callbacks
		change    int
	}, 0, len(ext.order))

	for _, name := range ext.order {
		w, ok := ext.watchers[name]
		if !ok {
			continue
		}
		change := 0
		if w.isLeader != isLeader {
			w.isLeader = isLeader
			if isLeader {
				change = 1
			} else {
				change = -1
			}
		}
		updates = append(updates, struct {
			callbacks Callbacks
			change    int
		}{callbacks: w.callbacks, change: change})
	}
	callbacks := make([]Callbacks, len(updates))
	changes := make([]int, len(updates))
	for i, u := range updates {
		callbacks[i] = u.callbacks
		changes[i] = u.change
	}
	ext.mu.Unlock()

	for i, cb := range callbacks {
		switch changes[i] {
		case 1:
			if cb.OnAcquire != nil {
				cb.OnAcquire(ctx, app, block)
			}
		case -1:
			if cb.OnLose != nil {
				cb.OnLose(ctx, app, block)
			}
		}
		if cb.OnEndBlock != nil {
			cb.OnEndBlock(ctx, app, block)
		}
	}

	return nil
}

func determineLeader(app *common.App, block *common.BlockContext) bool {
	if app == nil || app.Service == nil || block == nil || block.ChainContext == nil || block.ChainContext.NetworkParameters == nil {
		return false
	}
	nodeID := app.Service.Identity
	leader := block.ChainContext.NetworkParameters.Leader
	if len(nodeID) == 0 || leader.PublicKey == nil {
		return false
	}
	return string(nodeID) == string(leader.PublicKey.Bytes())
}

func Register(name string, callbacks Callbacks) error {
	ext := getExtension()
	ext.mu.Lock()
	defer ext.mu.Unlock()
	if name == "" {
		return fmt.Errorf("leaderwatch: name cannot be empty")
	}
	if _, exists := ext.watchers[name]; exists {
		return fmt.Errorf("leaderwatch: watcher %q already registered", name)
	}
	ext.watchers[name] = &watcher{callbacks: callbacks}
	ext.order = append(ext.order, name)
	return nil
}

func ResetForTest() {
	ext := getExtension()
	ext.mu.Lock()
	ext.watchers = make(map[string]*watcher)
	ext.order = nil
	ext.mu.Unlock()
}
