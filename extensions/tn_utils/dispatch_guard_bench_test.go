//go:build kwiltest

package tn_utils

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	databasesize "github.com/trufnetwork/node/extensions/database-size"
	"github.com/trufnetwork/node/extensions/tn_attestation"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/internal/migrations"
)

var registerMigrationExtensionsOnce sync.Once

// registerMigrationExtensions registers the other extensions that the
// migrations USE, so the whole migration set can be loaded.
func registerMigrationExtensions() {
	registerForEngineTests()
	registerMigrationExtensionsOnce.Do(func() {
		registered := precompiles.RegisteredPrecompiles()
		if _, ok := registered[tn_cache.ExtensionName]; !ok {
			if err := precompiles.RegisterInitializer(tn_cache.ExtensionName, tn_cache.InitializeCachePrecompile); err != nil {
				panic(err)
			}
		}
		if _, ok := registered[databasesize.ExtensionName]; !ok {
			if err := precompiles.RegisterInitializer(databasesize.ExtensionName, databasesize.InitializeDatabaseSizePrecompile); err != nil {
				panic(err)
			}
		}
		if _, ok := registered[tn_attestation.ExtensionName]; !ok {
			tn_attestation.InitializeExtension()
		}
	})
}

// engineActionsModifiersQuery reads the modifiers of a main action straight
// from the table the engine keeps its actions in, for comparison with the
// info.actions view that the guard reads.
const engineActionsModifiersQuery = `SELECT modifiers::TEXT[] FROM kwild_engine.actions WHERE namespace = 'main' AND name = $1`

// BenchmarkCallDispatchTargetLookup measures the check call_dispatch makes
// before it runs a target, with every migration loaded, next to a direct read
// of the engine's actions table and to a call and a dispatch of a small action.
//
//	go test -tags=kwiltest -run '^$' -bench BenchmarkCallDispatchTargetLookup ./extensions/tn_utils/
func BenchmarkCallDispatchTargetLookup(b *testing.B) {
	registerMigrationExtensions()
	orderedsync.ForTestingReset()
	erc20shim.ForTestingResetSingleton()
	erc20shim.ForTestingClearAllInstances(context.Background(), nil)

	const target = "get_record"
	err := kwilTesting.SchemaTest{
		Name:           "call_dispatch_target_lookup",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				caller := "0x00000000000000000000000000000000000c0de1"
				engineCtx := &common.EngineContext{TxContext: &common.TxContext{
					Ctx:    ctx,
					Signer: []byte(caller),
					Caller: caller,
					TxID:   platform.Txid(),
					BlockContext: &common.BlockContext{
						ChainContext: &common.ChainContext{ChainID: "tn-v2.1"},
						Height:       callDispatchPublicViewHeights["tn-v2.1"],
					},
				}}
				app := &common.App{Engine: platform.Engine, DB: platform.DB}

				// Both lookups must give the same answer for every main action,
				// or comparing their cost means nothing.
				var names []string
				viewMods := map[string][]string{}
				if err := platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
					`{info}SELECT name, access_modifiers FROM actions WHERE namespace = 'main'`, nil,
					func(r *common.Row) error {
						name, ok := r.Values[0].(string)
						if !ok {
							return fmt.Errorf("action name is %T", r.Values[0])
						}
						mods, err := toStringSliceArray(r.Values[1])
						if err != nil {
							return err
						}
						names = append(names, name)
						viewMods[name] = mods
						return nil
					}); err != nil {
					return err
				}
				publicViews := 0
				for _, name := range names {
					res, err := platform.DB.Execute(ctx, engineActionsModifiersQuery, name)
					if err != nil {
						return err
					}
					if len(res.Rows) != 1 {
						return fmt.Errorf("%s: %d rows in kwild_engine.actions", name, len(res.Rows))
					}
					tableMods, err := toStringSliceArray(res.Rows[0][0])
					if err != nil {
						return err
					}
					if !slices.Equal(tableMods, viewMods[name]) {
						return fmt.Errorf("%s: info.actions has %v, kwild_engine.actions has %v", name, viewMods[name], tableMods)
					}
					if isPublicView(tableMods) {
						publicViews++
					}
				}
				b.Logf("main actions: %d, of which PUBLIC VIEW: %d; both lookups agree on all of them", len(names), publicViews)

				b.Run("info_actions_view", func(b *testing.B) {
					for b.Loop() {
						if err := requirePublicViewTarget(engineCtx, app, target); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("kwild_engine_actions", func(b *testing.B) {
					for b.Loop() {
						res, err := platform.DB.Execute(ctx, engineActionsModifiersQuery, target)
						if err != nil {
							b.Fatal(err)
						}
						if len(res.Rows) != 1 {
							b.Fatalf("expected one row, got %d", len(res.Rows))
						}
						mods, err := toStringSliceArray(res.Rows[0][0])
						if err != nil {
							b.Fatal(err)
						}
						if !isPublicView(mods) {
							b.Fatalf("%s: modifiers %v", target, mods)
						}
					}
				})

				noArgs, err := EncodeActionArgs(nil)
				if err != nil {
					return err
				}
				b.Run("call_whoami", func(b *testing.B) {
					for b.Loop() {
						if err := callForBench(engineCtx, platform, "main", "whoami"); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("dispatch_whoami", func(b *testing.B) {
					for b.Loop() {
						if err := callForBench(engineCtx, platform, "tn_utils", "call_dispatch", "whoami", noArgs); err != nil {
							b.Fatal(err)
						}
					}
				})
				return nil
			},
		},
	}.Run(context.Background(), &kwilTesting.Options{UseTestContainer: true, SetupMetaStore: true, Logger: b})
	if err != nil {
		b.Fatal(err)
	}
}

// callForBench calls an action that returns one row and checks that it did.
func callForBench(engineCtx *common.EngineContext, platform *kwilTesting.Platform, namespace, action string, args ...any) error {
	rows := 0
	res, err := platform.Engine.Call(engineCtx, platform.DB, namespace, action, args, func(*common.Row) error {
		rows++
		return nil
	})
	if err == nil && res != nil && res.Error != nil {
		err = res.Error
	}
	if err == nil && rows != 1 {
		err = fmt.Errorf("%s.%s: expected one row, got %d", namespace, action, rows)
	}
	return err
}
