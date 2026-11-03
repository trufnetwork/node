package tn_utils

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// guardEngine answers the modifier lookup from a map and records whether the
// dispatched action was called.
type guardEngine struct {
	common.Engine
	modifiers map[string][]string // action name -> modifiers; absent = no such action
	lookupErr error
	lookups   []string
	called    []string
}

func (g *guardEngine) ExecuteWithoutEngineCtx(_ context.Context, _ sql.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	if statement != callDispatchModifiersQuery {
		return errors.New("unexpected statement: " + statement)
	}
	if g.lookupErr != nil {
		return g.lookupErr
	}
	name, _ := params["name"].(string)
	g.lookups = append(g.lookups, name)
	mods, ok := g.modifiers[name]
	if !ok {
		return nil
	}
	ptrs := make([]*string, len(mods))
	for i := range mods {
		ptrs[i] = &mods[i]
	}
	return fn(&common.Row{ColumnNames: []string{"access_modifiers"}, Values: []any{ptrs}})
}

func (g *guardEngine) Call(_ *common.EngineContext, _ sql.DB, namespace, action string, _ []any, _ func(*common.Row) error) (*common.CallResult, error) {
	g.called = append(g.called, namespace+"."+action)
	return &common.CallResult{}, nil
}

func blockCtx(chainID string, height int64) *common.EngineContext {
	return &common.EngineContext{TxContext: &common.TxContext{
		Ctx: context.Background(),
		BlockContext: &common.BlockContext{
			ChainContext: &common.ChainContext{ChainID: chainID},
			Height:       height,
		},
	}}
}

func dispatch(t *testing.T, ctx *common.EngineContext, eng *guardEngine, action string) error {
	t.Helper()
	argsBytes, err := EncodeActionArgs([]any{"a", int64(1)})
	if err != nil {
		t.Fatalf("encode args: %v", err)
	}
	return callDispatchHandler(ctx, &common.App{Engine: eng}, []any{action, argsBytes}, func([]any) error { return nil })
}

func TestCallDispatchRefusesNonPublicViewTargets(t *testing.T) {
	eng := &guardEngine{modifiers: map[string][]string{
		"private_action": {"PRIVATE"},
		"public_action":  {"PUBLIC"},
		"system_view":    {"SYSTEM", "VIEW"},
		"owner_view":     {"PUBLIC", "OWNER", "VIEW"},
		"private_view":   {"PRIVATE", "VIEW"},
		"get_record":     {"PUBLIC", "VIEW"},
	}}
	ctx := blockCtx("some-chain", 1)

	for _, action := range []string{"private_action", "public_action", "system_view", "owner_view", "private_view", "no_such_action"} {
		err := dispatch(t, ctx, eng, action)
		if err == nil {
			t.Fatalf("%s: expected refusal", action)
		}
		if !strings.Contains(err.Error(), action) {
			t.Fatalf("%s: error does not name the action: %v", action, err)
		}
	}
	if len(eng.called) != 0 {
		t.Fatalf("refused targets must not run, ran %v", eng.called)
	}

	if err := dispatch(t, ctx, eng, "get_record"); err != nil {
		t.Fatalf("get_record: %v", err)
	}
	if len(eng.called) != 1 || eng.called[0] != "main.get_record" {
		t.Fatalf("expected main.get_record to run, ran %v", eng.called)
	}
}

func TestCallDispatchLooksUpLowerCaseName(t *testing.T) {
	eng := &guardEngine{modifiers: map[string][]string{"private_action": {"PRIVATE"}}}
	if err := dispatch(t, blockCtx("some-chain", 1), eng, "Private_Action"); err == nil {
		t.Fatal("expected refusal for a mixed-case private action")
	}
	if len(eng.lookups) != 1 || eng.lookups[0] != "private_action" {
		t.Fatalf("expected a lower-case lookup, got %v", eng.lookups)
	}
	if len(eng.called) != 0 {
		t.Fatalf("refused target ran: %v", eng.called)
	}
}

func TestCallDispatchFailsClosedWhenLookupFails(t *testing.T) {
	eng := &guardEngine{lookupErr: errors.New("boom")}
	if err := dispatch(t, blockCtx("some-chain", 1), eng, "get_record"); err == nil {
		t.Fatal("expected an error when the modifier lookup fails")
	}
	if len(eng.called) != 0 {
		t.Fatalf("target ran after a failed lookup: %v", eng.called)
	}
}

func TestCallDispatchPublicViewHeights(t *testing.T) {
	mainnet := callDispatchPublicViewHeights["tn-v2.1"]
	if mainnet != 3010040 {
		t.Fatalf("mainnet height changed: %d", mainnet)
	}
	cases := []struct {
		name string
		ctx  *common.EngineContext
		want bool
	}{
		{"nil context", nil, true},
		{"no tx context", &common.EngineContext{}, true},
		{"no block context", &common.EngineContext{TxContext: &common.TxContext{}}, true},
		{"no chain context", &common.EngineContext{TxContext: &common.TxContext{BlockContext: &common.BlockContext{Height: 5}}}, true},
		{"unlisted chain", blockCtx("local-dev", 1), true},
		{"mainnet before", blockCtx("tn-v2.1", mainnet-1), false},
		{"mainnet at", blockCtx("tn-v2.1", mainnet), true},
		{"mainnet after", blockCtx("tn-v2.1", mainnet+1), true},
		{"testnet unscheduled", blockCtx("testnet-v1", math.MaxInt64-1), false},
	}
	for _, c := range cases {
		if got := dispatchRequiresPublicView(c.ctx); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCallDispatchKeepsOldBehaviourBelowHeight(t *testing.T) {
	// Below the mainnet height the target is not checked, so replaying old
	// blocks reaches the state they produced.
	mainnet := callDispatchPublicViewHeights["tn-v2.1"]
	eng := &guardEngine{modifiers: map[string][]string{"private_action": {"PRIVATE"}}}
	if err := dispatch(t, blockCtx("tn-v2.1", mainnet-1), eng, "private_action"); err != nil {
		t.Fatalf("below the height: %v", err)
	}
	if len(eng.lookups) != 0 {
		t.Fatalf("no lookup expected below the height, got %v", eng.lookups)
	}
	if len(eng.called) != 1 {
		t.Fatalf("expected the old behaviour below the height, ran %v", eng.called)
	}

	eng2 := &guardEngine{modifiers: map[string][]string{"private_action": {"PRIVATE"}}}
	if err := dispatch(t, blockCtx("tn-v2.1", mainnet), eng2, "private_action"); err == nil {
		t.Fatal("expected refusal at the height")
	}
	if len(eng2.called) != 0 {
		t.Fatalf("refused target ran at the height: %v", eng2.called)
	}
}

func TestIsPublicView(t *testing.T) {
	cases := map[string]struct {
		mods []string
		want bool
	}{
		"public view":       {[]string{"PUBLIC", "VIEW"}, true},
		"lower case":        {[]string{"public", "view"}, true},
		"public only":       {[]string{"PUBLIC"}, false},
		"view only":         {[]string{"VIEW"}, false},
		"private":           {[]string{"PRIVATE"}, false},
		"private view":      {[]string{"PRIVATE", "VIEW"}, false},
		"system view":       {[]string{"SYSTEM", "VIEW"}, false},
		"public owner view": {[]string{"PUBLIC", "OWNER", "VIEW"}, false},
		"empty":             {nil, false},
	}
	for name, c := range cases {
		if got := isPublicView(c.mods); got != c.want {
			t.Errorf("%s: got %v, want %v", name, got, c.want)
		}
	}
}
