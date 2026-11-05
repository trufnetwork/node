package tn_utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
)

// call_dispatch exists so that request_attestation can run read-only queries,
// which are PUBLIC VIEW actions in the main namespace. From the heights below it
// runs only such actions.
//
// Blocks below a chain's height keep the behaviour they were executed with, so a
// node that replays old blocks reaches the same state. A chain that is not
// listed applies the rule at every height.
var callDispatchPublicViewHeights = map[string]int64{
	"tn-v2.1":    3010040,  // mainnet
	"testnet-v1": 15500000, // testnet
}

// callDispatchModifiersQuery reads the access modifiers of a main action.
// Action names are stored in lower case, as the engine resolves them.
const callDispatchModifiersQuery = `{info}SELECT access_modifiers FROM actions WHERE namespace = 'main' AND name = $name`

// dispatchRequiresPublicView reports whether call_dispatch must check that its
// target is PUBLIC VIEW. Calls outside a block, such as read-only RPC calls,
// are checked as well.
func dispatchRequiresPublicView(ctx *common.EngineContext) bool {
	if ctx == nil || ctx.TxContext == nil || ctx.TxContext.BlockContext == nil {
		return true
	}
	block := ctx.TxContext.BlockContext
	if block.ChainContext == nil {
		return true
	}
	from, ok := callDispatchPublicViewHeights[block.ChainContext.ChainID]
	if !ok {
		return true
	}
	return block.Height >= from
}

// requirePublicViewTarget returns an error unless main.<actionName> exists and
// its modifiers include both PUBLIC and VIEW.
func requirePublicViewTarget(ctx *common.EngineContext, app *common.App, actionName string) error {
	if app == nil || app.Engine == nil {
		return fmt.Errorf("call_dispatch: no engine to check action '%s'", actionName)
	}

	goCtx := context.Background()
	if ctx != nil && ctx.TxContext != nil && ctx.TxContext.Ctx != nil {
		goCtx = ctx.TxContext.Ctx
	}

	found := false
	var modifiers []string
	err := app.Engine.ExecuteWithoutEngineCtx(goCtx, app.DB, callDispatchModifiersQuery,
		map[string]any{"name": strings.ToLower(actionName)},
		func(row *common.Row) error {
			if found {
				return fmt.Errorf("call_dispatch: more than one action named '%s'", actionName)
			}
			found = true
			if len(row.Values) != 1 {
				return fmt.Errorf("call_dispatch: unexpected modifier row for action '%s'", actionName)
			}
			mods, err := toStringSliceArray(row.Values[0])
			if err != nil {
				return fmt.Errorf("call_dispatch: modifiers of action '%s': %w", actionName, err)
			}
			modifiers = mods
			return nil
		})
	if err != nil {
		return fmt.Errorf("call_dispatch: cannot check action '%s': %w", actionName, err)
	}
	if !found {
		return fmt.Errorf("call_dispatch: action '%s' does not exist", actionName)
	}
	if !isPublicView(modifiers) {
		return fmt.Errorf("call_dispatch: action '%s' is not PUBLIC VIEW", actionName)
	}
	return nil
}

// isPublicView reports whether the modifiers include PUBLIC and VIEW and none
// of PRIVATE, SYSTEM or OWNER.
func isPublicView(modifiers []string) bool {
	public, view := false, false
	for _, m := range modifiers {
		switch strings.ToUpper(strings.TrimSpace(m)) {
		case "PUBLIC":
			public = true
		case "VIEW":
			view = true
		case "PRIVATE", "SYSTEM", "OWNER":
			return false
		}
	}
	return public && view
}
