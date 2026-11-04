//go:build kwiltest

package tn_utils

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

var registerOnce sync.Once

// registerForEngineTests registers this package's precompiles once per test
// binary, so the engine can run `USE tn_utils`.
func registerForEngineTests() {
	registerOnce.Do(func() {
		if _, ok := precompiles.RegisteredPrecompiles()[ExtensionName]; !ok {
			InitializeExtension()
		}
	})
}

// dispatchProbeSchema gives call_dispatch one target of each kind. Every write
// lands in dispatch_probe, so a refused call shows up as a missing row.
const dispatchProbeSchema = `
USE tn_utils AS tn_utils;

CREATE TABLE dispatch_probe (
    id INT8 PRIMARY KEY,
    source TEXT NOT NULL
);

CREATE OR REPLACE ACTION probe_private_write($id INT8) PRIVATE {
    INSERT INTO dispatch_probe (id, source) VALUES ($id, 'private');
};

CREATE OR REPLACE ACTION probe_public_write($id INT8) PUBLIC {
    INSERT INTO dispatch_probe (id, source) VALUES ($id, 'public');
};

CREATE OR REPLACE ACTION probe_count($id INT8) PUBLIC VIEW RETURNS (n INT8) {
    $n INT8 := 0;
    for $row in SELECT COUNT(*) AS c FROM dispatch_probe WHERE id = $id {
        $n := $row.c;
    }
    RETURN $n;
};

-- Writes, then dispatches from inside the action, as request_attestation does.
CREATE OR REPLACE ACTION probe_write_then_dispatch($id INT8, $action TEXT, $args BYTEA) PUBLIC {
    INSERT INTO dispatch_probe (id, source) VALUES ($id, 'caller');
    $result := tn_utils.call_dispatch($action, $args);
};
`

// probeBlock runs transactions the way a block does: each one inside its own
// savepoint, kept only when the call succeeds, and the next one runs whatever
// happened to the one before.
type probeBlock struct {
	ctx      context.Context
	platform *kwilTesting.Platform
	chainID  string
	height   int64
}

func (b *probeBlock) execTx(namespace, action string, args ...any) ([]*common.Row, error) {
	tx, err := b.platform.DB.BeginTx(b.ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(b.ctx) }() // no-op once committed

	caller := "0x00000000000000000000000000000000000c0de1"
	engineCtx := &common.EngineContext{TxContext: &common.TxContext{
		Ctx:    b.ctx,
		Signer: []byte(caller),
		Caller: caller,
		TxID:   b.platform.Txid(),
		BlockContext: &common.BlockContext{
			ChainContext: &common.ChainContext{ChainID: b.chainID},
			Height:       b.height,
		},
	}}

	var rows []*common.Row
	res, err := b.platform.Engine.Call(engineCtx, tx, namespace, action, args, func(r *common.Row) error {
		rows = append(rows, &common.Row{
			ColumnNames: append([]string(nil), r.ColumnNames...),
			Values:      append([]any(nil), r.Values...),
		})
		return nil
	})
	if err == nil && res != nil && res.Error != nil {
		err = res.Error
	}
	if err != nil {
		return nil, err
	}
	return rows, tx.Commit(b.ctx)
}

func TestCallDispatchRefusesInsideABlock(t *testing.T) {
	registerForEngineTests()

	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "call_dispatch_refuses_inside_a_block",
		SeedStatements: []string{dispatchProbeSchema},
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// A mainnet block at the height the rule applies from.
				b := &probeBlock{ctx: ctx, platform: platform, chainID: "tn-v2.1", height: callDispatchPublicViewHeights["tn-v2.1"]}

				args := map[int64][]byte{}
				for _, id := range []int64{1, 2, 4, 5, 6} {
					bts, err := EncodeActionArgs([]any{id})
					if err != nil {
						return err
					}
					args[id] = bts
				}
				refused := func(step string, err error, target string) error {
					want := fmt.Sprintf("call_dispatch: action '%s' is not PUBLIC VIEW", target)
					if err == nil {
						return fmt.Errorf("%s: expected the call to be refused", step)
					}
					if !strings.Contains(err.Error(), want) {
						return fmt.Errorf("%s: expected %q, got %v", step, want, err)
					}
					return nil
				}

				// A wallet dispatches straight to an action that is not PUBLIC VIEW.
				_, err := b.execTx("tn_utils", "call_dispatch", "probe_private_write", args[1])
				if err := refused("private target", err, "probe_private_write"); err != nil {
					return err
				}
				_, err = b.execTx("tn_utils", "call_dispatch", "probe_public_write", args[2])
				if err := refused("public target that is not a view", err, "probe_public_write"); err != nil {
					return err
				}

				// An action writes, then dispatches to a private action. The
				// transaction fails as a whole, its own write included.
				_, err = b.execTx("main", "probe_write_then_dispatch", int64(3), "probe_private_write", args[4])
				if err := refused("dispatch from inside an action", err, "probe_private_write"); err != nil {
					return err
				}

				// The block goes on: later transactions run and keep their writes.
				if _, err := b.execTx("main", "probe_public_write", int64(5)); err != nil {
					return fmt.Errorf("write after the refusals: %w", err)
				}
				rows, err := b.execTx("tn_utils", "call_dispatch", "probe_count", args[5])
				if err != nil {
					return fmt.Errorf("dispatch to a PUBLIC VIEW action: %w", err)
				}
				if len(rows) != 1 || len(rows[0].Values) != 1 {
					return fmt.Errorf("dispatch to a PUBLIC VIEW action: expected one result row, got %v", rows)
				}
				resultBytes, ok := rows[0].Values[0].([]byte)
				if !ok {
					return fmt.Errorf("dispatch result is %T, not bytes", rows[0].Values[0])
				}
				result, err := DecodeQueryResultCanonical(resultBytes)
				if err != nil {
					return fmt.Errorf("decode dispatch result: %w", err)
				}
				if len(result) != 1 || len(result[0].Values) != 1 {
					return fmt.Errorf("dispatched probe_count(5): expected one row with one value, got %d rows", len(result))
				}
				count := result[0].Values[0]
				if p, ok := count.(*int64); ok && p != nil {
					count = *p
				}
				if count != int64(1) {
					return fmt.Errorf("dispatched probe_count(5): expected 1, got %#v", result[0].Values[0])
				}
				if _, err := b.execTx("main", "probe_write_then_dispatch", int64(6), "probe_count", args[6]); err != nil {
					return fmt.Errorf("dispatch to a PUBLIC VIEW action from inside an action: %w", err)
				}

				// Only the transactions that succeeded left rows behind.
				got := map[int64]string{}
				err = platform.Engine.Execute(&common.EngineContext{
					TxContext: &common.TxContext{
						Ctx:          ctx,
						Signer:       platform.Deployer,
						Caller:       string(platform.Deployer),
						TxID:         platform.Txid(),
						BlockContext: &common.BlockContext{Height: b.height},
					},
					OverrideAuthz: true,
				}, platform.DB, "SELECT id, source FROM dispatch_probe ORDER BY id", nil, func(r *common.Row) error {
					got[r.Values[0].(int64)] = r.Values[1].(string)
					return nil
				})
				if err != nil {
					return fmt.Errorf("read dispatch_probe: %w", err)
				}
				want := map[int64]string{5: "public", 6: "caller"}
				if !reflect.DeepEqual(got, want) {
					return fmt.Errorf("dispatch_probe rows: got %v, want %v", got, want)
				}
				return nil
			},
		},
	}, &kwilTesting.Options{UseTestContainer: true, Logger: t})
}
