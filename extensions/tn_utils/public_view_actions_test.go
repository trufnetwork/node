package tn_utils

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/trufnetwork/kwil-db/node/engine/parse"
	"github.com/trufnetwork/node/internal/migrations"
)

// call_dispatch runs only PUBLIC VIEW actions in the main namespace
// (dispatch_guard.go). The engine does not check that the body of a VIEW action
// only reads, so the tests below keep every PUBLIC VIEW action in the
// migrations, and every action it calls, free of INSERT, UPDATE, DELETE and
// token methods.

// tokenMethods are the erc20 bridge methods that change balances or bridge
// state. A bridge alias exposes them as <alias>.<method>, for example
// hoodi_tt.transfer. The read methods (balance, info, decimals, ...) are not
// listed.
var tokenMethods = map[string]bool{
	"issue":      true,
	"transfer":   true,
	"lock":       true,
	"lock_admin": true,
	"unlock":     true,
	"bridge":     true,
	"vote_epoch": true,
}

// readOnlyExceptions lists actions whose own writes and token calls are allowed
// although a PUBLIC VIEW action runs them, keyed by action name, with the
// reason. The actions they call are still checked. Add an entry only when the
// write is harmless whoever runs it.
var readOnlyExceptions = map[string]string{}

// actionDef is one CREATE ACTION statement found in a migration file.
type actionDef struct {
	file      string
	line      int
	namespace string
	name      string
	modifiers []string
	writes    []string // "INSERT at line 12"
	calls     []actionCall
}

// actionCall is a call to an action, a function or an extension method.
type actionCall struct {
	namespace string // empty when the call is not qualified
	name      string
	line      int
}

func (d *actionDef) where() string { return fmt.Sprintf("%s:%d", d.file, d.line) }

func TestPublicViewActionsAreReadOnly(t *testing.T) {
	// The files the test harness loads, plus the *.prod.sql files that replace
	// some of their actions on mainnet.
	paths := migrations.GetSeedScriptPaths()
	dirs := map[string]bool{}
	for _, p := range paths {
		dirs[filepath.Dir(p)] = true
	}
	for dir := range dirs {
		prod, err := filepath.Glob(filepath.Join(dir, "*.prod.sql"))
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, prod...)
	}
	sort.Strings(paths)

	// Name files from the repository root, e.g. internal/migrations/022-whoami.sql.
	base := ""
	for dir := range dirs {
		if base == "" || len(dir) < len(base) {
			base = dir
		}
	}
	root := filepath.Dir(filepath.Dir(base))
	var defs []*actionDef
	for _, p := range paths {
		src, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		name, err := filepath.Rel(root, p)
		if err != nil {
			name = p
		}
		fileDefs, err := parseActionDefs(filepath.ToSlash(name), string(src))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defs = append(defs, fileDefs...)
	}

	// The migrations hold well over these counts. Fewer means the walk below
	// stopped seeing actions, writes or token calls, and the check would pass
	// without looking.
	publicViews, writers, tokenCalls := 0, 0, 0
	for _, d := range defs {
		if d.namespace == "main" && isPublicView(d.modifiers) {
			publicViews++
		}
		if len(d.writes) > 0 {
			writers++
		}
		for _, c := range d.calls {
			if c.namespace != "" && tokenMethods[c.name] {
				tokenCalls++
			}
		}
	}
	if publicViews < 50 || writers < 20 || tokenCalls < 10 {
		t.Fatalf("found only %d PUBLIC VIEW actions, %d actions that write and %d token method calls in %d files",
			publicViews, writers, tokenCalls, len(paths))
	}

	problems := readOnlyProblems(defs, readOnlyExceptions)
	for _, p := range problems {
		t.Error(p)
	}
	if len(problems) > 0 {
		t.Log("PUBLIC VIEW actions and the actions they call must only read: move the write or token call " +
			"into an action that is not PUBLIC VIEW, or list a harmless write in readOnlyExceptions")
	}
}

func TestReadOnlyCheckFindsWrites(t *testing.T) {
	const src = `
CREATE TABLE probe (id INT8 PRIMARY KEY);

CREATE OR REPLACE ACTION reads() PUBLIC VIEW RETURNS (n INT8) {
    $n INT8 := 0;
    FOR $row IN SELECT COUNT(*) AS c FROM probe {
        $n := $row.c;
    }
    RETURN $n;
};

CREATE OR REPLACE ACTION reads_through_helper() PUBLIC VIEW {
    reads();
    $b := some_bridge.balance('0x0000000000000000000000000000000000000001');
};

CREATE OR REPLACE ACTION inserts() PUBLIC VIEW {
    INSERT INTO probe (id) VALUES (1);
};

CREATE OR REPLACE ACTION private_update() PRIVATE {
    UPDATE probe SET id = 2 WHERE id = 1;
};

CREATE OR REPLACE ACTION calls_private_update() PUBLIC VIEW {
    private_update();
};

CREATE OR REPLACE ACTION deletes_in_branch($x INT8) PUBLIC VIEW {
    IF $x > 0 {
        FOR $row IN SELECT id FROM probe {
            DELETE FROM probe WHERE id = $row.id;
        }
    }
};

CREATE OR REPLACE ACTION moves_tokens() VIEW PUBLIC {
    some_bridge.unlock('0x0000000000000000000000000000000000000001', 1::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION writes_but_not_a_view() PUBLIC {
    INSERT INTO probe (id) VALUES (3);
};

CREATE OR REPLACE ACTION owner_view_writes() PUBLIC OWNER VIEW {
    INSERT INTO probe (id) VALUES (4);
};

CREATE OR REPLACE ACTION allowed_writer() PRIVATE {
    INSERT INTO probe (id) VALUES (5);
};

CREATE OR REPLACE ACTION calls_allowed_writer() PUBLIC VIEW {
    allowed_writer();
};

CREATE OR REPLACE ACTION allowed_view_writer() PUBLIC VIEW {
    INSERT INTO probe (id) VALUES (6);
};

CREATE OR REPLACE ACTION allowed_writer_calls_update() PRIVATE {
    INSERT INTO probe (id) VALUES (7);
    private_update();
};

CREATE OR REPLACE ACTION calls_allowed_writer_calls_update() PUBLIC VIEW {
    allowed_writer_calls_update();
};
`
	defs, err := parseActionDefs("probe.sql", src)
	if err != nil {
		t.Fatal(err)
	}
	problems := readOnlyProblems(defs, map[string]string{
		"allowed_writer":              "test exception",
		"allowed_view_writer":         "test exception",
		"allowed_writer_calls_update": "test exception",
	})

	flagged := map[string]bool{}
	for _, p := range problems {
		flagged[strings.Fields(p)[0]] = true
	}
	for _, name := range []string{"inserts", "calls_private_update", "deletes_in_branch", "moves_tokens", "calls_allowed_writer_calls_update"} {
		if !flagged[name] {
			t.Errorf("%s was not flagged; problems: %v", name, problems)
		}
	}
	for _, name := range []string{"reads", "reads_through_helper", "writes_but_not_a_view", "owner_view_writes", "calls_allowed_writer", "allowed_view_writer", "private_update", "allowed_writer"} {
		if flagged[name] {
			t.Errorf("%s was flagged; problems: %v", name, problems)
		}
	}
	// calls_allowed_writer_calls_update is flagged for the UPDATE in
	// private_update only, not for the excepted INSERT on its way there.
	for _, p := range problems {
		if strings.HasPrefix(p, "calls_allowed_writer_calls_update ") && !strings.Contains(p, "runs UPDATE") {
			t.Errorf("unexpected problem: %s", p)
		}
	}
	if len(problems) != 5 {
		t.Errorf("expected 5 problems, got %d: %v", len(problems), problems)
	}
}

// parseActionDefs parses one migration file and returns its actions, with the
// writes and calls found anywhere in each action's body.
func parseActionDefs(file, src string) ([]*actionDef, error) {
	stmts, err := parse.Parse(src)
	if err != nil {
		return nil, err
	}
	var defs []*actionDef
	namespace := "main" // each file starts in the default namespace
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *parse.SetCurrentNamespaceStatement:
			namespace = strings.ToLower(s.Namespace)
		case *parse.CreateActionStatement:
			d := &actionDef{
				file:      file,
				line:      lineOf(s.Position),
				namespace: namespace,
				name:      strings.ToLower(s.Name),
				modifiers: s.Modifiers,
			}
			if s.NamespacePrefix != "" {
				d.namespace = strings.ToLower(s.NamespacePrefix)
			}
			walkNodes(reflect.ValueOf(s.Statements), func(n any) {
				switch n := n.(type) {
				case *parse.InsertStatement:
					d.writes = append(d.writes, fmt.Sprintf("INSERT at line %d", lineOf(n.Position)))
				case *parse.UpdateStatement:
					d.writes = append(d.writes, fmt.Sprintf("UPDATE at line %d", lineOf(n.Position)))
				case *parse.DeleteStatement:
					d.writes = append(d.writes, fmt.Sprintf("DELETE at line %d", lineOf(n.Position)))
				case *parse.ExpressionFunctionCall:
					d.calls = append(d.calls, actionCall{
						namespace: strings.ToLower(n.Namespace),
						name:      strings.ToLower(n.Name),
						line:      lineOf(n.Position),
					})
				}
			})
			defs = append(defs, d)
		}
	}
	return defs, nil
}

// readOnlyProblems follows every PUBLIC VIEW action in main through the
// actions it calls and reports each write or token method it can reach. An
// action defined in more than one file (for example a *.prod.sql override) is
// followed through every definition.
func readOnlyProblems(defs []*actionDef, exceptions map[string]string) []string {
	byKey := map[string][]*actionDef{}
	for _, d := range defs {
		k := d.namespace + "." + d.name
		byKey[k] = append(byKey[k], d)
	}

	seenProblem := map[string]bool{}
	var problems []string
	report := func(p string) {
		if !seenProblem[p] {
			seenProblem[p] = true
			problems = append(problems, p)
		}
	}

	for _, root := range defs {
		if root.namespace != "main" || !isPublicView(root.modifiers) {
			continue
		}
		visited := map[*actionDef]bool{}
		var visit func(d *actionDef, path []string)
		visit = func(d *actionDef, path []string) {
			if visited[d] {
				return
			}
			visited[d] = true
			_, allowed := exceptions[d.name]
			via := ""
			if d != root {
				via = fmt.Sprintf(" through %s (%s)", strings.Join(path[1:], " -> "), d.where())
			}
			if !allowed {
				for _, w := range d.writes {
					report(fmt.Sprintf("%s (%s) runs %s%s", root.name, root.where(), w, via))
				}
			}
			for _, c := range d.calls {
				ns := c.namespace
				if ns == "" {
					ns = d.namespace
				}
				if callees, ok := byKey[ns+"."+c.name]; ok {
					next := append(append([]string(nil), path...), c.name)
					for _, callee := range callees {
						visit(callee, next)
					}
					continue
				}
				if !allowed && c.namespace != "" && c.namespace != d.namespace && tokenMethods[c.name] {
					report(fmt.Sprintf("%s (%s) calls %s.%s at line %d%s", root.name, root.where(), c.namespace, c.name, c.line, via))
				}
			}
		}
		visit(root, []string{root.name})
	}
	sort.Strings(problems)
	return problems
}

// walkNodes calls fn for every pointer reachable from v through exported
// fields, slices and interfaces. The parse tree has no cycles.
func walkNodes(v reflect.Value, fn func(any)) {
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			walkNodes(v.Elem(), fn)
		}
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		fn(v.Interface())
		walkNodes(v.Elem(), fn)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkNodes(v.Field(i), fn)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			walkNodes(v.Index(i), fn)
		}
	}
}

func lineOf(p parse.Position) int {
	if p.StartLine == nil {
		return 0
	}
	return *p.StartLine
}
