// package stream is an extension for the Truflation stream primitive.
// It allows data to be pulled from valid streams.
package stream

import (
	"errors"
	"fmt"
	"github.com/kwilteam/kwil-db/cmd/kwil-cli/config"
	"github.com/kwilteam/kwil-db/core/utils"
	"strings"

	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/truflation/tsn"
)

// InitializeStream initializes the stream extension.
// It takes no configs.
func InitializeStream(ctx *execution.DeploymentContext, metadata map[string]string) (execution.ExtensionNamespace, error) {
	if len(metadata) != 0 {
		return nil, errors.New("stream does not take any configs")
	}

	conf, err := config.LoadCliConfig()
	if err != nil {
		return nil, errors.New("failed to load config")
	}

	return &Stream{
		accountID: conf.Identity(),
	}, nil
}

// Stream is the namespace for the stream extension.
// Stream has two methods: "index" and "value".
// Both of them get the value of the target stream at the given time.
type Stream struct {
	accountID []byte
}

func (s *Stream) Call(scoper *execution.ProcedureContext, method string, inputs []any) ([]any, error) {
	switch strings.ToLower(method) {
	case string(knownMethodIndex):
		// do nothing
	case string(knownMethodValue):
		// do nothing
	default:
		return nil, fmt.Errorf("unknown method '%s'", method)
	}

	if len(inputs) != 2 {
		return nil, fmt.Errorf("expected 2 inputs, got %d", len(inputs))
	}

	name, ok := inputs[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[0])
	}

	date, ok := inputs[1].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[1])
	}

	if !tsn.IsValidDate(date) {
		return nil, fmt.Errorf("invalid date: %s", date)
	}

	dataset, err := scoper.Dataset(utils.GenerateDBID(name, s.accountID))
	if err != nil {
		return nil, err
	}

	// the stream protocol returns results as relations
	// we need to create a new scope to get the result
	newScope := scoper.NewScope()
	_, err = dataset.Call(newScope, method, []any{date})
	if err != nil {
		return nil, err
	}

	if newScope.Result == nil {
		return nil, fmt.Errorf("stream returned nil result")
	}

	if len(newScope.Result.Rows) != 1 {
		return nil, fmt.Errorf("stream returned %d results, expected 1", len(newScope.Result.Rows))
	}
	if len(newScope.Result.Rows[0]) != 1 {
		return nil, fmt.Errorf("stream returned %d columns, expected 1", len(newScope.Result.Rows[0]))
	}

	val, ok := newScope.Result.Rows[0][0].(int64)
	if !ok {
		return nil, fmt.Errorf("stream returned %T, expected int64", newScope.Result.Rows[0][0])
	}

	return []any{val}, nil
}

type knownMethod string

const (
	knownMethodIndex knownMethod = "get_index"
	knownMethodValue knownMethod = "get_value"
)
