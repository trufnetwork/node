// package stream is an extension for the Truflation stream primitive.
// It allows data to be pulled from valid streams.
package stream

import (
	"errors"
	"fmt"
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

	return &Stream{}, nil
}

// Stream is the namespace for the stream extension.
// Stream has two methods: "index" and "value".
// Both of them get the value of the target stream at the given time.
type Stream struct{}

func (s *Stream) Call(scoper *execution.ProcedureContext, method string, inputs []any) ([]any, error) {
	switch strings.ToLower(method) {
	case string(knownMethodIndex):
		// do nothing
	case string(knownMethodValue):
		// do nothing
	default:
		return nil, fmt.Errorf("unknown method '%s'", method)
	}

	if len(inputs) < 2 {
		return nil, fmt.Errorf("expected at least 2 inputs, got %d", len(inputs))
	}

	pathOrDBID, ok := inputs[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[0])
	}

	date, ok := inputs[1].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[1])
	}

	// date_to may be nil without issues
	var dateTo string
	if len(inputs) > 2 {
		dateTo, ok = inputs[2].(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", inputs[2])
		}
	}

	if !tsn.IsValidDate(date) {
		return nil, fmt.Errorf("invalid date: %s", date)
	}

	// target is the necessary path to compute the stream OR the DBID itself
	// if no "/" is present, it is the DBID
	// if it starts with a /, is from the same wallet namespace
	// or it is a full path, <walletaddress>/<db_name>

	target, err := getDBIDFromPath(scoper, pathOrDBID)
	if err != nil {
		return nil, err
	}

	return s.CallOnTargetDBID(scoper, method, err, target, date, dateTo)
}

func (s *Stream) CallOnTargetDBID(scoper *execution.ProcedureContext, method string, err error, target string,
	date string, dateTo string) ([]any, error) {
	dataset, err := scoper.Dataset(target)
	if err != nil {
		return nil, err
	}

	// the stream protocol returns results as relations
	// we need to create a new scope to get the result
	newScope := scoper.NewScope()
	_, err = dataset.Call(newScope, method, []any{date, dateTo})
	if err != nil {
		return nil, err
	}

	if newScope.Result == nil {
		return nil, fmt.Errorf("stream returned nil result")
	}

	// create a result array that will be the rows of the result
	result := make([]any, len(newScope.Result.Rows))
	for i, row := range newScope.Result.Rows {
		// expect all rows to return int64 results in 1 column only
		if len(row) != 1 {
			return nil, fmt.Errorf("stream returned %d columns, expected 1", len(row))
		}
		val, ok := row[0].(int64)
		if !ok {
			return nil, fmt.Errorf("stream returned %T, expected int64", row[0])
		}
		result[i] = val
	}

	return result, nil
}

// getDBIDFromPath returns the DBID from a path or a DBID.
// possible inputs:
// - xac760c4d5332844f0da28c01adb53c6c369be0a2c4bf530a0f3366bd (DBID)
// - <owner_wallet_address>/<db_name>
// - /<db_name> (will use the wallet address from the scoper)
func getDBIDFromPath(scoper *execution.ProcedureContext, pathOrDBID string) (string, error) {
	// if the path does not contain a "/", we assume it is a DBID
	if !strings.Contains(pathOrDBID, "/") {
		return pathOrDBID, nil
	}

	walletAddress := ""
	dbName := ""

	if strings.HasPrefix(pathOrDBID, "/") {
		// get the wallet address
		signer := scoper.Signer // []byte type
		walletAddress = string(signer)
		dbName = strings.Split(pathOrDBID, "/")[1]
	}

	// if walletAddress is empty, we assume the path is a full path
	if walletAddress == "" {
		walletAddress = strings.Split(pathOrDBID, "/")[0]
		dbName = strings.Split(pathOrDBID, "/")[1]
	}

	walledAddressBytes := []byte(walletAddress)
	DBID := utils.GenerateDBID(dbName, walledAddressBytes)

	return DBID, nil
}

type knownMethod string

const (
	knownMethodIndex knownMethod = "get_index"
	knownMethodValue knownMethod = "get_value"
)
