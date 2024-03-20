package stream

import (
	"errors"
	"fmt"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	utils2 "github.com/truflation/tsn-db/internal/utils"
	"strings"
)

// InitializeStream initializes the stream extension.
// It takes no configs.
func InitializeStream(ctx *precompiles.DeploymentContext, service *common.Service, metadata map[string]string) (precompiles.Instance, error) {
	if len(metadata) != 0 {
		return nil, errors.New("stream does not take any configs")
	}

	return &Stream{}, nil
}

// Stream is the namespace for the stream extension.
// Stream has two methods: "index" and "value".
// Both of them get the value of the target stream at the given time.
type Stream struct{}

func (s *Stream) Call(scoper *precompiles.ProcedureContext, app *common.App, method string, inputs []any) ([]any, error) {
	switch strings.ToLower(method) {
	case string(knownMethodIndex):
	case string(knownMethodValue):
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

	if !utils2.IsValidDate(date) || (dateTo != "" && !utils2.IsValidDate(dateTo)) {
		return nil, fmt.Errorf("invalid date: %s, date_to: %s", date, dateTo)
	}

	// target is the necessary path to compute the stream OR the DBID itself
	// if no "/" is present, it is the DBID
	// if it starts with a /, is from the same wallet namespace
	// or it is a full path, <walletaddress>/<db_name>

	target := getDBIDFromPath(scoper, pathOrDBID)
	scoper.SetValue("date", date)
	scoper.SetValue("date_to", dateTo)
	res, err := app.Engine.Execute(scoper.Ctx, app.DB, target, method, scoper.Values())
	if err != nil {
		return nil, err
	}

	scoper.Result = res
	return nil, nil
}

// getDBIDFromPath returns the DBID from a path or a DBID.
// possible inputs:
// - xac760c4d5332844f0da28c01adb53c6c369be0a2c4bf530a0f3366bd (DBID)
// - <owner_wallet_address>/<db_name>
// - /<db_name> (will use the wallet address from the scoper)
func getDBIDFromPath(scoper *precompiles.ProcedureContext, pathOrDBID string) string {
	// if the path does not contain a "/", we assume it is a DBID
	if !strings.Contains(pathOrDBID, "/") {
		return pathOrDBID
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

	return DBID
}

type knownMethod string

const (
	knownMethodIndex knownMethod = "get_index"
	knownMethodValue knownMethod = "get_value"
)
