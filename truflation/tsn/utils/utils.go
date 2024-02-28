package utils

import (
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"strings"
)

// GetDBIDFromPath returns the DBID from a path or a DBID.
// possible inputs:
// - xac760c4d5332844f0da28c01adb53c6c369be0a2c4bf530a0f3366bd (DBID)
// - <owner_wallet_address>/<db_name>
// - /<db_name> (will use the wallet address from the scoper)
func GetDBIDFromPath(ctx *execution.DeploymentContext, pathOrDBID string) (string, error) {
	// if the path does not contain a "/", we assume it is a DBID
	if !strings.Contains(pathOrDBID, "/") {
		return pathOrDBID, nil
	}

	var walletAddress []byte
	dbName := ""

	if strings.HasPrefix(pathOrDBID, "/") {
		// get the wallet address
		signer := ctx.Schema.Owner // []byte type
		walletAddress = signer
		dbName = strings.Split(pathOrDBID, "/")[1]
	}

	// if walletAddress is empty, we assume the path is a full path
	if walletAddress == nil {
		walletAddressStr := strings.Split(pathOrDBID, "/")[0]
		walletAddress = []byte(walletAddressStr)
		dbName = strings.Split(pathOrDBID, "/")[1]
	}

	DBID := utils.GenerateDBID(dbName, walletAddress)

	return DBID, nil
}
