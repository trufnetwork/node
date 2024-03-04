package utils

import (
	"fmt"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"math/big"
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

func Fraction(number int64, numerator int64, denominator int64) (int64, error) {
	if denominator == 0 {
		return 0, fmt.Errorf("denominator cannot be zero")
	}

	// we will simply rely on go's integer division to truncate (round down)
	// we will use big math to avoid overflow
	bigNumber := big.NewInt(number)
	bigNumerator := big.NewInt(numerator)
	bigDenominator := big.NewInt(denominator)

	// (numerator/denominator) * number

	// numerator * number
	bigProduct := new(big.Int).Mul(bigNumerator, bigNumber)

	// numerator * number / denominator
	result := new(big.Int).Div(bigProduct, bigDenominator).Int64()
	return result, nil
}
