package utils

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/internal/sql"
	"github.com/kwilteam/kwil-db/truflation/tsn"
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

// ValueWithDate is a struct that contains an arbitrary value and a date. Useful for time series results.
type ValueWithDate struct {
	Date  string
	Value int64
}

// GetScalarWithDate gets scalar values with respective dates from a query result.
// It is expecting a result that has two columns.
// Else, it will return an error.
func GetScalarWithDate(res *sql.ResultSet) ([]ValueWithDate, error) {
	if len(res.ReturnedColumns) != 2 {
		return nil, fmt.Errorf("stream expected one column, got %d", len(res.ReturnedColumns))
	}
	if len(res.Rows) == 0 {
		return []ValueWithDate{}, nil
	}

	rowsWithDate := make([]ValueWithDate, len(res.Rows))
	for i, row := range res.Rows {
		// we expect a row to contain: [date, value]
		date, err := row[0].(string)
		if !err {
			return nil, fmt.Errorf("expected string for date, got %T", row[0])
		}
		if !tsn.IsValidDate(date) {
			return nil, fmt.Errorf("invalid date: %s", date)
		}

		value, ok := row[1].(int64)
		if !ok {
			return nil, fmt.Errorf("expected value of type %T, got %T", int64(0), row[1])
		}

		rowsWithDate[i].Date = date
		rowsWithDate[i].Value = value
	}

	return rowsWithDate, nil
}
