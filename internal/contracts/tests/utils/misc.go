package testutils

import (
	"github.com/kwilteam/kwil-db/core/types/decimal"
)

func MustNewDecimal(value string) decimal.Decimal {
	dec, err := decimal.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return *dec
}

func ConvertDecimalToString(rows [][]any) [][]any {
	result := make([][]any, len(rows))
	for i, row := range rows {
		result[i] = make([]any, len(row))
		for j, val := range row {
			if dec, ok := val.(*decimal.Decimal); ok {
				result[i][j] = dec.String()
			} else {
				result[i][j] = val
			}
		}
	}
	return result
}
