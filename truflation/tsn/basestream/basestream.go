// package basestream implements the base stream extension.
// it is meant to be used for a Truflation primitive stream
// that tracks some time series data.
package basestream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/internal/engine/types"
	"github.com/kwilteam/kwil-db/internal/sql"
	"github.com/kwilteam/kwil-db/truflation/tsn"
)

func getOrDefault(m map[string]string, key string, defaultValue string) string {
	if value, ok := m[key]; ok {
		return value
	}
	return defaultValue
}

// InitializeBasestream initializes the basestream extension.
// It takes 3 configs: table, date_column, and value_column.
// The table is the table that the data is stored in.
// The date_column is the column that the date is stored in, stored as "YYYY-MM-DD".
// The value_column is the column that the value is stored in. It must be an integer.
func InitializeBasestream(ctx *execution.DeploymentContext, metadata map[string]string) (execution.ExtensionNamespace, error) {
	var table, dateColumn, valueColumn string
	var ok bool
	table, ok = metadata["table_name"]
	if !ok {
		return nil, errors.New("missing table config")
	}

	// get from
	dateColumn = getOrDefault(metadata, "date_column", "date_value")
	valueColumn = getOrDefault(metadata, "value_column", "value")

	foundTable := false
	foundDateColumn := false
	foundValueColumn := false
	// now we validate that the table and columns exist
	for _, tbl := range ctx.Schema.Tables {
		if strings.EqualFold(tbl.Name, table) {
			foundTable = true
			for _, col := range tbl.Columns {
				if strings.EqualFold(col.Name, dateColumn) {
					foundDateColumn = true
					if col.Type != types.TEXT {
						return nil, fmt.Errorf("date column %s must be of type TEXT", dateColumn)
					}
				}
				if strings.EqualFold(col.Name, valueColumn) {
					foundValueColumn = true
					if col.Type != types.INT {
						return nil, fmt.Errorf("value column %s must be of type INTEGER", valueColumn)
					}
				}
			}
		}
	}

	if !foundTable {
		return nil, fmt.Errorf("table %s not found", table)
	}
	if !foundDateColumn {
		return nil, fmt.Errorf("date column %s not found", dateColumn)
	}
	if !foundValueColumn {
		return nil, fmt.Errorf("value column %s not found", valueColumn)
	}

	return &BaseStreamExt{
		table:       table,
		dateColumn:  dateColumn,
		valueColumn: valueColumn,
	}, nil
}

var _ = execution.ExtensionInitializer(InitializeBasestream)

type BaseStreamExt struct {
	table       string
	dateColumn  string
	valueColumn string
}

func (b *BaseStreamExt) Call(scope *execution.ProcedureContext, method string, args []any) ([]any, error) {
	switch strings.ToLower(method) {
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	case "get_index":
		return getValue(scope, b.index, args...)
	case "get_value":
		return getValue(scope, b.value, args...)
	}
}

const (
	// getBaseValue gets the base value from a base stream, to be used in index calculation.
	sqlGetBaseValue     = `select %s from %s order by %s ASC LIMIT 1;`
	sqlGetLatestValue   = `select %s from %s order by %s DESC LIMIT 1;`
	sqlGetSpecificValue = `select %s from %s where %s = $date;`
	sqlGetRangeValue    = `select %s from %s where %s >= $date and %s <= $date_to;`
	zeroDate            = "0000-00-00"
)

func (b *BaseStreamExt) sqlGetBaseValue() string {
	return fmt.Sprintf(sqlGetBaseValue, b.valueColumn, b.table, b.dateColumn)
}

func (b *BaseStreamExt) sqlGetLatestValue() string {
	return fmt.Sprintf(sqlGetLatestValue, b.valueColumn, b.table, b.dateColumn)
}

func (b *BaseStreamExt) sqlGetSpecificValue() string {
	return fmt.Sprintf(sqlGetSpecificValue, b.valueColumn, b.table, b.dateColumn)
}

func (b *BaseStreamExt) sqlGetRangeValue() string {
	return fmt.Sprintf(sqlGetRangeValue, b.valueColumn, b.table, b.dateColumn, b.dateColumn)
}

// getValue gets the value for the specified function.
func getValue(scope *execution.ProcedureContext, fn func(context.Context, Querier, string, *string) ([]int64, error), args ...any) ([]any, error) {
	// usage: get_value($date, $date_to?)
	// behavior: 	if $date is not provided, it will return the latest value.
	// 				else if $date_to is provided, it will return the value for the date range.
	// returns either a single value or a range of values.

	dataset, err := scope.Dataset(scope.DBID)
	if err != nil {
		return nil, err
	}

	if len(args) < 1 {
		return nil, fmt.Errorf("expected at least one argument")
	}

	// date is optional
	date, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string for date, got %T", args[0])
	}

	// date_to should be nil if not provided. If provided, try to convert to string.
	var dateTo *string
	if args[1] != nil {
		dateToStr, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("expected string for date_to, got %T", args[1])
		}
		// if date_to is "", we should keep it nil
		if dateToStr != "" {
			dateTo = &dateToStr
		}
	}

	// Date validations
	// - date valid
	if !tsn.IsValidDate(date) {
		return nil, fmt.Errorf("invalid date: %s", date)
	}
	// - date_to valid
	if dateTo != nil && !tsn.IsValidDate(*dateTo) {
		return nil, fmt.Errorf("invalid date_to: %s", *dateTo)
	}
	// - date-to is after date
	if dateTo != nil && *dateTo < date {
		return nil, fmt.Errorf("date_to %s is before date %s", *dateTo, date)
	}

	val, err := fn(scope.Ctx, dataset, date, dateTo)
	if err != nil {
		return nil, err
	}

	// returned array here is an array because the caller can be expecting multiple values returned
	return []any{val}, nil
}

// Index returns the inflation index for a given date.
// This follows Truflation function of ((current_value/first_value)*100).
// It will multiplty the returned result by an additional 1000, since Kwil
// cannot handle decimals.
func (b *BaseStreamExt) index(ctx context.Context, dataset Querier, date string, dateTo *string) ([]int64, error) {

	// we will first get the first ever value
	baseValueArr, err := b.value(ctx, dataset, zeroDate, nil)
	if err != nil {
		return []int64{}, err
	}
	// expect single value
	if len(baseValueArr) != 1 {
		return []int64{}, errors.New("expected single value for base value")
	}
	baseValue := baseValueArr[0]

	// now we will get the value for the requested date
	currentValueArr, err := b.value(ctx, dataset, date, dateTo)
	if err != nil {
		return []int64{}, err
	}

	// if there's no date_to, we expect a single value
	if dateTo == nil && len(currentValueArr) != 1 {
		return []int64{}, errors.New("expected single value for current value")
	}

	// we can't do floating point division, but Truflation normally tracks
	// index precision to the thousandth, so we will multiply by 1000 before
	// performing integer division. This will round the result down (golang truncates
	// integer division results).
	// Truflations calculation is ((current_value/first_value)*100).
	// Therefore, we will alter the equation to ((current_value*100000)/first_value).
	// This essentially gives us the same result, but with an extra 3 digits of precision.
	//index := (currentValue * 100000) / baseValue
	indexes := make([]int64, len(currentValueArr))
	for i, currentValue := range currentValueArr {
		indexes[i] = (currentValue * 100000) / baseValue
	}

	return indexes, nil
}

// value returns the value for a given date.
// if no date is given, it will return the latest value.
func (b *BaseStreamExt) value(ctx context.Context, dataset Querier, date string, dateTo *string) ([]int64, error) {
	var res *sql.ResultSet
	var err error
	if date == zeroDate {
		res, err = dataset.Query(ctx, b.sqlGetBaseValue(), nil)
	} else if date == "" {
		res, err = dataset.Query(ctx, b.sqlGetLatestValue(), nil)
	} else if dateTo == nil {
		res, err = dataset.Query(ctx, b.sqlGetSpecificValue(), map[string]any{
			"$date": date,
		})
	} else {
		// kwild does not support ptr, so we need to convert dateTo to a value
		res, err = dataset.Query(ctx, b.sqlGetRangeValue(), map[string]any{
			"$date":    date,
			"$date_to": *dateTo,
		})
	}

	if err != nil {
		return []int64{}, errors.New(fmt.Sprintf("error getting current value: %s", err))
	}

	scalar, err := getScalar(res)
	if err != nil {
		return []int64{}, errors.New(fmt.Sprintf("error getting current scalar: %s", err))
	}

	values := make([]int64, len(scalar))
	for i, v := range scalar {
		value, ok := v.(int64)
		if !ok {
			return []int64{}, errors.New("expected int64 for current value")
		}
		values[i] = value
	}

	return values, nil
}

// getScalar gets a scalar value from a query result.
// It is expecting a result that has one row and one column.
// If it does not have one row and one column, it will return an error.
func getScalar(res *sql.ResultSet) ([]any, error) {
	if len(res.ReturnedColumns) != 1 {
		return nil, fmt.Errorf("stream expected one column, got %d", len(res.ReturnedColumns))
	}
	if len(res.Rows) == 0 {
		return nil, fmt.Errorf("stream has no data")
	}

	singleValueRows := make([]any, len(res.Rows))
	for i, row := range res.Rows {
		singleValueRows[i] = row[0]
	}

	return singleValueRows, nil
}

type Querier interface {
	Query(ctx context.Context, stmt string, params map[string]any) (*sql.ResultSet, error)
}
