// package basestream implements the base stream extension.
// it is meant to be used for a Truflation primitive stream
// that tracks some time series data.
package basestream

import (
	"errors"
	"fmt"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/truflation/tsn-db/internal/utils"
	"strings"
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
func InitializeBasestream(ctx *precompiles.DeploymentContext, service *common.Service, metadata map[string]string) (precompiles.Instance, error) {
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
					if col.Type != common.TEXT {
						return nil, fmt.Errorf("date column %s must be of type TEXT", dateColumn)
					}
				}
				if strings.EqualFold(col.Name, valueColumn) {
					foundValueColumn = true
					if col.Type != common.INT {
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

type BaseStreamExt struct {
	table       string
	dateColumn  string
	valueColumn string
}

func (b *BaseStreamExt) Call(scope *precompiles.ProcedureContext, app *common.App, method string, args []any) ([]any, error) {
	switch strings.ToLower(method) {
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	case "get_index":
		return getValue(scope, app, b.index, args...)
	case "get_value":
		return getValue(scope, app, b.value, args...)
	}
}

const (
	// getBaseValue gets the base value from a base stream, to be used in index calculation.
	sqlGetBaseValue     = `select %s, %s from %s WHERE %s != 0 order by %s ASC LIMIT 1;`
	sqlGetLatestValue   = `select %s, %s from %s order by %s DESC LIMIT 1;`
	sqlGetSpecificValue = `select %s, %s from %s where %s = '%s';`
	sqlGetLastBefore    = `select %s, %s from %s where %s <= '%s' order by %s DESC LIMIT 1;`
	sqlGetRangeValue    = `select %s, %s from %s where %s >= '%s' and %s <= '%s' order by %s ASC;` // I can't use @date, changed it to basic %s, please take a look
	zeroDate            = "0000-00-00"
)

func (b *BaseStreamExt) sqlGetBaseValue() string {
	return fmt.Sprintf(sqlGetBaseValue, b.dateColumn, b.valueColumn, b.table, b.valueColumn, b.dateColumn)
}

func (b *BaseStreamExt) sqlGetLatestValue() string {
	return fmt.Sprintf(sqlGetLatestValue, b.dateColumn, b.valueColumn, b.table, b.dateColumn)
}

func (b *BaseStreamExt) sqlGetSpecificValue(date string) string {
	return fmt.Sprintf(sqlGetSpecificValue, b.dateColumn, b.valueColumn, b.table, b.dateColumn, date)
}

func (b *BaseStreamExt) sqlGetLastBefore(date string) string {
	return fmt.Sprintf(sqlGetLastBefore, b.dateColumn, b.valueColumn, b.table, b.dateColumn, date, b.dateColumn)
}

func (b *BaseStreamExt) sqlGetRangeValue(date string, dateTo string) string {
	return fmt.Sprintf(sqlGetRangeValue, b.dateColumn, b.valueColumn, b.table, b.dateColumn, date, b.dateColumn, dateTo, b.dateColumn)
}

// getValue gets the value for the specified function.
// usage: get_value($date, $date_to?)
// behavior: 	if $date is not provided, it will return the latest value.
//
//	else if $date_to is provided, it will return the value for the date range.
//
// returns either a single value or a range of values.
func getValue(scope *precompiles.ProcedureContext, app *common.App, fn func(*precompiles.ProcedureContext, *common.App, string, *string) ([]utils.ValueWithDate, error), args ...any) ([]any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("expected 2 arguments, got %d", len(args))
	}

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
	if !utils.IsValidDate(date) {
		return nil, fmt.Errorf("invalid date: %s", date)
	}
	// - date_to valid
	if dateTo != nil && !utils.IsValidDate(*dateTo) {
		return nil, fmt.Errorf("invalid date_to: %s", *dateTo)
	}
	// - date_to is after date
	if dateTo != nil && *dateTo < date {
		return nil, fmt.Errorf("date_to %s is before date %s", *dateTo, date)
	}

	val, err := fn(scope, app, date, dateTo)
	if err != nil {
		return nil, fmt.Errorf("error getting value (dbid=%s): %w", scope.DBID, err)
	}

	// each row contains column values
	rowsResult := make([][]any, len(val))
	for i, v := range val {
		rowsResult[i] = []any{v.Date, v.Value}
	}

	newResultSet := sql.ResultSet{
		Columns: []string{"date", "value"},
		Rows:    rowsResult,
	}

	// result means the last query result
	// at kuneiform file, we must be sure there's no other query after this one on the action
	scope.Result = &newResultSet
	return []any{0}, nil
}

// Index returns the inflation index for a given date.
// This follows Truflation function of ((current_value/first_value)*100).
// It will multiplty the returned result by an additional 1000, since Kwil
// cannot handle decimals.
func (b *BaseStreamExt) index(scope *precompiles.ProcedureContext, app *common.App, date string, dateTo *string) ([]utils.ValueWithDate, error) {

	// we will first get the first ever value
	baseValueArr, err := b.value(scope, app, zeroDate, nil)
	if err != nil {
		return []utils.ValueWithDate{}, err
	}
	// expect single value
	if len(baseValueArr) != 1 {
		return []utils.ValueWithDate{}, errors.New("expected single value for base value")
	}
	baseValue := baseValueArr[0].Value

	// now we will get the value for the requested date
	currentValueArr, err := b.value(scope, app, date, dateTo)
	if err != nil {
		return []utils.ValueWithDate{}, err
	}

	// if there's no date_to, we expect a single value
	if dateTo == nil && len(currentValueArr) != 1 {
		return []utils.ValueWithDate{}, errors.New("expected single value for current value")
	}

	// we can't do floating point division, but Truflation normally tracks
	// index precision to the thousandth, so we will multiply by 1000 before
	// performing integer division. This will round the result down (golang truncates
	// integer division results).
	// Truflations calculation is ((current_value/first_value)*100).
	// Therefore, we will alter the equation to ((current_value*100000)/first_value).
	// This essentially gives us the same result, but with an extra 3 digits of precision.
	//index := (currentValue * 100000) / baseValue
	indexes := make([]utils.ValueWithDate, len(currentValueArr))
	for i, currentValue := range currentValueArr {
		indexes[i] = utils.ValueWithDate{Date: currentValue.Date, Value: (currentValue.Value * 100000) / baseValue}
	}

	return indexes, nil
}

// value returns the value for a given date.
// if no date is given, it will return the latest value.
func (b *BaseStreamExt) value(scope *precompiles.ProcedureContext, app *common.App, date string, dateTo *string) ([]utils.ValueWithDate, error) {
	var res *sql.ResultSet
	var err error
	if date == zeroDate {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetBaseValue(), nil)
	} else if date == "" {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetLatestValue(), nil)
	} else if dateTo == nil {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetSpecificValue(date), nil)
	} else {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetRangeValue(date, *dateTo), nil)
	}

	if err != nil {
		return []utils.ValueWithDate{}, fmt.Errorf("error getting current value on db execute: %w", err)
	}

	values, err := utils.GetScalarWithDate(res)

	if err != nil {
		return []utils.ValueWithDate{}, fmt.Errorf("error getting current value on get scalar with date: %w", err)
	}

	/*
		if:
		- there's no row in the answer OR;
		- the first row date is not the same as the requested first date
		we try to get the last value before the requested date
		and assign it to the first value as the specified date

		examples:
		given there's a value of 100 on 2000-01-01

		e.g. for requested 2000-02-01, the original response would be
		| date | value |
		|------|-------|
		| empty | empty |

		but the response should be
		| date | value |
		|------|-------|
		| 2000-02-01 | 100 |

		e.g. range response for 2000-02-01 to 2000-02-02 would be
		| date | value |
		|------|-------|
		| empty | empty |
		| 2000-02-02 | 200 |

		but the response should be
		| date | value |
		|------|-------|
		| 2000-02-01 | 100 |
		| 2000-02-02 | 200 |

		unless there's no data before these dates, in which case we return without modifications
	*/
	if (len(values) == 0 || values[0].Date != date) && (date != zeroDate && date != "") {
		// we will get the last value before the requested date
		lastValueBefore, err := app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetLastBefore(date), nil)
		if err != nil {
			return []utils.ValueWithDate{}, fmt.Errorf("error getting last value before requested date: %w", err)
		}

		lastValue, err := utils.GetScalarWithDate(lastValueBefore)
		if err != nil {
			return []utils.ValueWithDate{}, fmt.Errorf("error getting last value before requested date: %w", err)
		}

		switch true {
		case len(lastValue) == 0:
			// if there's no last value before, we just end the if clause
			break
		case len(lastValue) != 1:
			return []utils.ValueWithDate{}, fmt.Errorf("expected single value for last value before requested date")
			// let's append the last value before the requested date
		default:
			values = append(lastValue, values...)
		}
	}

	// if there's no data at all, we error out
	if len(values) == 0 {
		return []utils.ValueWithDate{}, errors.New("no data found")
	}

	return values, nil
}