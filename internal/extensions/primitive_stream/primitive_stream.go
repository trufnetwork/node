// package primitive stream implements the primitive stream extension.
// it is meant to be used for a Truflation primitive stream
// that tracks some time series data.
package primitive_stream

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/truflation/tsn-db/internal/utils"
)

func getOrDefault(m map[string]string, key string, defaultValue string) string {
	if value, ok := m[key]; ok {
		return value
	}
	return defaultValue
}

// InitializePrimitiveStream initializes the primitive_stream extension.
// It takes 3 configs: table, date_column, and value_column.
// The table is the table that the data is stored in.
// The date_column is the column that the date is stored in, stored as "YYYY-MM-DD".
// The value_column is the column that the value is stored in. It must be an integer.
func InitializePrimitiveStream(ctx *precompiles.DeploymentContext, service *common.Service, metadata map[string]string) (precompiles.Instance, error) {
	var table, dateColumn, valueColumn string
	var ok bool
	table, ok = metadata["table_name"]
	if !ok {
		return nil, errors.New("missing table config")
	}

	// get from
	dateColumn = getOrDefault(metadata, "date_column", "date_value")
	valueColumn = getOrDefault(metadata, "value_column", "value")
	createdAtColumn := getOrDefault(metadata, "created_at_column", "created_at")

	foundTable := false
	foundDateColumn := false
	foundValueColumn := false
	foundCreatedAtColumn := false
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
				if strings.EqualFold(col.Name, createdAtColumn) {
					foundCreatedAtColumn = true
					if col.Type != common.TEXT {
						return nil, fmt.Errorf("created_at column %s must be of type TEXT", createdAtColumn)
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
	if !foundCreatedAtColumn {
		return nil, fmt.Errorf("created_at column %s not found", createdAtColumn)
	}

	return &PrimitiveStreamExt{
		table:           table,
		dateColumn:      dateColumn,
		valueColumn:     valueColumn,
		createdAtColumn: createdAtColumn,
	}, nil
}

type PrimitiveStreamExt struct {
	table           string
	dateColumn      string
	valueColumn     string
	createdAtColumn string
}

func (b *PrimitiveStreamExt) Call(scope *precompiles.ProcedureContext, app *common.App, method string, args []any) ([]any, error) {
	switch strings.ToLower(method) {
	default:
		return nil, fmt.Errorf("unknown method: %s", method)
	case "get_index":
		return getValueForFn(scope, app, b.index, args...)
	case "get_primitive":
		return getValueForFn(scope, app, b.primitive, args...)
	}
}

const (
	// getBasePrimitive gets the base primitive from a base stream, to be used in index calculation.
	sqlGetBasePrimitive     = `SELECT %s, %s FROM %s WHERE %s != 0 ORDER BY %s ASC, %s DESC LIMIT 1;`
	sqlGetLatestPrimitive   = `SELECT %s, %s FROM %s ORDER by %s DESC, %s DESC LIMIT 1;`
	sqlGetSpecificPrimitive = `SELECT %s, %s FROM %s WHERE %s = '%s' ORDER BY %s DESC LIMIT 1;`
	sqlGetLastBefore        = `SELECT %s, %s FROM %s WHERE %s <= '%s' ORDER BY %s DESC, %s DESC LIMIT 1;`
	// we're sorting by created at too, because we need to get unique values from here later. We just delete the rows that are not unique.
	sqlGetRangePrimitive = `SELECT %s, %s FROM %s WHERE %s >= '%s' AND %s <= '%s' ORDER BY %s ASC, %s DESC;` // I can't use @date, changed it to basic %s, please take a look
	zeroDate             = "0000-00-00"
)

func (b *PrimitiveStreamExt) sqlGetBasePrimitive() string {
	return fmt.Sprintf(sqlGetBasePrimitive, b.dateColumn, b.valueColumn, b.table, b.valueColumn, b.dateColumn, b.createdAtColumn)
}

func (b *PrimitiveStreamExt) sqlGetLatestPrimitive() string {
	return fmt.Sprintf(sqlGetLatestPrimitive, b.dateColumn, b.valueColumn, b.table, b.dateColumn, b.createdAtColumn)
}

func (b *PrimitiveStreamExt) sqlGetSpecificPrimitive(date string) string {
	return fmt.Sprintf(sqlGetSpecificPrimitive, b.dateColumn, b.valueColumn, b.table, b.dateColumn, date, b.createdAtColumn)
}

func (b *PrimitiveStreamExt) sqlGetLastBefore(date string) string {
	return fmt.Sprintf(sqlGetLastBefore, b.dateColumn, b.valueColumn, b.table, b.dateColumn, date, b.dateColumn, b.createdAtColumn)
}

func (b *PrimitiveStreamExt) sqlGetRangePrimitive(date string, dateTo string) string {
	return fmt.Sprintf(sqlGetRangePrimitive, b.dateColumn, b.valueColumn, b.table, b.dateColumn, date, b.dateColumn, dateTo, b.dateColumn, b.createdAtColumn)
}

// getValueForFn gets the value for the specified function.
func getValueForFn(scope *precompiles.ProcedureContext, app *common.App, fn func(*precompiles.ProcedureContext, *common.App, string, *string) ([]utils.ValueWithDate, error), args ...any) ([]any, error) {
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
// This follows Truflation function of ((current_primitive/first_primitive)*100).
// It will multiplty the returned result by an additional 1000, since Kwil
// cannot handle decimals.
func (b *PrimitiveStreamExt) index(scope *precompiles.ProcedureContext, app *common.App, date string, dateTo *string) ([]utils.ValueWithDate, error) {

	// we will first get the first ever primitive
	basePrimitiveArr, err := b.primitive(scope, app, zeroDate, nil)
	if err != nil {
		return []utils.ValueWithDate{}, err
	}
	// expect single value
	if len(basePrimitiveArr) != 1 {
		return []utils.ValueWithDate{}, errors.New("expected single value for base primitive")
	}
	basePrimitive := basePrimitiveArr[0].Value

	// now we will get the primitive for the requested date
	currentPrimitiveArr, err := b.primitive(scope, app, date, dateTo)
	if err != nil {
		return []utils.ValueWithDate{}, err
	}

	// if there's no date_to, we expect a single value
	if dateTo == nil && len(currentPrimitiveArr) != 1 {
		return []utils.ValueWithDate{}, errors.New("expected single value for current primitive")
	}

	// we can't do floating point division, but Truflation normally tracks
	// index precision to the thousandth, so we will multiply by 1000 before
	// performing integer division. This will round the result down (golang truncates
	// integer division results).
	// Truflations calculation is ((current_primitive/first_primitive)*100).
	// Therefore, we will alter the equation to ((current_primitive*100000)/first_primitive).
	// This essentially gives us the same result, but with an extra 3 digits of precision.
	//index := (currentPrimitive * 100000) / basePrimitive
	indexes := make([]utils.ValueWithDate, len(currentPrimitiveArr))
	for i, currentPrimitive := range currentPrimitiveArr {
		indexes[i] = utils.ValueWithDate{Date: currentPrimitive.Date, Value: (currentPrimitive.Value * 100000) / basePrimitive}
	}

	return indexes, nil
}

// uniqueByCreatedAt deletes the duplicate rows with the same date column
// we know that this will behave correctly if the query is sorted by asc date and then desc created at
func (b *PrimitiveStreamExt) uniqueByCreatedAt(result *sql.ResultSet) (*sql.ResultSet, error) {
	// find date column
	dateIndex := -1
	for i, col := range result.Columns {
		if col == b.dateColumn {
			dateIndex = i
		}
	}

	// if dateIndex is not found, error out
	if dateIndex == -1 {
		return nil, errors.New("date column not found")
	}

	// we don't need to sort. The query is sorted already by desc created at. So if we find 2 consecutive rows with date, we delete it
	lastDate := ""

	for i := 0; i < len(result.Rows); i++ {
		if result.Rows[i][dateIndex] != lastDate {
			lastDate = result.Rows[i][dateIndex].(string)
		} else {
			result.Rows = append(result.Rows[:i], result.Rows[i+1:]...)
			i--
		}
	}

	return result, nil
}

// primitive returns the primitive for a given date.
// if no date is given, it will return the latest primitive.
func (b *PrimitiveStreamExt) primitive(scope *precompiles.ProcedureContext, app *common.App, date string, dateTo *string) ([]utils.ValueWithDate, error) {
	var res *sql.ResultSet
	var err error
	if date == zeroDate {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetBasePrimitive(), nil)
	} else if date == "" {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetLatestPrimitive(), nil)
	} else if dateTo == nil {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetSpecificPrimitive(date), nil)
	} else {
		res, err = app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetRangePrimitive(date, *dateTo), nil)
		if err == nil {
			res, err = b.uniqueByCreatedAt(res)
		}
	}

	if err != nil {
		return []utils.ValueWithDate{}, fmt.Errorf("error getting current primitive on db execute: %w", err)
	}

	primitives, err := utils.GetScalarWithDate(res)

	if err != nil {
		return []utils.ValueWithDate{}, fmt.Errorf("error getting current primitive on get scalar with date: %w", err)
	}

	/*
		if:
		- there's no row in the answer OR;
		- the first row date is not the same as the requested first date
		we try to get the last primitive before the requested date
		and assign it to the first primitive as the specified date

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
	if (len(primitives) == 0 || primitives[0].Date != date) && (date != zeroDate && date != "") {
		// we will get the last primitive before the requested date
		lastPrimitiveBefore, err := app.Engine.Execute(scope.Ctx, app.DB, scope.DBID, b.sqlGetLastBefore(date), nil)
		if err != nil {
			return []utils.ValueWithDate{}, fmt.Errorf("error getting last primitive before requested date: %w", err)
		}

		lastPrimitive, err := utils.GetScalarWithDate(lastPrimitiveBefore)
		if err != nil {
			return []utils.ValueWithDate{}, fmt.Errorf("error getting last primitive before requested date: %w", err)
		}

		switch true {
		case len(lastPrimitive) == 0:
			// if there's no last value before, we just end the if clause
			break
		case len(lastPrimitive) != 1:
			return []utils.ValueWithDate{}, fmt.Errorf("expected single value for last primitive before requested date")
			// let's append the last value before the requested date
		default:
			primitives = append(lastPrimitive, primitives...)
		}
	}

	// if there's no data at all, we error out
	if len(primitives) == 0 {
		return []utils.ValueWithDate{}, errors.New("no data found")
	}

	return primitives, nil
}
