package meats_poultry_fish_eggs

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

func InitializeMeatsPoultryFishEggs(ctx *execution.DeploymentContext, metadata map[string]string) (execution.ExtensionNamespace, error) {
	var table, dateColumn, cpiColumn, yahooColumn, nielsenColumn, numbeoColumn string
	var ok bool
	table, ok = metadata["table_name"]
	if !ok {
		return nil, errors.New("missing table config")
	}
	dateColumn, ok = metadata["date_column"]
	if !ok {
		return nil, errors.New("missing date_column config")
	}
	cpiColumn, ok = metadata["cpi_column"]
	if !ok {
		return nil, errors.New("missing cpi_column config")
	}
	yahooColumn, ok = metadata["yahoo_column"]
	if !ok {
		return nil, errors.New("missing yahoo_column config")
	}
	nielsenColumn, ok = metadata["nielsen_column"]
	if !ok {
		return nil, errors.New("missing nielsen_column config")
	}
	numbeoColumn, ok = metadata["numbeo_column"]
	if !ok {
		return nil, errors.New("missing numbeo_column config")
	}

	foundTable := false
	foundDateColumn := false
	foundCpiColumn := false
	foundYahooColumn := false
	foundNielsenColumn := false
	foundNumbeoColumn := false
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
				if strings.EqualFold(col.Name, cpiColumn) {
					foundCpiColumn = true
					if col.Type != types.INT {
						return nil, fmt.Errorf("value column %s must be of type INTEGER", cpiColumn)
					}
				}
				if strings.EqualFold(col.Name, yahooColumn) {
					foundYahooColumn = true
					if col.Type != types.INT {
						return nil, fmt.Errorf("value column %s must be of type INTEGER", yahooColumn)
					}
				}
				if strings.EqualFold(col.Name, nielsenColumn) {
					foundNielsenColumn = true
					if col.Type != types.INT {
						return nil, fmt.Errorf("value column %s must be of type INTEGER", nielsenColumn)
					}
				}
				if strings.EqualFold(col.Name, numbeoColumn) {
					foundNumbeoColumn = true
					if col.Type != types.INT {
						return nil, fmt.Errorf("value column %s must be of type INTEGER", numbeoColumn)
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
	if !foundCpiColumn {
		return nil, fmt.Errorf("cpi column %s not found", cpiColumn)
	}
	if !foundYahooColumn {
		return nil, fmt.Errorf("yahoo column %s not found", yahooColumn)
	}
	if !foundNielsenColumn {
		return nil, fmt.Errorf("nielsen column %s not found", nielsenColumn)
	}
	if !foundNumbeoColumn {
		return nil, fmt.Errorf("numbeo column %s not aa found", numbeoColumn)
	}

	return &MeatsPoultryFishEggsExt{
		table:         table,
		dateColumn:    dateColumn,
		cpiColumn:     cpiColumn,
		yahooColumn:   yahooColumn,
		nielsenColumn: nielsenColumn,
		numbeoColumn:  numbeoColumn,
	}, nil
}

var _ = execution.ExtensionInitializer(InitializeMeatsPoultryFishEggs)

type MeatsPoultryFishEggsExt struct {
	table         string
	dateColumn    string
	cpiColumn     string
	yahooColumn   string
	nielsenColumn string
	numbeoColumn  string
}

func (b *MeatsPoultryFishEggsExt) Call(scope *execution.ProcedureContext, method string, args []any) ([]any, error) {
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
	sqlGetBaseValue = `SELECT 	
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s ASC LIMIT 1) AS cpi,
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s ASC LIMIT 1) AS yahoo,
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s ASC LIMIT 1) AS nielsen,
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s ASC LIMIT 1) AS numbeo;`
	sqlGetLatestValue = `SELECT
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s DESC LIMIT 1) AS cpi,
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s DESC LIMIT 1) AS yahoo,
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s DESC LIMIT 1) AS nielsen,
		(SELECT %s FROM meats WHERE %s !=0 ORDER BY %s DESC LIMIT 1) AS numbeo;`
	sqlGetSpecificValue = `SELECT
		%s AS cpi, %s AS yahoo, %s AS nielsen, %s AS numbeo
		FROM %s
		WHERE %s = $date;`
	zeroDate = "0000-00-00"
)

func (b *MeatsPoultryFishEggsExt) sqlGetBaseValue() string {
	return fmt.Sprintf(sqlGetBaseValue, b.cpiColumn, b.cpiColumn, b.dateColumn, b.yahooColumn, b.yahooColumn, b.dateColumn, b.nielsenColumn, b.nielsenColumn, b.dateColumn, b.numbeoColumn, b.numbeoColumn, b.dateColumn)
}

func (b *MeatsPoultryFishEggsExt) sqlGetLatestValue() string {
	return fmt.Sprintf(sqlGetLatestValue, b.cpiColumn, b.cpiColumn, b.dateColumn, b.yahooColumn, b.yahooColumn, b.dateColumn, b.nielsenColumn, b.nielsenColumn, b.dateColumn, b.numbeoColumn, b.numbeoColumn, b.dateColumn)
}

func (b *MeatsPoultryFishEggsExt) sqlGetSpecificValue() string {
	return fmt.Sprintf(sqlGetSpecificValue, b.cpiColumn, b.yahooColumn, b.nielsenColumn, b.numbeoColumn, b.table, b.dateColumn)
}

func getValue(scope *execution.ProcedureContext, fn func(context.Context, Querier, string) (int64, error), args ...any) ([]any, error) {
	dataset, err := scope.Dataset(scope.DBID)
	if err != nil {
		return nil, err
	}

	if len(args) != 1 {
		return nil, fmt.Errorf("expected one argument")
	}

	date, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string for date argument")
	}

	if !tsn.IsValidDate(date) {
		return nil, fmt.Errorf("invalid date: %s", date)
	}

	val, err := fn(scope.Ctx, dataset, date)
	if err != nil {
		return nil, err
	}

	return []any{val}, nil
}

func (b *MeatsPoultryFishEggsExt) index(ctx context.Context, dataset Querier, date string) (int64, error) {
	res, err := dataset.Query(ctx, b.sqlGetBaseValue(), nil)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("error getting base value: %s", err))
	}

	scalarCPI, scalarYahoo, scalarNielsen, scalarNumbeo, err := getScalar(res)
	if err != nil {
		return 0, err
	}

	baseValueCPI, ok := scalarCPI.(int64)
	if !ok {
		return 0, errors.New("expected int64 for base value")
	}

	baseValueYahoo, ok := scalarYahoo.(int64)
	if !ok {
		return 0, errors.New("expected int64 for base value")
	}

	baseValueNielsen, ok := scalarNielsen.(int64)
	if !ok {
		return 0, errors.New("expected int64 for base value")
	}

	baseValueNumbeo, ok := scalarNumbeo.(int64)
	if !ok {
		return 0, errors.New("expected int64 for base value")
	}

	if date == zeroDate || date == "" {
		res, err = dataset.Query(ctx, b.sqlGetLatestValue(), nil)
	} else {
		res, err = dataset.Query(ctx, b.sqlGetSpecificValue(), map[string]any{
			"$date": date,
		})
	}
	if err != nil {
		return 0, errors.New(fmt.Sprintf("error getting current value: %s", err))
	}

	scalarCPI, scalarYahoo, scalarNielsen, scalarNumbeo, err = getScalar(res)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("error getting current scalar: %s", err))
	}

	currentValueCPI, ok := scalarCPI.(int64)
	if !ok {
		return 0, errors.New("expected int64 for current value")
	}

	currentValueYahoo, ok := scalarYahoo.(int64)
	if !ok {
		return 0, errors.New("expected int64 for current value")
	}

	currentValueNielsen, ok := scalarNielsen.(int64)
	if !ok {
		return 0, errors.New("expected int64 for current value")
	}

	currentValueNumbeo, ok := scalarNumbeo.(int64)
	if !ok {
		return 0, errors.New("expected int64 for current value")
	}

	cpi := (currentValueCPI * 100000) / baseValueCPI
	yahoo := (currentValueYahoo * 100000) / baseValueYahoo
	nielsen := (currentValueNielsen * 100000) / baseValueNielsen
	numbeo := (currentValueNumbeo * 100000) / baseValueNumbeo

	index := (cpi * 250 / 1000) + (nielsen * 500 / 1000) + (yahoo * 125 / 1000) + (numbeo * 125 / 1000)
	return index, nil
}

func (b *MeatsPoultryFishEggsExt) value(ctx context.Context, dataset Querier, date string) (int64, error) {
	var res *sql.ResultSet
	var err error
	if date == zeroDate || date == "" {
		res, err = dataset.Query(ctx, b.sqlGetLatestValue(), nil)
	} else {
		res, err = dataset.Query(ctx, b.sqlGetSpecificValue(), map[string]any{
			"$date": date,
		})
	}
	if err != nil {
		fmt.Println("error getting current value: ", err)
		return 0, err
	}

	scalarCPI, scalarYahoo, scalarNielsen, scalarNumbeo, err := getScalar(res)
	if err != nil {
		fmt.Println("error getting current scalar: ", err)
		return 0, err
	}

	valueCPI, ok := scalarCPI.(int64)
	if !ok {
		fmt.Println("expected int64 for current value cpi")
		return 0, errors.New("expected int64 for current value")
	}

	valueYahoo, ok := scalarYahoo.(int64)
	if !ok {
		fmt.Println("expected int64 for current value yahoo")
		return 0, errors.New("expected int64 for current value")
	}

	valueNielsen, ok := scalarNielsen.(int64)
	if !ok {
		fmt.Println("expected int64 for current value nielsen")
		return 0, errors.New("expected int64 for current value")
	}

	valueNumbeo, ok := scalarNumbeo.(int64)
	if !ok {
		fmt.Println("expected int64 for current value numbeo")
		return 0, errors.New("expected int64 for current value")
	}

	value := (valueCPI * 250 / 1000) + (valueNielsen * 500 / 1000) + (valueYahoo * 125 / 1000) + (valueNumbeo * 125 / 1000)
	return value, nil
}

func getScalar(res *sql.ResultSet) (any, any, any, any, error) {
	if len(res.ReturnedColumns) != 4 {
		return nil, nil, nil, nil, fmt.Errorf("stream expected four column, got %d", len(res.ReturnedColumns))
	}
	if len(res.Rows) == 0 {
		return nil, nil, nil, nil, fmt.Errorf("stream has no data")
	}
	if len(res.Rows) != 1 {
		return nil, nil, nil, nil, fmt.Errorf("stream expected one row, got %d", len(res.Rows))
	}

	return res.Rows[0][0], res.Rows[0][1], res.Rows[0][2], res.Rows[0][3], nil
}

type Querier interface {
	Query(ctx context.Context, stmt string, params map[string]any) (*sql.ResultSet, error)
}
