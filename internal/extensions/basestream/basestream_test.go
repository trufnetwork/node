package basestream

import (
	"context"
	"fmt"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/stretchr/testify/mock"
	"github.com/truflation/tsn-db/internal/utils"
	"github.com/truflation/tsn-db/mocks"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Index(t *testing.T) {
	scope := &precompiles.ProcedureContext{
		DBID: "dbid",
		Ctx:  context.Background(),
	}
	b := &BaseStreamExt{
		table:       "price",
		dateColumn:  "date",
		valueColumn: "value",
	}

	mockStmts := map[string]*sql.ResultSet{
		b.sqlGetBaseValue():                 mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 75000}}),  // 75.000
		b.sqlGetLatestValue():               mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}), // 200.000
		b.sqlGetSpecificValue("2024-01-01"): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}), // 150.000
		b.sqlGetRangeValue("2024-01-01", "2024-01-02"): mockDateScalar("value", []utils.ValueWithDate{
			{Date: "2024-01-01", Value: 150000},
			{Date: "2024-01-02", Value: 300000},
		}), // 150.000, 300.000
	}

	app := &common.App{
		Engine: newEngine(t, mockStmts),
	}

	returned, err := b.index(scope, app, "2024-01-01", nil)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}, returned) // 200.000 * 1000

	returned, err = b.index(scope, app, "", nil) // this should return the latest value
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 266666}}, returned) // 266.666 * 1000

	dateTo := "2024-01-02"
	returned, err = b.index(scope, app, "2024-01-01", &dateTo)

	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}, {Date: "2024-01-02", Value: 400000}}, returned) // 200.000 * 1000, 400.000 * 1000
}

func Test_Value(t *testing.T) {
	scope := &precompiles.ProcedureContext{
		DBID: "dbid",
		Ctx:  context.Background(),
	}
	b := &BaseStreamExt{
		table:       "price",
		dateColumn:  "date",
		valueColumn: "value",
	}

	mockStmts := map[string]*sql.ResultSet{
		b.sqlGetLatestValue():                          mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}),                                      // 200.000
		b.sqlGetSpecificValue("2024-01-01"):            mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}),                                      // 150.000
		b.sqlGetRangeValue("2024-01-01", "2024-01-02"): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}, {Date: "2024-01-02", Value: 300000}}), // 150.000, 300.000
	}

	app := &common.App{
		Engine: newEngine(t, mockStmts),
	}

	returned, err := b.value(scope, app, "2024-01-01", nil)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}, returned) // 150.000 * 1000

	returned, err = b.value(scope, app, "", nil) // this should return the latest value
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}, returned) // 200.000 * 1000

	dateTo := "2024-01-02"
	returned, err = b.value(scope, app, "2024-01-01", &dateTo)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}, {Date: "2024-01-02", Value: 300000}}, returned) // 150.000 * 1000, 300.000 * 1000
}

// mockDateScalar is a helper function that creates a new actions.Result that
// returns the given value as a row and column result with a date.
func mockDateScalar(column string, arrayOfResults []utils.ValueWithDate) *sql.ResultSet {
	mockedRows := make([][]any, len(arrayOfResults))
	for i, result := range arrayOfResults {
		mockedRows[i] = []any{result.Date, result.Value}
	}
	return &sql.ResultSet{
		Columns: []string{"date", column},
		Rows:    mockedRows,
	}
}

type mockQuerier struct {
	*mocks.Engine
	stmts map[string]*sql.ResultSet
}

func newEngine(t interface {
	mock.TestingT
	Cleanup(func())
},
	stmts map[string]*sql.ResultSet,
) *mockQuerier {
	return &mockQuerier{
		Engine: mocks.NewEngine(t),
		stmts:  stmts,
	}
}

// Execute(ctx context.Context, tx sql.DB, dbid, query string, values map[string]any) (*sql.ResultSet, error)
func (m *mockQuerier) Execute(ctx context.Context, tx sql.DB, dbid, query string, values map[string]any) (*sql.ResultSet, error) {
	res, ok := m.stmts[query]
	if !ok {
		return nil, fmt.Errorf("unexpected statement: %s", query)
	}
	return res, nil
}
