package primitive_stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/stretchr/testify/mock"
	"github.com/truflation/tsn-db/internal/utils"
	"github.com/truflation/tsn-db/mocks"

	"github.com/stretchr/testify/assert"
)

func Test_Index(t *testing.T) {
	scope := &precompiles.ProcedureContext{
		DBID: "dbid",
		Ctx:  context.Background(),
	}
	b := &PrimitiveStreamExt{
		table:       "price",
		dateColumn:  "date",
		valueColumn: "value",
	}

	mockStmts := map[string]*sql.ResultSet{
		b.sqlGetBasePrimitive():                 mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 75000}}),  // 75.000
		b.sqlGetLatestPrimitive():               mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}), // 200.000
		b.sqlGetSpecificPrimitive("2024-01-01"): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}), // 150.000
		b.sqlGetRangePrimitive("2024-01-01", "2024-01-02"): mockDateScalar("value", []utils.ValueWithDate{
			{Date: "2024-01-01", Value: 150000},
			{Date: "2024-01-02", Value: 300000},
		}),                                                                                                                    // 150.000, 300.000
		b.sqlGetLastBefore("2024-01-01"): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 266666}}), // 266.666
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

	returned, err = b.index(scope, app, "2024-01-01", nil)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}, returned)

	t.Run("validation - it should return an error expected single value when base value is not a single value", func(t *testing.T) {
		mockSql := map[string]*sql.ResultSet{
			b.sqlGetBasePrimitive(): mockDateScalar("value", []utils.ValueWithDate{
				{Date: "2024-01-01", Value: 75000},
				{Date: "2024-01-02", Value: 150000},
			}),
		}
		app.Engine = newEngine(t, mockSql)
		_, err = b.index(scope, app, "2024-01-01", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected single value")
	})

	t.Run("error - it should return an error if b.value returns an error", func(t *testing.T) {
		mockSql := map[string]*sql.ResultSet{
			b.sqlGetBasePrimitive(): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 75000}}), // 75.000
		}
		app.Engine = newEngine(t, mockSql)
		_, err = b.index(scope, app, "2024-01-01", nil)
		assert.Error(t, err)
	})
}

func Test_Value(t *testing.T) {
	scope := &precompiles.ProcedureContext{
		DBID: "dbid",
		Ctx:  context.Background(),
	}
	b := &PrimitiveStreamExt{
		table:       "price",
		dateColumn:  "date",
		valueColumn: "value",
	}

	mockStmts := map[string]*sql.ResultSet{
		b.sqlGetLatestPrimitive():                          mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}),                                      // 200.000
		b.sqlGetSpecificPrimitive("2024-01-01"):            mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}),                                      // 150.000
		b.sqlGetRangePrimitive("2024-01-01", "2024-01-02"): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}, {Date: "2024-01-02", Value: 300000}}), // 150.000, 300.000
	}

	app := &common.App{
		Engine: newEngine(t, mockStmts),
	}

	returned, err := b.primitive(scope, app, "2024-01-01", nil)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}, returned) // 150.000 * 1000

	returned, err = b.primitive(scope, app, "", nil) // this should return the latest value
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}, returned) // 200.000 * 1000

	dateTo := "2024-01-02"
	returned, err = b.primitive(scope, app, "2024-01-01", &dateTo)
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

type primitiveStreamTest struct {
	ctx             *precompiles.DeploymentContext
	scope           *precompiles.ProcedureContext
	app             *common.App
	primitiveStream *PrimitiveStreamExt
}

func newPrimitiveStreamTest() *primitiveStreamTest {
	return &primitiveStreamTest{
		ctx: &precompiles.DeploymentContext{
			Schema: &common.Schema{
				Tables: []*common.Table{
					{
						Name: "price",
						Columns: []*common.Column{
							{
								Name: "date",
								Type: common.TEXT,
							},
							{
								Name: "value",
								Type: common.INT,
							},
							{
								Name: "created_at",
								Type: common.TEXT,
							},
						},
					},
				},
			},
		},
		scope:           &precompiles.ProcedureContext{},
		app:             &common.App{},
		primitiveStream: &PrimitiveStreamExt{},
	}
}

func TestInitializePrimitiveStream(t *testing.T) {
	metadata := map[string]string{
		"table_name":        "price",
		"date_column":       "date",
		"value_column":      "value",
		"created_at_column": "created_at",
	}

	instance := newPrimitiveStreamTest()
	t.Run("success - it should initialize the primitive_stream", func(t *testing.T) {
		_, err := InitializePrimitiveStream(instance.ctx, nil, metadata)
		assert.NoError(t, err)
	})

	t.Run("validation - it should return an error if the table does not exist", func(t *testing.T) {
		wrongMetadata := map[string]string{
			"wrong_table_name": "price",
		}
		_, err := InitializePrimitiveStream(instance.ctx, nil, wrongMetadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing table")
	})

	t.Run("validation - it should return date type must be text", func(t *testing.T) {
		wrongInstance := newPrimitiveStreamTest()
		wrongInstance.ctx.Schema.Tables[0].Columns[0].Type = common.INT
		_, err := InitializePrimitiveStream(wrongInstance.ctx, nil, metadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "date column date must be of type TEXT")
	})

	t.Run("validation - it should return value type must be int", func(t *testing.T) {
		wrongInstance := newPrimitiveStreamTest()
		wrongInstance.ctx.Schema.Tables[0].Columns[1].Type = common.TEXT
		_, err := InitializePrimitiveStream(wrongInstance.ctx, nil, metadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value column value must be of type INT")
	})

	t.Run("validation - it should return an error if the date column does not exist", func(t *testing.T) {
		wrongMetadata := map[string]string{
			"table_name":   "price",
			"date_column":  "wrong_date",
			"value_column": "value",
		}
		_, err := InitializePrimitiveStream(instance.ctx, nil, wrongMetadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("validation - it should return an error if the value column does not exist", func(t *testing.T) {
		wrongMetadata := map[string]string{
			"table_name":   "price",
			"date_column":  "date",
			"value_column": "wrong_value",
		}
		_, err := InitializePrimitiveStream(instance.ctx, nil, wrongMetadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("validation - it should return an error if the table does not exist", func(t *testing.T) {
		wrongMetadata := map[string]string{
			"table_name":  "wrong_table",
			"date_column": "date",
		}
		_, err := InitializePrimitiveStream(instance.ctx, nil, wrongMetadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("validation - it should return an error if the created_at column does not exist", func(t *testing.T) {
		wrongMetadata := map[string]string{
			"table_name":        "price",
			"date_column":       "date",
			"value_column":      "value",
			"created_at_column": "wrong_created_at",
		}
		_, err := InitializePrimitiveStream(instance.ctx, nil, wrongMetadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestPrimitiveStreamExt_Call(t *testing.T) {
	instance := newPrimitiveStreamTest()
	mockEngine := mocks.NewEngine(t)
	instance.app.Engine = mockEngine
	//instance.scope.SetValue("caller", "caller")
	//instance.scope.SetValue("args", "args")

	t.Run("success - it should return the index", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}), nil)
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"2024-01-01", "2024-01-02"})
		assert.NoError(t, err)
	})

	t.Run("success - it should return the value", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}), nil)
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_primitive", []any{"2024-01-01", "2024-01-02"})
		assert.NoError(t, err)
	})

	t.Run("validation - it should return an error if the method is unknown", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "unknown", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown method")
	})

	t.Run("validation - it should return expected 2 inputs when args are not 2", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"2024-01-01"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected 2 arguments")
	})

	t.Run("validation - it should return expected string when date is not a string", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{1, "2024-01-02"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("validation - it should return invalid date_to when date_to is not a valid date", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"2024-01-01", 1})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected string for date_to")
	})

	t.Run("validation - it should return invalid date when date is not a valid date", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"wrong_date", "2024-01-02"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid date")
	})

	t.Run("validation - it should return invalid date when date_to is not a valid date", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"2024-01-01", "wrong_date"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid date")
	})

	t.Run("validation - it should return is before date when date_to is before date", func(t *testing.T) {
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"2024-01-02", "2024-01-01"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is before date")
	})

	t.Run("error - it should return error when the engine returns an error", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Execute(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		_, err := instance.primitiveStream.Call(instance.scope, instance.app, "get_index", []any{"2024-01-01", "2024-01-02"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error getting current primitive on db execute")
	})
}
