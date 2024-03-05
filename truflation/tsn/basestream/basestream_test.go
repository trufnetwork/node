package basestream

import (
	"context"
	"fmt"
	"github.com/kwilteam/kwil-db/truflation/tsn/utils"
	"testing"

	"github.com/kwilteam/kwil-db/internal/sql"
	"github.com/stretchr/testify/assert"
)

func Test_Index(t *testing.T) {
	ctx := context.Background()
	b := &BaseStreamExt{
		table:       "price",
		dateColumn:  "date",
		valueColumn: "value",
	}

	// when checking these values, we know that we are adding extra "precision" by returning values
	// as having been multiplied by 1000. This is because Kwil cannot handle decimals.
	// The number 1500 will be identified by Truflation stream clients as 1.500
	mockQ := &mockQuerier{
		stmts: map[string]*sql.ResultSet{
			b.sqlGetBaseValue():     mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 75000}}),  // 75.000
			b.sqlGetLatestValue():   mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}), // 200.000
			b.sqlGetSpecificValue(): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}), // 150.000
			b.sqlGetRangeValue(): mockDateScalar("value", []utils.ValueWithDate{
				{Date: "2024-01-01", Value: 150000},
				{Date: "2024-01-02", Value: 300000},
			}), // 150.000, 300.000
		},
	}

	returned, err := b.index(ctx, mockQ, "2024-01-01", nil)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}, returned) // 200.000 * 1000

	returned, err = b.index(ctx, mockQ, "", nil) // this should return the latest value
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 266666}}, returned) // 266.666 * 1000

	dateTo := "2024-01-02"
	returned, err = b.index(ctx, mockQ, "2024-01-01", &dateTo)

	assert.NoError(t, err)
	// 200%, 400%
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}, {Date: "2024-01-02", Value: 400000}}, returned) // 200.000 * 1000, 400.000 * 1000
}

func Test_Value(t *testing.T) {
	ctx := context.Background()
	b := &BaseStreamExt{
		table:       "price",
		dateColumn:  "date",
		valueColumn: "value",
	}

	mockQ := &mockQuerier{
		stmts: map[string]*sql.ResultSet{
			b.sqlGetLatestValue():   mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}),                                      // 200.000
			b.sqlGetSpecificValue(): mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}),                                      // 150.000
			b.sqlGetRangeValue():    mockDateScalar("value", []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}, {Date: "2024-01-02", Value: 300000}}), // 150.000, 300.000
		},
	}

	returned, err := b.value(ctx, mockQ, "2024-01-01", nil)
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 150000}}, returned) // 150.000 * 1000

	returned, err = b.value(ctx, mockQ, "", nil) // this should return the latest value
	assert.NoError(t, err)
	assert.Equal(t, []utils.ValueWithDate{{Date: "2024-01-01", Value: 200000}}, returned) // 200.000 * 1000

	dateTo := "2024-01-02"
	returned, err = b.value(ctx, mockQ, "2024-01-01", &dateTo)
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
		ReturnedColumns: []string{"date", column},
		Rows:            mockedRows,
	}
}

type mockQuerier struct {
	stmts map[string]*sql.ResultSet
}

func (m *mockQuerier) Query(ctx context.Context, stmt string, params map[string]any) (*sql.ResultSet, error) {
	res, ok := m.stmts[stmt]
	if !ok {
		return nil, fmt.Errorf("unexpected statement: %s", stmt)
	}
	return res, nil
}
