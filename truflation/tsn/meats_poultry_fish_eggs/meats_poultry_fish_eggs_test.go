package meats_poultry_fish_eggs

import (
	"context"
	"fmt"
	"testing"

	"github.com/kwilteam/kwil-db/internal/sql"
	"github.com/stretchr/testify/assert"
)

func Test_Index(t *testing.T) {
	ctx := context.Background()
	b := &MeatsPoultryFishEggsExt{
		table:         "price",
		dateColumn:    "date",
		cpiColumn:     "cpi",
		yahooColumn:   "yahoo",
		nielsenColumn: "nielsen",
		numbeoColumn:  "numbeo",
	}

	mockQ := &mockQuerier{
		stmts: map[string]*sql.ResultSet{
			b.sqlGetBaseValue():     mockScalar([]string{"cpi", "yahoo", "nielsen", "numbeo"}, []any{int64(75000), int64(75000), int64(75000), int64(75000)}),     // 75.000
			b.sqlGetLatestValue():   mockScalar([]string{"cpi", "yahoo", "nielsen", "numbeo"}, []any{int64(200000), int64(200000), int64(200000), int64(200000)}), // 200.000
			b.sqlGetSpecificValue(): mockScalar([]string{"cpi", "yahoo", "nielsen", "numbeo"}, []any{int64(150000), int64(150000), int64(150000), int64(150000)}), // 150.000
		},
	}

	returned, err := b.index(ctx, mockQ, "2024-01-01")
	assert.NoError(t, err)
	assert.Equal(t, int64(200000), returned)

	returned, err = b.index(ctx, mockQ, "")
	assert.NoError(t, err)
	returned2, err := b.index(ctx, mockQ, zeroDate)
	assert.NoError(t, err)
	assert.Equal(t, int64(266665), returned)
	assert.Equal(t, int64(266665), returned2)
}

func Test_Value(t *testing.T) {
	ctx := context.Background()
	b := &MeatsPoultryFishEggsExt{
		table:         "price",
		dateColumn:    "date",
		cpiColumn:     "cpi",
		yahooColumn:   "yahoo",
		nielsenColumn: "nielsen",
		numbeoColumn:  "numbeo",
	}

	mockQ := &mockQuerier{
		stmts: map[string]*sql.ResultSet{
			b.sqlGetLatestValue():   mockScalar([]string{"cpi", "yahoo", "nielsen", "numbeo"}, []any{int64(200000), int64(200000), int64(200000), int64(200000)}), // 200.000
			b.sqlGetSpecificValue(): mockScalar([]string{"cpi", "yahoo", "nielsen", "numbeo"}, []any{int64(150000), int64(150000), int64(150000), int64(150000)}), // 150.000
		},
	}

	returned, err := b.value(ctx, mockQ, "2024-01-01")
	assert.NoError(t, err)
	assert.Equal(t, int64(150000), returned)

	returned, err = b.value(ctx, mockQ, "") // this should return the latest value
	assert.NoError(t, err)
	returned2, err := b.value(ctx, mockQ, zeroDate) // this should return the latest value
	assert.NoError(t, err)
	assert.Equal(t, int64(200000), returned)
	assert.Equal(t, int64(200000), returned2)
}

func mockScalar(column []string, v []any) *sql.ResultSet {
	return &sql.ResultSet{
		ReturnedColumns: []string{column[0], column[1], column[2], column[3]},
		Rows: [][]any{
			{v[0], v[1], v[2], v[3]},
		},
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
