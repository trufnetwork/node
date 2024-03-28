package utils

import (
	"testing"

	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/stretchr/testify/assert"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
)

func TestGetDBIDFromPath(t *testing.T) {
	tests := []struct {
		name          string
		ctx           *precompiles.DeploymentContext
		pathOrDBID    string
		expectedDBID  string
		expectedError bool
	}{
		{
			name:          "DBIDWithoutSlash",
			ctx:           nil,
			pathOrDBID:    "dbwithoutslash",
			expectedDBID:  "dbwithoutslash",
			expectedError: false,
		},
		{
			name:          "DBIDWithLeadingSlash",
			ctx:           &precompiles.DeploymentContext{Schema: &common.Schema{Owner: []byte("owner1")}},
			pathOrDBID:    "/dbname",
			expectedDBID:  utils.GenerateDBID("dbname", []byte("owner1")),
			expectedError: false,
		},
		{
			name:          "DBIDWithSlashAndNoContext",
			ctx:           nil,
			pathOrDBID:    "wallet/dbname",
			expectedDBID:  utils.GenerateDBID("dbname", []byte("wallet")),
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbID := GetDBIDFromPath(tt.ctx, tt.pathOrDBID)
			if dbID != tt.expectedDBID {
				t.Errorf("DBID mismatch - want: %v, got: %v", tt.expectedDBID, dbID)
			}
		})
	}
}

func TestFraction(t *testing.T) {
	tests := []struct {
		name          string
		number        int64
		numerator     int64
		denominator   int64
		expectedValue int64
		expectedError bool
	}{
		{
			name:          "ValidFraction",
			number:        10,
			numerator:     1,
			denominator:   2,
			expectedValue: 5,
			expectedError: false,
		},
		{
			name:          "ZeroDenominator",
			number:        10,
			numerator:     1,
			denominator:   0,
			expectedValue: 0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := Fraction(tt.number, tt.numerator, tt.denominator)
			if tt.expectedError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectedError && err != nil {
				t.Errorf("Expected no error but got one: %v", err)
			}
			if value != tt.expectedValue {
				t.Errorf("Value mismatch - want: %v, got: %v", tt.expectedValue, value)
			}
		})
	}
}

func TestGetScalarWithDate(t *testing.T) {
	tests := []struct {
		name                  string
		res                   *sql.ResultSet
		expectedValueWithDate ValueWithDate
		expectedError         bool
	}{
		{
			name: "ValidValueWithDate",
			expectedValueWithDate: ValueWithDate{
				Date:  "2021-01-01",
				Value: 10,
			},
			res: &sql.ResultSet{
				Columns: []string{"date", "value"},
				Rows: [][]interface{}{
					{"2021-01-01", int64(10)},
				},
			},
		},
		{
			name: "WrongNumberOfColumns",
			res: &sql.ResultSet{
				Columns: []string{"date"},
				Rows:    [][]interface{}{},
			},
			expectedError: true,
		},
		{
			name: "InvalidDate",
			res: &sql.ResultSet{
				Columns: []string{"date", "value"},
				Rows: [][]interface{}{
					{"wrongDate", int64(10)},
				},
			},
			expectedError: true,
		},
		{
			name: "InvalidValue",
			res: &sql.ResultSet{
				Columns: []string{"date", "value"},
				Rows: [][]interface{}{
					{"2021-01-01", "wrongValue"},
				},
			},
			expectedError: true,
		},
		{
			name: "WrongTypeForDate",
			res: &sql.ResultSet{
				Columns: []string{"date", "value"},
				Rows: [][]interface{}{
					{10, int64(10)},
				},
			},
			expectedError: true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueWithDates, err := GetScalarWithDate(tt.res)
			if tt.expectedError && err != nil {
				return
			}
			if tt.expectedError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			if !tt.expectedError && err != nil {
				t.Errorf("Expected no error but got one: %v", err)
				return
			}
			if valueWithDates[i].Date != tt.expectedValueWithDate.Date {
				t.Errorf("Date mismatch - want: %v, got: %v", tt.expectedValueWithDate.Date, valueWithDates[i].Date)
				return
			}
			if valueWithDates[i].Value != tt.expectedValueWithDate.Value {
				t.Errorf("Value mismatch - want: %v, got: %v", tt.expectedValueWithDate.Value, valueWithDates[i].Value)
				return
			}
		})
	}
	t.Run("validation - it should return empty value with dates if there are no rows", func(t *testing.T) {
		valueWithDates, err := GetScalarWithDate(&sql.ResultSet{
			Columns: []string{"date", "value"},
			Rows:    [][]interface{}{},
		})
		assert.Nil(t, err)
		assert.Empty(t, valueWithDates)
	})
}

func TestIsValidDate(t *testing.T) {
	t.Run("success - it should return true if inputed date is empty string", func(t *testing.T) {
		assert.True(t, IsValidDate(""))
	})
}
