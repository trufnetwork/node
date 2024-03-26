package mathutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Fraction(t *testing.T) {
	type testcase struct {
		name        string
		numerator   int64
		denominator int64
		number      int64
		want        int64
	}
	// function does (numerator/denominator) * number, and rounds down
	tests := []testcase{
		{
			name:        "1/2 * 2",
			numerator:   1,
			denominator: 2,
			number:      2,
			want:        1,
		},
		{
			name:        "1/2 * 1",
			numerator:   1,
			denominator: 2,
			number:      1,
			want:        0,
		},
		{
			name:        "104892/32034 * 6932", // arbitrarily big numbers 1
			numerator:   104892,
			denominator: 32034,
			number:      6932,
			want:        22698,
		},
		{
			name:        "13/3234454 * 15734567318", // arbitrarily big numbers 2
			numerator:   13,
			denominator: 3234454,
			number:      15734567318,
			want:        63240,
		},
		{
			name:        "largest int64s",
			numerator:   9223372036854775807,
			denominator: 9223372036854775807,
			number:      9223372036854775807,
			want:        9223372036854775807,
		},
		{
			name:        "zero denominator",
			numerator:   1,
			denominator: 0,
			number:      1,
			want:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fraction(tt.number, tt.numerator, tt.denominator)
			if err != nil {
				if err.Error() == "denominator cannot be zero" {
					return
				}
				t.Errorf("fraction() error = %v", err)
				return
			}

			if got[0] != tt.want {
				t.Errorf("fraction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitializeMathUtil(t *testing.T) {
	t.Run("success - it should return mathUtilExt instance", func(t *testing.T) {
		_, err := InitializeMathUtil(nil, nil, nil)
		assert.NoError(t, err, "InitializeMathUtil returned an error")
	})

	t.Run("error - it should return error when metadata is not empty", func(t *testing.T) {
		_, err := InitializeMathUtil(nil, nil, map[string]string{"key": "value"})
		assert.EqualError(t, err, "mathutil does not take any configs")
	})
}

func TestMathUtilExt_Call(t *testing.T) {
	t.Run("success - it should return nil when method is fraction", func(t *testing.T) {
		instance := &mathUtilExt{}
		_, err := instance.Call(nil, nil, "fraction", []any{int64(1), int64(2), int64(2)})
		assert.NoError(t, err, "mathUtilExt.Call returned an error")
	})

	t.Run("validation - it should return error when method is unknown", func(t *testing.T) {
		instance := &mathUtilExt{}
		_, err := instance.Call(nil, nil, "unknown", nil)
		assert.Contains(t, err.Error(), "unknown method")
	})

	t.Run("validation - it should return error when inputs length is less than 3", func(t *testing.T) {
		instance := &mathUtilExt{}
		_, err := instance.Call(nil, nil, "fraction", []any{})
		assert.Contains(t, err.Error(), "expected 3 inputs")
	})

	t.Run("validation - it should return error when inputs[0] is not int64", func(t *testing.T) {
		instance := &mathUtilExt{}
		_, err := instance.Call(nil, nil, "fraction", []any{"string", int64(2), int64(2)})
		assert.Contains(t, err.Error(), "expected int64 for arg 1")
	})

	t.Run("validation - it should return error when inputs[1] is not int64", func(t *testing.T) {
		instance := &mathUtilExt{}
		_, err := instance.Call(nil, nil, "fraction", []any{int64(1), "string", int64(2)})
		assert.Contains(t, err.Error(), "expected int64 for arg 2")
	})

	t.Run("validation - it should return error when inputs[2] is not int64", func(t *testing.T) {
		instance := &mathUtilExt{}
		_, err := instance.Call(nil, nil, "fraction", []any{int64(1), int64(2), "string"})
		assert.Contains(t, err.Error(), "expected int64 for arg 3")
	})
}
