package testutils

import (
	"github.com/kwilteam/kwil-db/testing"
)

func Ptr[T any](v T) *T {
	return &v
}

// GetTestOptions returns the common test options
func GetTestOptions() *testing.Options {
	return &testing.Options{
		UseTestContainer: true,
	}
}
