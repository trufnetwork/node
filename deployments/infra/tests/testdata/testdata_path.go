package testdata

import (
	"path/filepath"
	"runtime"
)

// TestdataPath returns the path to the testdata directory.
func TestdataPath() string {
	// use runtime.Caller to locate this file at runtime
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("could not determine testdata path")
	}
	// return the directory containing this source file
	return filepath.Dir(file)
}
