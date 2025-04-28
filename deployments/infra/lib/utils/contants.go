package utils

import (
	"os"
	"path/filepath"
)

func GetProjectRootDir() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return filepath.Join(wd, "..", "..")
}
