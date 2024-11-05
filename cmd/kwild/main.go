package main

import (
	"go.uber.org/zap"
	"os"

	"github.com/kwilteam/kwil-db/cmd/kwild/root"
)

func main() {
	if err := root.RootCmd().Execute(); err != nil {
		zap.L().Fatal("Failed to execute root command", zap.Error(err))
	}
	os.Exit(0)
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))

	// initialize extensions here if needed
}
