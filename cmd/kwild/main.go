package main

import (
	"os"

	"github.com/trufnetwork/node/app"
	"go.uber.org/zap"
)

func main() {
	if err := app.RootCmd().Execute(); err != nil {
		zap.L().Fatal("Failed to execute root command", zap.Error(err))
	}
	os.Exit(0)
}

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))

	// initialize extensions here if needed
}
