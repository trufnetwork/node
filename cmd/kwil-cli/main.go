package main

import (
	"go.uber.org/zap"
	"os"

	// NOTE: if extensions are used to build a kwild with new transaction
	// payload types or serialization methods, the same extension packages that
	// register those types with core module packages would be imported here so
	// that the client can work with them too. While the client does is not
	// concerned with activation heights, it could need to use new functionality
	// introduced by the consensus extensions.

	root "github.com/kwilteam/kwil-db/cmd/kwil-cli/cmds"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

func main() {
	root := root.NewRootCmd()
	if err := root.Execute(); err != nil {
		zap.L().Fatal("Failed to execute root command", zap.Error(err))
	}
	os.Exit(0)
}
