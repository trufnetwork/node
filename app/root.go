package app

import (
	"github.com/kwilteam/kwil-db/app"
	"github.com/spf13/cobra"
	tnVersion "github.com/trufnetwork/node/cmd/version"
)

// RootCmd creates a root command that uses TN version instead of kwil-db version
func RootCmd() *cobra.Command {
	// Get the original kwil-db root command
	cmd := app.RootCmd()

	// Find and remove the original version command
	for _, subcmd := range cmd.Commands() {
		if subcmd.Name() == "version" {
			cmd.RemoveCommand(subcmd)
			break
		}
	}

	// Add our custom TN version command
	cmd.AddCommand(tnVersion.NewVersionCmd())

	return cmd
}
