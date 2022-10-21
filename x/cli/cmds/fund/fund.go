package fund

import (
	"github.com/spf13/cobra"
	"kwil/x/cli/util"
)

func NewCmdFund() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "fund",
		Short: "fund contains subcommands for funding",
		Long:  `With "fund" you can deposit, withdraw, and check your allowance.`,
	}

	util.BindKwilFlags(cmd.PersistentFlags())
	util.BindChainFlags(cmd.PersistentFlags())

	cmd.AddCommand(
		approveCmd(),
		depositCmd(),
		withdrawCmd(),
		balancesCmd(),
	)

	return cmd
}