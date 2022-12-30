package server

import (
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"kwil/x/gateway"
	"kwil/x/gateway/middleware"
	"kwil/x/gateway/middleware/auth"
	"kwil/x/gateway/middleware/cors"
	"os"
)

func Start() error {
	cmd := &cobra.Command{
		Use:   "api-gateway",
		Short: "http gateway to kwil service",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			mux := runtime.NewServeMux()
			addr := viper.GetString(gateway.ListenAddressName)
			gw := gateway.NewGWServer(mux, addr)

			if err := gw.SetupGrpcSvc(cmd.Context()); err != nil {
				return err
			}
			if err := gw.SetupHttpSvc(cmd.Context()); err != nil {
				return err
			}

			f, err := os.Open("keys.json")
			if err != nil {
				return err
			}

			keyManager, err := auth.NewKeyManager(f)
			if err != nil {
				return err
			}
			f.Close()

			_cors := viper.GetString(cors.GatewayCorsName)

			ms := []*middleware.NamedMiddleware{
				// from innermost middleware
				{"auth", auth.MAuth(keyManager)},
				{"cors", cors.MCors(_cors)},
			}

			gw.AddMiddleWares(ms)

			return gw.Serve()
		},
	}

	CliSetup(cmd)
	return cmd.Execute()
}
