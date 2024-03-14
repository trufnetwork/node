package whitelist

import (
	"fmt"
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"strings"
)

func checkWalletFormat(wallet string) error {
	if len(wallet) != 42 {
		return fmt.Errorf("invalid wallet address length")
	}
	if wallet[:2] != "0x" {
		return fmt.Errorf("invalid wallet address format")
	}
	return nil
}

//	metadata: {
//	  "whitelist_wallets"?: "0x1234,0x5678,0x9abc" // comma separated list of wallet addresses
//	}
func InitializeExtension(ctx *execution.DeploymentContext, metadata map[string]string) (execution.ExtensionNamespace, error) {
	extension_name := "whitelist"
	if len(metadata) > 1 {
		return nil, fmt.Errorf("extension %s has too many arguments used", extension_name)
	}

	ownerWallet := ctx.Schema.Owner // type: []byte
	// array type
	var whitelistedWallets [][]byte
	whitelistedWallets = append(whitelistedWallets, ownerWallet)

	walletsStrFromInput := metadata["whitelist_wallets"]
	if walletsStrFromInput != "" {
		wallets := strings.Split(walletsStrFromInput, ",")
		for _, wallet := range wallets {
			err := checkWalletFormat(wallet)
			if err != nil {
				return nil, fmt.Errorf("invalid address -> %s: %s", wallet, err)
			}
			whitelistedWallets = append(whitelistedWallets, []byte(wallet))
		}
	} else {
		// if the user provided a metadata, and it's not whitelist_wallets, we error out
		if len(metadata) > 0 {
			keys := make([]string, 0, len(metadata))
			for k := range metadata {
				keys = append(keys, k)
			}
			return nil, fmt.Errorf("metadata was provided, but none of the expected keys were found: %v", keys)
		}
	}

	return &WhitelistExt{
		whitelistedWallets: whitelistedWallets,
	}, nil
}

var _ = execution.ExtensionInitializer(InitializeExtension)

type WhitelistExt struct {
	whitelistedWallets [][]byte
}

func (w *WhitelistExt) Call(scoper *execution.ProcedureContext, method string, inputs []interface{}) ([]interface{}, error) {
	switch method {
	// usage example: use whitelist as w; w.check("0x1234")
	case "check":
		return w.check(inputs)
	default:
		return nil, fmt.Errorf("unknown method '%s'", method)
	}
}

func (w *WhitelistExt) check(inputs []interface{}) ([]interface{}, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("expected 1 input, got %d", len(inputs))
	}

	wallet, ok := inputs[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[0])
	}

	for _, wl := range w.whitelistedWallets {
		if string(wl) == wallet {
			return []interface{}{true}, nil
		}
	}

	// if we want to someday handle this at kuneiform schema, we return false
	//return []interface{}{false}, nil
	// we error out instead for now
	return nil, fmt.Errorf("wallet %s is not whitelisted", wallet)
}
