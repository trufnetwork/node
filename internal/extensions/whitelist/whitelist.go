package whitelist

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
)

func checkWalletFormat(wallet string) error {
	if len(wallet) != 42 {
		return fmt.Errorf("invalid wallet address length")
	}
	if wallet[:2] != "0x" {
		return fmt.Errorf("address should start with 0x")
	}
	return nil
}

//	metadata: {
//	  "whitelist_wallets"?: "0x1234,0x5678,0x9abc" // comma separated list of wallet addresses
//	}
func InitializeExtension(ctx *precompiles.DeploymentContext, service *common.Service, metadata map[string]string) (precompiles.Instance, error) {
	extension_name := "whitelist"
	if len(metadata) > 1 {
		return nil, fmt.Errorf("extension %s has too many arguments used", extension_name)
	}

	ownerWalletByte := ctx.Schema.Owner // type: []byte
	// []byte to hex string
	ownerWallet := "0x" + hex.EncodeToString(ownerWalletByte)
	// array type
	var whitelistedWallets []string

	whitelistedWallets = append(whitelistedWallets, ownerWallet)

	walletsStrFromInput, ok := metadata["whitelist_wallets"]
	if walletsStrFromInput != "" {
		wallets := strings.Split(walletsStrFromInput, ",")
		for _, wallet := range wallets {
			err := checkWalletFormat(wallet)
			if err != nil {
				return nil, fmt.Errorf("invalid address -> %s: %s", wallet, err)
			}
			whitelistedWallets = append(whitelistedWallets, wallet)
		}
	}

	// let's prevent typos in the parameters
	if !ok && len(metadata) > 0 {
		// to prevent typos, we error other keys without providing "whitelist_wallets"
		keys := make([]string, 0, len(metadata))
		for k := range metadata {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("probable typo, unknown keys used: %s", keys)
	}

	// make all wallets unique, and lowercase
	uniqueWallets := make(map[string]bool)
	for _, wallet := range whitelistedWallets {
		uniqueWallets[strings.ToLower(wallet)] = true
	}
	whitelistedWallets = make([]string, 0, len(uniqueWallets))
	for wallet := range uniqueWallets {
		whitelistedWallets = append(whitelistedWallets, wallet)
	}

	return &WhitelistExt{
		whitelistedWallets: whitelistedWallets,
	}, nil
}

type WhitelistExt struct {
	whitelistedWallets []string
}

func (w *WhitelistExt) Call(scoper *precompiles.ProcedureContext, app *common.App, method string, inputs []interface{}) ([]interface{}, error) {
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
	// to lower case
	wallet = strings.ToLower(wallet)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", inputs[0])
	}

	for _, wl := range w.whitelistedWallets {
		if wl == wallet {
			return []interface{}{true}, nil
		}
	}

	// if we want to someday handle this at kuneiform schema, we return false
	//return []interface{}{false}, nil
	// we error out instead for now
	return nil, fmt.Errorf("wallet %s is not whitelisted", wallet)
}
