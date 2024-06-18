package init_system_contract

import (
	"context"
	"fmt"
	"os"

	"github.com/kwilteam/kwil-db/core/client"
	"github.com/kwilteam/kwil-db/core/crypto"
	"github.com/kwilteam/kwil-db/core/crypto/auth"
	clientType "github.com/kwilteam/kwil-db/core/types/client"
	"github.com/kwilteam/kwil-db/parse"
)

type InitSystemContractOptions struct {
	PrivateKey         string
	ProviderUrl        string
	SystemContractPath string
}

// Helper function to check if a file exists
func fileExists(filePath string) bool {
	info, err := os.Stat(filePath)
	return err == nil && !info.IsDir()
}

func InitSystemContract(options InitSystemContractOptions) error {
	ctx := context.Background()

	// Check if the system contract file exists
	if !fileExists(options.SystemContractPath) {
		return fmt.Errorf("system contract not found at the expected location: %s", options.SystemContractPath)
	}

	pk, err := crypto.Secp256k1PrivateKeyFromHex(options.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	signer := &auth.EthPersonalSigner{Key: *pk}

	kwilClient, err := client.NewClient(ctx, options.ProviderUrl, &clientType.Options{
		Signer: signer,
	})
	if err != nil {
		return fmt.Errorf("failed to create kwil client: %w", err)
	}

	// Make sure the TSN is running. We expect to receive pong
	res, err := kwilClient.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping the network: %w", err)
	}

	fmt.Println("Received from TSN: ", res)

	// Read the system contract file
	contractContent, err := os.ReadFile(options.SystemContractPath)
	if err != nil {
		return fmt.Errorf("failed to read system contract file: %w", err)
	}

	schema, err := parse.Parse(contractContent)
	if err != nil {
		return fmt.Errorf("failed to parse system contract: %w", err)
	}

	fmt.Println("Deploying system contract...")
	// Deploy the system contract
	txHash, err := kwilClient.DeployDatabase(ctx, schema, clientType.WithSyncBroadcast(true))
	if err != nil {
		return fmt.Errorf("failed to deploy system contract: %w", err)
	}

	fmt.Println("System contract deployed")
	fmt.Println("Transaction hash:", txHash)

	return nil
}
