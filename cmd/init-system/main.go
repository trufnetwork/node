package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	init_system_contract "github.com/truflation/tsn-db/internal/init-system-contract"
)

type InitSystemContractEvent struct {
	PrivateKey         string `json:"private_key"`
	ProviderUrl        string `json:"provider_url"`
	SystemContractPath string `json:"system_contract_path"`
}

func HandleRequest(ctx context.Context, event InitSystemContractEvent) (string, error) {
	options := init_system_contract.InitSystemContractOptions{
		PrivateKey:         event.PrivateKey,
		ProviderUrl:        event.ProviderUrl,
		SystemContractPath: event.SystemContractPath,
	}

	err := init_system_contract.InitSystemContract(options)
	if err != nil {
		return "", fmt.Errorf("failed to initialize system contract: %w", err)
	}

	return "System contract successfully deployed", nil
}

func main() {
	lambda.Start(HandleRequest)
}
