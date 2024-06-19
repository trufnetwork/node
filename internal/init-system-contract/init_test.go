package init_system_contract

import (
	"context"
	"github.com/truflation/tsn-db/internal/contracts"
	"os"
	"testing"
	"time"
)

func TestInitSystemContract(t *testing.T) {
	isCi := os.Getenv("CI") == "true"
	t.Run("TestInitSystemContract", func(t *testing.T) {

		if isCi {
			t.Skip("Not prepared to run in CI environment")
		}

		ctx := context.Background()
		err := InitSystemContract(ctx, InitSystemContractOptions{
			PrivateKey:            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			ProviderUrl:           "http://localhost:8090",
			SystemContractContent: contracts.SystemContractContent,
			RetryTimeout:          15 * time.Minute,
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}
