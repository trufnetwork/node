package digest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	util "github.com/trufnetwork/sdk-go/core/util"
)

// GetDeployerOrDefault returns the deployer EthereumAddress from the platform,
// falling back to a deterministic default test address when unavailable.
func GetDeployerOrDefault(platform *kwilTesting.Platform) (util.EthereumAddress, error) {
	// Try to create from bytes first
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err == nil {
		return deployer, nil
	}

	// Fallback: deterministic default
	defaultAddr := make([]byte, 20)
	for i := range defaultAddr {
		defaultAddr[i] = byte(i % 256)
	}
	return util.NewEthereumAddressFromBytes(defaultAddr)
}

// NewTxContext builds a standard TxContext for engine calls/queries.
func NewTxContext(ctx context.Context, platform *kwilTesting.Platform, signer util.EthereumAddress) *common.TxContext {
	return &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       signer.Bytes(),
		Caller:       signer.Address(),
		TxID:         platform.Txid(),
	}
}

// AnalyzeTables performs ANALYZE on the provided table names in the default schema.
// Tables are analyzed individually to avoid hanging on non-existent or large tables.
func AnalyzeTables(ctx context.Context, platform *kwilTesting.Platform, tables []string) error {
	if len(tables) == 0 {
		return nil
	}

	var errors []string

	// Analyze each table individually to avoid hanging on problematic tables
	for _, table := range tables {
		if err := analyzeSingleTable(ctx, platform, table); err != nil {
			// Log the error but continue with other tables
			errors = append(errors, fmt.Sprintf("failed to analyze %s: %v", table, err))
		}
	}

	// If all tables failed, return an error
	if len(errors) == len(tables) {
		return fmt.Errorf("all table analyses failed: %s", strings.Join(errors, "; "))
	}

	// If some tables failed, log warnings but don't fail the entire operation
	if len(errors) > 0 {
		fmt.Printf("Warning: Some tables could not be analyzed: %s\n", strings.Join(errors, "; "))
	}

	return nil
}

// analyzeSingleTable analyzes a single table with timeout protection.
func analyzeSingleTable(ctx context.Context, platform *kwilTesting.Platform, table string) error {
	// Create a timeout context for the ANALYZE operation
	analyzeCtx, cancel := context.WithTimeout(ctx, 30*time.Second) // 30 second timeout
	defer cancel()

	// Build fully qualified table name
	fullQualified := fmt.Sprintf("%s.%s", DefaultSchema, table)

	query := fmt.Sprintf("ANALYZE %s;", fullQualified)

	_, err := platform.DB.Execute(analyzeCtx, query)
	if err != nil {
		return fmt.Errorf("error analyzing table %s: %w", fullQualified, err)
	}

	return nil
}
