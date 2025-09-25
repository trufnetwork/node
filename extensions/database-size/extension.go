package database_size

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

const ExtensionName = "database_size"

// InitializeDatabaseSizePrecompile initializes the database_size extension precompiles
func InitializeDatabaseSizePrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	// Return a simple precompile that just registers the extension
	// The actual functionality is provided by the ACTION implementations
	return precompiles.Precompile{
		// No special lifecycle hooks needed for this extension
		OnStart: func(ctx context.Context, app *common.App) error {
			if app.Service != nil && app.Service.Logger != nil {
				logger := app.Service.Logger.New(ExtensionName)
				logger.Info("database_size extension initialized", "alias", alias)
			}
			return nil
		},
	}, nil
}

// Helper functions for direct database size queries
// These are used by the ACTION implementations in the migrations

// GetDatabaseSize returns the total database size using PostgreSQL's pg_database_size function
func GetDatabaseSize(ctx context.Context, db sql.DB) (int64, error) {
	result, err := db.Execute(ctx, "SELECT pg_database_size('kwild')")
	if err != nil {
		return 0, fmt.Errorf("failed to get database size: %w", err)
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return 0, fmt.Errorf("no database size result returned")
	}

	size, ok := result.Rows[0][0].(int64)
	if !ok {
		return 0, fmt.Errorf("database size result is not int64: %T", result.Rows[0][0])
	}

	return size, nil
}

// GetDatabaseSizePretty returns the database size in human-readable format
func GetDatabaseSizePretty(ctx context.Context, db sql.DB) (string, error) {
	result, err := db.Execute(ctx, "SELECT pg_size_pretty(pg_database_size('kwild'))")
	if err != nil {
		return "", fmt.Errorf("failed to get pretty database size: %w", err)
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return "", fmt.Errorf("no pretty database size result returned")
	}

	prettySize, ok := result.Rows[0][0].(string)
	if !ok {
		return "", fmt.Errorf("pretty database size result is not string: %T", result.Rows[0][0])
	}

	return prettySize, nil
}

// TableSizeResult represents the size information for a table
type TableSizeResult struct {
	TableName  string `json:"table_name"`
	SizeBytes  int64  `json:"size_bytes"`
	SizePretty string `json:"size_pretty"`
}