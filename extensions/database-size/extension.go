package database_size

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

const (
	ExtensionName = "database_size"
	SchemaName    = "ext_database_size" // Can't be same as extension name per kwil patterns
)

// InitializeDatabaseSizePrecompile initializes the database_size extension precompiles
func InitializeDatabaseSizePrecompile(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	return precompiles.Precompile{
		Methods: []precompiles.Method{
			{
				Name:            "get_database_size",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters:      []precompiles.PrecompileValue{},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("database_size", types.IntType, false),
					},
				},
				Handler: HandleGetDatabaseSize,
			},
			{
				Name:            "get_database_size_pretty",
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Parameters:      []precompiles.PrecompileValue{},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("database_size_pretty", types.TextType, false),
					},
				},
				Handler: HandleGetDatabaseSizePretty,
			},
		},
		OnStart: func(ctx context.Context, app *common.App) error {
			if app.Service != nil && app.Service.Logger != nil {
				logger := app.Service.Logger.New(ExtensionName)
				logger.Info("database_size extension starting", "alias", alias, "schema", SchemaName)

				// Create schema and SQL functions for the extension
				if err := setupDatabaseSizeSchema(ctx, app.DB); err != nil {
					logger.Error("failed to setup database_size schema", "error", err)
					return fmt.Errorf("failed to setup database_size schema: %w", err)
				}

				logger.Info("database_size extension initialized successfully", "alias", alias)
				return nil
			}

			if err := setupDatabaseSizeSchema(ctx, app.DB); err != nil {
				return fmt.Errorf("failed to setup database_size schema: %w", err)
			}
			return nil
		},
	}, nil
}

// HandleGetDatabaseSize handles the get_database_size method
func HandleGetDatabaseSize(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	size, err := GetDatabaseSize(ctx.TxContext.Ctx, app.DB)
	if err != nil {
		return fmt.Errorf("failed to get database size: %w", err)
	}
	return resultFn([]any{size})
}

// HandleGetDatabaseSizePretty handles the get_database_size_pretty method
func HandleGetDatabaseSizePretty(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	prettySize, err := GetDatabaseSizePretty(ctx.TxContext.Ctx, app.DB)
	if err != nil {
		return fmt.Errorf("failed to get pretty database size: %w", err)
	}
	return resultFn([]any{prettySize})
}

// setupDatabaseSizeSchema creates the database_size schema and SQL functions
func setupDatabaseSizeSchema(ctx context.Context, db sql.DB) error {
	// Create schema - private schema prefixed with ext_, ignored by consensus
	if _, err := db.Execute(ctx, `CREATE SCHEMA IF NOT EXISTS `+SchemaName); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Create SQL function for get_database_size
	createGetDatabaseSizeFunc := `
		CREATE OR REPLACE FUNCTION ` + SchemaName + `.get_database_size()
		RETURNS BIGINT
		LANGUAGE sql
		AS $$
			SELECT pg_database_size('kwild')::BIGINT;
		$$;
	`
	if _, err := db.Execute(ctx, createGetDatabaseSizeFunc); err != nil {
		return fmt.Errorf("create get_database_size function: %w", err)
	}

	// Create SQL function for get_database_size_pretty
	createGetDatabaseSizePrettyFunc := `
		CREATE OR REPLACE FUNCTION ` + SchemaName + `.get_database_size_pretty()
		RETURNS TEXT
		LANGUAGE sql
		AS $$
			SELECT pg_size_pretty(pg_database_size('kwild'))::TEXT;
		$$;
	`
	if _, err := db.Execute(ctx, createGetDatabaseSizePrettyFunc); err != nil {
		return fmt.Errorf("create get_database_size_pretty function: %w", err)
	}

	return nil
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
