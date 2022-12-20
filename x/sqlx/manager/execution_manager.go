package manager

import (
	"context"
	"fmt"
	"kwil/x/sqlx/models"
	"kwil/x/sqlx/sqlclient"
)

type ExecutionManager interface {
	Execute(ctx context.Context, tx *models.QueryTx, caller string) error
}

type executionManager struct {
	cache  Cache
	client *sqlclient.DB
}

func NewExecutionManager(cache Cache, client *sqlclient.DB) *executionManager {
	return &executionManager{
		cache:  cache,
		client: client,
	}
}

func (m *executionManager) Execute(ctx context.Context, tx *models.QueryTx, caller string) error {
	// Checking if the wallet has permission to execute the query
	// right now, wallets can only be default or owner
	db := m.cache.Get(tx.GetSchemaName())
	if db == nil {
		return fmt.Errorf("database %s not found", tx.Database)
	}

	role, ok := db.GetRole(db.DefaultRole)
	if !ok {
		return fmt.Errorf("failed to get default role on database %s", db.GetSchemaName())
	}

	if !role.HasPermission(tx.Query) {
		if caller != db.Owner {
			return fmt.Errorf("wallet %s does not have permission to execute query %s", caller, tx.Query)
		}
	}

	// Now we can execute the query

	executable, ok := db.GetQuery(tx.Query)
	if !ok {
		return fmt.Errorf("query %s not found", tx.Query)
	}

	executableInputs, err := executable.PrepareInputs(caller, tx.Inputs)
	if err != nil {
		return fmt.Errorf("failed to prepare inputs: %w", err)
	}

	_, err = m.client.ExecContext(ctx, executable.Statement, executableInputs...)

	return err
}