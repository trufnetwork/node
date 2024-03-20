package mocks

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/core/types"
)

type Engine interface {
	// CreateDataset deploys a new dataset from a schema.
	// The dataset will be owned by the caller.
	CreateDataset(ctx context.Context, tx sql.DB, schema *common.Schema, caller []byte) error
	// DeleteDataset deletes a dataset.
	// The caller must be the owner of the dataset.
	DeleteDataset(ctx context.Context, tx sql.DB, dbid string, caller []byte) error
	// Procedure executes a procedure in a dataset. It can be given
	// either a readwrite or readonly database transaction. If it is
	// given a read-only transaction, it will not be able to execute
	// any procedures that are not `view`.
	Procedure(ctx context.Context, tx sql.DB, options *common.ExecutionData) (*sql.ResultSet, error)
	// GetSchema returns the schema of a dataset.
	// It will return an error if the dataset does not exist.
	GetSchema(ctx context.Context, dbid string) (*common.Schema, error)
	// ListDatasets returns a list of all datasets on the network.
	ListDatasets(ctx context.Context, caller []byte) ([]*types.DatasetIdentifier, error)
	// Execute executes a SQL statement on a dataset.
	// It uses Kwil's SQL dialect.
	Execute(ctx context.Context, tx sql.DB, dbid, query string, values map[string]any) (*sql.ResultSet, error)
}
