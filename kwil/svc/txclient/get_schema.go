package txclient

import (
	"context"
	"fmt"
	commonpb "kwil/api/protobuf/common/v0/gen/go"
	txpb "kwil/api/protobuf/tx/v0/gen/go"
	"kwil/x/types/databases"
	"kwil/x/utils/serialize"
)

func (c *client) GetSchema(ctx context.Context, db *databases.DatabaseIdentifier) (*databases.Database[[]byte], error) {
	res, err := c.txs.GetSchema(ctx, &txpb.GetSchemaRequest{
		Owner: db.Owner,
		Name:  db.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	return convertDatabase(res.Database)
}

func (c *client) GetSchemaById(ctx context.Context, id string) (*databases.Database[[]byte], error) {
	res, err := c.txs.GetSchemaById(ctx, &txpb.GetSchemaByIdRequest{
		Id: id,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	return convertDatabase(res.Database)
}

func convertDatabase(db *commonpb.Database) (*databases.Database[[]byte], error) {
	// convert tables
	// convert response to database
	dbRes, err := serialize.Convert[commonpb.Database, databases.Database[[]byte]](db)
	if err != nil {
		return nil, fmt.Errorf("failed to convert response: %w", err)
	}

	return dbRes, nil
}
