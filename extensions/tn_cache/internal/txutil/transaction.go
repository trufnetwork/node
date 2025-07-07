package txutil

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// WithTransaction executes an operation within a database transaction
// It automatically handles transaction lifecycle including commit/rollback
func WithTransaction(ctx context.Context, db sql.DB, logger log.Logger, operation func(tx sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	
	// Ensure rollback on error
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				logger.Error("failed to rollback transaction", "error", rbErr)
			}
		}
	}()
	
	// Execute the operation
	if err = operation(tx); err != nil {
		return err
	}
	
	// Commit on success
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	
	return nil
}