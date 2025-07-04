package utils

import (
	"context"

	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// MockDB implements sql.DB interface for testing
type MockDB struct {
	ExecuteFn func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error)
	BeginTxFn func(ctx context.Context) (sql.Tx, error)
}

func (m *MockDB) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	if m.ExecuteFn != nil {
		return m.ExecuteFn(ctx, stmt, args...)
	}
	return &sql.ResultSet{}, nil
}

func (m *MockDB) BeginTx(ctx context.Context) (sql.Tx, error) {
	if m.BeginTxFn != nil {
		return m.BeginTxFn(ctx)
	}
	return &MockTx{}, nil
}

// MockTx implements sql.Tx interface for testing
type MockTx struct {
	ExecuteFn  func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error)
	BeginTxFn  func(ctx context.Context) (sql.Tx, error)
	RollbackFn func(ctx context.Context) error
	CommitFn   func(ctx context.Context) error
}

func (m *MockTx) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	if m.ExecuteFn != nil {
		return m.ExecuteFn(ctx, stmt, args...)
	}
	return &sql.ResultSet{}, nil
}

func (m *MockTx) BeginTx(ctx context.Context) (sql.Tx, error) {
	if m.BeginTxFn != nil {
		return m.BeginTxFn(ctx)
	}
	return &MockTx{}, nil
}

func (m *MockTx) Rollback(ctx context.Context) error {
	if m.RollbackFn != nil {
		return m.RollbackFn(ctx)
	}
	return nil
}

func (m *MockTx) Commit(ctx context.Context) error {
	if m.CommitFn != nil {
		return m.CommitFn(ctx)
	}
	return nil
}