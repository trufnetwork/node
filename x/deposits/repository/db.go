// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0

package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func New(db DBTX) *Queries {
	return &Queries{db: db}
}

func Prepare(ctx context.Context, db DBTX) (*Queries, error) {
	q := Queries{db: db}
	var err error
	if q.addTxHashStmt, err = db.PrepareContext(ctx, addTxHash); err != nil {
		return nil, fmt.Errorf("error preparing query AddTxHash: %w", err)
	}
	if q.commitDepositsStmt, err = db.PrepareContext(ctx, commitDeposits); err != nil {
		return nil, fmt.Errorf("error preparing query CommitDeposits: %w", err)
	}
	if q.depositStmt, err = db.PrepareContext(ctx, deposit); err != nil {
		return nil, fmt.Errorf("error preparing query Deposit: %w", err)
	}
	if q.expireStmt, err = db.PrepareContext(ctx, expire); err != nil {
		return nil, fmt.Errorf("error preparing query Expire: %w", err)
	}
	if q.getBalanceAndSpentStmt, err = db.PrepareContext(ctx, getBalanceAndSpent); err != nil {
		return nil, fmt.Errorf("error preparing query GetBalanceAndSpent: %w", err)
	}
	if q.getHeightStmt, err = db.PrepareContext(ctx, getHeight); err != nil {
		return nil, fmt.Errorf("error preparing query GetHeight: %w", err)
	}
	if q.newWithdrawalStmt, err = db.PrepareContext(ctx, newWithdrawal); err != nil {
		return nil, fmt.Errorf("error preparing query NewWithdrawal: %w", err)
	}
	if q.setBalanceAndSpentStmt, err = db.PrepareContext(ctx, setBalanceAndSpent); err != nil {
		return nil, fmt.Errorf("error preparing query SetBalanceAndSpent: %w", err)
	}
	if q.setHeightStmt, err = db.PrepareContext(ctx, setHeight); err != nil {
		return nil, fmt.Errorf("error preparing query SetHeight: %w", err)
	}
	if q.spendStmt, err = db.PrepareContext(ctx, spend); err != nil {
		return nil, fmt.Errorf("error preparing query Spend: %w", err)
	}
	return &q, nil
}

func (q *Queries) Close() error {
	var err error
	if q.addTxHashStmt != nil {
		if cerr := q.addTxHashStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing addTxHashStmt: %w", cerr)
		}
	}
	if q.commitDepositsStmt != nil {
		if cerr := q.commitDepositsStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing commitDepositsStmt: %w", cerr)
		}
	}
	if q.depositStmt != nil {
		if cerr := q.depositStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing depositStmt: %w", cerr)
		}
	}
	if q.expireStmt != nil {
		if cerr := q.expireStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing expireStmt: %w", cerr)
		}
	}
	if q.getBalanceAndSpentStmt != nil {
		if cerr := q.getBalanceAndSpentStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing getBalanceAndSpentStmt: %w", cerr)
		}
	}
	if q.getHeightStmt != nil {
		if cerr := q.getHeightStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing getHeightStmt: %w", cerr)
		}
	}
	if q.newWithdrawalStmt != nil {
		if cerr := q.newWithdrawalStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing newWithdrawalStmt: %w", cerr)
		}
	}
	if q.setBalanceAndSpentStmt != nil {
		if cerr := q.setBalanceAndSpentStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing setBalanceAndSpentStmt: %w", cerr)
		}
	}
	if q.setHeightStmt != nil {
		if cerr := q.setHeightStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing setHeightStmt: %w", cerr)
		}
	}
	if q.spendStmt != nil {
		if cerr := q.spendStmt.Close(); cerr != nil {
			err = fmt.Errorf("error closing spendStmt: %w", cerr)
		}
	}
	return err
}

func (q *Queries) exec(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) (sql.Result, error) {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).ExecContext(ctx, args...)
	case stmt != nil:
		return stmt.ExecContext(ctx, args...)
	default:
		return q.db.ExecContext(ctx, query, args...)
	}
}

func (q *Queries) query(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) (*sql.Rows, error) {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).QueryContext(ctx, args...)
	case stmt != nil:
		return stmt.QueryContext(ctx, args...)
	default:
		return q.db.QueryContext(ctx, query, args...)
	}
}

func (q *Queries) queryRow(ctx context.Context, stmt *sql.Stmt, query string, args ...interface{}) *sql.Row {
	switch {
	case stmt != nil && q.tx != nil:
		return q.tx.StmtContext(ctx, stmt).QueryRowContext(ctx, args...)
	case stmt != nil:
		return stmt.QueryRowContext(ctx, args...)
	default:
		return q.db.QueryRowContext(ctx, query, args...)
	}
}

type Queries struct {
	db                     DBTX
	tx                     *sql.Tx
	addTxHashStmt          *sql.Stmt
	commitDepositsStmt     *sql.Stmt
	depositStmt            *sql.Stmt
	expireStmt             *sql.Stmt
	getBalanceAndSpentStmt *sql.Stmt
	getHeightStmt          *sql.Stmt
	newWithdrawalStmt      *sql.Stmt
	setBalanceAndSpentStmt *sql.Stmt
	setHeightStmt          *sql.Stmt
	spendStmt              *sql.Stmt
}

func (q *Queries) WithTx(tx *sql.Tx) *Queries {
	return &Queries{
		db:                     tx,
		tx:                     tx,
		addTxHashStmt:          q.addTxHashStmt,
		commitDepositsStmt:     q.commitDepositsStmt,
		depositStmt:            q.depositStmt,
		expireStmt:             q.expireStmt,
		getBalanceAndSpentStmt: q.getBalanceAndSpentStmt,
		getHeightStmt:          q.getHeightStmt,
		newWithdrawalStmt:      q.newWithdrawalStmt,
		setBalanceAndSpentStmt: q.setBalanceAndSpentStmt,
		setHeightStmt:          q.setHeightStmt,
		spendStmt:              q.spendStmt,
	}
}