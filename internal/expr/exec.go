package expr

import (
	"context"
	"database/sql"
	"fmt"
)

type ResultExpr struct {
	outputs []loc
	rows    *sql.Rows
	// rs stores the results from the current row
	rs []any
}

type DB struct {
	*sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{db}
}

func (db *DB) Query(ce *CompletedExpr) (*ResultExpr, error) {
	return db.QueryContext(ce, context.Background())
}

func (db *DB) QueryContext(ce *CompletedExpr, ctx context.Context) (*ResultExpr, error) {
	rows, err := db.DB.QueryContext(ctx, ce.SQL, ce.Args...)
	if err != nil {
		return nil, fmt.Errorf("database error: %s", err)
	}

	return &ResultExpr{ce.outputs, rows, nil}, nil
}

func (db *DB) Exec(ce *CompletedExpr) (sql.Result, error) {
	return db.ExecContext(ce, context.Background())
}

func (db *DB) ExecContext(ce *CompletedExpr, ctx context.Context) (sql.Result, error) {
	res, err := db.DB.ExecContext(ctx, ce.SQL, ce.Args...)
	if err != nil {
		return nil, fmt.Errorf("database error: %s", err)
	}
	return res, nil

}
