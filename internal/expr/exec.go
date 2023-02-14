package expr

import (
	"context"
	"database/sql"
	"fmt"
)

type ResultExpr struct {
	outputs []loc
	rows    *sql.Rows
	rs      []any
}

type res struct {
	tag string
	val any
}

func (pe *PreparedExpr) Query(db *sql.DB, args ...any) (*ResultExpr, error) {
	return pe.QueryContext(db, context.Background(), args...)
}

func (pe *PreparedExpr) QueryContext(db *sql.DB, ctx context.Context, args ...any) (*ResultExpr, error) {
	inputArgs, err := pe.Complete(args...)
	if err != nil {
		return nil, fmt.Errorf("argument error: %s", err)
	}
	rows, err := db.QueryContext(ctx, pe.SQL, inputArgs...)
	if err != nil {
		return nil, fmt.Errorf("database error: %s", err)
	}

	return &ResultExpr{pe.outputs, rows, nil}, nil
}

func (pe *PreparedExpr) Exec(db *sql.DB, args ...any) (sql.Result, error) {
	return pe.ExecContext(db, context.Background(), args...)
}

func (pe *PreparedExpr) ExecContext(db *sql.DB, ctx context.Context, args ...any) (sql.Result, error) {
	var qargs []any

	qargs, err := pe.Complete(args...)
	if err != nil {
		return nil, err
	}

	res, err := db.ExecContext(ctx, pe.SQL, qargs...)
	if err != nil {
		return nil, fmt.Errorf("database error: %s", err)
	}
	return res, nil

}
