package sqlair

import (
	"database/sql"

	"github.com/canonical/sqlair/internal/expr"
)

type Stmt struct {
	pe *expr.PreparedExpr
}

type Q struct {
	re *expr.ResultExpr
}

func Prepare(input string, args ...any) (*Stmt, error) {
	var p = expr.NewParser()
	parsedExpr, err := p.Parse(input)
	if err != nil {
		return nil, err
	}
	preparedExpr, err := parsedExpr.Prepare(args...)
	if err != nil {
		return nil, err
	}
	return &Stmt{pe: preparedExpr}, nil
}

func (s *Stmt) SQL() string {
	return s.pe.SQL
}

func (s *Stmt) ExtractArgs(args ...any) ([]any, error) {
	return s.pe.Complete(args...)
}

func (s *Stmt) Query(db *sql.DB, args ...any) (*Q, error) {
	re, err := s.pe.Query(db, args...)
	if err != nil {
		return nil, err
	}
	return &Q{re: re}, nil
}

func (s *Stmt) Exec(db *sql.DB, args ...any) (sql.Result, error) {
	return s.pe.Exec(db, args...)
}

func (q *Q) Next() (bool, error) {
	return q.re.Next()
}

func (q *Q) Decode(args ...any) error {
	return q.re.Decode(args...)
}

func (q *Q) Close() error {
	return q.re.Close()
}

func (q *Q) One(args ...any) error {
	return q.re.One(args...)
}

func (q *Q) All() ([][]any, error) {
	return q.re.All()
}
