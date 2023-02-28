package sqlair

import (
	"database/sql"

	"github.com/canonical/sqlair/internal/expr"
)

type M = expr.M

type Statement struct {
	pe *expr.PreparedExpr
}

type Q struct {
	re *expr.ResultExpr
}

type DB struct {
	expr.DB
}

func Prepare(input string, args ...any) (*Statement, error) {
	var p = expr.NewParser()
	parsedExpr, err := p.Parse(input)
	if err != nil {
		return nil, err
	}
	preparedExpr, err := parsedExpr.Prepare(args...)
	if err != nil {
		return nil, err
	}
	return &Statement{pe: preparedExpr}, nil
}

func (s *Statement) SQL() string {
	return s.pe.SQL
}

func NewDB(db *sql.DB) *DB {
	return &DB{expr.DB{db}}
}

func (db *DB) Query(s *Statement, args ...any) (*Q, error) {
	ce, err := s.pe.Complete(args...)
	if err != nil {
		return nil, err
	}

	re, err := db.DB.Query(ce)
	if err != nil {
		return nil, err
	}
	return &Q{re: re}, nil
}

func (db *DB) Exec(s *Statement, args ...any) (sql.Result, error) {
	ce, err := s.pe.Complete(args...)
	if err != nil {
		return nil, err
	}

	return db.DB.Exec(ce)
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
