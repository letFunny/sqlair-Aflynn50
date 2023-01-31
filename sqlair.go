package sqlair

import (
	"github.com/canonical/sqlair/internal/expr"
)

type Stmt struct {
	SQL          string
	Args         []any
	preparedExpr *expr.PreparedExpr
}

type Q struct {
	resultExpr *expr.ResultExpr
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
	return &Stmt{SQL: preparedExpr.SQL, Args: nil, preparedExpr: preparedExpr}, nil
}

func (s *Stmt) PrepareArgs(args ...any) error {
	args, err := s.preparedExpr.Complete(args...)
	if err != nil {
		return err
	}
	s.Args = args
	return nil
}

// func (s *Stmt) Query(db *sql.DB, args ...any) (*Q, error) {
// 	var qargs []any
//
// 	if len(args) > 0 {
// 		qargs, err := s.preparedExpr.Complete(args...)
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		qargs = s.Args
// 	}
//
// 	rows, err := db.Query(s.SQl, s.Args...)
// 	if err != nil {
// 		return nil, fmt.Errorf("database error: %s", err)
// 	}
//
// }
