package sqlair

import "database/sql"

type Stmt struct {
	SQL          string
	Args         []any
	preparedExpr *PreparedExpr
}

type Q struct {
	resultExpr *resultExpr
}

func Prep(input string, args ...any) (*Stmt, err) {
	var p = NewParser()
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

func (s *Stmt) PrepareArgs() ([]any, error) {
	args, err := s.preparedExpr.Complete(args...)
	if err != nil {
		return nil, err
	}
	s.Args = args
}

func (s *Stmt) Query(db *sql.DB, args ...any) *Q {
	var qargs []any

	if len(args) > 0 {
		qargs, err := s.preparedExpr.Complete(args...)
		if err != nil {
			return nil, err
		}
	} else {
		qargs = s.Args
	}

	db.Query
}
