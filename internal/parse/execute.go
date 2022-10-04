package parse

import (
	"database/sql"
	"fmt"
)

type ResultExpr struct {
	parsed *ParsedExpr
	rows   *sql.Rows
}

func (pe *PreparedExpr) Exec(db *sql.DB) (*ResultExpr, error) {
	rows, err := db.Query(pe.sql, pe.inputArgs...)
	if err != nil {
		return nil, fmt.Errorf("cannot exec: %s database error: %s", pe.sql, err)
	}
	return &ResultExpr{pe.parsed, rows}, nil
}
