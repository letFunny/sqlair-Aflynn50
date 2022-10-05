package parse

import (
	"database/sql"
	"fmt"
)

type colInfo struct {
	columns    []string
	values     []any
	valuePtrs  []any
	colToIndex map[string]int
}

type ResultExpr struct {
	parsed  *ParsedExpr
	rows    *sql.Rows
	colInfo *colInfo
}

func (pe *PreparedExpr) Exec(db *sql.DB) (*ResultExpr, error) {
	rows, err := db.Query(pe.sql, pe.inputArgs...)
	if err != nil {
		return nil, fmt.Errorf("cannot exec: %s database error: %s", pe.sql, err)
	}

	// We fill in colInfo on the first call to Next to avoid its generation in
	// cases where the user disregards the results
	return &ResultExpr{pe.parsed, rows, nil}, nil
}
