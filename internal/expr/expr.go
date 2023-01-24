package expr

func ParseAndPrepare(input string, structs ...any) (*PreparedExpr, error) {
	var p = NewParser()
	parsedExpr, err := p.Parse(input)
	if err != nil {
		return nil, err
	}
	return parsedExpr.Prepare(structs...)
}
