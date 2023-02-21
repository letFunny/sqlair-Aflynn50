package expr_test

import (
	"github.com/canonical/sqlair/internal/expr"
	. "gopkg.in/check.v1"
)

var mTypeSupportedValidTests = []struct {
	summary          string
	input            string
	expectedParsed   string
	prepareArgs      []any
	expectedPrepared string
}{{
	"input v1",
	"SELECT foo, bar FROM table WHERE foo = $M.key",
	"[Bypass[SELECT foo, bar FROM table WHERE foo = ] Input[[] [M.key]]]",
	[]any{expr.M{"key": 100}},
	`SELECT foo, bar FROM table WHERE foo = @sqlair_0`,
}, {
	"input v2",
	"SELECT p FROM person WHERE p.key = $M.key",
	"[Bypass[SELECT p FROM person WHERE p.key = ] Input[[] [M.key]]]",
	[]any{expr.M{"key": 100}},
	`SELECT p FROM person WHERE p.key = @sqlair_0`,
}, {
	"input v3",
	"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.key = $M.key",
	"[Bypass[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.key = ] Input[[] [M.key]]]",
	[]any{expr.M{"key": 100}},
	`SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.key = @sqlair_0`,
}, {
	"star output and input",
	"SELECT &Person.* FROM table WHERE foo = $M.address_id",
	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM table WHERE foo = ] Input[[] [M.address_id]]]",
	[]any{Person{}, expr.M{"address_id": 100}},
	`SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM table WHERE foo = @sqlair_0`,
}, {
	"output and quote",
	"SELECT foo, bar, &M.key FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, bar, ] Output[[] [M.key]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{expr.M{"key": 100}},
	"SELECT foo, bar, key AS _sqlair_0 FROM table WHERE foo = 'xx'",
}, {
	"single star output into single map key",
	"SELECT * AS &M.key FROM person AS p WHERE foo = 'xx'",
	"[Bypass[SELECT ] Output[[*] [M.key]] Bypass[ FROM person AS p WHERE foo = 'xx']]",
	[]any{expr.M{"key": 100}},
	"SELECT key AS _sqlair_0 FROM person AS p WHERE foo = 'xx'",
}, {
	"prefixed star output into single map key",
	"SELECT p.* AS &M.key FROM person AS p WHERE foo = 'xx'",
	"[Bypass[SELECT ] Output[[p.*] [M.key]] Bypass[ FROM person AS p WHERE foo = 'xx']]",
	[]any{expr.M{"key": 100}},
	"SELECT p.key AS _sqlair_0 FROM person AS p WHERE foo = 'xx'",
}, {
	"non-star non-prefixed output into single starred map",
	"SELECT key AS &M.* FROM person AS p WHERE foo = 'xx'",
	"[Bypass[SELECT ] Output[[key] [M.*]] Bypass[ FROM person AS p WHERE foo = 'xx']]",
	[]any{expr.M{"key": 100}},
	"SELECT key AS _sqlair_0 FROM person AS p WHERE foo = 'xx'",
}, {
	"non-star prefixed output into single starred map",
	"SELECT p.key AS &M.* FROM person AS p WHERE foo = 'xx'",
	"[Bypass[SELECT ] Output[[p.key] [M.*]] Bypass[ FROM person AS p WHERE foo = 'xx']]",
	[]any{expr.M{"key": 100}},
	"SELECT p.key AS _sqlair_0 FROM person AS p WHERE foo = 'xx'",
}, {
	"two outputs and quote",
	"SELECT foo, &M.key, bar, baz, &Manager.manager_name FROM table WHERE foo = 'xx'",
	"[Bypass[SELECT foo, ] Output[[] [M.key]] Bypass[, bar, baz, ] Output[[] [Manager.manager_name]] Bypass[ FROM table WHERE foo = 'xx']]",
	[]any{expr.M{"key": 100}, Manager{}},
	"SELECT foo, key AS _sqlair_0, bar, baz, manager_name AS _sqlair_1 FROM table WHERE foo = 'xx'",
}, {
	"multicolumn output v1",
	"SELECT (a.district, a.id) AS (&Address.district, &M.id) FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.id] [Address.district M.id]] Bypass[ FROM address AS a]]",
	[]any{Address{}, expr.M{"id": 100}},
	"SELECT a.district AS _sqlair_0, a.id AS _sqlair_1 FROM address AS a",
}, {
	"multicolumn output v2",
	"SELECT (a.district, a.street) AS (&Address.district, &Address.street), a.id AS &M.id FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.street] [Address.district Address.street]] Bypass[, ] Output[[a.id] [M.id]] Bypass[ FROM address AS a]]",
	[]any{Address{}, expr.M{"id": 100}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1, a.id AS _sqlair_2 FROM address AS a",
}, {
	"multicolumn output v3",
	"SELECT (a.district, a.id) AS (&Address.district, &M.id) FROM address AS a",
	"[Bypass[SELECT ] Output[[a.district a.id] [Address.district M.id]] Bypass[ FROM address AS a]]",
	[]any{Address{}, expr.M{"id": 100}},
	"SELECT a.district AS _sqlair_0, a.id AS _sqlair_1 FROM address AS a",
}, {
	"multicolumn output v4",
	"SELECT (a.district, a.street) AS &M.* FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[a.district a.street] [M.*]] Bypass[ FROM address AS a WHERE p.name = 'Fred']]",
	[]any{expr.M{"district": "", "street": ""}},
	"SELECT a.district AS _sqlair_0, a.street AS _sqlair_1 FROM address AS a WHERE p.name = 'Fred'",
}, {
	"multicolumn output v5",
	"SELECT (&Address.street, &M.id) FROM address AS a WHERE p.name = 'Fred'",
	"[Bypass[SELECT (] Output[[] [Address.street]] Bypass[, ] Output[[] [M.id]] Bypass[) FROM address AS a WHERE p.name = 'Fred']]",
	[]any{Address{}, expr.M{"id": 100}},
	"SELECT (street AS _sqlair_0, id AS _sqlair_1) FROM address AS a WHERE p.name = 'Fred'",
}, {
	"complex query v1",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &M.*, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [M.*]] Bypass[, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, expr.M{"district": "", "street": ""}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'`,
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &M.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [M.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, expr.M{"district": "", "street": ""}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &M.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [M.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[[] [Person.name]] Bypass[)]]",
	[]any{Person{}, expr.M{"district": "", "street": ""}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0)`,
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $M.name) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $M.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[[] [M.name]] Bypass[) UNION SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[[] [M.name]] Bypass[)]]",
	[]any{Person{}, Address{}, expr.M{"name": ""}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0) UNION SELECT p.address_id AS _sqlair_5, p.id AS _sqlair_6, p.name AS _sqlair_7, a.district AS _sqlair_8, a.street AS _sqlair_9 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_1)`,
}, {
	"complex query v5",
	"SELECT p.* AS &Person.*, &District.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $M.name AND p.address_id = $M.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[] [District.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[[] [M.name]] Bypass[ AND p.address_id = ] Input[[] [M.address_id]]]",
	[]any{Person{}, District{}, expr.M{"name": "", "address_id": 100}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2,  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = @sqlair_0 AND p.address_id = @sqlair_1`,
}, {
	"complex query v6",
	"SELECT p.* AS &Person.*, FROM person AS p INNER JOIN address AS a ON p.address_id = $M.id WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, FROM person AS p INNER JOIN address AS a ON p.address_id = ] Input[[] [M.id]] Bypass[ WHERE p.name = ] Input[[] [Person.name]] Bypass[ AND p.address_id = ] Input[[] [Person.address_id]]]",
	[]any{Person{}, Address{}, expr.M{"id": 100}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, FROM person AS p INNER JOIN address AS a ON p.address_id = @sqlair_0 WHERE p.name = @sqlair_1 AND p.address_id = @sqlair_2`,
}, {
	"insert v3",
	"INSERT INTO person (name, postalcode) VALUES ($M.name, $Address.id)",
	"[Bypass[INSERT INTO person ] Input[[name postalcode] [M.name Address.id]]]",
	[]any{Address{}, expr.M{"name": ""}},
	`INSERT INTO person (name, postalcode) VALUES (@sqlair_0, @sqlair_1)`,
}, {
	"insert multi input into lone star",
	"INSERT INTO person (*) VALUES ($Person.address_id, $Person.name, $M.time)",
	"[Bypass[INSERT INTO person ] Input[[*] [Person.address_id Person.name M.time]]]",
	[]any{Person{}, expr.M{"time": int64(1234)}},
	`INSERT INTO person (address_id, name, time) VALUES (@sqlair_0, @sqlair_1, @sqlair_2)`,
}, {
	"insert multi input into prefixed lone star",
	"INSERT INTO person (p.*) VALUES ($Person.address_id, $Person.name, $M.time)",
	"[Bypass[INSERT INTO person ] Input[[p.*] [Person.address_id Person.name M.time]]]",
	[]any{Person{}, expr.M{"time": int64(1234)}},
	`INSERT INTO person (p.address_id, p.name, p.time) VALUES (@sqlair_0, @sqlair_1, @sqlair_2)`,
}, {
	"input with no space",
	"SELECT p.*, a.district FROM person AS p WHERE p.name=$M.name",
	"[Bypass[SELECT p.*, a.district FROM person AS p WHERE p.name=] Input[[] [M.name]]]",
	[]any{expr.M{"name": " "}},
	`SELECT p.*, a.district FROM person AS p WHERE p.name=@sqlair_0`,
}, {
	"update",
	"UPDATE person SET person.address_id = $Address.id WHERE person.id = $M.id",
	"[Bypass[UPDATE person SET person.address_id = ] Input[[] [Address.id]] Bypass[ WHERE person.id = ] Input[[] [M.id]]]",
	[]any{expr.M{"id": 100}, Address{}},
	`UPDATE person SET person.address_id = @sqlair_0 WHERE person.id = @sqlair_1`,
}}

func (s *ExprSuite) TestMtypeSupportedValidParsePrepare(c *C) {
	for i, test := range mTypeSupportedValidTests {
		parser := expr.NewParser()
		var (
			parsedExpr   *expr.ParsedExpr
			preparedExpr *expr.PreparedExpr
			err          error
		)
		if parsedExpr, err = parser.Parse(test.input); err != nil {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedParsed, err)
		} else if parsedExpr.String() != test.expectedParsed {
			c.Errorf("test %d failed (Parse):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedParsed, parsedExpr.String())
		}

		if preparedExpr, err = parsedExpr.Prepare(test.prepareArgs...); err != nil {
			c.Errorf("test %d failed (Prepare):\nsummary: %s\ninput:    %s\nexpected: %s\nerr: %s\n", i, test.summary, test.input, test.expectedPrepared, err)
		} else {
			c.Check(preparedExpr.SQL, Equals, test.expectedPrepared,
				Commentf("test %d failed (Prepare):\nsummary: %s\ninput: %s\nexpected: %s\nactual:   %s\n", i, test.summary, test.input, test.expectedPrepared, preparedExpr.SQL))
		}
	}
}

func (s *ExprSuite) TestUnsupportedMapStar(c *C) {
	tests := []struct {
		summary string
		input   string
		expect  string
	}{{
		"all output into map star",
		"SELECT &M.* FROM person WHERE name = 'Fred'",
		"cannot prepare expression: map type with asterisk cannot be used when no column name is specified or column name is asterisk",
	}, {
		"all output into map star from table star",
		"SELECT p.* AS &M.* FROM person WHERE name = 'Fred'",
		"cannot prepare expression: map type with asterisk cannot be used when no column name is specified or column name is asterisk",
	}, {
		"all output into map star from lone star",
		"SELECT * AS &M.* FROM person WHERE name = 'Fred'",
		"cannot prepare expression: map type with asterisk cannot be used when no column name is specified or column name is asterisk",
	},
	}
	for _, test := range tests {
		parser := expr.NewParser()
		parsedExpr, err := parser.Parse(test.input)
		if err != nil {
			c.Fatal(err)
		}
		_, err = parsedExpr.Prepare(expr.M{})
		c.Assert(err, ErrorMatches, test.expect)
	}
}
