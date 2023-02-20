package expr_test

import (
	"github.com/canonical/sqlair/internal/expr"
	. "gopkg.in/check.v1"
)

var mTypeTests = []struct {
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
	[]any{expr.M{"district": " ", "street": " "}},
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
	[]any{Person{}, expr.M{"district": " ", "street": " "}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4, (5+7), (col1 * col2) AS calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'`,
}, {
	"complex query v2",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &M.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [M.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred']]",
	[]any{Person{}, expr.M{"district": " ", "street": " "}},
	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
}, {
	"complex query v3",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &M.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = $Person.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [M.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[[] [Person.name]] Bypass[)]]",
	[]any{Person{}, expr.M{"district": " ", "street": " "}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0)`,
}, {
	"complex query v4",
	"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $M.name) UNION SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = $M.name)",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[[] [M.name]] Bypass[) UNION SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[a.district a.street] [Address.*]] Bypass[ FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = ] Input[[] [M.name]] Bypass[)]]",
	[]any{Person{}, Address{}, expr.M{"name": " "}},
	`SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, a.district AS _sqlair_3, a.street AS _sqlair_4 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_0) UNION SELECT p.address_id AS _sqlair_5, p.id AS _sqlair_6, p.name AS _sqlair_7, a.district AS _sqlair_8, a.street AS _sqlair_9 FROM person WHERE p.name IN (SELECT name FROM table WHERE table.n = @sqlair_1)`,
}, {
	"complex query v5",
	"SELECT p.* AS &Person.*, &District.* FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = $M.name AND p.address_id = $M.address_id",
	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[] [District.*]] Bypass[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] Input[[] [M.name]] Bypass[ AND p.address_id = ] Input[[] [M.address_id]]]",
	[]any{Person{}, District{}, expr.M{"name": " ", "address_id": 100}},
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
	[]any{Address{}, expr.M{"name": " "}},
	`INSERT INTO person (name, postalcode) VALUES (@sqlair_0, @sqlair_1)`,
}, {
	"insert multi input into lone star",
	"INSERT INTO person (*) VALUES ($Person.address_id, $Person.name, $M.time)",
	"[Bypass[INSERT INTO person ] Input[[*] [Person.address_id Person.name M.time]]]",
	[]any{Person{}, expr.M{"time": int64(1234)}},
	`INSERT INTO person (address_id, name, time) VALUES (@sqlair_0, @sqlair_1, @sqlair_2)`,
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

func (s *ExprSuite) TestMtypeParsePrepare(c *C) {
	parser := expr.NewParser()
	for i, test := range mTypeTests {
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

// func (s *ExprSuite) TestValidMtypePrepare(c *C) {
// 	testList := []struct {
// 		sql      string
// 		types    []any
// 		expected string
// 	}{ {
// 		sql:	  "INSERT INTO person (*) ($Person.*, $M.time)",
// 		types:	  []any{Person{}, M{"time": int64(1234)}},
// 		expected: "INSERT INTO person (address_id, id, name, time) VALUES (@sqlair_0, @sqlair_1, @sqlair_2, @sqlair_3)",
// 	}, {
// 		sql:	  "INSERT INTO person (*) ($Person.address_id, $Person.name, $M.time)",
// 		types:	  []any{Person{}, M{"time": int64(1234)}},
// 		expected: "INSERT INTO person (address_id, name, time) VALUES (@sqlair_0, @sqlair_1, @sqlair_2)",
// 	}, {
// 		sql:	  "SELECT p.* FROM person AS p WHERE p.name = $Person.name",
// 		types:	  []any{Person{}, M{"time": int64(1234)}},
// 		expected:
// 	}

// 	{
// 		sql:      "SELECT street FROM t WHERE x = $Address.street",
// 		types:    []any{Address{}},
// 		expected: "SELECT street FROM t WHERE x = @sqlair_0",
// 	}, {
// 		sql:      "SELECT street FROM t WHERE x, y = ($Address.street, $Person.id)",
// 		types:    []any{Address{}, Person{}},
// 		expected: `SELECT street FROM t WHERE x, y = (@sqlair_0, @sqlair_1)`,
// 	}, {
// 		sql:      "SELECT p FROM t WHERE x = $Person.id",
// 		types:    []any{Person{}},
// 		expected: "SELECT p FROM t WHERE x = @sqlair_0",
// 	}, {
// 		sql:      "INSERT INTO person (*) VALUES ($Person.*)",
// 		types:    []any{Person{}},
// 		expected: `INSERT INTO person (address_id, id, name) VALUES (@sqlair_0, @sqlair_1, @sqlair_2)`,
// 	}, {
// 		sql:      "INSERT INTO person (name, id) VALUES ($Person.*)",
// 		types:    []any{Person{}},
// 		expected: `INSERT INTO person (name, id) VALUES (@sqlair_0, @sqlair_1)`,
// 	}, {
// 		sql:      "INSERT INTO person (name, postalcode) VALUES ($Person.name, $Address.id)",
// 		types:    []any{Person{}, Address{}},
// 		expected: `INSERT INTO person (name, postalcode) VALUES (@sqlair_0, @sqlair_1)`,
// 	}, {
// 		sql:      "SELECT (&Person.*, &Person.*) FROM t",
// 		types:    []any{Address{}, Person{}},
// 		expected: "SELECT (address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, address_id AS _sqlair_3, id AS _sqlair_4, name AS _sqlair_5) FROM t",
// 	}, {
// 		sql:      "SELECT * AS (&Manager.manager_name, &Person.*, &Address.id) FROM t",
// 		types:    []any{Address{}, Person{}, Manager{}},
// 		expected: "SELECT manager_name AS _sqlair_0, address_id AS _sqlair_1, id AS _sqlair_2, name AS _sqlair_3, id AS _sqlair_4 FROM t",
// 	}}

// 	for _, test := range testList {
// 		parser := expr.NewParser()
// 		parsedExpr, err := parser.Parse(test.sql)
// 		if err != nil {
// 			c.Fatal(err)
// 		}
// 		preparedExpr, err := parsedExpr.Prepare(test.types...)
// 		if err != nil {
// 			c.Fatal(err)
// 		}
// 		c.Assert(preparedExpr.SQL, Equals, test.expected)
// 	}
// }

// func (s *ExprSuite) TestMtypeComplete(c *C) {

// }

// unsupporteds

// {
// 	"star table as output",
// 	"SELECT p.* AS &Person.*",
// 	"[Bypass[SELECT ] Output[[p.*] [Person.*]]]",
// 	[]any{Person{}},
// 	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2",
// },

// {
// 	"quoted output expression",
// 	"SELECT p.* AS &Person.*, '&notAnOutputExpresion.*' AS literal FROM t",
// 	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, '&notAnOutputExpresion.*' AS literal FROM t]]",
// 	[]any{Person{}},
// 	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, '&notAnOutputExpresion.*' AS literal FROM t",
// }

// {
// 	"star as output",
// 	"SELECT * AS &Person.* FROM t",
// 	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM t]]",
// 	[]any{Person{}},
// 	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM t",
// }

// {
// 	"star as output and quote",
// 	"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
// 	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[ FROM person WHERE name = 'Fred']]",
// 	[]any{Person{}},
// 	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name = 'Fred'",
// }

// {
// 	"star output and quote",
// 	"SELECT &Person.* FROM person WHERE name = 'Fred'",
// 	"[Bypass[SELECT ] Output[[] [Person.*]] Bypass[ FROM person WHERE name = 'Fred']]",
// 	[]any{Person{}},
// 	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2 FROM person WHERE name = 'Fred'",
// }

// {
// 	"two star as outputs and quote",
// 	"SELECT * AS &Person.*, a.* AS &Address.* FROM person, address a WHERE name = 'Fred'",
// 	"[Bypass[SELECT ] Output[[*] [Person.*]] Bypass[, ] Output[[a.*] [Address.*]] Bypass[ FROM person, address a WHERE name = 'Fred']]",
// 	[]any{Person{}, Address{}},
// 	"SELECT address_id AS _sqlair_0, id AS _sqlair_1, name AS _sqlair_2, a.district AS _sqlair_3, a.id AS _sqlair_4, a.street AS _sqlair_5 FROM person, address a WHERE name = 'Fred'",
// }

// {
// 	"join v1",
// 	"SELECT p.* AS &Person.*, m.* AS &Manager.* FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred'",
// 	"[Bypass[SELECT ] Output[[p.*] [Person.*]] Bypass[, ] Output[[m.*] [Manager.*]] Bypass[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred']]",
// 	[]any{Person{}, Manager{}},
// 	"SELECT p.address_id AS _sqlair_0, p.id AS _sqlair_1, p.name AS _sqlair_2, m.manager_name AS _sqlair_3 FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred'",
// }

// {
// 	"insert v1",
// 	"INSERT INTO person (*) VALUES ($Person.*)",
// 	"[Bypass[INSERT INTO person ] Input[[*] [Person.*]]]",
// 	[]any{Person{}},
// 	`INSERT INTO person (address_id, id, name) VALUES (@sqlair_0, @sqlair_1, @sqlair_2)`,
// },

// {
// 	"insert v2",
// 	"INSERT INTO person (name, id) VALUES ($Person.*)",
// 	"[Bypass[INSERT INTO person ] Input[[name id] [Person.*]]]",
// 	[]any{Person{}},
// 	`INSERT INTO person (name, id) VALUES (@sqlair_0, @sqlair_1)`,
// }
