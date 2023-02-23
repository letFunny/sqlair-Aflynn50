package sqlair_test

import (
	_ "github.com/mattn/go-sqlite3"

	"github.com/canonical/sqlair"
	. "gopkg.in/check.v1"
)

type M map[string]any

func (s *PackageSuite) TestDecodeMtype(c *C) {
	var tests = []struct {
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summary:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &M.id FROM person AS p, address AS a",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}, &M{"id": 0}}, {&Person{}, &M{"id": 0}}, {&Person{}, &M{"id": 0}}, {&Person{}, &M{"id": 0}}},
		expected: [][]any{{&Person{ID: 30}, &M{"id": 25}}, {&Person{ID: 30}, &M{"id": 30}}, {&Person{ID: 30}, &M{"id": 10}}, {&Person{ID: 20}, &M{"id": 25}}},
	},
	// {
	// 	summary:  "simple select person",
	// 	query:    "SELECT * AS &Person.* FROM person",
	// 	types:    []any{Person{}},
	// 	inputs:   []any{},
	// 	outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{PostalCode: "6000"}}},
	// 	expected: [][]any{{&Person{30, "Fred", "1000"}}, {&Person{20, "Mark", "1500"}}, {&Person{0, "Mary", "3500"}}, {&Person{35, "James", "4500"}}},
	// }, {
	// 	summary:  "select multiple with extras",
	// 	query:    "SELECT name, * AS (&Person.*, &Address.id, &Manager.*), id FROM person WHERE id = $Address.id",
	// 	types:    []any{Person{}, Address{}, Manager{}},
	// 	inputs:   []any{Address{ID: 30}},
	// 	outputs:  [][]any{{&Person{}, &Address{}, &Manager{}}},
	// 	expected: [][]any{{&Person{30, "Fred", "1000"}, &Address{ID: 30}, &Manager{30, "Fred", "1000"}}},
	// }, {
	// 	summary:  "select with renaming",
	// 	query:    "SELECT (name, postcode) AS (&Address.street, &Address.district) FROM person WHERE id = $Manager.id",
	// 	types:    []any{Address{}, Manager{}},
	// 	inputs:   []any{Manager{ID: 30}},
	// 	outputs:  [][]any{{&Address{}}},
	// 	expected: [][]any{{&Address{Street: "Fred", District: "1000"}}},
	// }, {
	// 	summary:  "select into star struct",
	// 	query:    "SELECT (name, postcode) AS &Person.* FROM person WHERE postcode IN ($Manager.postcode, $Address.district)",
	// 	types:    []any{Person{}, Address{}, Manager{}},
	// 	inputs:   []any{Manager{PostalCode: "1000"}, Address{District: "2000"}},
	// 	outputs:  [][]any{{&Person{}}},
	// 	expected: [][]any{{&Person{Fullname: "Fred", PostalCode: "1000"}}},
	// }
	}

	drop, db, err := sqlairPersonAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := sqlair.NewDB(db)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Error(err)
			continue
		}
		q, err := sqlairDB.Query(stmt, t.inputs...)
		if err != nil {
			c.Error(err)
			continue
		}
		for i, os := range t.outputs {
			ok, err := q.Next()
			if err != nil {
				c.Fatal(err)
			} else if !ok {
				c.Fatal("no more rows in query")
			}
			err = q.Decode(os...)
			if err != nil {
				c.Fatal(err)
			}
			for j, o := range os {
				c.Assert(o, DeepEquals, t.expected[i][j])
			}
		}
		q.Close()
	}

	_, err = db.Exec(drop)
	if err != nil {
		c.Fatal(err)
	}
}
