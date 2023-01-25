package sqlair

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/canonical/sqlair/internal/expr"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestExpr(t *testing.T) { TestingT(t) }

type PackageSuite struct{}

var _ = Suite(&PackageSuite{})

func setupDB() (*sql.DB, error) {
	return sql.Open("sqlite3", ":memory:")
}

type Address struct {
	ID       int64  `db:"id"`
	District string `db:"district"`
	Street   string `db:"street"`
}

type Person struct {
	ID         int64  `db:"id"`
	Fullname   string `db:"name"`
	PostalCode string `db:"postcode,omitempty"`
}

func createExampleDB(create string, inserts []string) (*sql.DB, error) {
	db, err := setupDB()
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(create)
	if err != nil {
		return nil, err
	}
	for _, insert := range inserts {
		_, err := db.Exec(insert)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func personAndAddressDB() (string, *sql.DB, error) {
	create := `
CREATE TABLE person (
	name text,
	id integer,
	postcode text,
	email text
);
CREATE TABLE address (
	id integer,
	district text,
	street text
);

`
	drop := `
	 drop table person;
	 drop table address;
	 `

	inserts := []string{
		"INSERT INTO person VALUES ('Fred', 30, '1000', 'fred@email.com');",
		"INSERT INTO person VALUES ('Mark', 20, '1500', 'mark@email.com');",
		"INSERT INTO person VALUES ('Mary', NULL, '3500', 'mary@email.com');",
		"INSERT INTO person VALUES ('James', 35, NULL, 'james@email.com');",
		"INSERT INTO address VALUES (25, 'Happy Land', 'Main Street');",
		"INSERT INTO address VALUES (30, 'Sad World', 'Church Road');",
		"INSERT INTO address VALUES (10, 'Ambivilent Commons', 'Station Lane');",
	}

	db, err := createExampleDB(create, inserts)
	if err != nil {
		return "", nil, err
	}
	return drop, db, nil

}

func (s *PackageSuite) TestDecode(c *C) {
	var tests = []struct {
		summery  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summery:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{PostalCode: "6000"}}},
		expected: [][]any{{&Person{30, "Fred", "1000"}}, {&Person{20, "Mark", "1500"}}, {&Person{0, "Mary", "3500"}}, {&Person{35, "James", "6000"}}},
	}, {
		summery:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}},
		expected: [][]any{{&Person{ID: 30}, &Address{ID: 25}}, {&Person{ID: 30}, &Address{ID: 30}}, {&Person{ID: 30}, &Address{ID: 10}}, {&Person{ID: 20}, &Address{ID: 25}}},
	}}

	drop, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	for _, t := range tests {
		pe, err := expr.ParseAndPrepare(t.query, t.types...)
		if err != nil {
			c.Error(err)
			continue
		}
		re, err := pe.Exec(db)
		if err != nil {
			c.Error(err)
			continue
		}
		for i, os := range t.outputs {
			ok, err := re.Next()
			if err != nil {
				c.Fatal(err)
			} else if !ok {
				c.Fatal("no more rows in query")
			}
			err = re.Decode(os...)
			if err != nil {
				c.Fatal(err)
			}
			for j, o := range os {
				c.Assert(o, DeepEquals, t.expected[i][j])
			}
		}
		re.Close()
	}

	_, err = db.Exec(drop)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *PackageSuite) TestAll(c *C) {
	var tests = []struct {
		summery  string
		query    string
		types    []any
		inputs   []any
		expected []any
	}{{
		summery:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		expected: []any{Person{30, "Fred", "1000"}, Person{20, "Mark", "1500"}, Person{0, "Mary", "3500"}, Person{35, "James", ""}},
	}}

	drop, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	for _, t := range tests {
		pe, err := expr.ParseAndPrepare(t.query, t.types...)
		if err != nil {
			c.Error(err)
			continue
		}
		re, err := pe.Exec(db)
		if err != nil {
			c.Error(err)
			continue
		}
		var people = []Person{}
		err = re.All(&people)
		if err != nil {
			c.Error(err)
			continue
		}

		for i, e := range t.expected {
			c.Assert(people[i], DeepEquals, e)
		}
	}

	_, err = db.Exec(drop)
	if err != nil {
		c.Fatal(err)
	}
}
