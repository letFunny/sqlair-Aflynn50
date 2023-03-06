package sqlair_test

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/canonical/sqlair"
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

type Manager Person

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

func sqlairPersonAndAddressDB() (string, *sql.DB, error) {
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
	var people = []Person{
		{ID: 30, Fullname: "Fred", PostalCode: "1000"},
		{ID: 20, Fullname: "Mark", PostalCode: "1500"},
		{ID: 0, Fullname: "Mary", PostalCode: "3500"},
		{ID: 35, Fullname: "James", PostalCode: "4500"},
	}

	var addresses = []Address{
		{ID: 25, District: "Happy Land", Street: "Main Street"},
		{ID: 30, District: "Sad World", Street: "Church Road"},
		{ID: 10, District: "Ambivilent Commons", Street: "Station Lane"},
	}

	db, err := setupDB()
	if err != nil {
		return "", nil, err
	}

	_, err = db.Exec(create)
	if err != nil {
		return "", nil, err
	}

	var inserts []*sqlair.Statement
	var args []any

	stmt, err := sqlair.Prepare("INSERT INTO person (*) VALUES ($Person.*);", Person{})
	if err != nil {
		return "", nil, err
	}

	for _, p := range people {
		inserts = append(inserts, stmt)
		args = append(args, p)
	}

	stmt, err = sqlair.Prepare("INSERT INTO address (*) VALUES ($Address.*);", Address{})
	if err != nil {
		return "", nil, err
	}

	for _, a := range addresses {
		inserts = append(inserts, stmt)
		args = append(args, a)
	}

	sqlairDB := sqlair.NewDB(db)

	for i, s := range inserts {
		_, err := sqlairDB.Exec(s, args[i])
		if err != nil {
			return "", nil, err
		}
	}
	return drop, db, nil
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
		"INSERT INTO person VALUES ('James', 35, '4500', 'james@email.com');",
		"INSERT INTO person VALUES ('Sam', 32, '1000', 'sam@email.com');",
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
		summary  string
		query    string
		types    []any
		inputs   []any
		outputs  [][]any
		expected [][]any
	}{{
		summary:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}, {&Person{}, &Address{}}},
		expected: [][]any{{&Person{ID: 30}, &Address{ID: 25}}, {&Person{ID: 30}, &Address{ID: 30}}, {&Person{ID: 30}, &Address{ID: 10}}, {&Person{ID: 20}, &Address{ID: 25}}},
	}, {
		summary:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		outputs:  [][]any{{&Person{}}, {&Person{}}, {&Person{}}, {&Person{PostalCode: "6000"}}},
		expected: [][]any{{&Person{30, "Fred", "1000"}}, {&Person{20, "Mark", "1500"}}, {&Person{0, "Mary", "3500"}}, {&Person{35, "James", "4500"}}},
	}, {
		summary:  "select multiple with extras",
		query:    "SELECT name, * AS (&Person.*, &Address.id, &Manager.*), id FROM person WHERE id = $Address.id",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Address{ID: 30}},
		outputs:  [][]any{{&Person{}, &Address{}, &Manager{}}},
		expected: [][]any{{&Person{30, "Fred", "1000"}, &Address{ID: 30}, &Manager{30, "Fred", "1000"}}},
	}, {
		summary:  "select with renaming",
		query:    "SELECT (name, postcode) AS (&Address.street, &Address.district) FROM person WHERE id = $Manager.id",
		types:    []any{Address{}, Manager{}},
		inputs:   []any{Manager{ID: 30}},
		outputs:  [][]any{{&Address{}}},
		expected: [][]any{{&Address{Street: "Fred", District: "1000"}}},
	}, {
		summary:  "select into star struct",
		query:    "SELECT (name, postcode) AS &Person.* FROM person WHERE postcode IN ($Manager.postcode, $Address.district)",
		types:    []any{Person{}, Address{}, Manager{}},
		inputs:   []any{Manager{PostalCode: "1000"}, Address{District: "2000"}},
		outputs:  [][]any{{&Person{}}},
		expected: [][]any{{&Person{Fullname: "Fred", PostalCode: "1000"}}},
	}}

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

func (s *PackageSuite) TestAll(c *C) {
	var tests = []struct {
		summery  string
		query    string
		types    []any
		inputs   []any
		expected [][]any
	}{{
		summery:  "double select with name clash",
		query:    "SELECT p.id AS &Person.*, a.id AS &Address.* FROM person AS p, address AS a",
		types:    []any{Person{}, Address{}},
		inputs:   []any{},
		expected: [][]any{{Person{ID: 30}, Address{ID: 25}}, {Person{ID: 30}, Address{ID: 30}}, {Person{ID: 30}, Address{ID: 10}}, {Person{ID: 20}, Address{ID: 25}}},
	}, {
		summery:  "simple select person",
		query:    "SELECT * AS &Person.* FROM person",
		types:    []any{Person{}},
		inputs:   []any{},
		expected: [][]any{{Person{30, "Fred", "1000"}}, {Person{20, "Mark", "1500"}}, {Person{0, "Mary", "3500"}}, {Person{35, "James", "4500"}}},
	}}

	drop, db, err := personAndAddressDB()
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
		q, err := sqlairDB.Query(stmt)
		if err != nil {
			c.Error(err)
			continue
		}
		res, err := q.All()
		if err != nil {
			c.Error(err)
			continue
		}

		for i, es := range t.expected {
			for j, e := range es {
				c.Assert(res[i][j], DeepEquals, e)
			}
		}
	}

	_, err = db.Exec(drop)
	if err != nil {
		c.Fatal(err)
	}
}

type JujuLeaseKey struct {
	Namespace string `db:"type"`
	ModelUUID string `db:"model_uuid"`
	Lease     string `db:"name"`
}

type JujuLeaseInfo struct {
	Holder string `db:"holder"`
	Expiry int64  `db:"expiry"`
}

func JujuStoreLeaseDB() (string, *sql.DB, error) {
	create := `
CREATE TABLE lease (
	model_uuid text,
	name text,
	holder text,
	expiry integer,
	lease_type_id text
);
CREATE TABLE lease_type (
	id text,
	type text
);
`
	drop := `
drop table lease;
drop table lease_type;
`

	inserts := []string{
		"INSERT INTO lease VALUES ('uuid1', 'name1', 'holder1', 1, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid2', 'name2', 'holder2', 4, 'type_id1');",
		"INSERT INTO lease VALUES ('uuid3', 'name3', 'holder3', 7, 'type_id2');",
		"INSERT INTO lease_type VALUES ('type_id1', 'type1');",
		"INSERT INTO lease_type VALUES ('type_id2', 'type2');",
	}

	db, err := createExampleDB(create, inserts)
	if err != nil {
		return "", nil, err
	}
	return drop, db, nil

}

func (s *PackageSuite) TestJujuStore(c *C) {
	var tests = []struct {
		summery  string
		query    string
		types    []any
		inputs   []any
		expected [][]any
	}{{
		summery: "juju store lease group query",
		query: `
SELECT (t.type, l.model_uuid, l.name) AS &JujuLeaseKey.*, (l.holder, l.expiry) AS &JujuLeaseInfo.*
FROM   lease l JOIN lease_type t ON l.lease_type_id = t.id
WHERE  t.type = $JujuLeaseKey.type
AND    l.model_uuid = $JujuLeaseKey.model_uuid`,
		types:    []any{JujuLeaseKey{}, JujuLeaseInfo{}},
		inputs:   []any{JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1"}},
		expected: [][]any{{JujuLeaseKey{Namespace: "type1", ModelUUID: "uuid1", Lease: "name1"}, JujuLeaseInfo{Holder: "holder1", Expiry: 1}}},
	}}

	drop, db, err := JujuStoreLeaseDB()
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
		res, err := q.All()
		if err != nil {
			c.Error(err)
			continue
		}

		c.Assert(res, DeepEquals, t.expected)
	}

	_, err = db.Exec(drop)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *PackageSuite) TestCommonErrors(c *C) {
	var tests = []struct {
		summery string
		query   string
		types   []any
		inputs  []any
		err     string
	}{{
		summery: "unqualified type",
		query:   "SELECT &Person FROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		err:     "cannot parse expression: column 14: type not qualified",
	}, {
		summery: "",
		query:   "SELECT &Person.nameFROM person",
		types:   []any{Person{}},
		inputs:  []any{},
		err:     `cannot prepare expression: type Person has no "nameFROM" db tag`,
	}}

	drop, db, err := personAndAddressDB()
	if err != nil {
		c.Fatal(err)
	}

	sqlairDB := sqlair.NewDB(db)

	for _, t := range tests {
		stmt, err := sqlair.Prepare(t.query, t.types...)
		if err != nil {
			c.Assert(err.Error(), Equals, t.err)
			continue
		}
		_, err = sqlairDB.Query(stmt, t.inputs...)
		if err != nil {
			c.Assert(err.Error(), Equals, t.err)
			continue
		}
		c.Errorf("expected %s to throw an error, none was thrown", t.summery)
	}
	_, err = db.Exec(drop)
	if err != nil {
		c.Fatal(err)
	}
}
