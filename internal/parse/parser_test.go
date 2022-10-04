package parse

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type parseHelperTest struct {
	bytef    func(byte) bool
	stringf  func(string) bool
	stringf0 func() bool
	result   []bool
	input    string
	data     []string
}

var p = NewParser()

var parseTests = []parseHelperTest{
	{bytef: p.peekByte, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.peekByte, result: []bool{false}, input: "b", data: []string{"a"}},
	{bytef: p.peekByte, result: []bool{true}, input: "a", data: []string{"a"}},

	{bytef: p.skipByte, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.skipByte, result: []bool{false}, input: "abc", data: []string{"b"}},
	{bytef: p.skipByte, result: []bool{true, true}, input: "abc", data: []string{"a", "b"}},

	{bytef: p.skipByteFind, result: []bool{false}, input: "", data: []string{"a"}},
	{bytef: p.skipByteFind, result: []bool{false, true, true}, input: "abcde", data: []string{"x", "b", "c"}},
	{bytef: p.skipByteFind, result: []bool{true, false}, input: "abcde ", data: []string{" ", " "}},

	{stringf0: p.skipSpaces, result: []bool{false}, input: "", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{false}, input: "abc    d", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{true}, input: "     abcd", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{true}, input: "  \t  abcd", data: []string{}},
	{stringf0: p.skipSpaces, result: []bool{false}, input: "\t  abcd", data: []string{}},

	{stringf: p.skipString, result: []bool{false}, input: "", data: []string{"a"}},
	{stringf: p.skipString, result: []bool{true, true}, input: "helloworld", data: []string{"hElLo", "w"}},
	{stringf: p.skipString, result: []bool{true, true}, input: "hello world", data: []string{"hello", " "}},
}

func TestRunTable(t *testing.T) {
	for _, v := range parseTests {
		p.init(v.input)
		for i := range v.result {
			var result bool
			if v.bytef != nil {
				result = v.bytef(v.data[i][0])
			}
			if v.stringf != nil {
				result = v.stringf(v.data[i])
			}
			if v.stringf0 != nil {
				result = v.stringf0()
			}
			if v.result[i] != result {
				log.Printf("Test %#v failed. Expected: '%t', got '%t'\n", v, result, v.result[i])
			}
		}
	}
}

// func TestInit(t *testing.T) {
// 	p := NewParser()
// 	expr, err := p.Parse("select foo from bar")
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, (*ParsedExpr)(nil), expr)
// }
//
// func TestOutputParser(t *testing.T) {
// 	var expr *ParsedExpr
//
// 	p := NewParser()
//
// 	expr, err := p.Parse("Select &Person From")
// 	assert.Equal(t, nil, err)
// 	assert.Equal(t, "", expr.String()) // Finish this
// }

type Address struct {
	ID int64 `db:"id"`
}

type Person struct {
	ID         int64  `db:"id"`
	Fullname   string `db:"name"`
	PostalCode int64  `db:"address_id"`
}

type Manager struct {
	Name string `db:"manager_name"`
}

type District struct {
}

type M map[string]any

func TestRound(t *testing.T) {
	var tests = []struct {
		input             string
		expectedParsed    string
		prepArgs          []any
		completeArgs      []any
		expectedCompleted string
	}{
		{
			"select p.* as &Person.*",
			"ParsedExpr[BypassPart[select ] OutputPart[Columns:p.* Target:Person.*]]",
			[]any{},
			[]any{&Person{}},
			"select p.*",
		},
		{
			"select p.* AS&Person.*",
			"ParsedExpr[BypassPart[select ] OutputPart[Columns:p.* Target:Person.*]]",
			[]any{},
			[]any{&Person{}},
			"select p.*",
		},
		{
			"select p.* as &Person.*, '&notAnOutputExpresion.*' as literal from t",
			"ParsedExpr[BypassPart[select ] " +
				"OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] " +
				"BypassPart['&notAnOutputExpresion.*'] " +
				"BypassPart[ as literal from t]]",
			[]any{},
			[]any{&Person{}},
			"select p.*, '&notAnOutputExpresion.*' as literal from t",
		},
		{
			"select * as &Person.* from t",
			"ParsedExpr[BypassPart[select ] " +
				"OutputPart[Columns:* Target:Person.*] " +
				"BypassPart[ from t]]",
			[]any{},
			[]any{&Person{}},
			"select * from t",
		},
		{
			"select foo, bar from table where foo = $Person.id",
			"ParsedExpr[BypassPart[select foo, bar from table where foo = ] " +
				"InputPart[Person.id]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select foo, bar from table where foo = ?",
		},
		{
			"select &Person from table where foo = $Address.id",
			"ParsedExpr[BypassPart[select ] OutputPart[Columns: Target:Person] " +
				"BypassPart[ from table where foo = ] " +
				"InputPart[Address.id]]",
			[]any{&Address{}},
			[]any{&Person{}, &Address{}},
			"select * from table where foo = ?",
		},
		{
			"select &Person.* from table where foo = $Address.id",
			"ParsedExpr[BypassPart[select ] " +
				"OutputPart[Columns: Target:Person.*] " +
				"BypassPart[ from table where foo = ] " +
				"InputPart[Address.id]]",
			[]any{&Address{}},
			[]any{&Person{}, &Address{}},
			"select * from table where foo = ?",
		},
		{
			"select foo, bar, &Person.id from table where foo = 'xx'",
			"ParsedExpr[BypassPart[select foo, bar, ] " +
				"OutputPart[Columns: Target:Person.id] " +
				"BypassPart[ from table where foo = ] " +
				"BypassPart['xx']]",
			[]any{},
			[]any{&Person{}},
			"select foo, bar, id from table where foo = 'xx'",
		},
		{
			"select foo, &Person.id, bar, baz, &Manager.manager_name from table where foo = 'xx'",
			"ParsedExpr[BypassPart[select foo, ] " +
				"OutputPart[Columns: Target:Person.id] " +
				"BypassPart[, bar, baz, ] " +
				"OutputPart[Columns: Target:Manager.manager_name] " +
				"BypassPart[ from table where foo = ] " +
				"BypassPart['xx']]",
			[]any{},
			[]any{&Person{}, &Manager{}},
			"select foo, id, bar, baz, manager_name from table where foo = 'xx'",
		},
		{
			"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:* Target:Person.*] " +
				"BypassPart[ FROM person WHERE name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{&Person{}},
			"SELECT * FROM person WHERE name = 'Fred'",
		},
		{
			"SELECT &Person.* FROM person WHERE name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns: Target:Person.*] " +
				"BypassPart[ FROM person WHERE name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{&Person{}},
			"SELECT * FROM person WHERE name = 'Fred'",
		},
		{
			"SELECT * AS &Person.*, a.* as &Address.* FROM person, address a WHERE name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:* Target:Person.*] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.* Target:Address.*] " +
				"BypassPart[ FROM person, address a WHERE name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{&Person{}, &Address{}},
			"SELECT *, a.* FROM person, address a WHERE name = 'Fred'",
		},
		{
			"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:a.district a.street Target:Address.*] " +
				"BypassPart[ FROM address AS a WHERE p.name = ] BypassPart['Fred']]",
			[]any{},
			[]any{&Address{}},
			"SELECT a.district, a.street FROM address AS a WHERE p.name = 'Fred'",
		},
		{
			"SELECT 1 FROM person WHERE p.name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT 1 FROM person WHERE p.name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{},
			"SELECT 1 FROM person WHERE p.name = 'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
				"(5+7), (col1 * col2) as calculated_value FROM person AS p " +
				"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.district a.street Target:Address.*] " +
				"BypassPart[, (5+7), (col1 * col2) as calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{&Person{}, &Address{}},
			"SELECT p.*, a.district, a.street, (5+7), (col1 * col2) as calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
				"WHERE p.name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.district a.street Target:Address.*] " +
				"BypassPart[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{&Person{}, &Address{}},
			"SELECT p.*, a.district, a.street FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name = 'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name in (select name from table where table.n = $Person.name)",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.district a.street Target:Address.*] " +
				"BypassPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (select name from table where table.n = ] " +
				"InputPart[Person.name] " +
				"BypassPart[)]]",
			[]any{&Person{}},
			[]any{&Person{}, &Address{}, &Person{}},
			"SELECT p.*, a.district, a.street FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (select name from table where table.n = ?)",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person WHERE p.name in (select name from table " +
				"where table.n = $Person.name) UNION " +
				"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person WHERE p.name in " +
				"(select name from table where table.n = $Person.name)",
			"ParsedExpr[BypassPart[SELECT ] OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] OutputPart[Columns:a.district a.street Target:Address.*] " +
				"BypassPart[ FROM person WHERE p.name in (select name from table where table.n = ] " +
				"InputPart[Person.name] " +
				"BypassPart[) UNION SELECT ] " +
				"OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.district a.street Target:Address.*] " +
				"BypassPart[ FROM person WHERE p.name in (select name from table where table.n = ] " +
				"InputPart[Person.name] " +
				"BypassPart[)]]",
			[]any{&Person{}, &Person{}},
			[]any{&Person{}, &Address{}, &Person{}, &Person{}, &Address{}, &Person{}},
			"SELECT p.*, a.district, a.street FROM person WHERE p.name in (select name from table where table.n = ?) UNION SELECT p.*, a.district, a.street FROM person WHERE p.name in (select name from table where table.n = ?)",
		},
		{
			"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
				"FROM person AS p JOIN person AS m " +
				"ON p.manager_id = m.id WHERE p.name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:p.* Target:Person.*] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:m.* Target:Manager.*] " +
				"BypassPart[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{&Person{}, &Manager{}},
			"SELECT p.*, m.* FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name = 'Fred'",
		},
		//{
		//	"SELECT (person.*, address.district) AS &M.* " +
		//		"FROM person JOIN address ON person.address_id = address.id " +
		//		"WHERE person.name = 'Fred'",
		//	"ParsedExpr[BypassPart[SELECT ] " +
		//		"OutputPart[Columns:person.* address.district Target:M.*] " +
		//		"BypassPart[ FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] " +
		//		"BypassPart['Fred' ]]",
		//	[]any{&M{}},
		//	[]any{&M{}},
		//},
		//{
		//	"SELECT p.*, a.district " +
		//		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		//		"WHERE p.name = $M.name",
		//	"ParsedExpr[BypassPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
		//		"InputPart[M.name]]",
		//	[]any{&M{}},
		//	[]any{&M{}},
		//},
		{
			"SELECT person.*, address.district FROM person JOIN address " +
				"ON person.address_id = address.id WHERE person.name = 'Fred'",
			"ParsedExpr[BypassPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = ] " +
				"BypassPart['Fred']]",
			[]any{},
			[]any{},
			"SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name = 'Fred'",
		},
		{
			"SELECT p FROM person WHERE p.name = $Person.name",
			"ParsedExpr[BypassPart[SELECT p FROM person WHERE p.name = ] InputPart[Person.name]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT p FROM person WHERE p.name = ?",
		},
		{
			"SELECT p.* AS &Person, a.District AS &District " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:p.* Target:Person] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.District Target:District] " +
				"BypassPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
				"InputPart[Person.name] " +
				"BypassPart[ AND p.address_id = ] " +
				"InputPart[Person.address_id]]",
			[]any{&Person{}, &Person{}},
			[]any{&Person{}, &District{}, &Person{}, &Person{}},
			"SELECT p.*, a.District FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ? AND p.address_id = ?",
		},
		{
			"SELECT p.* AS &Person, a.District AS &District " +
				"FROM person AS p INNER JOIN address AS a " +
				"ON p.address_id = $Address.id " +
				"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
			"ParsedExpr[BypassPart[SELECT ] " +
				"OutputPart[Columns:p.* Target:Person] " +
				"BypassPart[, ] " +
				"OutputPart[Columns:a.District Target:District] " +
				"BypassPart[ FROM person AS p INNER JOIN address AS a ON p.address_id = ] " +
				"InputPart[Address.id] " +
				"BypassPart[ WHERE p.name = ] " +
				"InputPart[Person.name] " +
				"BypassPart[ AND p.address_id = ] " +
				"InputPart[Person.address_id]]",
			[]any{&Address{}, &Person{}, &Person{}},
			[]any{&Person{}, &District{}, &Address{}, &Person{}, &Person{}},
			"SELECT p.*, a.District FROM person AS p INNER JOIN address AS a ON p.address_id = ? WHERE p.name = ? AND p.address_id = ?",
		},
		{
			"SELECT p.*, a.district " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name = $Person.*",
			"ParsedExpr[BypassPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ] " +
				"InputPart[Person.*]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ?",
		},
		{
			"INSERT INTO person (name) VALUES $Person.name",
			"ParsedExpr[BypassPart[INSERT INTO person (name) VALUES ] " +
				"InputPart[Person.name]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"INSERT INTO person (name) VALUES ?",
		},
		{
			"INSERT INTO person VALUES $Person.*",
			"ParsedExpr[BypassPart[INSERT INTO person VALUES ] " +
				"InputPart[Person.*]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"INSERT INTO person VALUES ?",
		},
		{
			"UPDATE person SET person.address_id = $Address.id " +
				"WHERE person.id = $Person.id",
			"ParsedExpr[BypassPart[UPDATE person SET person.address_id = ] " +
				"InputPart[Address.id] " +
				"BypassPart[ WHERE person.id = ] " +
				"InputPart[Person.id]]",
			[]any{&Address{}, &Person{}},
			[]any{&Address{}, &Person{}},
			"UPDATE person SET person.address_id = ? WHERE person.id = ?",
		},
	}

	parser := NewParser()
	for i, test := range tests {
		var parsedExpr *ParsedExpr
		var preparedExpr *PreparedExpr
		var err error
		if parsedExpr, _ = parser.Parse(test.input); parsedExpr.String() !=
			test.expectedParsed {
			t.Errorf("Test %d Failed (Parse): input: %s\nexpected: %s\nactual  : %s\n", i, test.input, test.expectedParsed, parsedExpr.String())
		}
		if preparedExpr, err = parsedExpr.Prepare(test.prepArgs...); err != nil {
			t.Errorf("Test %d Failed (Prepare): input: %s\nparsed AST: %s\nerror: %s\n",
				i, test.input, test.expectedParsed, err)
		} else if preparedExpr.sql != test.expectedCompleted {
			t.Errorf("Test %d Failed (Complete):\nsql     : '%s'\nexpected: '%s'"+
				"\nerror: %s types:%#v\n",
				i, preparedExpr.sql, test.expectedCompleted, err, test.prepArgs)
		}
	}
}

func createDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("Create table people (id int, name varchar, address_id int);")
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}
	inserts := []string{"INSERT INTO people VALUES (30, 'Fred', 1000);",
		"INSERT INTO people VALUES (20, 'Mark', 1500);",
		"INSERT INTO people VALUES (25, 'Mary', 3500);",
		"INSERT INTO people VALUES (25, 'James', 3500);"}
	for _, q := range inserts {
		_, err := db.Exec(q)
		if err != nil {
			return nil, fmt.Errorf("error inserting data: %v", err)
		}
	}

	_, err = db.Exec("commit;")
	return db, nil
}

func execDisguardingResults(db *sql.DB, query string, inputs ...any) error {
	p = NewParser()
	parseExpr, err := p.Parse(query)
	if err != nil {
		return err
	}
	prepExpr, err := parseExpr.Prepare(inputs...)
	if err != nil {
		return err
	}
	_, err = prepExpr.Exec(db)
	if err != nil {
		return err
	}
	return nil
}

//
//func sqlairCreateDb() (*sql.DB, error) {
//	var people = []*Person{
//		&Person{30, "Fred", 1000},
//		&Person{20, "Mark", 1500},
//		&Person{25, "Mary", 3500},
//		&Person{25, "James", 3500},
//	}
//
//	db, err := sql.Open("sqlite3", ":memory:")
//	if err != nil {
//		return nil, fmt.Errorf("error creating db: %v", err)
//	}
//	err = execDisguardingResults(db, "Create table people (id int, name varchar, address_id int);")
//	if err != nil {
//		return nil, fmt.Errorf("error creating table: %v", err)
//	}
//	for _, p := range people {
//		_, err = db.Exec("Create table people (id int, name varchar, address_id int);")
//		err = execDisguardingResults(db, "INSERT INTO people VALUES ($Person.id, $Person.name, $Person.address_id);", p, p, p)
//		if err != nil {
//			return nil, fmt.Errorf("error inserting value: %v", err)
//		}
//	}
//
//	_, err = db.Exec("commit;")
//
//	return db, err
//
//}

func TestScan(t *testing.T) {
	var people = []any{
		&Person{30, "Fred", 1000},
		&Person{20, "Mark", 1500},
		&Person{25, "Mary", 3500},
		&Person{25, "James", 3500},
	}
	var tests = []struct {
		index           int
		input           string
		outArgs         []any
		expectedResults []any
	}{
		{
			1,
			"select * as &Person.* from people",
			[]any{&Person{}, &Person{}, &Person{}, &Person{}},
			people,
		},
		{
			2,
			"select * as &Person.* from people where name = 'Fred'",
			[]any{&Person{}},
			[]any{&Person{30, "Fred", 1000}},
		},
		{
			3,
			"select &Person.* from people where name = 'Fred'",
			[]any{&Person{}},
			[]any{&Person{30, "Fred", 1000}},
		},
		{
			4,
			"select (id, name) as &Person.* from people where name = 'Fred'",
			[]any{&Person{}},
			[]any{&Person{30, "Fred", 0}},
		},
		{
			5,
			"select people.* as &Person.*, people.id as &Person from people where name = 'Fred'",
			[]any{&Person{}, &Person{}},
			[]any{&Person{30, "Fred", 1000}, &Person{30, "", 0}},
		},
	}
	var err error
	var parsedExpr *ParsedExpr
	var preparedExpr *PreparedExpr
	var resultExpr *ResultExpr

	database, err := createDb()
	if err != nil {
		t.Errorf(err.Error())
	}
	parser := NewParser()
	for _, test := range tests {
		if parsedExpr, err = parser.Parse(test.input); err == nil {
			if preparedExpr, err = parsedExpr.Prepare(); err == nil {
				if resultExpr, err = preparedExpr.Exec(database); err == nil {
					var i int
					var res any
					for i, res = range test.expectedResults {
						if resultExpr.Next() {
							if err = resultExpr.Scan(test.outArgs[i]); err != nil {
								t.Errorf("scan error: %s", err)
							}
							if !reflect.DeepEqual(test.outArgs[i], res) {
								t.Errorf("Test %d Failed (Scan):\n sql:%s\nparsed AST: %s\nexpected result: %#v\nactual result:   %#v",
									test.index, test.input, parsedExpr.queryParts, res, test.outArgs[i])
							}
						}
					}
					resultExpr.Close()
					if i != len(test.expectedResults)-1 {
						t.Errorf("Test %d Failed (Wrong number of results):\n expected number: %d\n, actual number: %d\n",
							test.index, len(test.expectedResults)-1, i)
					}
				} else if err != nil {
					t.Errorf("test exec error: %s", err)
				}
			} else if err != nil {
				t.Errorf("test prepared error %s", err)
			}
		} else if err != nil {
			t.Errorf("test parse error %s", err)
		}
		_, err = database.Exec("commit;")
	}
}
