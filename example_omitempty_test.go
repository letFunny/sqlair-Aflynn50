package sqlair_test

import (
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair"
)

func Example_omitempty() {
	type Person struct {
		ID         int    `db:"id,omitempty"`
		Fullname   string `db:"name"`
		PostalCode int    `db:"postal_code"`
	}

	fred := Person{Fullname: "Fred", PostalCode: 1000}
	mark := Person{Fullname: "Mark", PostalCode: 1500}
	mary := Person{Fullname: "Mary", PostalCode: 3500}
	dave := Person{Fullname: "James", PostalCode: 4500}
	allPeople := []Person{fred, mark, mary, dave}

	sqldb, err := sql.Open("sqlite3", "file:exampleOmitTest.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	db := sqlair.NewDB(sqldb)

	createPerson := sqlair.MustPrepare(`
		CREATE TABLE person (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name text,
			postal_code integer
		);
	`)
	err = db.Query(nil, createPerson).Run()
	if err != nil {
		panic(err)
	}
	defer func() {
		stmt, err := sqlair.Prepare("DROP TABLE person")
		if err != nil {
			panic(err)
		}
		err = db.Query(nil, stmt).Run()
		if err != nil {
			panic(err)
		}
	}()

	insertPerson := sqlair.MustPrepare("INSERT INTO person (*) VALUES ($Person.*)", Person{})
	for _, p := range allPeople {
		err := db.Query(nil, insertPerson, p).Run()
		if err != nil {
			panic(err)
		}
	}

	outPeople := []Person{}
	getAllPeople := sqlair.MustPrepare("SELECT * AS &Person.* FROM person ORDER BY id", Person{})
	err = db.Query(nil, getAllPeople).GetAll(&outPeople)
	if err != nil {
		panic(err)
	}
	fmt.Println(outPeople)

	// Output:
	// [{1 Fred 1000} {2 Mark 1500} {3 Mary 3500} {4 James 4500}]
}
