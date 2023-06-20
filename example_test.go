package sqlair_test

import (
	"database/sql"
	"fmt"

	"github.com/canonical/sqlair"

	_ "github.com/mattn/go-sqlite3"
)

type Location struct {
	ID   int    `db:"room_id"`
	Name string `db:"room_name"`
}

type Employee struct {
	ID     int    `db:"id"`
	TeamID int    `db:"team_id"`
	Name   string `db:"name"`
}

type Team struct {
	ID   int    `db:"id"`
	Name string `db:"team_name"`
}

func Example() {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}

	db := sqlair.NewDB(sqldb)
	create := sqlair.MustPrepare(`
	CREATE TABLE locations (
		id integer,
		room_name text,
	);
	CREATE TABLE employees (
		id integer,
		team_id integer,
		name text
	);
	CREATE TABLE teams (
		id integer,
		room_id integer,
		team_name text
	)`)
	err = db.Query(nil, create).Run()
	if err != nil {
		panic(err)
	}

	// Statement to populate the locations table.
	insertLocation := sqlair.MustPrepare(`
		INSERT INTO location (name, room_id) 
		VALUES ($Location.name, $Location.room_id)`,
		Location{},
	)

	var l1 = Location{ID: 1, Name: "The Basement"}
	var l2 = Location{ID: 2, Name: "Court"}
	var l3 = Location{ID: 3, Name: "The Market"}
	var l4 = Location{ID: 4, Name: "The Bar"}
	var l5 = Location{ID: 5, Name: "The Penthouse"}
	var locations = []Location{l1, l2, l3, l4, l5}
	for _, l := range locations {
		err := db.Query(nil, insertLocation, l).Run()
		if err != nil {
			panic(err)
		}
	}

	// Statement to populate the employees table.
	insertEmployee := sqlair.MustPrepare(`
		INSERT INTO employees (id, name, team_id)
		VALUES ($Employee.id, $Employee.name, $Employee.team_id);`,
		Employee{},
	)

	var e1 = Employee{ID: 1, TeamID: 1, Name: "Alastair"}
	var e2 = Employee{ID: 2, TeamID: 1, Name: "Ed"}
	var e3 = Employee{ID: 3, TeamID: 1, Name: "Marco"}
	var e4 = Employee{ID: 4, TeamID: 2, Name: "Pedro"}
	var e5 = Employee{ID: 5, TeamID: 3, Name: "Serdar"}
	var e6 = Employee{ID: 6, TeamID: 3, Name: "Lina"}
	var e7 = Employee{ID: 7, TeamID: 4, Name: "Joe"}
	var e8 = Employee{ID: 8, TeamID: 5, Name: "Ben"}
	var e9 = Employee{ID: 9, TeamID: 5, Name: "Jenny"}
	var e10 = Employee{ID: 10, TeamID: 6, Name: "Sam"}
	var e11 = Employee{ID: 11, TeamID: 7, Name: "Melody"}
	var e12 = Employee{ID: 12, TeamID: 8, Name: "Mark"}
	var e13 = Employee{ID: 13, TeamID: 8, Name: "Gustavo"}
	var employees = []Employees{e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13}
	for _, p := range people {
		err := db.Query(nil, insertEmployee, p).Run()
		if err != nil {
			panic(err)
		}
	}

	// Statement to populate the teams table.
	insertTeam := sqlair.MustPrepare(`
		INSERT INTO teams (id, name)
		VALUES ($Team.id, $Team.name);`,
		Team{},
	)

	var t1 = Team{1, 1, "Engineering"}
	var t2 = Team{2, "Management"}
	var t3 = Team{3, "Presentation Engineering"}
	var t4 = Team{4, "Marketing"}
	var t5 = Team{5, "Legal"}
	var t6 = Team{6, "HR"}
	var t7 = Team{7, "Sales"}
	var t8 = Team{8, "Leadership"}
	var teams = []Teams{t1, t2, t3, t4, t5, t6, t7, t8}
	for _, t := range teams {
		err := db.Query(nil, insertTeam, p).Run()
		if err != nil {
			panic(err)
		}
	}

	// Example 1
	// Find someone on the engineering team.

	// A map with a key type of string is used to
	// pass arguments that are not fields of structs.
	// sqlair.M is of type map[string]any but if
	// the map has a key type of string it can be used.
	selectSomeoneInTeam := sqlair.MustPrepare(`
		SELECT &Employee.*
		FROM employees
		WHERE team_id = $Team.id`,
		Employee{}, Team{},
	)

	// Get returns a single result.
	var pal = Employee{}
	team := "engineering"
	err = db.Query(nil, selectSomeoneInTeam, t1).Get(&pal)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s is on the %s team\n", pal.Name, team)

	// Example 2
	// Find out who is in location l1.
	selectPeopleInRoom := sqlair.MustPrepare(`
		SELECT &Employee.*
		FROM person
		WHERE team = $Location.team`,
		Location{}, Employee{},
	)

	// GetAll returns all the results.
	var roomDwellers = []Employee{}
	err = db.Query(nil, selectPeopleInRoom, l1).GetAll(&roomDwellers)
	if err != nil {
		panic(err)
	}

	for _, p := range roomDwellers {
		fmt.Printf("%s, ", p.Name)
	}
	fmt.Printf("are in %s\n", l1.Name)

	// Example 3
	// Print out who is in which room.
	selectPeopleAndRoom := sqlair.MustPrepare(`
		SELECT l.* AS &Location.*, (p.name, p.team) AS &Employee.*
		FROM locations AS l
		JOIN employees AS e
		ON e.team_id = l.team_id`,
		Location{}, Employee{},
	)

	// Results can be iterated through with an Iterable.
	// iter.Next prepares the next result.
	// iter.Get reads it into structs.
	// iter.Close closes the query returning any errors. It must be called after iteration is finished.
	iter := db.Query(nil, selectPeopleAndRoom).Iter()
	for iter.Next() {
		var l = Location{}
		var p = Employee{}

		err := iter.Get(&l, &p)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s is in %s\n", p.Name, l.Name)
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}

	drop := sqlair.MustPrepare(`
		DROP TABLE person;
		DROP TABLE location;`,
	)
	err = db.Query(nil, drop).Run()
	if err != nil {
		panic(err)
	}

	// Output:
	// Ed is on the engineering team
	// Ed, Alastair, Marco, are in The Basement
	// Alastair is in The Basement
	// Ed is in The Basement
	// Marco is in The Basement
	// Serdar is in Floor 2
	// Pedro is in Floor 3
	// Sam is in Floors 4 to 89
	// Ben is in Court
	// Joe is in The Market
	// Mark is in The Penthouse
}
