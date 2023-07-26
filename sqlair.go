package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/canonical/sqlair/internal/expr"
)

// M is a type that, as with other map types, can be used in SQLair input and
// output expressions. It can be used in queries to pass arbitrary values
// referenced by their key.
type M map[string]any

var ErrNoRows = sql.ErrNoRows

// Statement represents a verified SQLair statement ready to be run on a DB.
// A statement can be used with any DB.
type Statement struct {
	pe *expr.PreparedExpr
}

// Prepare validates SQLair expressions in the query and generates a SQLair
// statement Statement.
// typeSamples must contain an instance of every type mentioned in the
// SQLair expressions in the query. These are used only for type information.
func Prepare(query string, typeSamples ...any) (*Statement, error) {
	parser := expr.NewParser()
	parsedExpr, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	preparedExpr, err := parsedExpr.Prepare(typeSamples...)
	if err != nil {
		return nil, err
	}
	return &Statement{pe: preparedExpr}, nil
}

// MustPrepare is the same as Prepare except that it panics on error.
func MustPrepare(query string, typeSamples ...any) *Statement {
	s, err := Prepare(query, typeSamples...)
	if err != nil {
		panic(err)
	}
	return s
}

type DB struct {
	db *sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{db: db}
}

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.db
}

// querySubstrate abstracts the different surfaces that the query can be run on.
// For example, the database or a transaction.
type querySubstrate interface {
	QueryContext(ctx context.Context, sql string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sql string, args ...any) (sql.Result, error)
}

// Query represents a query on a database. It is designed to be run once.
type Query struct {
	qe  *expr.QueryExpr
	qs  querySubstrate
	ctx context.Context
	err error
}

// Iterator is used to iterate over the results of the query row by row.
// Next is used to advance to the next row and must be run before the first
// Get unless getting the Outcome.
// Close must be run once iteration is done.
type Iterator struct {
	qe      *expr.QueryExpr
	rows    *sql.Rows
	cols    []string
	err     error
	result  sql.Result
	started bool
}

// Query takes a context, prepared SQLair Statement and the structs mentioned
// in the query arguments. The query is run on the database when one of Iter,
// Run, Get or GetAll is executed on the Query.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	qe, err := s.pe.Query(inputArgs...)
	return &Query{qs: db.db, qe: qe, ctx: ctx, err: err}
}

// Run is an alias for Get that takes no arguments.
// Run is used to run a query on a database and disregard any results.
func (q *Query) Run() error {
	return q.Get()
}

// Get runs the query and decodes the first result into the provided output
// arguments.
// It returns ErrNoRows if output arguments were provided but no results were
// found.
// An Outcome struct may be provided as the first output variable to get
// information about query execution.
func (q *Query) Get(outputArgs ...any) error {
	if q.err != nil {
		return q.err
	}
	var outcome *Outcome
	if len(outputArgs) > 0 {
		if oc, ok := outputArgs[0].(*Outcome); ok {
			outcome = oc
			outputArgs = outputArgs[1:]
		}
	}
	if !q.qe.HasOutputs() && len(outputArgs) > 0 {
		return fmt.Errorf("cannot get results: output variables provided but not referenced in query")
	}

	var err error
	iter := q.Iter()
	if outcome != nil {
		err = iter.Get(outcome)
	}
	if err == nil && !iter.Next() {
		err = iter.Close()
		if err == nil && q.qe.HasOutputs() {
			err = ErrNoRows
		}
		return err
	}
	if err == nil {
		err = iter.Get(outputArgs...)
	}
	if cerr := iter.Close(); err == nil {
		err = cerr
	}
	return err
}

// Iter returns an Iterator to iterate through the results row by row.
func (q *Query) Iter() *Iterator {
	if q.err != nil {
		return &Iterator{err: q.err}
	}
	var result sql.Result
	var rows *sql.Rows
	var err error
	var cols []string
	if q.qe.HasOutputs() {
		rows, err = q.qs.QueryContext(q.ctx, q.qe.QuerySQL(), q.qe.QueryArgs()...)
		if err == nil {
			cols, err = rows.Columns()
		}
	} else {
		result, err = q.qs.ExecContext(q.ctx, q.qe.QuerySQL(), q.qe.QueryArgs()...)
	}
	return &Iterator{qe: q.qe, rows: rows, cols: cols, err: err, result: result}
}

// Next prepares the next row for Get.
// If an error occurs during iteration it will be returned with Iter.Close.
func (iter *Iterator) Next() bool {
	iter.started = true
	if iter.err != nil || iter.rows == nil {
		return false
	}
	return iter.rows.Next()
}

// Get decodes the result from the previous Next call into the provided output
// arguments.
// Before the first call of Next an Outcome struct may be passed to Get as the
// only argument to get information about query execution.
func (iter *Iterator) Get(outputArgs ...any) (err error) {
	if iter.err != nil {
		return iter.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot get result: %s", err)
		}
	}()

	if !iter.started {
		if oc, ok := outputArgs[0].(*Outcome); ok && len(outputArgs) == 1 {
			oc.result = iter.result
			return nil
		}
		return fmt.Errorf("cannot call Get before Next unless getting outcome")
	}

	if iter.rows == nil {
		return fmt.Errorf("iteration ended")
	}

	ptrs, onSuccess, err := iter.qe.ScanArgs(iter.cols, outputArgs)
	if err != nil {
		return err
	}
	if err := iter.rows.Scan(ptrs...); err != nil {
		return err
	}
	onSuccess()
	return nil
}

// Close finishes the iteration and returns any errors encountered.
func (iter *Iterator) Close() error {
	iter.started = true
	if iter.rows == nil {
		return iter.err
	}
	err := iter.rows.Close()
	iter.rows = nil
	if iter.err != nil {
		return iter.err
	}
	return err
}

// Outcome holds metadata about executed queries, and can be provided as the
// first output argument to any of the Get methods to populate it with
// information about the query execution.
type Outcome struct {
	result sql.Result
}

// Result returns a sql.Result containing information about the query
// execution.
func (o *Outcome) Result() sql.Result {
	return o.result
}

// GetAll iterates over the query and scans all rows into the provided slices.
// sliceArgs must contain pointers to slices of each of the output types.
// An Outcome struct may be provided as the first output variable to get
// information about query execution.
func (q *Query) GetAll(sliceArgs ...any) (err error) {
	if q.err != nil {
		return q.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot populate slice: %s", err)
		}
	}()

	if len(sliceArgs) > 0 {
		if outcome, ok := sliceArgs[0].(*Outcome); ok {
			outcome.result = nil
			sliceArgs = sliceArgs[1:]
		}
	}
	// Check slice are as expected using reflection.
	var slicePtrVals = []reflect.Value{}
	var sliceVals = []reflect.Value{}
	for _, ptr := range sliceArgs {
		ptrVal := reflect.ValueOf(ptr)
		if ptrVal.Kind() != reflect.Pointer {
			return fmt.Errorf("need pointer to slice, got %s", ptrVal.Kind())
		}
		if ptrVal.IsNil() {
			return fmt.Errorf("need pointer to slice, got nil")
		}
		slicePtrVals = append(slicePtrVals, ptrVal)
		sliceVal := ptrVal.Elem()
		if sliceVal.Kind() != reflect.Slice {
			return fmt.Errorf("need pointer to slice, got pointer to %s", sliceVal.Kind())
		}
		sliceVals = append(sliceVals, sliceVal)
	}

	// Iterate through the query results.
	iter := q.Iter()
	for iter.Next() {
		var outputArgs = []any{}
		for _, sliceVal := range sliceVals {
			elemType := sliceVal.Type().Elem()
			var outputArg reflect.Value
			switch elemType.Kind() {
			case reflect.Pointer:
				if elemType.Elem().Kind() != reflect.Struct {
					iter.Close()
					return fmt.Errorf("need slice of structs/maps, got slice of pointer to %s", elemType.Elem().Kind())
				}
				outputArg = reflect.New(elemType.Elem())
			case reflect.Struct:
				outputArg = reflect.New(elemType)
			case reflect.Map:
				outputArg = reflect.MakeMap(elemType)
			default:
				iter.Close()
				return fmt.Errorf("need slice of structs/maps, got slice of %s", elemType.Kind())
			}
			outputArgs = append(outputArgs, outputArg.Interface())
		}
		if err := iter.Get(outputArgs...); err != nil {
			iter.Close()
			return err
		}
		for i, outputArg := range outputArgs {
			switch k := sliceVals[i].Type().Elem().Kind(); k {
			case reflect.Pointer, reflect.Map:
				sliceVals[i] = reflect.Append(sliceVals[i], reflect.ValueOf(outputArg))
			case reflect.Struct:
				sliceVals[i] = reflect.Append(sliceVals[i], reflect.ValueOf(outputArg).Elem())
			default:
				iter.Close()
				return fmt.Errorf("internal error: output arg has unexpected kind %s", k)
			}
		}
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	for i, ptrVal := range slicePtrVals {
		ptrVal.Elem().Set(sliceVals[i])
	}

	return nil
}

// TX represents a transaction on the database. A transaction must end with a
// Commit or Rollback.
type TX struct {
	tx *sql.Tx
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := db.db.BeginTx(ctx, opts.plainTXOptions())
	return &TX{tx: tx}, err
}

// Commit commits the transaction.
func (tx *TX) Commit() error {
	return tx.tx.Commit()
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	return tx.tx.Rollback()
}

// TXOptions holds the transaction options to be used in DB.Begin.
type TXOptions struct {
	// Isolation is the transaction isolation level.
	// If zero, the driver or database's default level is used.
	Isolation sql.IsolationLevel
	ReadOnly  bool
}

func (txopts *TXOptions) plainTXOptions() *sql.TxOptions {
	if txopts == nil {
		return nil
	}
	return &sql.TxOptions{Isolation: txopts.Isolation, ReadOnly: txopts.ReadOnly}
}

// Query takes a context, prepared SQLair Statement and the structs mentioned
// in the query arguments. The query is run on the database when one of Iter,
// Run, Get or GetAll is executed on the Query.
func (tx *TX) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	qe, err := s.pe.Query(inputArgs...)
	return &Query{qs: tx.tx, qe: qe, ctx: ctx, err: err}
}
