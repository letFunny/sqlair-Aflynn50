package sqlair

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/canonical/sqlair/internal/expr"
)

// M is a type that, as with other map types, can be used in SQLair input and
// output expressions. It can be used in queries to pass arbitrary values
// referenced by their key.
type M map[string]any

var ErrNoRows = sql.ErrNoRows
var ErrTXDone = sql.ErrTxDone

var stmtIDCount int64
var dbIDCount int64

type dbID = int64
type stmtID = int64

// A SQLair Statement is prepared on a database when a Query method is run on a
// DB/TX. The prepared statement is then stored in the stmtDBCache and a flag
// is set in dbStmtCache.
// A finalizer function is set on the Statement when it is placed in the cache.
// On garbage collection, the finalizer cycles through the open databases in
// the cache and closes each matching sql.Stmt. The finalizer then removes the
// stmtID from stmtDBCache and dbStmtCache.
// Similarly, a finalizer is set on the SQLair DB which closes all statements
// prepared on the DB and then the sql.DB itself. It removes the dbID from
// dbStmtCache and stmtDBCache.
var stmtDBCache = make(map[stmtID]map[dbID]*sql.Stmt)
var dbStmtCache = make(map[dbID]map[stmtID]bool)
var cacheMutex sync.RWMutex

// Statement represents a verified SQLair statement ready to be run on a DB.
// A statement can be used with any DB.
type Statement struct {
	cacheID stmtID
	pe      *expr.PreparedExpr
}

// stmtFinalizer removes a Statement from the statement caches and closes it.
func stmtFinalizer(s *Statement) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	dbCache := stmtDBCache[s.cacheID]
	for dbCacheID, sqlstmt := range dbCache {
		sqlstmt.Close()
		delete(dbStmtCache[dbCacheID], s.cacheID)
	}
	delete(stmtDBCache, s.cacheID)
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

	cacheID := atomic.AddInt64(&stmtIDCount, 1)
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	stmtDBCache[cacheID] = make(map[dbID]*sql.Stmt)
	var s = &Statement{pe: preparedExpr, cacheID: cacheID}
	runtime.SetFinalizer(s, stmtFinalizer)
	return s, nil
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
	cacheID dbID
	sqldb   *sql.DB
}

// dbFinalizer closes and removes from the cache all statements prepared on db.
// It then closes the associated sql.DB.
func dbFinalizer(db *DB) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	stmtCache := dbStmtCache[db.cacheID]
	for stmtCacheID, _ := range stmtCache {
		dbCache := stmtDBCache[stmtCacheID]
		dbCache[db.cacheID].Close()
		delete(dbCache, db.cacheID)
	}
	delete(dbStmtCache, db.cacheID)
	db.sqldb.Close()
}

// NewDB creates a new SQLair DB from a sql.DB.
func NewDB(sqldb *sql.DB) *DB {
	cacheID := atomic.AddInt64(&dbIDCount, 1)
	cacheMutex.Lock()
	dbStmtCache[cacheID] = make(map[stmtID]bool)
	cacheMutex.Unlock()
	var db = DB{sqldb: sqldb, cacheID: cacheID}
	runtime.SetFinalizer(&db, dbFinalizer)
	return &db
}

// PlainDB returns the underlying database object.
func (db *DB) PlainDB() *sql.DB {
	return db.sqldb
}

// Query represents a query on a database. It is designed to be run once.
type Query struct {
	qe      *expr.QueryExpr
	sqlstmt *sql.Stmt
	ctx     context.Context
	err     error
	tx      *TX // tx is only set for queries in transactions.
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
	close   func() error
}

// Query takes a context, prepared SQLair Statement and the structs mentioned
// in the query arguments. The query is run on the database when one of Iter,
// Run, Get or GetAll is executed on the Query.
func (db *DB) Query(ctx context.Context, s *Statement, inputArgs ...any) *Query {
	if ctx == nil {
		ctx = context.Background()
	}

	sqlstmt, err := db.prepareStmt(ctx, db.sqldb, s)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	qe, err := s.pe.Query(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	return &Query{sqlstmt: sqlstmt, qe: qe, ctx: ctx, err: nil}
}

// prepareSubstrate is an object that queries can be prepared on, e.g. a sql.DB
// or sql.Conn. It is used in prepareStmt.
type prepareSubstrate interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

// prepareStmt prepares a Statement on a prepareSubstrate. It first checks in
// the cache to see if it has already been prepared on the DB.
// The prepareSubstrate must be assosiated with the same DB that prepareStmt is
// a method of.
func (db *DB) prepareStmt(ctx context.Context, ps prepareSubstrate, s *Statement) (*sql.Stmt, error) {
	var err error
	cacheMutex.RLock()
	// The statement ID is only removed from the cache when the finalizer is
	// run, so it is always in stmtDBCache.
	sqlstmt, ok := stmtDBCache[s.cacheID][db.cacheID]
	cacheMutex.RUnlock()
	if !ok {
		sqlstmt, err = ps.PrepareContext(ctx, s.pe.SQL())
		if err != nil {
			return nil, err
		}
		cacheMutex.Lock()
		// Check if a statement has been inserted by someone else since we last
		// checked.
		sqlstmtAlt, ok := stmtDBCache[s.cacheID][db.cacheID]
		if ok {
			sqlstmt.Close()
			sqlstmt = sqlstmtAlt
		} else {
			stmtDBCache[s.cacheID][db.cacheID] = sqlstmt
			dbStmtCache[db.cacheID][s.cacheID] = true
		}
		cacheMutex.Unlock()
	}
	return sqlstmt, nil
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
	if q.tx != nil && q.tx.isDone() {
		return &Iterator{err: ErrTXDone}
	}

	var result sql.Result
	var rows *sql.Rows
	var err error
	var cols []string
	var close func() error
	sqlstmt := q.sqlstmt
	if q.tx != nil {
		sqlstmt = q.tx.sqltx.Stmt(q.sqlstmt)
		close = sqlstmt.Close
	}
	if q.qe.HasOutputs() {
		rows, err = sqlstmt.QueryContext(q.ctx, q.qe.QueryArgs()...)
		if err == nil {
			cols, err = rows.Columns()
		}
	} else {
		result, err = sqlstmt.ExecContext(q.ctx, q.qe.QueryArgs()...)
	}
	if err != nil {
		if close != nil {
			close()
		}
		return &Iterator{qe: q.qe, err: err}
	}

	return &Iterator{qe: q.qe, rows: rows, cols: cols, err: err, result: result, close: close}
}

// Next prepares the next row for Get.
// If an error occurs during iteration it will be returned with Iter.Close.
func (iter *Iterator) Next() bool {
	iter.started = true
	if iter.err != nil || iter.rows == nil {
		return false
	}
	if !iter.rows.Next() {
		if iter.close != nil {
			err := iter.close()
			iter.close = nil
			if iter.err == nil {
				iter.err = err
			}
		}
		return false
	}
	return true
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
	var cerr error
	if iter.close != nil {
		cerr = iter.close()
		iter.close = nil
	}
	iter.started = true
	if iter.rows == nil {
		return iter.err
	}
	err := iter.rows.Close()
	iter.rows = nil
	if iter.err != nil {
		return iter.err
	}
	if err == nil {
		err = cerr
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
// execution. If no result is set Result returns nil.
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
	if !q.qe.HasOutputs() && len(sliceArgs) > 0 {
		return fmt.Errorf("output variables provided but not referenced in query")
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
	sqltx   *sql.Tx
	sqlconn *sql.Conn
	db      *DB
	done    int32
}

func (tx *TX) isDone() bool {
	return atomic.LoadInt32(&tx.done) == 1
}

func (tx *TX) setDone() error {
	if !atomic.CompareAndSwapInt32(&tx.done, 0, 1) {
		return ErrTXDone
	}
	return nil
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context, opts *TXOptions) (*TX, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sqlconn, err := db.sqldb.Conn(ctx)
	if err != nil {
		return nil, err
	}
	sqltx, err := sqlconn.BeginTx(ctx, opts.plainTXOptions())
	if err != nil {
		return nil, err
	}
	return &TX{sqltx: sqltx, sqlconn: sqlconn, db: db}, nil
}

// Commit commits the transaction.
func (tx *TX) Commit() error {
	err := tx.setDone()
	if err == nil {
		err = tx.sqltx.Commit()
	}
	if cerr := tx.sqlconn.Close(); err == nil {
		err = cerr
	}
	return err
}

// Rollback aborts the transaction.
func (tx *TX) Rollback() error {
	err := tx.setDone()
	if err == nil {
		err = tx.sqltx.Rollback()
	}
	if cerr := tx.sqlconn.Close(); err == nil {
		err = cerr
	}
	return err
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
	if tx.isDone() {
		return &Query{ctx: ctx, err: ErrTXDone}
	}

	sqlstmt, err := tx.db.prepareStmt(ctx, tx.sqlconn, s)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	qe, err := s.pe.Query(inputArgs...)
	if err != nil {
		return &Query{ctx: ctx, err: err}
	}

	return &Query{sqlstmt: sqlstmt, qe: qe, tx: tx, ctx: ctx, err: nil}
}
