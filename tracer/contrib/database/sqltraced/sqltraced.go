package sqltraced

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"

	t "github.com/DataDog/dd-trace-go/tracer"
	"github.com/DataDog/dd-trace-go/tracer/ext"
)

// Warning: the `name` must be different from the name used by the initial driver.
// E.g. for the mysql driver you can't use "mysql, but you can use "tracedMysql".
func Register(name, service string, driver driver.Driver, tracer *t.Tracer) {
	if driver == nil {
		panic("RegisterTracedDriver: driver is nil")
	}
	if tracer == nil {
		tracer = t.NewTracer()
	}

	td := TracedDriver{
		Driver:  driver,
		name:    name,
		service: service,
		tracer:  tracer,
	}
	// If the new tracedDriver is not registered, we do it.
	// It panics if we try to register twice the same driver.
	if !stringInSlice(name, sql.Drivers()) {
		sql.Register(name, td)
	} else {
		panic("RegisterTracedDriver: " + name + "already registered")
	}
}

// TracedDriver is a driver we use as a middleware between the database/sql package
// and the driver chosen (e.g. mysql, postgresql...).
// It implements the driver.Driver interface and add the tracing features on top
// of the driver's methods.
type TracedDriver struct {
	driver.Driver
	name    string
	service string
	tracer  *t.Tracer
}

func (d TracedDriver) Open(dsn string) (driver.Conn, error) {
	// Register the service to Datadog tracing API
	d.tracer.SetServiceInfo(d.service, d.name, ext.AppTypeDB)

	conn, err := d.Driver.Open(dsn)
	if err != nil {
		return nil, err
	}

	return TracedConn{Conn: conn, name: d.name, service: d.service, tracer: d.tracer}, nil
}

type TracedConn struct {
	driver.Conn
	name    string
	service string
	tracer  *t.Tracer
}

func (c TracedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	span := c.tracer.NewChildSpanFromContext(c.name+".connection.begin", ctx)
	span.Service = c.service
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	if connBeginTx, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return TracedTx{name: c.name, service: c.service, parent: tx, tracer: c.tracer, ctx: ctx}, nil
	}

	tx, err = c.Conn.Begin()
	if err != nil {
		return nil, err
	}

	return TracedTx{name: c.name, service: c.service, parent: tx, tracer: c.tracer, ctx: ctx}, nil
}

func (c TracedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	name := fmt.Sprintf("%s.connection.prepare", c.name)
	span := getSpan(name, c.service, query, nil, c.tracer, ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	if connPrepareCtx, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err := connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return TracedStmt{name: c.name, service: c.service, parent: stmt, tracer: c.tracer, ctx: ctx}, nil
	}

	return c.Prepare(query)
}

func (c TracedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if execer, ok := c.Conn.(driver.Execer); ok {
		return execer.Exec(query, args)
	}

	return nil, driver.ErrSkip
}

func (c TracedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	name := fmt.Sprintf("%s.connection.exec", c.name)
	span := getSpan(name, c.service, query, args, c.tracer, ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	if execContext, ok := c.Conn.(driver.ExecerContext); ok {
		res, err := execContext.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return TracedResult{name: c.name, service: c.service, parent: res, tracer: c.tracer, ctx: ctx}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Exec(query, dargs)
}

func (c TracedConn) Ping(ctx context.Context) (err error) {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		span := c.tracer.NewChildSpanFromContext(fmt.Sprintf("%s.connection.ping", c.name), ctx)
		defer func() {
			span.SetError(err)
			span.Finish()
		}()

		return pinger.Ping(ctx)
	}

	return nil
}

func (c TracedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.Queryer); ok {
		return queryer.Query(query, args)
	}

	return nil, driver.ErrSkip
}

func (c TracedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	name := fmt.Sprintf("%s.connection.query", c.name)
	span := getSpan(name, c.service, query, args, c.tracer, ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	if queryerContext, ok := c.Conn.(driver.QueryerContext); ok {
		rows, err := queryerContext.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return &TracedRows{name: c.name, service: c.service, parent: rows, tracer: c.tracer, ctx: ctx}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Query(query, dargs)
}

type TracedTx struct {
	name    string
	service string
	parent  driver.Tx
	tracer  *t.Tracer
	ctx     context.Context
}

func (t TracedTx) Commit() (err error) {
	span := t.tracer.NewChildSpanFromContext(t.name+".transaction.commit", t.ctx)
	span.Service = t.service
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	return t.parent.Commit()
}

func (t TracedTx) Rollback() (err error) {
	span := t.tracer.NewChildSpanFromContext(t.name+".transaction.rollback", t.ctx)
	span.Service = t.service
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	return t.parent.Rollback()
}

type TracedStmt struct {
	name    string
	service string
	query   string
	parent  driver.Stmt
	tracer  *t.Tracer
	ctx     context.Context
}

func (s TracedStmt) Close() (err error) {
	span := s.tracer.NewChildSpanFromContext(s.name+".statement.close", s.ctx)
	span.Service = s.service
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	return s.parent.Close()
}

func (s TracedStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s TracedStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	name := fmt.Sprintf("%s.statement.exec", s.name)
	span := getSpan(name, s.service, s.query, args, s.tracer, s.ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	res, err = s.parent.Exec(args)
	if err != nil {
		return nil, err
	}

	return TracedResult{name: s.name, service: s.service, parent: res, tracer: s.tracer, ctx: s.ctx}, nil
}

func (s TracedStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	name := fmt.Sprintf("%s.statement.query", s.name)
	span := getSpan(name, s.service, s.query, args, s.tracer, s.ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	rows, err = s.parent.Query(args)
	if err != nil {
		return nil, err
	}

	return &TracedRows{name: s.name, service: s.service, parent: rows, tracer: s.tracer, ctx: s.ctx}, nil
}

func (s TracedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	name := fmt.Sprintf("%s.statement.exec", s.name)
	span := getSpan(name, s.service, s.query, args, s.tracer, s.ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	if stmtExecContext, ok := s.parent.(driver.StmtExecContext); ok {
		res, err = stmtExecContext.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return TracedResult{name: s.name, service: s.service, parent: res, tracer: s.tracer, ctx: ctx}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Exec(dargs)
}

func (s TracedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	name := fmt.Sprintf("%s.statement.query", s.name)
	span := getSpan(name, s.service, s.query, args, s.tracer, ctx)
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	if stmtQueryContext, ok := s.parent.(driver.StmtQueryContext); ok {
		rows, err = stmtQueryContext.QueryContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return &TracedRows{name: s.name, service: s.service, parent: rows, tracer: s.tracer, ctx: ctx}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Query(dargs)
}

type TracedResult struct {
	name    string
	service string
	parent  driver.Result
	tracer  *t.Tracer
	ctx     context.Context
}

func (r TracedResult) LastInsertId() (id int64, err error) {
	span := r.tracer.NewChildSpanFromContext(r.name+".result.last_insert_id", r.ctx)
	span.Service = r.service
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	return r.parent.LastInsertId()
}

func (r TracedResult) RowsAffected() (num int64, err error) {
	span := r.tracer.NewChildSpanFromContext(r.name+".result.rows_affected", r.ctx)
	span.Service = r.service
	defer func() {
		span.SetError(err)
		span.Finish()
	}()

	return r.parent.RowsAffected()
}

type TracedRows struct {
	name    string
	service string
	rows    int
	parent  driver.Rows
	tracer  *t.Tracer
	span    *t.Span
	ctx     context.Context
}

func (r TracedRows) Columns() []string {
	return r.parent.Columns()
}

func (r TracedRows) Close() error {
	return r.parent.Close()
}

func (r *TracedRows) Next(dest []driver.Value) (err error) {
	if r.span == nil {
		r.span = r.tracer.NewChildSpanFromContext(r.name+".rows.iter", r.ctx)
		r.span.Service = r.service
	}

	defer func() {
		if err != nil {
			if err != io.EOF {
				r.span.SetError(err)
			}
			r.span.SetMeta("rows", strconv.Itoa(r.rows))
			r.span.Finish()
		}
		r.rows++
	}()

	return r.parent.Next(dest)
}