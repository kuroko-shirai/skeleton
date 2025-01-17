package mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics interface {
	WriteTimingAndCounter(startTime time.Time, query string, success bool)
}

func (d *Database) WithMetrics(metrics metrics) (*Database, error) {
	if metrics == nil {
		return nil, fmt.Errorf("Возникли проблемы при создании подключения с пустым сервисом метрик. Error: %w", errors.New("invalid metrics"))
	}

	prometheus.MustRegister(sqlstats.NewStatsCollector(d.connection.DriverName(), d.connection))
	d.metrics = metrics

	return d, nil
}

func (d *Database) sendMetric(start time.Time, name string, status bool) {
	if d.metrics != nil {
		d.metrics.WriteTimingAndCounter(start, name, status)
	}
}

// Query executes a query that returns rows, typically a
// SELECT. The args are for any placeholder parameters in
// the query.
// Query uses context.Background internally; to specify the
// context, use [DB.QueryContext].
func (d *Database) Query(name, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.connection.Query(query, args...)
	d.sendMetric(start, name, err == nil)

	return rows, err
}

// QueryContext executes a query that returns rows,
// typically a SELECT. The args are for any placeholder
// parameters in the query.
func (d *Database) QueryContext(ctx context.Context, name, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.connection.QueryContext(ctx, query, args...)
	d.sendMetric(start, name, err == nil)

	return rows, err
}

// QueryContext executes a query that returns rows,
// typically a SELECT. The args are for any placeholder
// parameters in the query.
func (d *Database) QueryxContext(ctx context.Context, name, query string, args ...any) (*sqlx.Rows, error) {
	start := time.Now()
	rows, err := d.connection.QueryxContext(ctx, query, args...)
	d.sendMetric(start, name, err == nil)

	return rows, err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses context.Background internally; to specify the
// context, use [DB.ExecContext].
func (d *Database) Exec(name, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := d.connection.Exec(query, args...)
	d.sendMetric(start, name, err == nil)

	return result, err
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (d *Database) ExecContext(ctx context.Context, name, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := d.connection.ExecContext(ctx, query, args...)
	d.sendMetric(start, name, err == nil)

	return result, err
}

// MustExec (panic) runs MustExec using this database. Any
// placeholder parameters are replaced with supplied args.
func (d *Database) MustExec(name, query string, args ...any) sql.Result {
	start := time.Now()
	result := d.connection.MustExec(query, args...)
	d.sendMetric(start, name, false)

	return result
}

// MustExecContext (panic) runs MustExec using this
// database. Any placeholder parameters are replaced with
// supplied args.
func (d *Database) MustExecContext(ctx context.Context, name, query string, args ...any) sql.Result {
	start := time.Now()
	result := d.connection.MustExecContext(ctx, query, args...)
	d.sendMetric(start, name, false)

	return result
}

// QueryRow executes a query that is expected to return at
// most one row. QueryRow always returns a non-nil value.
// Errors are deferred until [Row]'s Scan method is called.
// If the query selects no rows, the [*Row.Scan] will return
// [ErrNoRows]. Otherwise, [*Row.Scan] scans the first
// selected row and discards the rest.
// QueryRow uses context.Background internally; to specify
// the context, use [DB.QueryRowContext].
func (d *Database) QueryRow(name, query string, args ...any) *sql.Row {
	start := time.Now()
	row := d.connection.QueryRow(query, args...)
	d.sendMetric(start, name, false)

	return row
}

// QueryRowContext executes a query that is expected to
// return at most one row. QueryRowContext always returns a
// non-nil value. Errors are deferred until [Row]'s Scan
// method is called. If the query selects no rows, the
// [*Row.Scan] will return [ErrNoRows]. Otherwise,
// [*Row.Scan] scans the first selected row and discards the
// rest.
func (d *Database) QueryRowContext(ctx context.Context, name, query string, args ...any) *sql.Row {
	start := time.Now()
	row := d.connection.QueryRowContext(ctx, query, args...)
	d.sendMetric(start, name, false)

	return row
}

// QueryRowx queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied
// args.
func (d *Database) QueryRowx(name, query string, args ...any) *sqlx.Row {
	start := time.Now()
	row := d.connection.QueryRowx(query, args...)
	d.sendMetric(start, name, false)

	return row
}

// QueryRowxContext queries the database and returns an
// *sqlx.Row. Any placeholder parameters are replaced with
// supplied args.
func (d *Database) QueryRowxContext(ctx context.Context, name, query string, args ...any) *sqlx.Row {
	start := time.Now()
	row := d.connection.QueryRowxContext(ctx, query, args...)
	d.sendMetric(start, name, false)

	return row
}

// Select using this DB. Any placeholder parameters are
// replaced with supplied args.
func (d *Database) Select(name string, dest interface{}, query string, args ...any) error {
	start := time.Now()
	err := d.connection.Select(dest, query, args...)
	d.sendMetric(start, name, err == nil)

	return err
}

// SelectContext using this DB. Any placeholder parameters
// are replaced with supplied args.
func (d *Database) SelectContext(ctx context.Context, name string, dest interface{}, query string, args ...any) error {
	start := time.Now()
	err := d.connection.SelectContext(ctx, dest, query, args...)
	d.sendMetric(start, name, err == nil)

	return err
}

// Get using this DB. Any placeholder parameters are
// replaced with supplied args. An error is returned if the
// result set is empty.
func (d *Database) Get(name string, dest interface{}, query string, args ...any) error {
	start := time.Now()
	err := d.connection.Get(dest, query, args...)
	d.sendMetric(start, name, err == nil)

	return err
}

// GetContext using this DB. Any placeholder parameters are
// replaced with supplied args. An error is returned if the
// result set is empty.
func (d *Database) GetContext(ctx context.Context, name string, dest interface{}, query string, args ...any) error {
	start := time.Now()
	err := d.connection.GetContext(ctx, dest, query, args...)
	d.sendMetric(start, name, err == nil)

	return err
}

func (d *Database) Preparex(query string) (*sqlx.Stmt, error) {
	return d.connection.Preparex(query)
}
