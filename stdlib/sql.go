package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	d1 "github.com/kofj/gorm-driver-d1"
)

var defaultTimeFields = []string{
	"created_at", "updated_at", "deleted_at",
	"creation_time", "update_time", "delete_time",
}

func init() {
	sql.Register(d1.DriverName, &Driver{})
}

// Driver implements the sql/driver.Driver interface.
var _ driver.Driver = (*Driver)(nil)

type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := d1.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &Conn{conn}, nil
}

// Conn implements the sql/driver.Conn interface.
var _ driver.Conn = (*Conn)(nil)

type Conn struct {
	*d1.Connection
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{Stmt: query, Conn: c}, nil
}

func (c *Conn) Close() error {
	c.Connection.Close()
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return &Tx{}, nil
}

// Tx implements the sql/driver.Tx interface.
// this is not required by the driver.
var _ driver.Tx = (*Tx)(nil)

type Tx struct{}

func (tx *Tx) Commit() error {
	// no-op
	return nil
}

func (tx *Tx) Rollback() error {
	// no-op
	return nil
}

// Stmt implements the sql/driver.Stmt interface.
var _ driver.Stmt = (*Stmt)(nil)

type Stmt struct {
	Stmt string
	Conn *Conn
}

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return -1
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	a := make([]interface{}, len(args))
	for i, v := range args {
		a[i] = v
	}
	var stmt = d1.ParameterizedStatement{SQL: s.Stmt, Params: a}
	result, err := s.Conn.WriteParameterizedContext(context.Background(), stmt)
	if err != nil {
		d1.Trace("%s: Exec failed(AuditlogId=%s): %+v", s.Conn.ID, result.AuditlogId, err)
		return nil, err
	}

	d1.Trace("%s: Exec OK(AuditlogId=%s): %+v", s.Conn.ID, result.AuditlogId, result)
	return &Result{&result}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	a := make([]interface{}, len(args))
	for i, v := range args {
		a[i] = v
	}
	var stmt = d1.ParameterizedStatement{SQL: s.Stmt, Params: a}
	result, err := s.Conn.WriteParameterizedContext(context.Background(), stmt)
	if err != nil {
		d1.Trace("%s: Query failed: %+v", s.Conn.ID, err)
		return nil, err
	}
	d1.Trace("%s: Query OK: %+v", s.Conn.ID, result)

	return &Rows{connId: s.Conn.ID, results: &result.Result[0].Results}, nil
}

// Result implements the sql/driver.Result interface.
var _ driver.Result = (*Result)(nil)

type Result struct {
	*d1.D1Resp
}

func (r *Result) LastInsertId() (int64, error) {
	return r.Result[0].Meta.LastRowID, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.Result[0].Meta.Changes, nil
}

// Rows implements the sql/driver.Rows interface.
var _ driver.Rows = (*Rows)(nil)

type Rows struct {
	connId  string
	results *d1.D1RespQueryResults
	index   int
}

func (r *Rows) Columns() []string {
	if len(r.results.Columns) == 0 {
		return nil
	}

	d1.Trace("%s: Columns: %+v", r.connId, r.results.Columns)
	return r.results.Columns
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) (err error) {
	if len(r.results.Rows) == 0 || r.index >= len(r.results.Rows) {
		return io.EOF
	}
	var row = r.results.Rows[r.index]
	r.index++
	d1.Trace("%s: Next: dest(%d)=%#+v", r.connId, len(dest), dest)
	d1.Trace("%s, Next: %+v, %+v", r.connId, r.results.Columns, row)
	for i := range row {
		switch row[i].(type) {
		case bool:
			dest[i] = row[i].(bool)
		case time.Time:
			dest[i] = row[i].(time.Time)
		case float64:
			fv := row[i].(float64)
			if math.Trunc(fv) == fv {
				dest[i] = int64(fv)
			} else {
				dest[i] = fv
			}
		case int64:
			dest[i] = row[i].(int64)
		case string:
			sv := row[i].(string)
			if slices.Contains(defaultTimeFields, strings.ToLower(r.results.Columns[i])) {
				dest[i], err = time.Parse(time.RFC3339Nano, sv)
				return
			}

			all := d1.IsFullyUnicodeEscaped(sv)
			d1.Trace("Next string: %s, %t", sv, all)
			if all {
				bytes, err := d1.UnescapeUnicode(sv)
				if err != nil {
					return err
				}
				dest[i] = bytes
			} else {
				dest[i] = sv
			}

		case []byte:
			dest[i] = []byte(row[i].(string))
		default:
			var err = errors.New("unsupported type")
			return err
		}
	}

	return nil
}
