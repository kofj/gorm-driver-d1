package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	d1 "github.com/kofj/gorm-driver-d1"
	"github.com/tidwall/gjson"
)

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
	_, result, err := s.Conn.WriteParameterizedContext(context.Background(), stmt)
	if err != nil {
		d1.Trace("%s: Exec failed: %+v", s.Conn.ID, err)
		return nil, err
	}

	d1.Trace("%s: Exec OK: %+v", s.Conn.ID, result)
	return &Result{result: result}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	a := make([]interface{}, len(args))
	for i, v := range args {
		a[i] = v
	}
	var stmt = d1.ParameterizedStatement{SQL: s.Stmt, Params: a}
	_, result, err := s.Conn.WriteParameterizedContext(context.Background(), stmt)
	if err != nil {
		d1.Trace("%s: Query failed: %+v", s.Conn.ID, err)
		return nil, err
	}
	d1.Trace("%s: Query OK: %+v", s.Conn.ID, result)

	var cols []string
	var vals []interface{}
	gjson.ParseBytes(result.Results[0]).ForEach(func(key, value gjson.Result) bool {
		d1.Trace("key: %s, value: %s", key.String(), value.String())
		cols = append(cols, key.String())
		vals = append(vals, value.Value())
		return true
	})

	return &Rows{connId: s.Conn.ID, result: result, cols: cols, vals: vals}, nil
}

// Result implements the sql/driver.Result interface.
var _ driver.Result = (*Result)(nil)

type Result struct {
	result *d1.D1RespQueryResult
}

func (r *Result) LastInsertId() (int64, error) {
	return r.result.Meta.LastRowID, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.result.Meta.Changes, nil
}

// Rows implements the sql/driver.Rows interface.
var _ driver.Rows = (*Rows)(nil)

type Rows struct {
	connId string
	result *d1.D1RespQueryResult
	cols   []string
	vals   []interface{}
}

func (r *Rows) Columns() []string {
	if r.result.Meta.RowsRead == 0 || len(r.result.Results) == 0 {
		return nil
	}
	d1.Trace("%s: Columns: %+v", r.connId, r.cols)
	return r.cols
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if len(r.cols) == 0 {
		return io.EOF
	}
	d1.Trace("%s: Next: dest(%d)=%#+v", r.connId, len(dest), dest)
	d1.Trace("%s, Next: %+v, %+v", r.connId, r.cols, r.vals)
	for i := range dest {
		switch dest[i].(type) {
		case *int64:
			dest[i] = r.vals[i].(int64)
		case *float64:
			dest[i] = r.vals[i].(float64)
		case *string:
			dest[i] = r.vals[i].(string)
		case *[]byte:
			dest[i] = []byte(r.vals[i].(string))
		default:
			var err = errors.New("unsupported type")
			return err
		}
	}

	return nil
}
