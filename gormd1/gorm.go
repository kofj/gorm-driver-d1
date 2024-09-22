package gormd1

import (
	"database/sql"
	"strings"

	d1 "github.com/kofj/gorm-driver-d1"
	_ "github.com/kofj/gorm-driver-d1/stdlib"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type Dialector struct {
	dsn  string
	Conn gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{dsn: dsn}
}

func (dialector Dialector) Name() string {
	return d1.DriverName
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		LastInsertIDReversed: true,
	})

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(d1.DriverName, dialector.dsn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "string"
	case schema.Int, schema.Uint:
		if field.AutoIncrement && !field.PrimaryKey {
			// https://www.sqlite.org/autoinc.html
			return "integer PRIMARY KEY AUTOINCREMENT"
		} else {
			return "integer"
		}
	case schema.Float:
		return "real"
	case schema.String:
		return "text"
	case schema.Time:
		return "datetime"
	case schema.Bytes:
		return "blob"
	}

	return string(field.DataType)
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.AutoIncrement {
		return clause.Expr{SQL: "DEFAULT"}
	}
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	logrus.WithField("v", v).Info("call BindVarTo")
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	logrus.WithField("str", str).Info("call QuoteTo")
	writer.WriteByte('`')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(".`")
			}
			writer.WriteString(str)
			writer.WriteByte('`')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('`')
	}
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	var explainSql = logger.ExplainSQL(sql, nil, `"`, vars...)
	logrus.WithField("sql", sql).WithField("vars", vars).WithField("sql", explainSql).Info("call Explain")
	return "EXPLAIN " + explainSql
}
