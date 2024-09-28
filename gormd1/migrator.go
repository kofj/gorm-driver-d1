package gormd1

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

var (
	ErrConstraintsNotImplemented = errors.New("constraints not implemented on d1, consider using DisableForeignKeyConstraintWhenMigrating")
)

type Migrator struct {
	migrator.Migrator
}

var _ gorm.Migrator = &Migrator{}

func (dialector *Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (m *Migrator) RunWithoutForeignKey(fc func() error) error {
	var enabled int
	m.DB.Raw("PRAGMA foreign_keys").Scan(&enabled)
	if enabled == 1 {
		m.DB.Exec("PRAGMA foreign_keys = OFF")
		defer m.DB.Exec("PRAGMA foreign_keys = ON")
	}

	return fc()
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int
	m.Migrator.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", stmt.Table).Row().Scan(&count)
	})
	return count > 0
}

func (m Migrator) DropTable(values ...interface{}) error {
	return m.RunWithoutForeignKey(func() error {
		values = m.ReorderModels(values, false)
		tx := m.DB.Session(&gorm.Session{})

		for i := len(values) - 1; i >= 0; i-- {
			if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
				return tx.Exec("DROP TABLE IF EXISTS ?", clause.Table{Name: stmt.Table}).Error
			}); err != nil {
				return err
			}
		}

		return nil
	})

}

func (m Migrator) HasColumn(value interface{}, name string) bool {
	var count int
	m.Migrator.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(name); field != nil {
			name = field.DBName
		}

		if name != "" {
			m.DB.Raw(
				"SELECT count(*) FROM sqlite_master WHERE type = ? AND tbl_name = ? AND (sql LIKE ? OR sql LIKE ? OR sql LIKE ?)",
				"table", stmt.Table, `%"`+name+`" %`, `%`+name+` %`, "%`"+name+"`%",
			).Row().Scan(&count)
		}
		return nil
	})
	return count > 0
}

func (m Migrator) AlterColumn(value interface{}, name string) error {
	return m.RunWithoutForeignKey(func() error {
		return m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if field := stmt.Schema.LookUpField(name); field != nil {
				var (
					createSQL    string
					newTableName = stmt.Table + "__temp"
				)

				m.DB.Raw("SELECT sql FROM sqlite_master WHERE type = ? AND tbl_name = ? AND name = ?", "table", stmt.Table, stmt.Table).Row().Scan(&createSQL)

				if reg, err := regexp.Compile("(`|'|\"| )" + field.DBName + "(`|'|\"| ) .*?,"); err == nil {
					tableReg, err := regexp.Compile(" ('|`|\"| )" + stmt.Table + "('|`|\"| ) ")
					if err != nil {
						return err
					}

					createSQL = tableReg.ReplaceAllString(createSQL, fmt.Sprintf(" `%v` ", newTableName))
					createSQL = reg.ReplaceAllString(createSQL, fmt.Sprintf("`%v` ?,", field.DBName))

					var columns []string
					columnTypes, _ := m.DB.Migrator().ColumnTypes(value)
					for _, columnType := range columnTypes {
						columns = append(columns, fmt.Sprintf("`%v`", columnType.Name()))
					}

					return m.DB.Transaction(func(tx *gorm.DB) error {
						queries := []string{
							createSQL,
							fmt.Sprintf("INSERT INTO `%v`(%v) SELECT %v FROM `%v`", newTableName, strings.Join(columns, ","), strings.Join(columns, ","), stmt.Table),
							fmt.Sprintf("DROP TABLE `%v`", stmt.Table),
							fmt.Sprintf("ALTER TABLE `%v` RENAME TO `%v`", newTableName, stmt.Table),
						}
						for _, query := range queries {
							if err := tx.Exec(query, m.FullDataTypeOf(field)).Error; err != nil {
								return err
							}
						}
						return nil
					})
				} else {
					return err
				}
			} else {
				return fmt.Errorf("failed to alter field with name %v", name)
			}
		})
	})
}

func (m Migrator) DropColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(name); field != nil {
			name = field.DBName
		}

		var (
			createSQL    string
			newTableName = stmt.Table + "__temp"
		)

		m.DB.Raw("SELECT sql FROM sqlite_master WHERE type = ? AND tbl_name = ? AND name = ?", "table", stmt.Table, stmt.Table).Row().Scan(&createSQL)

		if reg, err := regexp.Compile("(`|'|\"| )" + name + "(`|'|\"| ) .*?,"); err == nil {
			tableReg, err := regexp.Compile(" ('|`|\"| )" + stmt.Table + "('|`|\"| ) ")
			if err != nil {
				return err
			}

			createSQL = tableReg.ReplaceAllString(createSQL, fmt.Sprintf(" `%v` ", newTableName))
			createSQL = reg.ReplaceAllString(createSQL, "")

			var columns []string
			columnTypes, _ := m.DB.Migrator().ColumnTypes(value)
			for _, columnType := range columnTypes {
				if columnType.Name() != name {
					columns = append(columns, fmt.Sprintf("`%v`", columnType.Name()))
				}
			}

			return m.DB.Transaction(func(tx *gorm.DB) error {
				queries := []string{
					createSQL,
					fmt.Sprintf("INSERT INTO `%v`(%v) SELECT %v FROM `%v`", newTableName, strings.Join(columns, ","), strings.Join(columns, ","), stmt.Table),
					fmt.Sprintf("DROP TABLE `%v`", stmt.Table),
					fmt.Sprintf("ALTER TABLE `%v` RENAME TO `%v`", newTableName, stmt.Table),
				}
				for _, query := range queries {
					if err := tx.Exec(query).Error; err != nil {
						return err
					}
				}
				return nil
			})
		} else {
			return err
		}
	})
}

func (m Migrator) CreateConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintInterfaceAndTable(stmt, name)

		return m.recreateTable(value, &table,
			func(ddl *ddl, stmt *gorm.Statement) (*ddl, []interface{}, error) {
				var (
					constraintName   string
					constraintSql    string
					constraintValues []interface{}
				)

				if constraint != nil {
					constraintName = constraint.GetName()
					constraintSql, constraintValues = constraint.Build()
				} else {
					return nil, nil, nil
				}

				ddl.addConstraint(constraintName, constraintSql)
				return ddl, constraintValues, nil
			})
	})
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintInterfaceAndTable(stmt, name)
		if constraint != nil {
			name = constraint.GetName()
		}

		return m.recreateTable(value, &table,
			func(ddl *ddl, stmt *gorm.Statement) (*ddl, []interface{}, error) {
				ddl.removeConstraint(name)
				return ddl, nil, nil
			})
	})
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintInterfaceAndTable(stmt, name)
		if constraint != nil {
			name = constraint.GetName()
		}

		m.DB.Raw(
			"SELECT count(*) FROM sqlite_master WHERE type = ? AND tbl_name = ? AND (sql LIKE ? OR sql LIKE ? OR sql LIKE ? OR sql LIKE ? OR sql LIKE ?)",
			"table", table, `%CONSTRAINT "`+name+`" %`, `%CONSTRAINT `+name+` %`, "%CONSTRAINT `"+name+"`%", "%CONSTRAINT ["+name+"]%", "%CONSTRAINT \t"+name+"\t%",
		).Row().Scan(&count)

		return nil
	})

	return count > 0
}

func (m Migrator) CurrentDatabase() (name string) {
	var null interface{}
	m.DB.Raw("PRAGMA database_list").Row().Scan(&null, &name, &null)
	return
}

func (m Migrator) BuildIndexOptions(opts []schema.IndexOption, stmt *gorm.Statement) (results []interface{}) {
	for _, opt := range opts {
		str := stmt.Quote(opt.DBName)
		if opt.Expression != "" {
			str = opt.Expression
		}

		if opt.Collate != "" {
			str += " COLLATE " + opt.Collate
		}

		if opt.Sort != "" {
			str += " " + opt.Sort
		}
		results = append(results, clause.Expr{SQL: str})
	}
	return
}

func (m Migrator) CreateIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			opts := m.BuildIndexOptions(idx.Fields, stmt)
			values := []interface{}{clause.Column{Name: idx.Name}, clause.Table{Name: stmt.Table}, opts}

			createIndexSQL := "CREATE "
			if idx.Class != "" {
				createIndexSQL += idx.Class + " "
			}
			createIndexSQL += "INDEX ?"

			if idx.Type != "" {
				createIndexSQL += " USING " + idx.Type
			}
			createIndexSQL += " ON ??"

			if idx.Where != "" {
				createIndexSQL += " WHERE " + idx.Where
			}

			return m.DB.Exec(createIndexSQL, values...).Error
		}

		return fmt.Errorf("failed to create index with name %v", name)
	})
}

func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		if name != "" {
			m.DB.Raw(
				"SELECT count(*) FROM sqlite_master WHERE type = ? AND tbl_name = ? AND name = ?", "index", stmt.Table, name,
			).Row().Scan(&count)
		}
		return nil
	})
	return count > 0
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var sql string
		m.DB.Raw("SELECT sql FROM sqlite_master WHERE type = ? AND tbl_name = ? AND name = ?", "index", stmt.Table, oldName).Row().Scan(&sql)
		if sql != "" {
			var err = m.DB.Exec(strings.Replace(sql, oldName, newName, 1)).Error
			if err != nil {
				return err
			}
			return m.DB.Exec("DROP INDEX ?", clause.Column{Name: oldName}).Error
		}
		return fmt.Errorf("failed to find index with name %v", oldName)
	})
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}).Error
	})
}

func (m Migrator) getRawDDL(table string) (string, error) {
	var createSQL string
	m.DB.Raw("SELECT sql FROM sqlite_master WHERE type = ? AND tbl_name = ? AND name = ?", "table", table, table).Row().Scan(&createSQL)

	if m.DB.Error != nil {
		return "", m.DB.Error
	}
	return createSQL, nil
}

func (m Migrator) recreateTable(
	value interface{}, tablePtr *string,
	getCreateSQL func(ddl *ddl, stmt *gorm.Statement) (sql *ddl, sqlArgs []interface{}, err error),
) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		table := stmt.Table
		if tablePtr != nil {
			table = *tablePtr
		}

		rawDDL, err := m.getRawDDL(table)
		if err != nil {
			return err
		}

		originDDL, err := parseDDL(rawDDL)
		if err != nil {
			return err
		}

		createDDL, sqlArgs, err := getCreateSQL(originDDL.clone(), stmt)
		if err != nil {
			return err
		}
		if createDDL == nil {
			return nil
		}

		newTableName := table + "__temp"
		if err := createDDL.renameTable(newTableName, table); err != nil {
			return err
		}

		columns := createDDL.getColumns()
		createSQL := createDDL.compile()

		return m.DB.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(createSQL, sqlArgs...).Error; err != nil {
				return err
			}

			queries := []string{
				fmt.Sprintf("INSERT INTO `%v`(%v) SELECT %v FROM `%v`", newTableName, strings.Join(columns, ","), strings.Join(columns, ","), table),
				fmt.Sprintf("DROP TABLE `%v`", table),
				fmt.Sprintf("ALTER TABLE `%v` RENAME TO `%v`", newTableName, table),
			}
			for _, query := range queries {
				if err := tx.Exec(query).Error; err != nil {
					return err
				}
			}
			return nil
		})
	})
}
