package gormd1_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	d1 "github.com/kofj/gorm-driver-d1"
	"github.com/kofj/gorm-driver-d1/gormd1"
	_ "github.com/kofj/gorm-driver-d1/stdlib"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var defaultDSN string
var invalidDSN string
var gdb *gorm.DB
var apiToken string
var accountId string
var datebaseId string

func TestMain(m *testing.M) {
	var err = godotenv.Load("../dev.env")
	if err != nil {
		panic(err)
	}
	apiToken = os.Getenv("API_TOKEN")
	accountId = os.Getenv("ACCOUNT_ID")
	datebaseId = os.Getenv("DATABASE_ID")

	defaultDSN = fmt.Sprintf("d1://%s:%s@%s", accountId, apiToken, datebaseId)
	invalidDSN = fmt.Sprintf("d1://%s:%s@%s", accountId, "errToken", datebaseId)

	d1.TraceOn(os.Stdout)

	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold: time.Second, // Slow SQL threshold
			LogLevel:      logger.Info, // Log level
			// IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries: true, // Don't include params in the SQL log
			Colorful:             true, // Disable color
		},
	)

	gdb, err = gorm.Open(gormd1.Open(defaultDSN), &gorm.Config{
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   newLogger,
	})
	if err != nil {
		panic(err)
	}

	exitCode := m.Run()

	os.Exit(exitCode)
}

func TestDeriverName(t *testing.T) {
	if (gormd1.Dialector{}).Name() != d1.DriverName {
		t.Errorf("Expected DriverName to be 'd1'; got %s", d1.DriverName)
	}
}

func TestDialector(t *testing.T) {

	rows := []struct {
		description  string
		dialector    gorm.Dialector
		openSuccess  bool
		query        string
		querySuccess bool
		err          error
	}{
		{
			description:  "Default DSN",
			dialector:    gormd1.Open(defaultDSN),
			openSuccess:  true,
			query:        "SELECT 1+1;",
			querySuccess: true,
		},
		{
			description: "empty dsn",
			dialector:   gormd1.Open(""),
			openSuccess: false,
			err:         d1.ErrEmptyDSN,
		},
		{
			description: "short dsn",
			dialector:   gormd1.Open("d1://"),
			openSuccess: false,
			err:         d1.ErrShortDSN,
		},
		{
			description: "short dsn",
			dialector:   gormd1.Open("mysql://"),
			openSuccess: false,
			err:         d1.ErrNotD1,
		},
		{
			description: "short dsn",
			dialector:   gormd1.Open(invalidDSN),
			openSuccess: false,
		},
	}
	for rowIndex, row := range rows {
		t.Run(fmt.Sprintf("%d/%s", rowIndex, row.description), func(t *testing.T) {
			db, err := gorm.Open(row.dialector, &gorm.Config{})
			if !row.openSuccess {
				if err == nil {
					t.Errorf("Expected Open to fail.")
				}
				if row.err != nil && err != row.err {
					t.Errorf("Expected Open to fail with error: %v; got error: %v", row.err, err)
				}
				return
			}

			if err != nil {
				t.Errorf("Expected Open to succeed; got error: %v", err)
			}
			if db == nil {
				t.Errorf("Expected db to be non-nil.")
			}
			if row.query != "" {
				err = db.Exec(row.query).Error
				if !row.querySuccess {
					if err == nil {
						t.Errorf("Expected query to fail.")
					}
					return
				}

				if err != nil {
					t.Errorf("Expected query to succeed; got error: %v", err)
				}
			}
		})
	}
}
