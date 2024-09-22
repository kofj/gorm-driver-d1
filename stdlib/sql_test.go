package stdlib_test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	d1 "github.com/kofj/gorm-driver-d1"
	_ "github.com/kofj/gorm-driver-d1/stdlib"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var globalDB *sql.DB
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

	d1.TraceOn(os.Stdout)
	db, err := sql.Open(d1.DriverName, fmt.Sprintf(
		"d1://%s:%s@%s", accountId, apiToken, datebaseId),
	)
	if err != nil {
		log.WithError(err).Fatal("open database failed")
	}
	globalDB = db

	exitCode := m.Run()

	err = db.Close()
	if err != nil {
		log.WithError(err).Fatalf("close database failed")
	}

	os.Exit(exitCode)
}

func testTableName() string {
	tableName := os.Getenv("D1_TEST_TABLE_STDLIB")
	if tableName == "" {
		tableName = "d1_test_stdlib"
	}
	return tableName
}

func TestTable(t *testing.T) {
	result, err := globalDB.Exec("CREATE TABLE IF NOT EXISTS " + testTableName() + " (id INTEGER, name TEXT, wallet REAL, bankrupt INTEGER, payload BLOB, ts DATETIME)")
	if !assert.Nilf(t, err, "create tabale failed") {
		return
	}
	assert.NotNilf(t, result, "result is nil")
	affected, err := result.RowsAffected()
	if !assert.Nilf(t, err, "get RowsAffected failed") {
		return
	}
	assert.Equalf(t, affected, int64(0), "RowsAffected is not 0")

	t.Cleanup(func() {
		_, err := globalDB.Exec("DROP TABLE " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %v", err)
		}
	})

	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	var stmt = "INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, ts) VALUES ( ?, ?, ?, ?, '{\"met\":\"" + met + "\"}', ?)"

	var items = []struct {
		id       int
		name     string
		wallet   float64
		bankrupt int
		ts       *time.Time
	}{
		{1, "Romulan", rand.Float64() * 100, 0, nil},
		{2, "Vulcan", rand.Float64() * 100, 0, nil},
		// {3, "Klingon", rand.Float64() * 100, 1},
		// {4, "Ferengi", rand.Float64() * 100, 0},
		// {5, "Cardassian", rand.Float64() * 100, 1},
	}

	for _, item := range items {
		t.Run("Exec INSERT/"+item.name, func(t *testing.T) {
			var ts = time.Now().UTC()
			item.ts = &ts
			result, err = globalDB.Exec(stmt, item.id, item.name, item.wallet, item.bankrupt, item.ts)
			if !assert.Nilf(t, err, "insert: %v", err) {
				return
			}
			assert.NotNilf(t, result, "result is nil")
			affected, err := result.RowsAffected()
			if !assert.Nilf(t, err, "get RowsAffected failed") {
				return
			}
			assert.Equalf(t, affected, int64(1), "RowsAffected is not 1")
		})
	}

	for _, item := range items {
		t.Run("Query SELECT/"+item.name, func(t *testing.T) {
			rows, err := globalDB.Query("SELECT * FROM "+testTableName()+" WHERE name = ?", item.name)
			if !assert.Nilf(t, err, "query: %v", err) {
				return
			}
			defer rows.Close()

			var id int
			var name string
			var wallet float64
			var bankrupt int
			var payload []byte
			var ts time.Time

			for rows.Next() {
				err = rows.Scan(&id, &name, &wallet, &bankrupt, &payload, &ts)
				if !assert.Nilf(t, err, "scan: %v", err) {
					return
				}
				assert.Equalf(t, item.id, id, "id")
				assert.Equalf(t, item.name, name, "name")
				assert.Equalf(t, item.wallet, wallet, "wallet")
				assert.Equalf(t, item.bankrupt, bankrupt, "bankrupt")
				assert.Equalf(t, fmt.Sprintf("{\"met\":\"%s\"}", met), string(payload), "payload")
				assert.Equalf(t, item.ts.Unix(), ts.Unix(), "ts")
			}
		})
	}
}
