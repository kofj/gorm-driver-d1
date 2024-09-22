package stdlib_test

import (
	mrand "math/rand"
	"sync"

	"crypto/rand"
	"database/sql"
	"fmt"
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

var once = sync.Once{}
var _defalultTestTableName string

func testTableName() string {
	once.Do(func() {
		tableName := os.Getenv("D1_TEST_TABLE_STDLIB")
		if tableName == "" {
			tableName = "d1dev_stdlib" + time.Now().Format("0102_1504_05")
		}
		_defalultTestTableName = tableName
	})

	return _defalultTestTableName
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func TestTable(t *testing.T) {
	result, err := globalDB.Exec("CREATE TABLE IF NOT EXISTS " + testTableName() + " (id INTEGER, name VARCHAR(20), wallet REAL, bankrupt INTEGER, payload TEXT, content BLOB, created_at DATETIME)")
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

	t.Run("Table Info", func(t *testing.T) {
		rows, err := globalDB.Query("PRAGMA table_info(" + testTableName() + ")")
		if !assert.Nilf(t, err, "query: %v", err) {
			d1.Trace("query failed: %v\n", err)
			return
		}
		defer rows.Close()

		type tableInfo struct {
			cid        int
			name       string
			typ        string
			notnull    int
			dflt_value interface{}
			pk         int
		}

		var rowsInfo = []tableInfo{
			{0, "id", "INTEGER", 0, nil, 0},
			{1, "name", "VARCHAR(20)", 0, nil, 0},
			{2, "wallet", "REAL", 0, nil, 0},
			{3, "bankrupt", "INTEGER", 0, nil, 0},
			{4, "payload", "TEXT", 0, nil, 0},
			{5, "content", "BLOB", 0, nil, 0},
			{6, "created_at", "DATETIME", 0, nil, 0},
		}

		var index int = 0
		for rows.Next() {
			var rowInfo tableInfo

			err = rows.Scan(&rowInfo.cid, &rowInfo.name, &rowInfo.typ, &rowInfo.notnull, &rowInfo.dflt_value, &rowInfo.pk)
			if !assert.Nilf(t, err, "scan: %v", err) {
				d1.Trace("scan failed: %v\n", err)
				return
			}
			d1.Trace("scan %d", index)
			var expected = rowsInfo[index]
			assert.Equalf(t, expected.cid, rowInfo.cid, "cid")
			assert.Equalf(t, expected.name, rowInfo.name, "name")
			assert.Equalf(t, expected.typ, rowInfo.typ, "typ")
			assert.Equalf(t, expected.notnull, rowInfo.notnull, "notnull")
			assert.Equalf(t, expected.dflt_value, rowInfo.dflt_value, "dflt_value")
			assert.Equalf(t, expected.pk, rowInfo.pk, "pk")

			index++
		}

	})

	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	var stmt = "INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, content, created_at) VALUES ( ?, ?, ?, ?, '{\"met\":\"" + met + "\"}', ?, ?)"

	var dt = time.Time{}
	var items = []struct {
		id        int
		name      string
		wallet    float64
		bankrupt  bool
		content   []byte
		createdAt time.Time
	}{
		{1, "Romulan", float64(mrand.Float32() * 10), false, randBytes(32), dt},
		{2, "Vulcan", float64(mrand.Float32() * 10), false, randBytes(12), dt},
		{3, "Klingon", float64(mrand.Float32() * 10), true, randBytes(7), dt},
		{4, "Ferengi", float64(mrand.Float32() * 10), false, randBytes(3), dt},
		{5, "Cardassian", float64(mrand.Float32() * 10), true, randBytes(1), dt},
	}

	for idx, item := range items {
		t.Run("Exec INSERT/"+item.name, func(t *testing.T) {
			items[idx].createdAt = time.Now()
			result, err = globalDB.Exec(stmt, item.id, item.name, item.wallet, item.bankrupt, item.content, items[idx].createdAt)
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

	t.Run("Query MULT", func(t *testing.T) {
		rows, err := globalDB.Query("SELECT id,name,wallet,bankrupt,created_at FROM "+testTableName()+" ORDER BY id LIMIT ?", 2)
		if !assert.Nilf(t, err, "query: %v", err) {
			d1.Trace("query failed: %v\n", err)
			return
		}
		defer rows.Close()

		var index int = 0
		for rows.Next() {
			var id int
			var name string
			var wallet float64
			var bankrupt bool
			var createdAt time.Time

			err = rows.Scan(&id, &name, &wallet, &bankrupt, &createdAt)
			if !assert.Nilf(t, err, "scan: %v", err) {
				d1.Trace("scan failed: %v\n", err)
				return
			}
			d1.Trace("scan %d", index)
			assert.Equalf(t, items[index].id, id, "id")
			assert.Equalf(t, items[index].name, name, "name")
			assert.Equalf(t, items[index].wallet, wallet, "wallet")
			assert.Equalf(t, items[index].bankrupt, bankrupt, "bankrupt")
			assert.Truef(t, items[index].createdAt.Equal(createdAt), "createdAt")

			index++

		}

	})

	for _, item := range items {
		t.Run("Query SELECT/"+item.name, func(t *testing.T) {
			rows, err := globalDB.Query("SELECT * FROM "+testTableName()+" WHERE name = ?", item.name)
			if !assert.Nilf(t, err, "query: %v", err) {
				d1.Trace("query failed: %v\n", err)
				return
			}
			defer rows.Close()

			var id int
			var name string
			var wallet float64
			var bankrupt bool
			var payload []byte
			var content []byte
			var createdAt time.Time

			for rows.Next() {
				d1.Trace("rows.Next()")
				err = rows.Scan(&id, &name, &wallet, &bankrupt, &payload, &content, &createdAt)
				if !assert.Nilf(t, err, "scan: %v", err) {
					d1.Trace("scan failed: %v\n", err)
					return
				}
				d1.Trace("scan id: %d, name: %s, wallet: %f, bankrupt: %t, payload: %s, createdAt: %d\n", id, name, wallet, bankrupt, string(payload), createdAt)
				assert.Equalf(t, item.id, id, "id")
				assert.Equalf(t, item.name, name, "name")
				assert.Equalf(t, item.wallet, wallet, "wallet")
				assert.Equalf(t, item.bankrupt, bankrupt, "bankrupt")
				assert.Equalf(t, fmt.Sprintf("{\"met\":\"%s\"}", met), string(payload), "payload")
				assert.Equalf(t, item.content, content, "content")
				assert.Truef(t, item.createdAt.Equal(createdAt), "createdAt")
			}
		})
	}

	t.Run("Invalid Query", func(t *testing.T) {
		_, err := globalDB.Query("INVALID QUERY")
		if err == nil {
			t.Errorf("expected error for invalid query, got nil")
		}
	})
}
