# Cloudflare D1 Driver
It is an unofficail `sql.Driver`, and `gorm.Dialector` for Cloudflare D1 datebase.

## NOTE
current support date types list.
- ✅ Full support.
- ⚠️ Part support.
- ❌ Not support.

| Go type | D1 JSON | Support | Notes |
|:---|:---|:---|:---|
| bool | String | ✅ | |
| int,int32,int64 | Number | ⚠️ | auto convert to int64 if math.Trunc euqal. |
| float32,float64 | Number | ✅ ||
| string | String| ✅ | |
| []byte | String | ✅ | auto convert between unicode escape and bytes|
| time.Time| String | ⚠️ | if column name in `github.com/kofj/gorm-driver-d1/stdlib.defaultTimeFields` slice. |


## Useage
example for sql.
```go
package main

import (
	"database/sql"
	"fmt"

	d1 "github.com/kofj/gorm-driver-d1"
	_ "github.com/kofj/gorm-driver-d1/stdlib"
	log "github.com/sirupsen/logrus"
)

var (
	accountId  = "xxx-xxx-xx"
	apiToken   = "adcdef_xxx"
	datebaseId = "yyy-yyy-yyy"
)

func main() {
	db, err := sql.Open(d1.DriverName,
		fmt.Sprintf("d1://%s:%s@%s", accountId, apiToken, datebaseId),
	)
	if err != nil {
		log.WithError(err).Fatal("open database failed")
		return
	}
	rows, err := db.Query("PRAGMA table_info(users)")
	if err != nil {
		log.WithError(err).Error("query failed")
		return
	}
	for rows.Next() {
		var cid int
		var name, fieldType string
		err = rows.Scan(&cid, &name, &fieldType)
		if err != nil {
			log.WithError(err).Error("scan failed")
			return
		}
		log.WithField("cid", cid).WithField("name", name).WithField("type", fieldType).Info("scan result")
	}
}
```