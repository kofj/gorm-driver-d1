package d1

import (
	"context"
	nurl "net/url"

	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var wantsTrace bool
var traceOut io.Writer
var dlpStrs []string

const defaultTimeoutSecond = 30

type Connection struct {
	accountId  string
	apiToken   string
	databaseId string

	// variables below this line need to be initialized in Open()
	hasBeenClosed bool   //   false
	ID            string //   generated in init()
	client        http.Client
}

// Close will mark the connection as closed. It is safe to be called
// multiple times.
func (conn *Connection) Close() {
	conn.hasBeenClosed = true
}

func (conn *Connection) init(dsn string) error {
	// do some sanity checks.  You know users.

	if len(dsn) == 0 {
		return ErrEmptyDSN
	}

	if len(dsn) < 7 {
		return ErrShortDSN
	}

	if !strings.HasPrefix(dsn, "d1") {
		return ErrNotD1
	}

	u, err := nurl.Parse(dsn)
	if err != nil {
		return err
	}
	Trace("%s: net.url.Parse() OK", conn.ID)

	// specs say Username() is always populated even if empty
	if u.User == nil {
		conn.accountId = ""
		conn.apiToken = ""
	} else {
		// guaranteed, but could be empty which is ok
		conn.accountId = u.User.Username()

		// not guaranteed, so test if set
		pass, isset := u.User.Password()
		if isset {
			conn.apiToken = pass
			dlpStrs = append(dlpStrs, conn.apiToken)
		} else {
			conn.apiToken = ""
		}
	}

	if u.Host == "" || len(u.Host) != 36 {
		return ErrInvalidDB
	}
	conn.databaseId = u.Host

	// parse query params
	query := u.Query()

	timeout := defaultTimeoutSecond
	if query.Get("timeout") != "" {
		customTimeout, err := strconv.Atoi(query.Get("timeout"))
		if err != nil {
			return errors.New("invalid timeout specified: " + err.Error())
		}
		timeout = customTimeout
	}

	// Initialize http client for connection
	conn.client = http.Client{
		Transport: http.DefaultTransport,
		Timeout:   time.Second * time.Duration(timeout),
	}

	Trace("%s:    %s -> %s", conn.ID, "accountId", conn.accountId)
	Trace("%s:    %s -> %s", conn.ID, "apiToken", conn.apiToken)
	Trace("%s:    %s -> %s", conn.ID, "databaseId", conn.databaseId)

	// verify connection
	return conn.VerifyApiTokenContext(context.Background())
}
