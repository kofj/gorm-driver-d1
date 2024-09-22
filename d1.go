package d1

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

const DriverName = "d1"
const v4base = "https://api.cloudflare.com/client/v4"

var (
	ErrInvalidAPI = errors.New("invalid api")
	ErrClosed     = errors.New("d1: connection is closed")
	ErrEmptyDSN   = errors.New("dsn is empty")
	ErrShortDSN   = errors.New("dsn specified is impossibly short")
	ErrNotD1      = errors.New("dsn does not start with 'd1'")
	ErrInvalidDB  = errors.New("invalid database id")
)

// Open opens a new connection to the database.
// The dsn looks like:
//
//	d1://apiToken:accountId@databaseId?timeout=10
func Open(dsn string) (conn *Connection, err error) {
	conn = &Connection{}

	// generate our uuid for trace
	b := make([]byte, 16)
	_, err = rand.Read(b)
	if err != nil {
		return conn, err
	}
	conn.ID = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	// set defaults
	conn.hasBeenClosed = false
	err = conn.init(dsn)
	Trace("%s: Open() called for dsn: %s, err: %v", conn.ID, dsn, err)

	return
}

func Trace(pattern string, args ...interface{}) {
	// don't do the probably expensive Sprintf() if not needed
	if !wantsTrace {
		return
	}

	// this could all be made into one long statement but we have
	// compilers to do such things for us. let's sip a mint julep
	// and spell this out in glorious exposition.

	// make sure there is one and only one newline
	nlPattern := strings.TrimSpace(pattern) + "\n"
	msg := time.Now().Format("06-01-02 15:04:05.00000 ") + fmt.Sprintf(nlPattern, args...)
	for _, dlp := range dlpStrs {
		msg = strings.Replace(msg, dlp, "*****", -1)
	}
	_, err := traceOut.Write([]byte(msg))
	if err != nil {
		log.Printf("Trace() failed: %s", err)
	}
}

// TraceOn turns on tracing output to the io.Writer of your choice.
//
// Trace output is very detailed and verbose, as you might expect.
//
// Normally, you should run with tracing off, as it makes absolutely
// no concession to performance and is intended for debugging/dev use.
func TraceOn(w io.Writer) {
	traceOut = w
	wantsTrace = true
}

// TraceOff turns off tracing output. Once you call TraceOff(), no further
// info is sent to the io.Writer, unless it is TraceOn'd again.
func TraceOff() {
	wantsTrace = false
	traceOut = io.Discard
}
