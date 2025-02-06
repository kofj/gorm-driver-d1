package d1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type apiOps int

const (
	api_LIST apiOps = iota
	api_QUERY
	API_TOKEN
)

type D1RespMessage struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Type    interface{} `json:"type"`
}

type D1RespError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type D1RespResultInfo struct {
	Count      int `json:"count"`
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	TotalCount int `json:"total_count"`
}

type D1RespQueryResultMeta struct {
	ChangedDb   bool    `json:"changed_db"`
	Changes     int64   `json:"changes"`
	Duration    float64 `json:"duration"`
	LastRowID   int64   `json:"last_row_id"`
	RowsRead    int64   `json:"rows_read"`
	RowsWritten int64   `json:"rows_written"`
	ServedBy    string  `json:"served_by"`
	SizeAfter   int64   `json:"size_after"`
}

type D1RespQueryResults struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

type D1RespQueryResult struct {
	Meta    D1RespQueryResultMeta `json:"meta"`
	Results D1RespQueryResults    `json:"results"`
}

type D1Resp struct {
	Errors     []D1RespError        `json:"errors"`
	Messages   []D1RespMessage      `json:"messages"`
	Result     []*D1RespQueryResult `json:"result"`
	ResultInfo *D1RespResultInfo    `json:"result_info"`
	Success    bool                 `json:"success"`
	AuditlogId string               `json:"-"`
}

type ParameterizedStatement struct {
	SQL    string        `json:"sql"`
	Params []interface{} `json:"params"`
}

func (c *Connection) apiOpsToEndpoint(apiOps apiOps, account string) string {
	switch apiOps {
	case api_LIST:
		return "/accounts/" + account + "/d1/database"
	case api_QUERY:
		return "/accounts/" + account + "/d1/database/" + c.databaseId + "/raw"
	case API_TOKEN:
		return "/accounts/" + account + "/tokens/verify"
	default:
		return ""
	}
}

func (c *Connection) d1ApiCall(ctx context.Context, apiOps apiOps, method string, reqBody []byte) (respBody []byte, auditlogId string, duration time.Duration, err error) {
	var endpoint = c.apiOpsToEndpoint(apiOps, c.accountId)
	if endpoint == "" {
		err = ErrInvalidAPI
		return
	}
	var bodyReader io.Reader
	if reqBody != nil {
		bodyReader = bytes.NewBuffer(reqBody)
	}
	var api = fmt.Sprintf("%s%s", v4base, endpoint)
	req, err := http.NewRequestWithContext(ctx, method, api, bodyReader)
	if err != nil {
		return
	}
	Trace("%s: http.NewRequest() OK, body: %s", c.ID, reqBody)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiToken))

	var start = time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		duration = time.Since(start)
		Trace("%s: client.Do() failed: %s", c.ID, err)
		return
	}
	auditlogId = resp.Header.Get("cf-auditlog-id")
	duration = time.Since(start)
	defer resp.Body.Close()

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		Trace("%s: ioutil.ReadAll() failed: %s", c.ID, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("http status: %d, body: %s", resp.StatusCode, respBody)
		Trace("%s: client.Do(%d) failed: %s", c.ID, resp.StatusCode, err)
		return
	}

	Trace("%s: client.Do(%s) OK, status: %d, body: %s", c.ID, req.URL, resp.StatusCode, respBody)

	return
}

func (c *Connection) WriteParameterizedContext(ctx context.Context, stmt ParameterizedStatement) (resp D1Resp, err error) {
	if c.hasBeenClosed {
		var errResult D1Resp
		errResult.Success = false
		errResult.Errors = append(errResult.Errors, D1RespError{Code: 0, Message: "Connection has been closed"})
		return errResult, ErrClosed
	}

	Trace("%s: Write() for %d statement args", c.ID, len(stmt.Params))

	for idx, param := range stmt.Params {
		Trace("%s: param[%d]: %v", c.ID, idx, param)
		switch param := param.(type) {
		case time.Time:
			stmt.Params[idx] = param.Format(time.RFC3339Nano)
		case []byte:
			stmt.Params[idx] = BytesToUnicodeEscapes(param)
		}
	}

	reqBody, err := json.Marshal(stmt)
	if err != nil {
		Trace("%s: reqBody json.Marshal() failed: %s", c.ID, err)
		return
	}
	Trace("%s, reqBody: %s", c.ID, reqBody)

	respBody, auditlogId, duration, err := c.d1ApiCall(ctx, api_QUERY, "POST", reqBody)
	if err != nil {
		Trace("%s: d1ApiCall() failed: %s, duration: %s", c.ID, err, duration)
		return
	}
	Trace("%s: d1ApiCall() OK, duration: %s", c.ID, duration)

	resp = D1Resp{}
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		Trace("%s: resp json.Unmarshal() failed: %s", c.ID, err)
		return
	}
	resp.AuditlogId = auditlogId
	Trace("%s: resp json.Unmarshal() OK", c.ID)

	if !resp.Success {
		var errs = []string{}
		for idx, e := range resp.Errors {
			errs = append(errs, fmt.Sprintf("[%d] code: %d, message: '%s'", idx, e.Code, e.Message))
		}
		err = errors.New(strings.Join(errs, "\n"))
		Trace("%s: api call failed, err: %s, %+v", c.ID, err, resp)
		return
	}

	return
}

func (c *Connection) VerifyApiTokenContext(ctx context.Context) (err error) {
	if c.hasBeenClosed {
		return ErrClosed
	}

	Trace("%s: VerifyApiToken()", c.ID)

	_, auditlogId, duration, err := c.d1ApiCall(ctx, API_TOKEN, "GET", nil)
	if err != nil {
		Trace("%s: d1ApiCall() failed: %s, duration: %s", c.ID, err, duration)
		return
	}
	Trace("%s: d1ApiCall() OK, duration: %s, auditlogId: %s", c.ID, duration, auditlogId)
	return
}
