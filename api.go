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

type D1RespDbListItem struct {
	CreatedAt time.Time `json:"created_at"`
	FileSize  int       `json:"file_size"`
	Name      string    `json:"name"`
	NumTables int       `json:"num_tables"`
	UUID      string    `json:"uuid"`
	Version   string    `json:"version"`
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

type D1RespQueryResult struct {
	Meta    D1RespQueryResultMeta `json:"meta"`
	Results []json.RawMessage     `json:"results"`
	Success bool                  `json:"success"`
}

type D1Resp struct {
	Errors     []D1RespError     `json:"errors"`
	Messages   []D1RespMessage   `json:"messages"`
	Result     *json.RawMessage  `json:"result"`
	ResultInfo *D1RespResultInfo `json:"result_info"`
	Success    bool              `json:"success"`
}

type ParameterizedStatement struct {
	SQL    string        `json:"sql"`
	Params []interface{} `json:"params"`
}

func (c *Connection) apiOpsToEndpoint(apiOps apiOps) string {
	switch apiOps {
	case api_LIST:
		return "/d1/database"
	case api_QUERY:
		return "/d1/database/" + c.databaseId + "/query"
	default:
		return ""
	}
}

func (c *Connection) d1ApiCall(ctx context.Context, apiOps apiOps, method string, reqBody []byte) (respBody []byte, duration time.Duration, err error) {
	var endpoint = c.apiOpsToEndpoint(apiOps)
	if endpoint == "" {
		err = ErrInvalidAPI
		return
	}
	var bodyReader io.Reader
	if reqBody != nil {
		bodyReader = bytes.NewBuffer(reqBody)
	}
	var api = fmt.Sprintf("%s/accounts/%s%s", v4base, c.accountId, endpoint)
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
	duration = time.Since(start)
	defer resp.Body.Close()

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		Trace("%s: ioutil.ReadAll() failed: %s", c.ID, err)
		return
	}
	Trace("%s: ioutil.ReadAll() OK", c.ID)

	// if resp.StatusCode != http.StatusOK {
	// 	trace("%s: HTTP status code %d, body: %s", c.ID, resp.StatusCode, respBody)
	// 	err = fmt.Errorf("HTTP status code %d", resp.StatusCode)
	// 	return
	// }
	Trace("%s: client.Do(%s) OK, status: %d, body: %s", req.URL, c.ID, resp.StatusCode, respBody)

	return
}

func (c *Connection) WriteParameterizedContext(ctx context.Context, stmt ParameterizedStatement) (resp D1Resp, result *D1RespQueryResult, err error) {
	if c.hasBeenClosed {
		var errResult D1Resp
		errResult.Success = false
		errResult.Errors = append(errResult.Errors, D1RespError{Code: 0, Message: "Connection has been closed"})
		return errResult, nil, ErrClosed
	}

	Trace("%s: Write() for %d statement args", c.ID, len(stmt.Params))

	reqBody, err := json.Marshal(stmt)
	if err != nil {
		Trace("%s: reqBody json.Marshal() failed: %s", c.ID, err)
		return
	}

	respBody, duration, err := c.d1ApiCall(ctx, api_QUERY, "POST", reqBody)
	if err != nil {
		Trace("%s: d1ApiCall() failed: %s, duration: %s", c.ID, err, duration)
		return
	}
	Trace("%s: d1ApiCall() OK, duration: %s", c.ID, duration)

	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		Trace("%s: resp json.Unmarshal() failed: %s", c.ID, err)
		return
	}
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

	if resp.Result != nil {
		var results = []*D1RespQueryResult{}
		err = json.Unmarshal(*resp.Result, &results)
		if err != nil {
			Trace("%s: result json.Unmarshal() failed: %s", c.ID, err)
			return
		}
		if len(results) != 1 {
			err = errors.New("result should have exactly one element")
			Trace("%s: result should have exactly one element", c.ID)
			return
		}
		result = results[0]
		Trace("%s: result json.Unmarshal() OK", c.ID)
	}

	return
}
