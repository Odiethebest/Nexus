package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"nexus/internal/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultAPIBase = "https://api.k6.io"
)

var (
	ErrCircuitOpen = errors.New("loadtest: upstream circuit open")
)

// ClientConfig configures the k6 API client.
type ClientConfig struct {
	BaseURL    string
	APIToken   string
	StackID    string
	HTTPClient *http.Client

	RetryMaxAttempts               int
	RetryBaseDelay                 time.Duration
	RetryMaxDelay                  time.Duration
	CircuitBreakerFailureThreshold int
	CircuitBreakerOpenDuration     time.Duration
	Now                            func() time.Time
	RandSeed                       int64
}

// APIError wraps non-2xx responses from the k6 API.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("loadtest: k6 API error (%d): %s", e.StatusCode, e.Body)
}

// CircuitOpenError indicates that requests are blocked until the breaker closes.
type CircuitOpenError struct {
	Until time.Time
}

func (e *CircuitOpenError) Error() string {
	return fmt.Sprintf("%v: retry after %s", ErrCircuitOpen, e.Until.UTC().Format(time.RFC3339))
}

func (e *CircuitOpenError) Unwrap() error { return ErrCircuitOpen }

// Client talks to Grafana Cloud k6 APIs.
type Client struct {
	baseURL string
	token   string
	stackID string
	http    *http.Client

	retryMax   int
	retryBase  time.Duration
	retryCap   time.Duration
	breakerN   int
	breakerFor time.Duration
	now        func() time.Time

	randMu sync.Mutex
	rand   *rand.Rand

	breakerMu          sync.Mutex
	consecutiveFailure int
	openUntil          time.Time
}

// NewClient creates a validated client.
func NewClient(cfg ClientConfig) (*Client, error) {
	base := strings.TrimSpace(cfg.BaseURL)
	if base == "" {
		base = defaultAPIBase
	}
	base = strings.TrimRight(base, "/")
	if cfg.APIToken == "" {
		return nil, fmt.Errorf("loadtest: missing API token")
	}
	if cfg.StackID == "" {
		return nil, fmt.Errorf("loadtest: missing stack ID")
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}

	retryMax := cfg.RetryMaxAttempts
	if retryMax < 0 {
		retryMax = 0
	}
	retryBase := cfg.RetryBaseDelay
	if retryBase <= 0 {
		retryBase = 250 * time.Millisecond
	}
	retryCap := cfg.RetryMaxDelay
	if retryCap <= 0 {
		retryCap = 2 * time.Second
	}
	if retryCap < retryBase {
		retryCap = retryBase
	}

	breakerThreshold := cfg.CircuitBreakerFailureThreshold
	if breakerThreshold <= 0 {
		breakerThreshold = 5
	}
	breakerFor := cfg.CircuitBreakerOpenDuration
	if breakerFor <= 0 {
		breakerFor = 30 * time.Second
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	seed := cfg.RandSeed
	if seed == 0 {
		seed = nowFn().UnixNano()
	}

	return &Client{
		baseURL:    base,
		token:      cfg.APIToken,
		stackID:    cfg.StackID,
		http:       httpClient,
		retryMax:   retryMax,
		retryBase:  retryBase,
		retryCap:   retryCap,
		breakerN:   breakerThreshold,
		breakerFor: breakerFor,
		now:        nowFn,
		rand:       rand.New(rand.NewSource(seed)),
	}, nil
}

// StartLoadTest starts a configured cloud load test and returns the run model.
func (c *Client) StartLoadTest(ctx context.Context, testID int64, req StartOptions) (TestRun, error) {
	path := fmt.Sprintf("/cloud/v6/load_tests/%d/start", testID)

	// Keep payload conservative for compatibility.
	var body any
	if req.Note != "" {
		body = struct {
			Note string `json:"note,omitempty"`
		}{Note: req.Note}
	}

	raw, err := c.doJSON(ctx, http.MethodPost, path, nil, body)
	if err != nil {
		return TestRun{}, err
	}
	return decodeTestRun(raw)
}

// GetTestRun fetches run status and metadata.
func (c *Client) GetTestRun(ctx context.Context, runID int64) (TestRun, error) {
	path := fmt.Sprintf("/cloud/v6/test_runs/%d", runID)
	raw, err := c.doJSON(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return TestRun{}, err
	}
	return decodeTestRun(raw)
}

// AbortTestRun aborts a running cloud test.
// A 409 response (already non-running) is treated as a no-op.
func (c *Client) AbortTestRun(ctx context.Context, runID int64) error {
	path := fmt.Sprintf("/cloud/v6/test_runs/%d/abort", runID)
	_, err := c.doJSON(ctx, http.MethodPost, path, nil, nil)
	if err == nil {
		return nil
	}

	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return nil
	}
	return err
}

// QueryRangeK6 returns a merged timeseries for a metric query.
func (c *Client) QueryRangeK6(
	ctx context.Context,
	runID int64,
	metric string,
	query string,
	stepSeconds int,
) ([]MetricPoint, error) {
	path := fmt.Sprintf("/cloud/v5/test_runs/%d/query_range_k6", runID)
	path = buildK6OperationPath(path, []k6OperationParam{
		{key: "metric", value: metric, quoted: true},
		{key: "query", value: query, quoted: true},
		{key: "step", value: strconv.Itoa(stepSeconds), quoted: false, include: stepSeconds > 0},
	})

	raw, err := c.doJSON(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := decodeAny(raw, &resp); err != nil {
		return nil, fmt.Errorf("loadtest: decode range query: %w", err)
	}

	merged := make(map[int64]float64)
	for _, series := range resp.Data.Result {
		for _, pair := range series.Values {
			if len(pair) < 2 {
				continue
			}
			ts, err := parseUnix(pair[0])
			if err != nil {
				continue
			}
			v, err := parseFloat(pair[1])
			if err != nil {
				continue
			}
			merged[ts] += v
		}
	}
	return sortedPoints(merged), nil
}

// QueryAggregateK6 returns a scalar aggregate value for a metric query.
func (c *Client) QueryAggregateK6(
	ctx context.Context,
	runID int64,
	metric string,
	query string,
) (float64, error) {
	path := fmt.Sprintf("/cloud/v5/test_runs/%d/query_aggregate_k6", runID)
	path = buildK6OperationPath(path, []k6OperationParam{
		{key: "metric", value: metric, quoted: true},
		{key: "query", value: query, quoted: true},
	})

	raw, err := c.doJSON(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return 0, err
	}

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Values [][]any `json:"values"`
				Value  []any   `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := decodeAny(raw, &resp); err != nil {
		return 0, fmt.Errorf("loadtest: decode aggregate query: %w", err)
	}

	var sum float64
	for _, series := range resp.Data.Result {
		var rawValue any
		switch {
		case len(series.Values) > 0 && len(series.Values[len(series.Values)-1]) >= 2:
			rawValue = series.Values[len(series.Values)-1][1]
		case len(series.Value) >= 2:
			rawValue = series.Value[1]
		default:
			continue
		}

		v, err := parseFloat(rawValue)
		if err != nil {
			continue
		}
		sum += v
	}
	return sum, nil
}

func (c *Client) doJSON(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body any,
) ([]byte, error) {
	endpoint := c.baseURL + path
	endpointLabel := classifyUpstreamEndpoint(path)
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("loadtest: marshal request body: %w", err)
		}
		payload = b
	}

	maxAttempts := c.retryMax + 1
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.checkCircuit(); err != nil {
			return nil, err
		}

		raw, err := c.doJSONOnce(ctx, method, endpoint, endpointLabel, payload)
		if err == nil {
			c.recordSuccess()
			return raw, nil
		}

		lastErr = err
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
			// Caller context expired; do not retry.
			return nil, err
		}

		c.recordFailure()

		retryable := isRetryableUpstreamErr(err)
		if !retryable || attempt == maxAttempts {
			break
		}

		delay := c.backoffDelay(attempt)
		if !sleepCtx(ctx, delay) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("loadtest: request failed without explicit error")
	}
	return nil, lastErr
}

func (c *Client) doJSONOnce(
	ctx context.Context,
	method string,
	endpoint string,
	endpointLabel string,
	payload []byte,
) ([]byte, error) {
	start := time.Now()
	defer func() {
		metrics.LoadtestUpstreamLatency.WithLabelValues(endpointLabel).Observe(time.Since(start).Seconds())
	}()

	var bodyReader io.Reader
	if payload != nil {
		bodyReader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("loadtest: build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Stack-Id", c.stackID)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("loadtest: execute request: %w", err)
	}
	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("loadtest: read response body: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		msg := strings.TrimSpace(string(raw))
		if msg == "" {
			msg = http.StatusText(res.StatusCode)
		}
		return nil, &APIError{StatusCode: res.StatusCode, Body: msg}
	}
	return raw, nil
}

func (c *Client) checkCircuit() error {
	c.breakerMu.Lock()
	defer c.breakerMu.Unlock()

	now := c.now().UTC()
	if !c.openUntil.IsZero() && now.Before(c.openUntil) {
		return &CircuitOpenError{Until: c.openUntil}
	}
	return nil
}

func (c *Client) recordSuccess() {
	c.breakerMu.Lock()
	defer c.breakerMu.Unlock()
	c.consecutiveFailure = 0
	c.openUntil = time.Time{}
}

func (c *Client) recordFailure() {
	c.breakerMu.Lock()
	defer c.breakerMu.Unlock()

	c.consecutiveFailure++
	if c.consecutiveFailure < c.breakerN {
		return
	}

	c.openUntil = c.now().UTC().Add(c.breakerFor)
	c.consecutiveFailure = 0
}

func (c *Client) backoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// Exponential backoff with cap.
	delay := c.retryBase
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= c.retryCap {
			delay = c.retryCap
			break
		}
	}

	// Add +-25% jitter.
	jitterMax := delay / 4
	if jitterMax <= 0 {
		return delay
	}

	c.randMu.Lock()
	j := c.rand.Int63n(int64(jitterMax)*2+1) - int64(jitterMax)
	c.randMu.Unlock()

	next := delay + time.Duration(j)
	if next < 0 {
		return 0
	}
	if next > c.retryCap {
		return c.retryCap
	}
	return next
}

func isRetryableUpstreamErr(err error) bool {
	if err == nil {
		return false
	}

	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusTooManyRequests ||
			apiErr.StatusCode == http.StatusRequestTimeout ||
			apiErr.StatusCode >= 500
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func classifyUpstreamEndpoint(path string) string {
	switch {
	case strings.HasPrefix(path, "/cloud/v6/load_tests/"):
		return "start"
	case strings.HasPrefix(path, "/cloud/v6/test_runs/"):
		return "run"
	case strings.HasPrefix(path, "/cloud/v5/test_runs/"):
		return "query"
	default:
		return "other"
	}
}

type k6OperationParam struct {
	key     string
	value   string
	quoted  bool
	include bool
}

func buildK6OperationPath(base string, params []k6OperationParam) string {
	if len(params) == 0 {
		return base
	}

	parts := make([]string, 0, len(params))
	for _, param := range params {
		if param.key == "" {
			continue
		}
		if !param.include && param.value == "" {
			continue
		}
		if !param.include && !param.quoted {
			continue
		}

		value := param.value
		if param.quoted {
			value = quoteK6Param(value)
			value = url.PathEscape(value)
		}
		parts = append(parts, param.key+"="+value)
	}
	if len(parts) == 0 {
		return base
	}
	return base + "(" + strings.Join(parts, ",") + ")"
}

func quoteK6Param(v string) string {
	replacer := strings.NewReplacer(
		`\\`, `\\\\`,
		`'`, `\\'`,
	)
	return "'" + replacer.Replace(v) + "'"
}

type testRunDTO struct {
	ID            int64         `json:"id"`
	TestID        int64         `json:"test_id"`
	ProjectID     int64         `json:"project_id"`
	Status        RunStatus     `json:"status"`
	Result        string        `json:"result"`
	Created       string        `json:"created"`
	Ended         *string       `json:"ended"`
	Note          string        `json:"note"`
	Cost          *TestCost     `json:"cost"`
	StatusDetails StatusDetails `json:"status_details"`
}

func decodeTestRun(raw []byte) (TestRun, error) {
	var dto testRunDTO
	if err := decodeAny(raw, &dto); err == nil && dto.ID > 0 {
		return dto.toModel()
	}

	var wrapped struct {
		Value json.RawMessage `json:"value"`
	}
	if err := decodeAny(raw, &wrapped); err != nil {
		return TestRun{}, fmt.Errorf("loadtest: decode run response: %w", err)
	}
	if len(wrapped.Value) == 0 {
		return TestRun{}, fmt.Errorf("loadtest: run response missing value")
	}

	if wrapped.Value[0] == '[' {
		var arr []testRunDTO
		if err := decodeAny(wrapped.Value, &arr); err != nil {
			return TestRun{}, fmt.Errorf("loadtest: decode wrapped run list: %w", err)
		}
		if len(arr) == 0 {
			return TestRun{}, fmt.Errorf("loadtest: wrapped run list is empty")
		}
		return arr[0].toModel()
	}

	if err := decodeAny(wrapped.Value, &dto); err != nil {
		return TestRun{}, fmt.Errorf("loadtest: decode wrapped run: %w", err)
	}
	return dto.toModel()
}

func (d testRunDTO) toModel() (TestRun, error) {
	created, err := parseAPITime(d.Created)
	if err != nil {
		return TestRun{}, fmt.Errorf("loadtest: parse created time: %w", err)
	}

	var ended *time.Time
	if d.Ended != nil && *d.Ended != "" {
		t, err := parseAPITime(*d.Ended)
		if err != nil {
			return TestRun{}, fmt.Errorf("loadtest: parse ended time: %w", err)
		}
		ended = &t
	}

	statusDetails := d.StatusDetails
	if statusDetails.Type == "" {
		statusDetails.Type = d.Status
	}

	return TestRun{
		ID:            d.ID,
		TestID:        d.TestID,
		ProjectID:     d.ProjectID,
		Status:        d.Status,
		Result:        d.Result,
		Created:       created,
		Ended:         ended,
		Note:          d.Note,
		Cost:          d.Cost,
		StatusDetails: statusDetails,
	}, nil
}

func parseAPITime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}

	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05.999",
		"2006-01-02T15:04:05",
	}

	var lastErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func decodeAny(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}

func parseUnix(raw any) (int64, error) {
	switch v := raw.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, err
		}
		return int64(f), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, err
		}
		return int64(f), nil
	default:
		return 0, fmt.Errorf("unsupported unix timestamp type %T", raw)
	}
}

func parseFloat(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unsupported float type %T", raw)
	}
}

func sortedPoints(byTimestamp map[int64]float64) []MetricPoint {
	if len(byTimestamp) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(byTimestamp))
	for ts := range byTimestamp {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]MetricPoint, 0, len(keys))
	for _, ts := range keys {
		out = append(out, MetricPoint{
			Timestamp: time.Unix(ts, 0).UTC(),
			Value:     byTimestamp[ts],
		})
	}
	return out
}
