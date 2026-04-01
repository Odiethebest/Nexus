package loadtest

import "time"

// RunStatus mirrors Grafana Cloud k6 run status values.
type RunStatus string

const (
	StatusCreated           RunStatus = "created"
	StatusQueued            RunStatus = "queued"
	StatusInitializing      RunStatus = "initializing"
	StatusRunning           RunStatus = "running"
	StatusProcessingMetrics RunStatus = "processing_metrics"
	StatusCompleted         RunStatus = "completed"
	StatusAborted           RunStatus = "aborted"
)

// IsTerminal reports whether a run is finished.
func (s RunStatus) IsTerminal() bool {
	return s == StatusCompleted || s == StatusAborted
}

// StatusExtra carries details returned by the k6 API for a status transition.
type StatusExtra struct {
	ByUser  string `json:"by_user,omitempty"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// StatusDetails is the structured status payload returned by the k6 API.
type StatusDetails struct {
	Type  RunStatus    `json:"type"`
	Extra *StatusExtra `json:"extra,omitempty"`
}

// CostBreakdown holds run billing details.
type CostBreakdown struct {
	BaseTotalVUH  float64 `json:"base_total_vuh"`
	BrowserVUH    float64 `json:"browser_vuh"`
	ProtocolVUH   float64 `json:"protocol_vuh"`
	ReductionRate float64 `json:"reduction_rate"`
}

// TestCost holds total billed usage.
type TestCost struct {
	Breakdown CostBreakdown `json:"breakdown"`
	TotalVUH  float64       `json:"total_vuh"`
}

// TestRun is the normalized k6 run model.
type TestRun struct {
	ID            int64         `json:"id"`
	TestID        int64         `json:"test_id"`
	ProjectID     int64         `json:"project_id"`
	Status        RunStatus     `json:"status"`
	Result        string        `json:"result,omitempty"`
	Created       time.Time     `json:"created"`
	Ended         *time.Time    `json:"ended,omitempty"`
	Note          string        `json:"note,omitempty"`
	Cost          *TestCost     `json:"cost,omitempty"`
	StatusDetails StatusDetails `json:"status_details"`
}

// StartOptions carries app-level start options.
// Scenario/Preset are app concerns; only supported fields are forwarded to k6.
type StartOptions struct {
	Scenario string `json:"scenario,omitempty"`
	Preset   string `json:"preset,omitempty"`
	Note     string `json:"note,omitempty"`
}

// StartResult is returned by Service.Start.
type StartResult struct {
	RunID     int64         `json:"run_id"`
	TestID    int64         `json:"test_id"`
	Status    RunStatus     `json:"status"`
	StartedAt time.Time     `json:"started_at"`
	PollAfter time.Duration `json:"poll_after"`
}

// MetricPoint is a time-value sample.
type MetricPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// CoreSeries holds the visualization-ready performance series.
type CoreSeries struct {
	RPS          []MetricPoint `json:"rps"`
	P95MS        []MetricPoint `json:"p95_ms"`
	ErrorRatePct []MetricPoint `json:"error_rate_pct"`
	VUs          []MetricPoint `json:"vus"`
}

// InsightSnapshot holds the latest values used by the front-end summary cards.
type InsightSnapshot struct {
	RPS          float64 `json:"rps"`
	P95MS        float64 `json:"p95_ms"`
	ErrorRatePct float64 `json:"error_rate_pct"`
	VUs          float64 `json:"vus"`
	Insight      string  `json:"insight"`
}

// RunInsight is the normalized response model for UI polling endpoints.
type RunInsight struct {
	Run         TestRun         `json:"run"`
	Series      CoreSeries      `json:"series"`
	Snapshot    InsightSnapshot `json:"snapshot"`
	HealthScore int             `json:"health_score"`
	Signals     []string        `json:"signals"`
	Warnings    []string        `json:"warnings,omitempty"`
}
