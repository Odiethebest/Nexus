// loadgen is an in-repo load generator for the Nexus pipeline. It drives
// two request streams against the producer:
//
//  1. POST /events at a target rate (writes)
//  2. GET /notifications/{message_id} at ratio × writes (reads)
//
// The read stream is deliberately biased toward recently-published ids so
// the same message_id is looked up multiple times within the 60s cache
// TTL — this reflects the "same user checks the same notification a few
// times in a row" workload the cache-aside path is designed for, and
// drives the by_id hit rate the RUNBOOK reports.
//
// Usage:
//
//	go run ./cmd/loadgen \
//	    -base http://localhost:8080 \
//	    -metrics http://localhost:8080/metrics \
//	    -rate 2000 -dur 30s -read-ratio 2.0
//
// Emits a single JSON summary to stdout on completion — RPS achieved,
// publish p99, GET p99, read hit rate (computed from Prometheus), and
// e2e lag p99 (from the producer /api/metrics/summary endpoint).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	baseURL     string
	metricsURL  string
	summaryURL  string
	rate        int
	duration    time.Duration
	readRatio   float64
	concurrency int
}

func main() {
	cfg := parseFlags()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration+15*time.Second)
	defer cancel()

	log.Printf("loadgen start: rate=%d/s duration=%s read_ratio=%.2f base=%s",
		cfg.rate, cfg.duration, cfg.readRatio, cfg.baseURL)

	// Snapshot cache counters before we start so we can compute the hit
	// rate over the load window alone (not lifetime-of-process).
	beforeHits, beforeMisses := readCacheCounters(ctx, cfg.metricsURL)

	// idBuf holds recently-published message_ids for the read stream to
	// pick from. Bounded ring; older entries fall out to keep reads
	// biased toward hot ids.
	idBuf := newIDBuffer(4096)

	pubLatencies := &latencyRecorder{}
	getLatencies := &latencyRecorder{}
	var publishAttempts, publishOK, getAttempts, getOK atomic.Int64

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Rate limiter: one tick per (1/rate)s. If real requests can't keep
	// up, ticks pile in the channel and later goroutines drain them —
	// so we measure the achievable rate, not a synthetic one.
	interval := time.Second / time.Duration(cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	deadline := time.Now().Add(cfg.duration)
	tickCh := make(chan time.Time, cfg.concurrency*4)

	// Fan out.
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tickCh {
				if t.After(deadline) {
					return
				}
				runOneCycle(ctx, client, cfg, idBuf,
					pubLatencies, getLatencies,
					&publishAttempts, &publishOK,
					&getAttempts, &getOK)
			}
		}()
	}

	// Producer of ticks.
	go func() {
		for now := range ticker.C {
			if now.After(deadline) {
				close(tickCh)
				return
			}
			tickCh <- now
		}
	}()

	wg.Wait()

	afterHits, afterMisses := readCacheCounters(ctx, cfg.metricsURL)
	summary := fetchSummary(ctx, cfg.summaryURL)

	realRate := float64(publishOK.Load()) / cfg.duration.Seconds()
	hitDelta := afterHits - beforeHits
	missDelta := afterMisses - beforeMisses
	var hitRate float64
	if hitDelta+missDelta > 0 {
		hitRate = hitDelta / (hitDelta + missDelta)
	}

	report := map[string]any{
		"target_rate_per_sec":   cfg.rate,
		"achieved_rate_per_sec": round1(realRate),
		"duration_seconds":      cfg.duration.Seconds(),
		"read_ratio":            cfg.readRatio,
		"publish_attempts":      publishAttempts.Load(),
		"publish_ok":            publishOK.Load(),
		"get_attempts":          getAttempts.Load(),
		"get_ok":                getOK.Load(),
		"publish_p50_ms":        pubLatencies.pct(0.50),
		"publish_p95_ms":        pubLatencies.pct(0.95),
		"publish_p99_ms":        pubLatencies.pct(0.99),
		"get_p99_ms":            getLatencies.pct(0.99),
		"cache_by_id_hit_rate":  round4(hitRate),
		"e2e_lag_p99_seconds":   summary.E2ELagP99Seconds,
		"processed_rate":        summary.ProcessedRatePerSec,
		"dlq_count":             summary.DLQCount,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}

func parseFlags() config {
	base := flag.String("base", "http://localhost:8080", "producer base URL")
	metrics := flag.String("metrics", "", "producer /metrics URL (defaults to base+/metrics)")
	summary := flag.String("summary", "", "producer /api/metrics/summary URL (defaults to base+/api/metrics/summary)")
	rate := flag.Int("rate", 500, "target publish rate per second")
	dur := flag.Duration("dur", 30*time.Second, "load window duration")
	ratio := flag.Float64("read-ratio", 2.0, "reads per publish (drives cache hit rate)")
	conc := flag.Int("concurrency", 64, "worker goroutines")
	flag.Parse()

	if *rate <= 0 {
		log.Fatal("rate must be > 0")
	}
	cfg := config{
		baseURL:     strings.TrimRight(*base, "/"),
		metricsURL:  *metrics,
		summaryURL:  *summary,
		rate:        *rate,
		duration:    *dur,
		readRatio:   *ratio,
		concurrency: *conc,
	}
	if cfg.metricsURL == "" {
		cfg.metricsURL = cfg.baseURL + "/metrics"
	}
	if cfg.summaryURL == "" {
		cfg.summaryURL = cfg.baseURL + "/api/metrics/summary"
	}
	return cfg
}

func runOneCycle(
	ctx context.Context, client *http.Client, cfg config, ids *idBuffer,
	pubLat, getLat *latencyRecorder,
	pubAttempts, pubOK, getAttempts, getOK *atomic.Int64,
) {
	msgID, publishOK, dur := doPublish(ctx, client, cfg.baseURL)
	pubAttempts.Add(1)
	pubLat.record(dur)
	if publishOK && msgID != "" {
		pubOK.Add(1)
		ids.add(msgID)
	}

	// Reads = ratio × writes. Since we control this here, a ratio of 2
	// means each publish is followed by two reads.
	reads := probabilisticInt(cfg.readRatio)
	for i := 0; i < reads; i++ {
		id, ok := ids.random()
		if !ok {
			return
		}
		gotOK, gDur := doGet(ctx, client, cfg.baseURL, id)
		getAttempts.Add(1)
		getLat.record(gDur)
		if gotOK {
			getOK.Add(1)
		}
	}
}

func doPublish(ctx context.Context, client *http.Client, base string) (msgID string, ok bool, latency time.Duration) {
	body := `{"type":"payment.completed","priority":"normal","payload":{"amount":100,"currency":"USD"}}`
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := client.Do(req)
	latency = time.Since(start)
	if err != nil {
		return "", false, latency
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		io.Copy(io.Discard, resp.Body)
		return "", false, latency
	}
	var r struct {
		MessageID string `json:"message_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", false, latency
	}
	return r.MessageID, true, latency
}

func doGet(ctx context.Context, client *http.Client, base, id string) (ok bool, latency time.Duration) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/notifications/"+id, nil)
	start := time.Now()
	resp, err := client.Do(req)
	latency = time.Since(start)
	if err != nil {
		return false, latency
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	// 404 is a valid outcome if the worker has not yet persisted — count
	// it as a completed request but not "ok".
	return resp.StatusCode == http.StatusOK, latency
}

// probabilisticInt turns a float rate (e.g. 2.3) into 2 with 70% probability
// or 3 with 30% probability, so the long-run average matches.
func probabilisticInt(v float64) int {
	base := int(v)
	frac := v - float64(base)
	if rand.Float64() < frac {
		return base + 1
	}
	return base
}

type idBuffer struct {
	mu   sync.Mutex
	buf  []string
	next int
	size int
}

func newIDBuffer(cap int) *idBuffer {
	return &idBuffer{buf: make([]string, cap), size: 0}
}

func (b *idBuffer) add(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf[b.next] = id
	b.next = (b.next + 1) % len(b.buf)
	if b.size < len(b.buf) {
		b.size++
	}
}

func (b *idBuffer) random() (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size == 0 {
		return "", false
	}
	return b.buf[rand.IntN(b.size)], true
}

type latencyRecorder struct {
	mu  sync.Mutex
	obs []float64 // milliseconds
}

func (l *latencyRecorder) record(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.obs = append(l.obs, float64(d.Microseconds())/1000.0)
}

func (l *latencyRecorder) pct(p float64) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.obs) == 0 {
		return 0
	}
	// Copy so we can sort in-place without racing with concurrent record().
	// We hold the lock during sort — that's fine, only one caller (final
	// summary).
	sorted := make([]float64, len(l.obs))
	copy(sorted, l.obs)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	return round1(sorted[idx])
}

type summaryResp struct {
	E2ELagP99Seconds    float64 `json:"e2e_lag_p99_seconds"`
	ProcessedRatePerSec float64 `json:"processed_rate_per_sec"`
	DLQCount            int     `json:"dlq_count"`
}

func fetchSummary(ctx context.Context, url string) summaryResp {
	client := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return summaryResp{}
	}
	defer resp.Body.Close()
	var s summaryResp
	_ = json.NewDecoder(resp.Body).Decode(&s)
	return s
}

// readCacheCounters returns the current sum of cache_hits and cache_misses
// (scope="by_id") from the producer's /metrics endpoint. Called before/after
// the load window so hit rate is computed over the window, not lifetime.
func readCacheCounters(ctx context.Context, metricsURL string) (hits, misses float64) {
	client := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0
	}
	// Very small parser tuned for the two metrics we care about, so we
	// don't pull in prometheus/common as a build dep.
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "nexus_cache_hits_total") && strings.Contains(line, `scope="by_id"`) {
			hits += parseTrailingFloat(line)
		}
		if strings.HasPrefix(line, "nexus_cache_misses_total") && strings.Contains(line, `scope="by_id"`) {
			misses += parseTrailingFloat(line)
		}
	}
	return hits, misses
}

func parseTrailingFloat(line string) float64 {
	sp := strings.LastIndexByte(line, ' ')
	if sp < 0 {
		return 0
	}
	var v float64
	_, _ = fmt.Sscanf(line[sp+1:], "%f", &v)
	return v
}

func round1(v float64) float64 {
	return float64(int(v*10+0.5)) / 10
}

func round4(v float64) float64 {
	return float64(int(v*10000+0.5)) / 10000
}
