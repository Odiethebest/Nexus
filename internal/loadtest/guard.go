package loadtest

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrUnauthorized   = errors.New("loadtest: unauthorized")
	ErrDisabled       = errors.New("loadtest: disabled")
	ErrParallelLimit  = errors.New("loadtest: maximum parallel runs reached")
	ErrCooldown       = errors.New("loadtest: cooldown active")
	ErrThrottled      = errors.New("loadtest: start throttled")
	ErrBudgetExceeded = errors.New("loadtest: daily VUH budget exceeded")
)

// CooldownError exposes the remaining wait duration.
type CooldownError struct {
	Remaining time.Duration
}

func (e *CooldownError) Error() string {
	return fmt.Sprintf("%v: retry in %s", ErrCooldown, e.Remaining.Truncate(time.Second))
}

func (e *CooldownError) Unwrap() error { return ErrCooldown }

// ThrottleError exposes the remaining actor-specific throttle wait.
type ThrottleError struct {
	Remaining time.Duration
}

func (e *ThrottleError) Error() string {
	return fmt.Sprintf("%v: retry in %s", ErrThrottled, e.Remaining.Truncate(time.Second))
}

func (e *ThrottleError) Unwrap() error { return ErrThrottled }

// GuardConfig contains server-side policy controls.
type GuardConfig struct {
	AdminKey         string
	MaxParallel      int
	Cooldown         time.Duration
	MinStartInterval time.Duration
}

// Guard enforces auth, parallelism, cooldown and throttle checks.
type Guard struct {
	cfg GuardConfig

	mu sync.Mutex

	inflight           int
	activeRunStartedAt map[int64]time.Time
	lastFinishedAt     time.Time
	lastAttemptByActor map[string]time.Time
}

// StartReservation tracks an in-flight start attempt. Call Commit on success
// or Cancel on failure.
type StartReservation struct {
	guard *Guard
	once  sync.Once
}

// NewGuard creates a policy guard with safe defaults.
func NewGuard(cfg GuardConfig) *Guard {
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = 1
	}
	return &Guard{
		cfg:                cfg,
		activeRunStartedAt: make(map[int64]time.Time),
		lastAttemptByActor: make(map[string]time.Time),
	}
}

// Authorize validates the admin key using constant-time comparison.
func (g *Guard) Authorize(given string) error {
	if g.cfg.AdminKey == "" {
		return nil
	}
	if subtle.ConstantTimeCompare([]byte(g.cfg.AdminKey), []byte(given)) != 1 {
		return ErrUnauthorized
	}
	return nil
}

// ReserveStart performs policy checks and reserves one start slot.
func (g *Guard) ReserveStart(actor string, now time.Time) (*StartReservation, error) {
	if actor == "" {
		actor = "global"
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cfg.MinStartInterval > 0 {
		if prev, ok := g.lastAttemptByActor[actor]; ok {
			remaining := g.cfg.MinStartInterval - now.Sub(prev)
			if remaining > 0 {
				return nil, &ThrottleError{Remaining: remaining}
			}
		}
	}

	if g.cfg.Cooldown > 0 && !g.lastFinishedAt.IsZero() {
		remaining := g.cfg.Cooldown - now.Sub(g.lastFinishedAt)
		if remaining > 0 {
			return nil, &CooldownError{Remaining: remaining}
		}
	}

	if len(g.activeRunStartedAt)+g.inflight >= g.cfg.MaxParallel {
		return nil, ErrParallelLimit
	}

	g.inflight++
	g.lastAttemptByActor[actor] = now

	return &StartReservation{guard: g}, nil
}

// Commit finalizes a successful run start and marks the run as active.
func (r *StartReservation) Commit(runID int64, startedAt time.Time) {
	r.once.Do(func() {
		g := r.guard
		g.mu.Lock()
		defer g.mu.Unlock()

		if g.inflight > 0 {
			g.inflight--
		}
		g.activeRunStartedAt[runID] = startedAt
	})
}

// Cancel releases the in-flight reservation when start fails.
func (r *StartReservation) Cancel() {
	r.once.Do(func() {
		g := r.guard
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.inflight > 0 {
			g.inflight--
		}
	})
}

// MarkFinished removes an active run and starts cooldown.
func (g *Guard) MarkFinished(runID int64, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.activeRunStartedAt[runID]; !ok {
		return false
	}
	delete(g.activeRunStartedAt, runID)
	g.lastFinishedAt = now
	return true
}

// ActiveCount returns active run count (excluding in-flight starts).
func (g *Guard) ActiveCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.activeRunStartedAt)
}

// ActiveRunStartedAt returns the tracked start timestamp for a run.
func (g *Guard) ActiveRunStartedAt(runID int64) (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	startedAt, ok := g.activeRunStartedAt[runID]
	return startedAt, ok
}
