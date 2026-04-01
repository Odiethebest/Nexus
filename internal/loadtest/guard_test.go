package loadtest

import (
	"errors"
	"testing"
	"time"
)

func TestGuard_ParallelLimitAndMinStartInterval(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	g := NewGuard(GuardConfig{
		MaxParallel:      1,
		MinStartInterval: 2 * time.Second,
	})

	resA, err := g.ReserveStart("actor-a", now)
	if err != nil {
		t.Fatalf("reserve actor-a: %v", err)
	}

	if _, err := g.ReserveStart("actor-b", now); !errors.Is(err, ErrParallelLimit) {
		t.Fatalf("expected ErrParallelLimit, got %v", err)
	}

	resA.Commit(1001, now)
	if !g.MarkFinished(1001, now) {
		t.Fatalf("expected run 1001 to be marked finished")
	}

	_, err = g.ReserveStart("actor-a", now.Add(1*time.Second))
	if !errors.Is(err, ErrThrottled) {
		t.Fatalf("expected ErrThrottled, got %v", err)
	}

	resB, err := g.ReserveStart("actor-a", now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("reserve after min interval: %v", err)
	}
	resB.Cancel()
}

func TestGuard_Cooldown(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	g := NewGuard(GuardConfig{
		MaxParallel: 1,
		Cooldown:    90 * time.Second,
	})

	res, err := g.ReserveStart("actor-a", now)
	if err != nil {
		t.Fatalf("reserve start: %v", err)
	}
	res.Commit(2001, now)
	if !g.MarkFinished(2001, now) {
		t.Fatalf("expected run 2001 to be marked finished")
	}

	_, err = g.ReserveStart("actor-a", now.Add(30*time.Second))
	if !errors.Is(err, ErrCooldown) {
		t.Fatalf("expected ErrCooldown, got %v", err)
	}

	res2, err := g.ReserveStart("actor-a", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("reserve after cooldown: %v", err)
	}
	res2.Cancel()
}
