package circuit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clk struct{ t time.Time }

func (c *clk) now() time.Time      { return c.t }
func (c *clk) add(d time.Duration) { c.t = c.t.Add(d) }

func newBreaker(t *testing.T, threshold uint32) (*Breaker, *clk) {
	c := &clk{t: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)}
	return New(threshold, 5*time.Second, 200*time.Millisecond, c.now), c
}

func logB(t *testing.T, b *Breaker, phase string, allow bool) {
	t.Helper()
	t.Logf("%-28s state=%s consecFails=%d allow=%v counters=%+v",
		phase, b.RawState(), b.ConsecutiveFailures(), allow, b.Counters())
}

func TestOpensAfterThresholdAndFastFails(t *testing.T) {
	b, c := newBreaker(t, 3)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		allow, _ := b.Before(ctx)
		logB(t, b, "before failure", allow)
		if !allow {
			t.Fatalf("failure #%d must be allowed while closed", i+1)
		}
		b.Result(false)
	}
	if b.RawState() != StateOpen {
		t.Fatalf("after 3 consecutive failures want open, got %s", b.RawState())
	}
	// During cooldown everything fast-fails.
	for i := 0; i < 5; i++ {
		allow, state := b.Before(ctx)
		logB(t, b, "during cooldown", allow)
		if allow || state != StateOpen {
			t.Fatalf("request during cooldown must fast-fail open, got allow=%v state=%s", allow, state)
		}
	}
	// Cooldown not fully elapsed -> still open.
	c.add(4999 * time.Millisecond)
	if allow, _ := b.Before(ctx); allow {
		t.Fatal("1ms before cooldown end must still fast-fail")
	}
}

func TestProbeSuccessCloses(t *testing.T) {
	b, c := newBreaker(t, 2)
	ctx := context.Background()
	b.Before(ctx)
	b.Result(false)
	b.Before(ctx)
	b.Result(false)
	if b.RawState() != StateOpen {
		t.Fatal("want open")
	}
	c.add(5 * time.Second)

	// First call after cooldown is the unique probe.
	allow, state := b.Before(ctx)
	logB(t, b, "probe taken", allow)
	if !allow || state != StateHalfOpen {
		t.Fatalf("post-cooldown call must be the half-open probe, got allow=%v state=%s", allow, state)
	}
	// Concurrent arrivals while probe outstanding must fast-fail.
	var probesAdmitted int64
	var wg sync.WaitGroup
	var gate sync.WaitGroup
	gate.Add(1)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.Wait()
			if ok, _ := b.Before(ctx); ok {
				atomic.AddInt64(&probesAdmitted, 1)
			}
		}()
	}
	gate.Done()
	wg.Wait()
	if probesAdmitted != 0 {
		t.Fatalf("second concurrent probe must not be admitted, got %d", probesAdmitted)
	}

	// Probe succeeds -> closed, normal traffic resumes, streak reset.
	b.Result(true)
	logB(t, b, "after probe success", true)
	if b.RawState() != StateClosed || b.ConsecutiveFailures() != 0 {
		t.Fatalf("want closed with zero streak, got %s fails=%d", b.RawState(), b.ConsecutiveFailures())
	}
	if allow, _ := b.Before(ctx); !allow {
		t.Fatal("traffic must resume after successful probe")
	}
}

func TestProbeFailureReopens(t *testing.T) {
	b, c := newBreaker(t, 1)
	ctx := context.Background()
	b.Before(ctx)
	b.Result(false) // open immediately (threshold 1)
	c.add(5 * time.Second)
	allow, _ := b.Before(ctx)
	if !allow || b.RawState() != StateHalfOpen {
		t.Fatal("want half-open probe")
	}
	b.Result(false)
	logB(t, b, "after failed probe", false)
	if b.RawState() != StateOpen {
		t.Fatalf("failed probe must re-open, got %s", b.RawState())
	}
	if allow, state := b.Before(ctx); allow || state != StateOpen {
		t.Fatal("must fast-fail again after failed probe")
	}
	if b.Counters().OpenTransitions != 2 {
		t.Fatalf("want 2 open transitions, got %d", b.Counters().OpenTransitions)
	}
}

func TestHealthyUpstreamNotTripped(t *testing.T) {
	b, _ := newBreaker(t, 3)
	ctx := context.Background()
	// Interleaved failures stay below threshold because successes reset.
	for cycle := 0; cycle < 10; cycle++ {
		for i := 0; i < 2; i++ {
			if ok, _ := b.Before(ctx); !ok {
				t.Fatal("healthy-ish upstream must not fast-fail")
			}
			b.Result(false)
		}
		if ok, _ := b.Before(ctx); !ok {
			t.Fatal("must stay closed")
		}
		b.Result(true) // resets streak
	}
	if b.RawState() != StateClosed {
		t.Fatalf("streak never reached threshold, want closed, got %s", b.RawState())
	}
	// All-success workload.
	for i := 0; i < 100; i++ {
		b.Before(ctx)
		b.Result(true)
	}
	if b.RawState() != StateClosed || b.Counters().FastFailed != 0 {
		t.Fatal("all-success upstream must never open or fast-fail")
	}
}
