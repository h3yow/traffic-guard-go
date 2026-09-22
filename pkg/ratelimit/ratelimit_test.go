package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a controllable time source.
type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func newTestLimiter(t *testing.T, rate, burst, global int) (*Limiter, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)}
	return New(rate, burst, global, clk.now), clk
}

func logDecision(t *testing.T, client string, snap Snapshot, allowed bool, reason Reason) {
	t.Helper()
	t.Logf("request client=%s | tokens=%.3f/%.0f inFlight=%d/%d global=%d/%d => allowed=%v reason=%q",
		client, snap.TokensAvail, snap.BucketCap, snap.InFlight, snap.BurstLimit,
		snap.GlobalInFlight, snap.GlobalLimit, allowed, reason)
}

func TestBurstAndGlobalReasons(t *testing.T) {
	// rate=1000 so tokens never bind; burst per client 2; global 3.
	l, _ := newTestLimiter(t, 1000, 2, 3)
	ctx := context.Background()

	var releases []func()
	admit := func(client string) (bool, Reason) {
		r, snap, ok, reason := l.Acquire(ctx, client)
		logDecision(t, client, snap, ok, reason)
		if ok {
			releases = append(releases, r)
		}
		return ok, reason
	}

	if ok, _ := admit("a"); !ok {
		t.Fatal("a#1 must pass")
	}
	if ok, _ := admit("a"); !ok {
		t.Fatal("a#2 must pass")
	}
	// Third for same client hits burst before global.
	if ok, reason := admit("a"); ok || reason != ReasonBurst {
		t.Fatalf("a#3: want burst reject, got ok=%v reason=%v", ok, reason)
	}
	// Other client: two slots remain globally (used 2/3).
	if ok, _ := admit("b"); !ok {
		t.Fatal("b#1 must pass")
	}
	// Global now 3/3 -> global rejection for any client.
	if ok, reason := admit("b"); ok || reason != ReasonGlobal {
		t.Fatalf("b#2: want global reject, got ok=%v reason=%v", ok, reason)
	}
	if ok, reason := admit("c"); ok || reason != ReasonGlobal {
		t.Fatalf("c#1: want global reject, got ok=%v reason=%v", ok, reason)
	}
	// Release one a-slot: a is still at burst (2), so b should now get in.
	releases[0]()
	if ok, reason := admit("b"); !ok {
		t.Fatalf("after release b must pass, reason=%v", reason)
	}
}

func TestRateRefill(t *testing.T) {
	// rate=2/sec, burst 10 (irrelevant), global huge.
	l, clk := newTestLimiter(t, 2, 10, 1000)
	ctx := context.Background()
	take := func() (bool, Reason) {
		r, snap, ok, reason := l.Acquire(ctx, "c")
		logDecision(t, "c", snap, ok, reason)
		if ok {
			r()
		}
		return ok, reason
	}
	// Initial bucket is full at 2 tokens: 2 admitted immediately.
	if ok, _ := take(); !ok {
		t.Fatal("token 1")
	}
	if ok, _ := take(); !ok {
		t.Fatal("token 2")
	}
	// Third straight away: bucket empty -> rate reason.
	if ok, reason := take(); ok || reason != ReasonRate {
		t.Fatalf("want rate reject, got %v %v", ok, reason)
	}
	// Advance 500ms -> ~1 refilled token.
	clk.advance(500 * time.Millisecond)
	if ok, _ := take(); !ok {
		t.Fatal("after 500ms one token must be available")
	}
	if ok, reason := take(); ok || reason != ReasonRate {
		t.Fatalf("second take after 1 token must rate-reject, got %v %v", ok, reason)
	}
	// Long wait never exceeds capacity (2).
	clk.advance(10 * time.Second)
	if ok, _ := take(); !ok {
		t.Fatal("token after long wait 1")
	}
	if ok, _ := take(); !ok {
		t.Fatal("token after long wait 2")
	}
	if ok, reason := take(); ok || reason != ReasonRate {
		t.Fatalf("bucket must cap at rate, got %v %v", ok, reason)
	}
}

func TestConcurrentNoOverAdmission(t *testing.T) {
	// Huge rate so tokens never bind; per-client burst 5; global cap 12.
	l, _ := newTestLimiter(t, 100000, 5, 12)
	ctx := context.Background()

	const goroutines = 200
	var wg sync.WaitGroup
	var inFlightA, maxInFlightA int64
	var globalMax int64
	var mu sync.Mutex // guards max snapshots under real concurrency
	var admitted int64
	var rejRate, rejBurst, rejGlobal int64
	var gate sync.WaitGroup
	gate.Add(1)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		client := fmt.Sprintf("c%d", i%4) // 4 clients
		go func(client string) {
			defer wg.Done()
			gate.Wait() // maximize contention: all goroutines start together
			release, snap, ok, reason := l.Acquire(ctx, client)
			t.Logf("concurrent request client=%s tokens=%.2f inFlight=%d/%d global=%d/%d => allowed=%v reason=%q",
				client, snap.TokensAvail, snap.InFlight, snap.BurstLimit,
				snap.GlobalInFlight, snap.GlobalLimit, ok, reason)
			if !ok {
				switch reason {
				case ReasonRate:
					atomic.AddInt64(&rejRate, 1)
				case ReasonBurst:
					atomic.AddInt64(&rejBurst, 1)
				case ReasonGlobal:
					atomic.AddInt64(&rejGlobal, 1)
				}
				return
			}
			atomic.AddInt64(&admitted, 1)
			curGlobal := int64(0)
			if client == "c0" {
				curGlobal = int64(atomic.AddInt64(&inFlightA, 1))
			}
			mu.Lock()
			if curGlobal > maxInFlightA {
				maxInFlightA = curGlobal
			}
			g := int64(l.InFlight())
			if g > globalMax {
				globalMax = g
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
			if client == "c0" {
				atomic.AddInt64(&inFlightA, -1)
			}
			release()
		}(client)
	}
	gate.Done()
	wg.Wait()

	if globalMax > 12 {
		t.Fatalf("global cap violated: peak in-flight=%d > 12", globalMax)
	}
	if maxInFlightA > 5 {
		t.Fatalf("client burst violated: peak c0 in-flight=%d > 5", maxInFlightA)
	}
	if l.InFlight() != 0 {
		t.Fatalf("all slots must be returned, got %d", l.InFlight())
	}
	st := l.Stats()
	if st.Allowed != admitted {
		t.Errorf("allowed counter drift: stats=%d observed=%d", st.Allowed, admitted)
	}
	if st.Allowed+st.RejectedRate+st.RejectedBurst+st.RejectedGlobal != goroutines {
		t.Errorf("accounting lost/duplicated: %+v (total=%d)", st, goroutines)
	}
	t.Logf("SUMMARY admitted=%d rejected(rate=%d burst=%d global=%d) peakGlobal=%d",
		admitted, rejRate, rejBurst, rejGlobal, globalMax)
}

func TestReleaseIdempotent(t *testing.T) {
	l, _ := newTestLimiter(t, 1000, 1, 1)
	r, _, ok, _ := l.Acquire(context.Background(), "x")
	if !ok {
		t.Fatal("admit")
	}
	r()
	r()
	r()
	if l.InFlight() != 0 {
		t.Fatalf("duplicate release must not double-count: inFlight=%d", l.InFlight())
	}
	if _, _, ok, _ := l.Acquire(context.Background(), "x"); !ok {
		t.Fatal("slot must be available after release")
	}
}
