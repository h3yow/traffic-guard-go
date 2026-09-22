package guard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/traffic-guard-go/pkg/config"
	"github.com/h3yow/traffic-guard-go/pkg/upstream"
)

func testCfg(rate, burst, global int, threshold uint32, cooldownMS, probeMS int64) config.Config {
	b := burst
	th := threshold
	cd := cooldownMS
	pt := probeMS
	return config.Config{
		RateLimit: config.RateLimitConfig{RatePerSecond: rate, Burst: &b, GlobalMaxInFlight: global},
		Circuit:   config.CircuitConfig{FailureThreshold: &th, OpenCooldownMS: &cd, ProbeTimeoutMS: &pt},
	}
}

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func okCall() upstream.Func {
	return func(ctx context.Context) error { return nil }
}

func failCall(err error) upstream.Func {
	return func(ctx context.Context) error { return err }
}

func TestRejectionReasonsDistinguishable(t *testing.T) {
	g := NewWithLogger(testCfg(10000, 2, 3, 5, 5000, 2000), testLogger{t})
	ctx := context.Background()

	// Accepted requests run in goroutines and block on gate; Serve itself
	// only returns when the call finishes, so we keep them in flight.
	gate := make(chan struct{})
	var serveWG sync.WaitGroup
	admitAsync := func(client string) <-chan Decision {
		ch := make(chan Decision, 1)
		serveWG.Add(1)
		go func() {
			defer serveWG.Done()
			ch <- g.Serve(ctx, client, func(ctx context.Context) error {
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		return ch
	}

	admitAsync("a") // a#1 accepted
	admitAsync("a") // a#2 accepted
	waitInflight(t, g, 2)

	// a#3: client burst (deterministic precedence over global).
	if d := g.Serve(ctx, "a", okCall()); d.Outcome != OutcomeRejectedBurst {
		t.Fatalf("a#3 want burst, got %s (%s)", d.Outcome, d.Detail)
	}
	// b#1: global 2/3 -> accepted.
	admitAsync("b")
	waitInflight(t, g, 3)
	// b#2 and c#1: global 3/3 -> global rejection.
	if d := g.Serve(ctx, "b", okCall()); d.Outcome != OutcomeRejectedGlobal {
		t.Fatalf("b#2 want global, got %s", d.Outcome)
	}
	if d := g.Serve(ctx, "c", okCall()); d.Outcome != OutcomeRejectedGlobal {
		t.Fatalf("c#1 want global, got %s", d.Outcome)
	}
	close(gate)
	serveWG.Wait()
	if g.InFlight() != 0 {
		t.Fatalf("in-flight must drain: %d", g.InFlight())
	}

	// Rate reason: rate=1/s, first call takes the initial token.
	g2 := NewWithLogger(testCfg(1, 10, 100, 5, 5000, 2000), testLogger{t})
	if d := g2.Serve(ctx, "z", okCall()); d.Outcome != OutcomeOK {
		t.Fatalf("z1 %s", d.Outcome)
	}
	if d := g2.Serve(ctx, "z", okCall()); d.Outcome != OutcomeRejectedRate {
		t.Fatalf("z2 want rate, got %s", d.Outcome)
	}
}

func waitInflight(t *testing.T, g *Guard, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for g.InFlight() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if g.InFlight() != want {
		t.Fatalf("want %d in flight, got %d", want, g.InFlight())
	}
}

func TestUpstreamFailuresNormalizedAndBreak(t *testing.T) {
	cfg := testCfg(10000, 100, 100, 3, 1000, 2000)
	g := NewWithLogger(cfg, testLogger{t})
	ctx := context.Background()

	// Two timeouts + one connection failure -> 3 consecutive failures -> open.
	feeds := []error{
		context.DeadlineExceeded,
		&upstream.TimeoutError{Detail: "slow"},
		fmt.Errorf("dial tcp: connection refused"),
	}
	wantKinds := []Outcome{OutcomeUpstreamTimeout, OutcomeUpstreamTimeout, OutcomeUpstreamConn}
	for i, err := range feeds {
		d := g.Serve(ctx, "c", failCall(err))
		if d.Outcome != wantKinds[i] {
			t.Fatalf("call %d want %s got %s", i+1, wantKinds[i], d.Outcome)
		}
		if d.Accepted != true {
			t.Fatal("upstream failure means request was accepted and executed")
		}
	}
	// 4th call: circuit open fast-fail; upstream must NOT be invoked.
	var invoked int32
	d := g.Serve(ctx, "c", func(ctx context.Context) error {
		atomic.AddInt32(&invoked, 1)
		return nil
	})
	if d.Outcome != OutcomeCircuitOpen || invoked != 0 {
		t.Fatalf("want circuit open without invoking upstream, got %s invoked=%d", d.Outcome, invoked)
	}
	if d.Quota.GlobalInFlight != 0 {
		t.Fatalf("fast-failed request must hold no quota, globalInFlight=%d", d.Quota.GlobalInFlight)
	}

	// After cooldown: one probe. Business failure kind still distinguishable.
	time.Sleep(1100 * time.Millisecond)
	d = g.Serve(ctx, "c", failCall(&upstream.BusinessError{Code: 500, Msg: "boom"}))
	if d.Outcome != OutcomeUpstreamBiz {
		t.Fatalf("half-open probe business error: got %s", d.Outcome)
	}
	if g.breaker.RawState() != "open" {
		t.Fatalf("failed probe must reopen, got %s", g.breaker.RawState())
	}
	// Probe success after another cooldown closes the circuit.
	time.Sleep(1100 * time.Millisecond)
	d = g.Serve(ctx, "c", okCall())
	if d.Outcome != OutcomeOK || g.breaker.RawState() != "closed" {
		t.Fatalf("successful probe must close, outcome=%s state=%s", d.Outcome, g.breaker.RawState())
	}
}

func TestProbeTimeoutBound(t *testing.T) {
	// threshold=1 so first failure opens; cooldown 50ms; probe timeout 80ms.
	g := NewWithLogger(testCfg(10000, 10, 100, 1, 50, 80), testLogger{t})
	d := g.Serve(context.Background(), "c", failCall(errors.New("boom")))
	if d.Outcome != OutcomeUpstreamBiz {
		t.Fatal(d.Outcome)
	}
	time.Sleep(60 * time.Millisecond)
	start := time.Now()
	d = g.Serve(context.Background(), "c", func(ctx context.Context) error {
		// Respect the probe deadline the guard attaches.
		<-ctx.Done()
		return ctx.Err() // normalized as timeout
	})
	elapsed := time.Since(start)
	if d.Outcome != OutcomeUpstreamTimeout {
		t.Fatalf("probe must conclude as timeout, got %s", d.Outcome)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("probe was not bounded by ProbeTimeout: %v", elapsed)
	}
	t.Logf("probe bounded in %v", elapsed)
}

func TestPanicRecoveredNotDropped(t *testing.T) {
	g := NewWithLogger(testCfg(10000, 1, 10, 5, 5000, 2000), testLogger{t})
	d := g.Serve(context.Background(), "c", func(ctx context.Context) error {
		panic("kaboom")
	})
	if d.Outcome != OutcomeUpstreamBiz {
		t.Fatalf("panic must normalize to business failure, got %s", d.Outcome)
	}
	if g.InFlight() != 0 {
		t.Fatal("panicked request must settle and release quota")
	}
	// Quota reusable.
	if d2 := g.Serve(context.Background(), "c", okCall()); d2.Outcome != OutcomeOK {
		t.Fatalf("quota not released after panic: %s", d2.Outcome)
	}
}

func TestGracefulShutdown(t *testing.T) {
	g := NewWithLogger(testCfg(10000, 50, 50, 5, 5000, 2000), testLogger{t})

	var started int64
	var completed int64
	release := make(chan struct{})
	var wg sync.WaitGroup
	const n = 20
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := fmt.Sprintf("cli-%d", i%5)
			d := g.Serve(context.Background(), client, func(ctx context.Context) error {
				atomic.AddInt64(&started, 1)
				select {
				case <-release:
					atomic.AddInt64(&completed, 1)
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if d.Outcome != OutcomeOK {
				t.Errorf("accepted request must conclude ok, got %s", d.Outcome)
			}
		}(i)
	}
	// Ensure all requests were accepted and are in flight.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&started) != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt64(&started) != n {
		t.Fatalf("only %d/%d started", atomic.LoadInt64(&started), n)
	}

	// Begin shutdown while requests are executing: no new requests allowed.
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- g.Shutdown(context.Background()) }()

	// Give Shutdown a moment to flip the gate, then prove new requests fail.
	time.Sleep(30 * time.Millisecond)
	d := g.Serve(context.Background(), "late-client", okCall())
	if d.Outcome != OutcomeShuttingDown {
		t.Fatalf("request after shutdown begins must be rejected as shutting down, got %s", d.Outcome)
	}
	if d.Accepted {
		t.Fatal("shutdown rejection must not be accepted/billed")
	}

	// Shutdown must still be waiting because accepted requests are running.
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before in-flight drained: %v", err)
	default:
	}

	// Release accepted requests; they must all finish and be counted once.
	close(release)
	wg.Wait()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("clean shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not return after drain")
	}
	if completed != n {
		t.Fatalf("accepted requests silently dropped: completed=%d want %d", completed, n)
	}
	st := g.Stats()
	if st.InFlight != 0 || g.limiter.InFlight() != 0 {
		t.Fatalf("quota leaked after shutdown: guard=%d limiter=%d", st.InFlight, g.limiter.InFlight())
	}
	if st.Outcomes[OutcomeOK] != n {
		t.Fatalf("ok tally=%d want %d (no double/missing billing)", st.Outcomes[OutcomeOK], n)
	}
	if st.Outcomes[OutcomeShuttingDown] < 1 {
		t.Fatal("shutdown rejection must be tallied")
	}
	// After shutdown every further request is rejected.
	if d := g.Serve(context.Background(), "x", okCall()); d.Outcome != OutcomeShuttingDown {
		t.Fatalf("post-shutdown request: %s", d.Outcome)
	}
}

func TestShutdownTimeoutReportsRemaining(t *testing.T) {
	g := NewWithLogger(testCfg(10000, 10, 10, 5, 5000, 2000), testLogger{t})
	block := make(chan struct{})
	go g.Serve(context.Background(), "c", func(ctx context.Context) error {
		<-block
		return nil
	})
	time.Sleep(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := g.Shutdown(ctx)
	if err == nil {
		t.Fatal("shutdown with a stuck request must time out with an error")
	}
	t.Logf("expected shutdown error: %v", err)
	close(block)
}

func TestOutcomeCountersSum(t *testing.T) {
	g := NewWithLogger(testCfg(1, 1, 2, 5, 5000, 2000), testLogger{t})
	ctx := context.Background()
	var total int64
	for i := 0; i < 50; i++ {
		g.Serve(ctx, fmt.Sprintf("c%d", i), func(ctx context.Context) error { return io.EOF })
		total++
	}
	st := g.Stats()
	var sum int64
	for _, v := range st.Outcomes {
		sum += v
	}
	if sum != total {
		t.Fatalf("outcome tally %d != requests %d (%+v)", sum, total, st.Outcomes)
	}
}

func TestCanceledNotBilledAsFailure(t *testing.T) {
	// threshold=2: a canceled request must not count toward opening.
	g := NewWithLogger(testCfg(10000, 5, 100, 2, 5000, 2000), testLogger{t})

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan Decision, 1)
	go func() {
		done1 <- g.Serve(ctx1, "c", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	time.Sleep(30 * time.Millisecond) // accepted and in flight
	cancel1()
	d := <-done1
	if d.Outcome != OutcomeCanceled {
		t.Fatalf("canceled call => %s, want request_canceled", d.Outcome)
	}
	if d.ConsecFail != 0 {
		t.Fatalf("canceled call must not raise failure streak, got %d", d.ConsecFail)
	}
	// One genuine business failure (below threshold 2) must still leave the
	// breaker closed, proving cancellation did not consume failure budget.
	if d := g.Serve(context.Background(), "c", failCall(&upstream.BusinessError{Msg: "x"})); d.Outcome != OutcomeUpstreamBiz {
		t.Fatalf("got %s", d.Outcome)
	}
	if g.breaker.RawState() != "closed" {
		t.Fatalf("breaker must stay closed: %s", g.breaker.RawState())
	}
	if g.InFlight() != 0 {
		t.Fatal("canceled request must settle and release quota")
	}
}
