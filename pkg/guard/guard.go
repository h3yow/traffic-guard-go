// Package guard wires rate limiting, quota accounting, circuit breaking and
// upstream error classification into one entry-traffic protection component.
//
// Request pipeline (fixed order, every request):
//
//  1. shutdown gate — once Shutdown begins, no new request is admitted.
//  2. rate-limit/quota reservation — rate -> burst -> global precedence;
//     rejected requests book nothing and stop here.
//  3. circuit precheck — open (or probe already running) fast-fails; the
//     reserved quota is released and nothing is billed to the upstream.
//  4. upstream execution — a half-open probe runs under ProbeTimeout.
//  5. classification — the raw upstream error is normalized into one of
//     timeout / connection / business and fed back to the breaker.
//  6. settlement — quota is released exactly once and the outcome is tallied.
//
// Steps 1-3 and step 6 happen under one admission lock, so a request is
// either fully accepted or fully rejected; there is no window in which a
// slot is booked without an owner or an accepted request is dropped.
package guard

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/traffic-guard-go/pkg/circuit"
	"github.com/h3yow/traffic-guard-go/pkg/config"
	"github.com/h3yow/traffic-guard-go/pkg/ratelimit"
	"github.com/h3yow/traffic-guard-go/pkg/upstream"
)

// Outcome is the final distinguishable conclusion of a guarded request.
type Outcome string

const (
	OutcomeOK              Outcome = "ok"
	OutcomeRejectedRate    Outcome = "rejected_rate_exceeded"
	OutcomeRejectedBurst   Outcome = "rejected_burst_exceeded"
	OutcomeRejectedGlobal  Outcome = "rejected_global_cap"
	OutcomeCircuitOpen     Outcome = "circuit_open_fast_fail"
	OutcomeUpstreamTimeout Outcome = "upstream_timeout"
	OutcomeUpstreamConn    Outcome = "upstream_connection_failure"
	OutcomeUpstreamBiz     Outcome = "upstream_business_error"
	// OutcomeCanceled: the request was accepted but the caller abandoned it
	// (client disconnect / process shutdown) before the upstream answered.
	// It is settled and billed exactly once but is NOT an upstream failure.
	OutcomeCanceled     Outcome = "request_canceled"
	OutcomeShuttingDown Outcome = "rejected_shutting_down"
)

// Decision fully explains what happened to one request; it is logged and
// returned to callers/tests.
type Decision struct {
	Time       time.Time
	Client     string
	Outcome    Outcome
	Accepted   bool
	Quota      ratelimit.Snapshot
	Circuit    circuit.State
	ConsecFail uint32
	FailKind   upstream.Kind
	Detail     string
}

// Guard is the entry-traffic protection component.
type Guard struct {
	limiter *ratelimit.Limiter
	breaker *circuit.Breaker
	log     Logger

	now func() time.Time

	// Admission/settlement lock. Guards shutting, inFlight and outcomes.
	mu       sync.Mutex
	shutting bool
	inFlight int
	cond     *sync.Cond
	outcomes map[Outcome]int64

	wg sync.WaitGroup
}

// New builds a Guard from already-defaulted and validated configuration.
func New(cfg config.Config) *Guard {
	return NewWithLogger(cfg, nopLogger{})
}

// NewWithLogger builds a Guard that reports each decision to log.
func NewWithLogger(cfg config.Config, log Logger) *Guard {
	g := &Guard{
		limiter: ratelimit.New(
			cfg.RateLimit.RatePerSecond,
			*cfg.RateLimit.Burst,
			cfg.RateLimit.GlobalMaxInFlight,
			nil,
		),
		breaker: circuit.New(
			*cfg.Circuit.FailureThreshold,
			time.Duration(*cfg.Circuit.OpenCooldownMS)*time.Millisecond,
			time.Duration(*cfg.Circuit.ProbeTimeoutMS)*time.Millisecond,
			nil,
		),
		log:      log,
		now:      time.Now,
		outcomes: make(map[Outcome]int64),
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// newForTest injects a clock.
func newForTest(cfg config.Config, log Logger, now func() time.Time) *Guard {
	g := NewWithLogger(cfg, log)
	g.now = now
	g.limiter = ratelimit.New(
		cfg.RateLimit.RatePerSecond, *cfg.RateLimit.Burst,
		cfg.RateLimit.GlobalMaxInFlight, now)
	g.breaker = circuit.New(
		*cfg.Circuit.FailureThreshold,
		time.Duration(*cfg.Circuit.OpenCooldownMS)*time.Millisecond,
		time.Duration(*cfg.Circuit.ProbeTimeoutMS)*time.Millisecond, now)
	return g
}

// Serve runs one guarded request end to end and returns its explained
// Decision. It never returns until the request has a final conclusion; call
// errors are never propagated raw (see Outcome*Upstream*).
func (g *Guard) Serve(ctx context.Context, client string, call upstream.Func) (d Decision) {
	d.Time = g.now()
	d.Client = client

	// ---- Admission phase (atomic w.r.t. Shutdown) ----
	g.mu.Lock()
	if g.shutting {
		d.Outcome = OutcomeShuttingDown
		d.Detail = "shutdown in progress: new requests are not admitted"
		g.tallyLocked(d)
		g.mu.Unlock()
		LogDecision(g.log, d)
		return d
	}

	release, snap, ok, reason := g.limiter.Acquire(ctx, client)
	d.Quota = snap
	if !ok {
		switch reason {
		case ratelimit.ReasonRate:
			d.Outcome = OutcomeRejectedRate
			d.Detail = "token bucket empty: sustained rate exceeded"
		case ratelimit.ReasonBurst:
			d.Outcome = OutcomeRejectedBurst
			d.Detail = "client in-flight at burst cap"
		case ratelimit.ReasonGlobal:
			d.Outcome = OutcomeRejectedGlobal
			d.Detail = "process-wide in-flight cap reached"
		}
		g.tallyLocked(d)
		g.mu.Unlock()
		LogDecision(g.log, d)
		return d
	}

	allow, cstate := g.breaker.Before(ctx)
	d.Circuit = cstate
	d.ConsecFail = g.breaker.ConsecutiveFailures()
	if !allow {
		d.Outcome = OutcomeCircuitOpen
		d.Detail = fmt.Sprintf("circuit %s: fast-fail, consecutive failures=%d", cstate, d.ConsecFail)
		release() // quota booked in step 2 is returned; upstream is never called
		g.tallyLocked(d)
		g.mu.Unlock()
		LogDecision(g.log, d)
		return d
	}

	// Fully accepted: register before unlocking so Shutdown cannot miss it.
	g.inFlight++
	g.wg.Add(1)
	isProbe := cstate == circuit.StateHalfOpen
	g.mu.Unlock()

	// ---- Execution + settlement (outside admission lock) ----
	settled := false
	settle := func(outcome Outcome, kind upstream.Kind, detail string, success bool) {
		if settled {
			return
		}
		settled = true
		g.breaker.Result(success)
		release() // exactly once (release itself is idempotent too)
		d.Outcome = outcome
		d.FailKind = kind
		d.Detail = detail
		d.Circuit = g.breaker.RawState()
		d.ConsecFail = g.breaker.ConsecutiveFailures()
		d.Accepted = true
		g.mu.Lock()
		g.inFlight--
		g.cond.Broadcast()
		g.tallyLocked(d)
		g.mu.Unlock()
		LogDecision(g.log, d)
		g.wg.Done()
	}
	// settleIgnore settles a request whose result says nothing about
	// upstream health (caller cancellation): quota is released and the
	// request tallied, but the breaker streak is left untouched while closed.
	settleIgnore := func(outcome Outcome, kind upstream.Kind, detail string) {
		if settled {
			return
		}
		settled = true
		g.breaker.Ignore()
		release()
		d.Outcome = outcome
		d.FailKind = kind
		d.Detail = detail
		d.Circuit = g.breaker.RawState()
		d.ConsecFail = g.breaker.ConsecutiveFailures()
		d.Accepted = true
		g.mu.Lock()
		g.inFlight--
		g.cond.Broadcast()
		g.tallyLocked(d)
		g.mu.Unlock()
		LogDecision(g.log, d)
		g.wg.Done()
	}
	// Even a panicking upstream call must be settled: never silently dropped.
	defer func() {
		if r := recover(); r != nil {
			settle(OutcomeUpstreamBiz, upstream.KindBusiness,
				fmt.Sprintf("upstream panic recovered: %v", r), false)
		}
	}()

	callCtx := ctx
	if isProbe {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, g.breaker.ProbeTimeout())
		defer cancel()
	}
	rawErr := call(callCtx)
	if f := upstream.Classify(rawErr); f != nil {
		switch f.Kind {
		case upstream.KindCanceled:
			settleIgnore(OutcomeCanceled, f.Kind,
				"request canceled by caller or shutdown; not billed as upstream failure")
		case upstream.KindTimeout:
			settle(OutcomeUpstreamTimeout, f.Kind, f.Msg, false)
		case upstream.KindConnFailure:
			settle(OutcomeUpstreamConn, f.Kind, f.Msg, false)
		default:
			settle(OutcomeUpstreamBiz, f.Kind, f.Msg, false)
		}
		return d
	}
	settle(OutcomeOK, "", "upstream succeeded", true)
	return d
}

// tallyLocked records an outcome. Caller holds g.mu.
func (g *Guard) tallyLocked(d Decision) {
	g.outcomes[d.Outcome]++
}

// Shutdown performs deterministic wind-down in a fixed order:
//  1. close the admission gate (every later Serve returns OutcomeShuttingDown);
//  2. wait for every already-accepted request to settle (or ctx deadline).
//
// Accepted requests are never abandoned: their quota is released by their own
// settlement, and the wait ends only when in-flight reaches zero.
func (g *Guard) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	g.shutting = true
	g.mu.Unlock()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		remaining := g.inFlight
		g.mu.Unlock()
		return fmt.Errorf("shutdown timed out with %d request(s) in flight: %w", remaining, ctx.Err())
	}
}

// IsShuttingDown reports whether the admission gate is closed.
func (g *Guard) IsShuttingDown() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.shutting
}

// InFlight returns the number of accepted requests currently executing.
func (g *Guard) InFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlight
}

// Stats returns a point-in-time snapshot of all counters.
func (g *Guard) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[Outcome]int64, len(g.outcomes))
	for k, v := range g.outcomes {
		out[k] = v
	}
	return Stats{
		Limiter:  g.limiter.Stats(),
		Breaker:  g.breaker.Counters(),
		Outcomes: out,
		InFlight: g.inFlight,
	}
}

// Stats aggregates limiter and breaker counters plus outcome tallies.
type Stats struct {
	Limiter  ratelimit.Stats
	Breaker  circuit.Stats
	Outcomes map[Outcome]int64
	InFlight int
}
