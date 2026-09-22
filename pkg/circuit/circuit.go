// Package circuit provides a three-state (closed/open/half-open) circuit
// breaker for upstream calls.
//
// Rules (documented in README.md):
//
//   - closed: every call is allowed; consecutive failures are counted; a
//     success resets the streak to zero. When the streak reaches
//     FailureThreshold the breaker transitions to open.
//   - open: all calls fast-fail until OpenCooldown elapses. Then the breaker
//     moves to half-open and admits exactly ONE trial probe; concurrent
//     arrivals while the probe is outstanding fast-fail.
//   - half-open: probe success -> closed (streak reset, traffic resumed);
//     probe failure -> open again with a fresh cooldown.
//
// All state transitions happen under one mutex; decisions are therefore
// serial and deterministic.
package circuit

import (
	"context"
	"sync"
	"time"
)

// State of the breaker.
type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

// Breaker guards upstream execution.
type Breaker struct {
	threshold uint32
	cooldown  time.Duration
	probeTO   time.Duration
	now       func() time.Time

	mu          sync.Mutex
	state       State
	fails       uint32
	openedAt    time.Time
	probeActive bool

	successes       int64
	failures        int64
	fastFailed      int64
	openTransitions int64
}

// New creates a Breaker. now may be nil (time.Now); tests inject a clock.
func New(failureThreshold uint32, openCooldown, probeTimeout time.Duration, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	return &Breaker{
		threshold: failureThreshold,
		cooldown:  openCooldown,
		probeTO:   probeTimeout,
		now:       now,
		state:     StateClosed,
	}
}

// ProbeTimeout returns the configured bound for a half-open probe call.
func (b *Breaker) ProbeTimeout() time.Duration { return b.probeTO }

// Before is consulted before executing the upstream call.
// allow=false means fast-fail (circuit open, or a half-open probe is already
// in flight). It may transition open->half-open as a side effect when the
// cooldown has elapsed.
func (b *Breaker) Before(ctx context.Context) (allow bool, state State) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true, StateClosed
	case StateOpen:
		if b.now().Sub(b.openedAt) < b.cooldown {
			b.fastFailed++
			return false, StateOpen
		}
		// Cooldown elapsed: become half-open and admit the single probe.
		b.state = StateHalfOpen
		b.probeActive = true
		return true, StateHalfOpen
	default: // half-open: exactly one probe at a time
		if b.probeActive {
			b.fastFailed++
			return false, StateHalfOpen
		}
		b.probeActive = true
		return true, StateHalfOpen
	}
}

// Result records the outcome of an allowed call and drives transitions.
func (b *Breaker) Result(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if success {
		b.successes++
		b.fails = 0
		b.probeActive = false
		b.state = StateClosed
		return
	}

	b.failures++
	b.fails++
	b.probeActive = false
	switch b.state {
	case StateHalfOpen:
		// Failed probe: re-open with a fresh cooldown.
		b.toOpenLocked()
	case StateClosed:
		if b.fails >= b.threshold {
			b.toOpenLocked()
		}
	case StateOpen:
		// Defensive; normally open calls never execute.
		b.toOpenLocked()
	}
}

// Ignore records a call whose outcome says nothing about upstream health
// (e.g. the caller canceled before the upstream answered, or the process is
// shutting down). It neither raises nor resets the failure streak while
// closed. A half-open probe with an unknown outcome conservatively re-opens
// the circuit rather than resuming full traffic on unverified health.
func (b *Breaker) Ignore() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeActive = false
	if b.state == StateHalfOpen {
		b.toOpenLocked()
	}
}

// toOpenLocked opens the circuit. Caller holds b.mu.
func (b *Breaker) toOpenLocked() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.openTransitions++
}

// State returns the current state (applying cooldown expiry for observability).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		return StateHalfOpen // probe not yet taken, but observably ready
	}
	return b.state
}

// RawState returns the state without the cooldown-expiry projection.
func (b *Breaker) RawState() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// ConsecutiveFailures returns the current failure streak.
func (b *Breaker) ConsecutiveFailures() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fails
}

// Stats holds breaker counters.
type Stats struct {
	Successes       int64
	Failures        int64
	FastFailed      int64
	OpenTransitions int64
}

// Counters returns breaker statistics.
func (b *Breaker) Counters() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		Successes:       b.successes,
		Failures:        b.failures,
		FastFailed:      b.fastFailed,
		OpenTransitions: b.openTransitions,
	}
}
