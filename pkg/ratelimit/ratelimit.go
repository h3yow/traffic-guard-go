// Package ratelimit implements per-client token-bucket rate limiting plus
// per-client and global in-flight (outstanding request) caps.
//
// Accounting rules (the "billing model"):
//
//   - Each accepted request consumes exactly one token from its client's
//     bucket and books exactly one in-flight slot for the client and one
//     global slot. The three bookings are committed atomically under a
//     single mutex; a rejected request books nothing.
//   - Slots are returned exactly once when the request finishes, even if the
//     caller invokes release more than once.
//   - Tokens refill continuously at RatePerSecond tokens/sec capped at the
//     bucket capacity (== RatePerSecond). Empty bucket => rate rejection.
//   - Client in-flight >= Burst => burst rejection.
//   - Global in-flight >= GlobalMaxInFlight => global rejection.
//   - When several limits would fail simultaneously the precedence is
//     rate -> burst -> global, so the reason is deterministic.
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Reason identifies why a request was rejected.
type Reason string

const (
	ReasonNone Reason = ""
	// ReasonRate: sustained rate exceeded (token bucket empty).
	ReasonRate Reason = "rate_exceeded"
	// ReasonBurst: client instantaneous in-flight quota exceeded.
	ReasonBurst Reason = "client_burst_exceeded"
	// ReasonGlobal: process-wide in-flight cap exceeded.
	ReasonGlobal Reason = "global_cap_exceeded"
)

// Snapshot is a point-in-time view of quota state captured for every
// decision, making admissions explainable in logs and tests.
type Snapshot struct {
	Client         string
	TokensAvail    float64
	BucketCap      float64
	InFlight       int
	BurstLimit     int
	GlobalInFlight int
	GlobalLimit    int
}

type bucket struct {
	tokens   float64
	lastFill time.Time
	inFlight int
}

// Limiter decides admission and tracks outstanding requests.
type Limiter struct {
	rate       float64 // tokens per second
	burst      int
	globalCap  int
	mu         sync.Mutex
	buckets    map[string]*bucket
	globalUsed int
	now        func() time.Time

	allowed        int64
	rejectedRate   int64
	rejectedBurst  int64
	rejectedGlobal int64
}

// New creates a Limiter. now may be nil (time.Now); tests inject a fake clock.
func New(ratePerSecond int, burstPerClient int, globalMaxInFlight int, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{
		rate:      float64(ratePerSecond),
		burst:     burstPerClient,
		globalCap: globalMaxInFlight,
		buckets:   make(map[string]*bucket),
		now:       now,
	}
}

// refill must be called with l.mu held.
func (l *Limiter) refill(b *bucket, now time.Time) {
	if b.lastFill.IsZero() {
		b.tokens = l.rate
		b.lastFill = now
		return
	}
	elapsed := now.Sub(b.lastFill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.rate {
			b.tokens = l.rate
		}
		b.lastFill = now
	}
}

// Acquire attempts to reserve quota for a request from client.
//
// On success it returns a release function the caller MUST invoke exactly
// once when the request finishes; repeated invocations are harmless. The
// returned Snapshot reflects state BEFORE the booking.
//
// Reservation is all-or-nothing: either token + client slot + global slot are
// all booked, or none is.
func (l *Limiter) Acquire(ctx context.Context, client string) (release func(), snap Snapshot, allowed bool, reason Reason) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b := l.buckets[client]
	if b == nil {
		b = &bucket{}
		l.buckets[client] = b
	}
	l.refill(b, now)

	snap = Snapshot{
		Client:         client,
		TokensAvail:    b.tokens,
		BucketCap:      l.rate,
		InFlight:       b.inFlight,
		BurstLimit:     l.burst,
		GlobalInFlight: l.globalUsed,
		GlobalLimit:    l.globalCap,
	}

	// Deterministic precedence: rate -> burst -> global.
	if b.tokens < 1 {
		l.rejectedRate++
		return nil, snap, false, ReasonRate
	}
	if b.inFlight >= l.burst {
		l.rejectedBurst++
		return nil, snap, false, ReasonBurst
	}
	if l.globalUsed >= l.globalCap {
		l.rejectedGlobal++
		return nil, snap, false, ReasonGlobal
	}

	// Commit all three bookings atomically.
	b.tokens -= 1
	b.inFlight++
	l.globalUsed++
	l.allowed++

	var once sync.Once
	release = func() {
		once.Do(func() {
			l.mu.Lock()
			b.inFlight--
			l.globalUsed--
			l.mu.Unlock()
		})
	}
	return release, snap, true, ReasonNone
}

// InFlight returns current global in-flight count.
func (l *Limiter) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.globalUsed
}

// Stats returns aggregate admission counters since construction.
func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{
		Allowed:        l.allowed,
		RejectedRate:   l.rejectedRate,
		RejectedBurst:  l.rejectedBurst,
		RejectedGlobal: l.rejectedGlobal,
	}
}

// Stats are monotonic admission counters.
type Stats struct {
	Allowed        int64
	RejectedRate   int64
	RejectedBurst  int64
	RejectedGlobal int64
}
