// Package config defines configuration for the traffic guard.
//
// Optional fields use pointer types so that "field absent" (nil -> default)
// can be distinguished from "field explicitly set to an invalid value"
// (non-nil zero/negative -> validation error). Unknown keys in JSON are
// rejected so that misconfiguration is never silently ignored.
package config

// Config is the top-level traffic guard configuration.
type Config struct {
	RateLimit RateLimitConfig
	Circuit   CircuitConfig
}

// RateLimitConfig configures per-client and global quota.
type RateLimitConfig struct {
	// RatePerSecond is the sustained token refill rate per client (tokens/sec).
	// Required; must be > 0.
	RatePerSecond int
	// Burst is the per-client maximum instantaneous outstanding (in-flight)
	// requests. Optional: defaults to RatePerSecond. Must be > 0 when set.
	Burst *int
	// GlobalMaxInFlight is the process-wide cap on concurrently accepted
	// requests across all clients. Required; must be > 0.
	GlobalMaxInFlight int
}

// CircuitConfig configures the upstream circuit breaker. All fields are
// optional; nil means "use the documented default". A non-nil zero/negative
// value is an error rather than a silent fallback.
type CircuitConfig struct {
	// FailureThreshold consecutive failures that open the circuit.
	FailureThreshold *uint32
	// OpenCooldownMS the circuit stays open before a trial probe.
	OpenCooldownMS *int64
	// ProbeTimeoutMS bounds the execution time of a half-open probe.
	ProbeTimeoutMS *int64
}

// OpenCooldown returns the cooldown as a Duration after defaults are applied.
func (c CircuitConfig) OpenCooldown() Duration {
	if c.OpenCooldownMS == nil {
		return Duration{}
	}
	return Duration{Millis: *c.OpenCooldownMS}
}

// ProbeTimeout returns the probe timeout as a Duration after defaults.
func (c CircuitConfig) ProbeTimeout() Duration {
	if c.ProbeTimeoutMS == nil {
		return Duration{}
	}
	return Duration{Millis: *c.ProbeTimeoutMS}
}

// Duration is a duration expressed in milliseconds.
type Duration struct {
	Millis int64
}
