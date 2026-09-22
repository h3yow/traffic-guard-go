package config

import (
	"errors"
	"fmt"
	"time"
)

// Default configuration values; documented in README.md.
const (
	DefaultFailureThreshold = uint32(5)
	DefaultOpenCooldownMS   = int64(5000)
	DefaultProbeTimeoutMS   = int64(2000)
)

// ErrInvalidConfig is returned when configuration values are illegal.
var ErrInvalidConfig = errors.New("invalid config")

// ApplyDefaults returns a copy of c with defaults filled in for unset fields.
// An explicitly-set (even illegal) value is preserved here so Validate can
// reject it: absent -> default, present-but-illegal -> error.
func (c Config) ApplyDefaults() Config {
	out := c
	if out.RateLimit.Burst == nil {
		b := out.RateLimit.RatePerSecond
		out.RateLimit.Burst = &b
	}
	if out.Circuit.FailureThreshold == nil {
		v := DefaultFailureThreshold
		out.Circuit.FailureThreshold = &v
	}
	if out.Circuit.OpenCooldownMS == nil {
		v := DefaultOpenCooldownMS
		out.Circuit.OpenCooldownMS = &v
	}
	if out.Circuit.ProbeTimeoutMS == nil {
		v := DefaultProbeTimeoutMS
		out.Circuit.ProbeTimeoutMS = &v
	}
	return out
}

// Validate checks every field. Call it AFTER ApplyDefaults; nil checks below
// guard programmatic construction that skipped defaults, while zero/negative
// non-nil values catch explicitly-set invalid configuration.
func (c Config) Validate() error {
	rl := c.RateLimit
	if rl.RatePerSecond <= 0 {
		return fmt.Errorf("%w: rate_limit.rate_per_second must be > 0, got %d", ErrInvalidConfig, rl.RatePerSecond)
	}
	if rl.Burst == nil || *rl.Burst <= 0 {
		return fmt.Errorf("%w: rate_limit.burst must be > 0", ErrInvalidConfig)
	}
	if rl.GlobalMaxInFlight <= 0 {
		return fmt.Errorf("%w: rate_limit.global_max_in_flight must be > 0, got %d", ErrInvalidConfig, rl.GlobalMaxInFlight)
	}
	cb := c.Circuit
	if cb.FailureThreshold == nil || *cb.FailureThreshold == 0 {
		return fmt.Errorf("%w: circuit.failure_threshold must be > 0", ErrInvalidConfig)
	}
	if cb.OpenCooldownMS == nil || *cb.OpenCooldownMS <= 0 {
		return fmt.Errorf("%w: circuit.open_cooldown_ms must be > 0", ErrInvalidConfig)
	}
	if cb.ProbeTimeoutMS == nil || *cb.ProbeTimeoutMS <= 0 {
		return fmt.Errorf("%w: circuit.probe_timeout_ms must be > 0", ErrInvalidConfig)
	}
	return nil
}

// Std converts the millisecond duration to time.Duration.
func (d Duration) Std() time.Duration {
	return time.Duration(d.Millis) * time.Millisecond
}
