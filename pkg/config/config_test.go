package config

import "testing"

func intp(i int) *int       { return &i }
func i64p(i int64) *int64   { return &i }
func u32p(i uint32) *uint32 { return &i }

func TestApplyDefaultsAndValidate(t *testing.T) {
	// Minimal required fields; all optionals absent -> documented defaults.
	c := Config{RateLimit: RateLimitConfig{RatePerSecond: 10, GlobalMaxInFlight: 20}}
	c = c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("defaults should validate: %v", err)
	}
	if *c.RateLimit.Burst != 10 {
		t.Errorf("burst default = %d, want 10", *c.RateLimit.Burst)
	}
	if *c.Circuit.FailureThreshold != DefaultFailureThreshold {
		t.Errorf("failure threshold default = %d", *c.Circuit.FailureThreshold)
	}
	if *c.Circuit.OpenCooldownMS != DefaultOpenCooldownMS {
		t.Errorf("cooldown default = %d", *c.Circuit.OpenCooldownMS)
	}
	if *c.Circuit.ProbeTimeoutMS != DefaultProbeTimeoutMS {
		t.Errorf("probe timeout default = %d", *c.Circuit.ProbeTimeoutMS)
	}

	// Explicit zero on an optional field must NOT be replaced by default:
	// it must be rejected.
	bad := Config{RateLimit: RateLimitConfig{RatePerSecond: 1, GlobalMaxInFlight: 1,
		Burst: intp(0)}}
	bad = bad.ApplyDefaults()
	if err := bad.Validate(); err == nil {
		t.Fatal("explicit burst=0 must be invalid, got nil")
	}

	// Missing required fields.
	for name, bad := range map[string]Config{
		"rate zero":   {RateLimit: RateLimitConfig{GlobalMaxInFlight: 1}},
		"global zero": {RateLimit: RateLimitConfig{RatePerSecond: 1}},
		"global neg":  {RateLimit: RateLimitConfig{RatePerSecond: 1, GlobalMaxInFlight: -3}},
		"cooldown zero": {RateLimit: RateLimitConfig{RatePerSecond: 1, GlobalMaxInFlight: 1},
			Circuit: CircuitConfig{OpenCooldownMS: i64p(0)}},
		"threshold zero": {RateLimit: RateLimitConfig{RatePerSecond: 1, GlobalMaxInFlight: 1},
			Circuit: CircuitConfig{FailureThreshold: u32p(0)}},
	} {
		bad = bad.ApplyDefaults()
		if err := bad.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestLoadJSON(t *testing.T) {
	// New/unknown keys are rejected (no silent ignore).
	if _, err := LoadJSON([]byte(`{"rate_limit":{"rate_per_second":2,"global_max_in_flight":3},"mystery":1}`)); err == nil {
		t.Fatal("unknown key must fail")
	}
	// Explicit zero optional must fail rather than fall back.
	if _, err := LoadJSON([]byte(`{"rate_limit":{"rate_per_second":2,"global_max_in_flight":3},"circuit":{"probe_timeout_ms":0}}`)); err == nil {
		t.Fatal("explicit probe_timeout_ms=0 must fail")
	}
	// Malformed JSON must fail.
	if _, err := LoadJSON([]byte(`{not json`)); err == nil {
		t.Fatal("malformed json must fail")
	}
	// Happy path with custom optionals.
	c, err := LoadJSON([]byte(`{
		"rate_limit":{"rate_per_second":4,"burst":7,"global_max_in_flight":9},
		"circuit":{"failure_threshold":3,"open_cooldown_ms":1200,"probe_timeout_ms":300}
	}`))
	if err != nil {
		t.Fatalf("valid json: %v", err)
	}
	if *c.RateLimit.Burst != 7 || *c.Circuit.FailureThreshold != 3 ||
		*c.Circuit.OpenCooldownMS != 1200 || *c.Circuit.ProbeTimeoutMS != 300 {
		t.Errorf("custom values not parsed: %+v", c)
	}
	// Missing required fields -> error.
	if _, err := LoadJSON([]byte(`{"rate_limit":{"rate_per_second":4}}`)); err == nil {
		t.Fatal("missing global_max_in_flight must fail")
	}
}
