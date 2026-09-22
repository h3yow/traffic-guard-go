package config

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSONConfig is the JSON on-disk representation. Optional numeric fields are
// pointers so absent keys can be distinguished from explicit zero values.
type JSONConfig struct {
	RateLimit struct {
		RatePerSecond     int  `json:"rate_per_second"`
		Burst             *int `json:"burst"`
		GlobalMaxInFlight int  `json:"global_max_in_flight"`
	} `json:"rate_limit"`
	Circuit struct {
		FailureThreshold *uint32 `json:"failure_threshold"`
		OpenCooldownMS   *int64  `json:"open_cooldown_ms"`
		ProbeTimeoutMS   *int64  `json:"probe_timeout_ms"`
	} `json:"circuit"`
}

// LoadJSON parses strict JSON (unknown fields rejected), fills defaults and
// validates. Illegal configuration is never silently ignored.
func LoadJSON(data []byte) (Config, error) {
	var jc JSONConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&jc); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	c := Config{
		RateLimit: RateLimitConfig{
			RatePerSecond:     jc.RateLimit.RatePerSecond,
			Burst:             jc.RateLimit.Burst,
			GlobalMaxInFlight: jc.RateLimit.GlobalMaxInFlight,
		},
		Circuit: CircuitConfig{
			FailureThreshold: jc.Circuit.FailureThreshold,
			OpenCooldownMS:   jc.Circuit.OpenCooldownMS,
			ProbeTimeoutMS:   jc.Circuit.ProbeTimeoutMS,
		},
	}
	c = c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}
