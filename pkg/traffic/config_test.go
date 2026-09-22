package traffic

import (
	"strings"
	"testing"
)

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
	if got := DefaultConfig().BurstWindow(); got.Milliseconds() != 100 {
		t.Fatalf("default burst window = %v, want 100ms", got)
	}
}

func TestLoadConfigMissingFieldsFallBackToDefaults(t *testing.T) {
	// 旧版本配置：只有两个字段，新增字段必须回落到缺省值。
	cfg, err := ParseConfig([]byte(`{"rate_per_sec":10,"burst":5}`))
	if err != nil {
		t.Fatalf("parse legacy config: %v", err)
	}
	d := DefaultConfig()
	if cfg.RatePerSec != 10 || cfg.Burst != 5 {
		t.Fatalf("explicit fields not applied: %+v", cfg)
	}
	if cfg.GlobalInFlight != d.GlobalInFlight ||
		cfg.FailureThreshold != d.FailureThreshold ||
		cfg.BurstWindowMS != d.BurstWindowMS ||
		cfg.OpenCooldownMS != d.OpenCooldownMS ||
		cfg.UpstreamTimeoutMS != d.UpstreamTimeoutMS {
		t.Fatalf("missing fields must take defaults: got %+v want defaults %+v", cfg, d)
	}
}

func TestInvalidConfigsRejected(t *testing.T) {
	cases := map[string]string{
		"zero rate":        `{"rate_per_sec":0,"burst":1}`,
		"negative burst":   `{"rate_per_sec":10,"burst":-1}`,
		"burst above rate": `{"rate_per_sec":10,"burst":11}`,
		"bad burst window": `{"rate_per_sec":10,"burst":10,"burst_window_ms":1001}`,
		"zero global":      `{"rate_per_sec":10,"burst":10,"global_in_flight":0}`,
		"zero threshold":   `{"rate_per_sec":10,"burst":10,"failure_threshold":0}`,
		"unknown field":    `{"rate_per_sec":10,"burst":10,"rate_per_second":10}`,
		"duplicate key":    `{"rate_per_sec":10,"rate_per_sec":20,"burst":10}`,
		"trailing content": `{"rate_per_sec":10,"burst":10}{}`,
		"wrong type":       `{"rate_per_sec":"10","burst":10}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseConfig([]byte(body))
			if err == nil {
				t.Fatalf("config must be rejected: %s", body)
			}
			if !strings.Contains(err.Error(), "config") {
				t.Fatalf("error should mention config, got: %v", err)
			}
		})
	}
}
