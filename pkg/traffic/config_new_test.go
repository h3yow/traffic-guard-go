package traffic

import "testing"

func TestParseConfigWithCostsAndGroups(t *testing.T) {
	body := `{
	  "rate_per_sec": 50,
	  "burst": 10,
	  "endpoint_costs": {"heavy": 5, "/light": 1},
	  "groups": {
	    "tenant-a": {"rate_per_sec": 20, "burst": 5, "in_flight": 8},
	    "tenant-b": {"rate_per_sec": 30, "burst": 30, "in_flight": 40}
	  },
	  "client_groups": {"c1": "tenant-a", "c2": "tenant-b"}
	}`
	cfg, err := ParseConfig([]byte(body))
	if err != nil {
		t.Fatalf("valid enriched config must parse: %v", err)
	}
	if got := cfg.CostOf("heavy"); got != 5 {
		t.Fatalf("heavy cost = %d, want 5", got)
	}
	if got := cfg.CostOf("/light"); got != 1 {
		t.Fatalf("light cost = %d, want 1", got)
	}
	// 未声明入口与空入口一律缺省成本 1。
	if got := cfg.CostOf("unknown"); got != 1 {
		t.Fatalf("undeclared endpoint cost = %d, want 1", got)
	}
	if got := cfg.CostOf(""); got != 1 {
		t.Fatalf("empty endpoint cost = %d, want 1", got)
	}
	if g := cfg.GroupOf("c1"); g != "tenant-a" {
		t.Fatalf("c1 group = %q, want tenant-a", g)
	}
	if g := cfg.GroupOf("nobody"); g != "" {
		t.Fatalf("unmapped client group = %q, want empty", g)
	}
	// 映射指向未知分组：不报错，按无分组处理（允许分阶段上线）。
	cfg.ClientGroups["late"] = "not-yet-defined"
	if g := cfg.GroupOf("late"); g != "" {
		t.Fatalf("unknown group mapping = %q, want empty", g)
	}
}

func TestInvalidEnrichedConfigsRejected(t *testing.T) {
	cases := map[string]string{
		"zero endpoint cost":    `{"endpoint_costs":{"heavy":0}}`,
		"negative group rate":   `{"groups":{"g":{"rate_per_sec":-1,"burst":1,"in_flight":1}}}`,
		"group burst over rate": `{"groups":{"g":{"rate_per_sec":2,"burst":3,"in_flight":1}}}`,
		"zero group inflight":   `{"groups":{"g":{"rate_per_sec":2,"burst":2,"in_flight":0}}}`,
		"empty group key":       `{"groups":{"":{"rate_per_sec":2,"burst":2,"in_flight":1}}}`,
		"empty mapped group":    `{"client_groups":{"c":""}}`,
		"duplicate endpoint":    `{"endpoint_costs":{"heavy":5},"endpoint_costs":{"heavy":6}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(body)); err == nil {
				t.Fatalf("config must be rejected: %s", body)
			}
		})
	}
}
