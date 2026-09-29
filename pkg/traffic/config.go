package traffic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config 入口流量防护配置。新增配置项必须保持向后兼容：
// 新增字段在旧配置中缺失时一律采用本文件中的确定缺省语义。
type Config struct {
	// RatePerSec 每客户端每速率窗（固定 1s）放行上限。必须 > 0。
	RatePerSec int `json:"rate_per_sec"`
	// Burst 每客户端每突发窗放行上限。必须 > 0 且 <= RatePerSec。
	Burst int `json:"burst"`
	// BurstWindowMS 突发窗长度（毫秒），缺省 100ms，取值 (0,1000]。
	BurstWindowMS int `json:"burst_window_ms"`
	// GlobalInFlight 全局同时在途请求总量上限。必须 > 0。
	GlobalInFlight int `json:"global_in_flight"`
	// FailureThreshold 连续故障多少次后熔断。必须 > 0。
	FailureThreshold int `json:"failure_threshold"`
	// OpenCooldownMS 熔断打开后冷却多久才允许探测，缺省 5000ms，必须 > 0。
	OpenCooldownMS int `json:"open_cooldown_ms"`
	// UpstreamTimeoutMS 上游调用超时，缺省 2000ms，必须 > 0。
	UpstreamTimeoutMS int `json:"upstream_timeout_ms"`
	// EndpointCosts 入口成本声明：endpoint 标识 -> 单次请求消耗的成本。
	// 缺省 nil（全部入口按成本 1 计）。键必须非空，值必须 > 0；
	// 未声明的入口（包括请求未携带 endpoint 标识）成本一律按 1 计算。
	// 成本会同时计入客户端/分组的速率窗、突发窗与全局在途总量。
	EndpointCosts map[string]int `json:"endpoint_costs,omitempty"`
	// Groups 分组（租户）维度额度。缺省 nil（不启用分组层）。
	// 键为分组标识，必须非空；每个分组有独立的速率/突发/在途上限。
	Groups map[string]GroupConfig `json:"groups,omitempty"`
	// ClientGroups 客户端 -> 分组 的归属映射。键/值都必须非空。
	// 解析不到分组（未配置或指向未知分组）的请求不经过分组层核算，
	// 仍正常经过客户端层与全局层；配置指向未知分组不视为非法，
	// 以允许分组配置与映射分先后上线。
	ClientGroups map[string]string `json:"client_groups,omitempty"`
}

// GroupConfig 单个分组（租户）的额度上限。
// 窗口长度与客户端层一致：速率窗固定 1s，突发窗取全局 BurstWindow。
type GroupConfig struct {
	// RatePerSec 分组每速率窗（1s）放行的成本上限。必须 > 0。
	RatePerSec int `json:"rate_per_sec"`
	// Burst 分组每突发窗放行的成本上限。必须 > 0 且 <= RatePerSec。
	Burst int `json:"burst"`
	// InFlight 分组同时在途的成本总量上限。必须 > 0。
	InFlight int `json:"in_flight"`
}

// DefaultConfig 返回带有确定缺省语义的配置。
// 旧配置文件中缺失的字段一律回落到这里的取值。
func DefaultConfig() Config {
	return Config{
		RatePerSec:        100,
		Burst:             20,
		BurstWindowMS:     100,
		GlobalInFlight:    1000,
		FailureThreshold:  5,
		OpenCooldownMS:    5000,
		UpstreamTimeoutMS: 2000,
	}
}

// Validate 校验配置；任何非法项都显式报错，绝不静默忽略或悄悄纠正。
func (c Config) Validate() error {
	var problems []string
	if c.RatePerSec <= 0 {
		problems = append(problems, "rate_per_sec must be > 0")
	}
	if c.Burst <= 0 {
		problems = append(problems, "burst must be > 0")
	}
	if c.RatePerSec > 0 && c.Burst > c.RatePerSec {
		problems = append(problems, fmt.Sprintf(
			"burst (%d) must be <= rate_per_sec (%d), otherwise burst quota could never be granted",
			c.Burst, c.RatePerSec))
	}
	if c.BurstWindowMS <= 0 || c.BurstWindowMS > 1000 {
		problems = append(problems, "burst_window_ms must be in (0,1000]")
	}
	if c.GlobalInFlight <= 0 {
		problems = append(problems, "global_in_flight must be > 0")
	}
	if c.FailureThreshold <= 0 {
		problems = append(problems, "failure_threshold must be > 0")
	}
	if c.OpenCooldownMS <= 0 {
		problems = append(problems, "open_cooldown_ms must be > 0")
	}
	if c.UpstreamTimeoutMS <= 0 {
		problems = append(problems, "upstream_timeout_ms must be > 0")
	}
	for ep, cost := range c.EndpointCosts {
		if ep == "" {
			problems = append(problems, "endpoint_costs contains empty endpoint key")
		}
		if cost <= 0 {
			problems = append(problems, fmt.Sprintf(
				"endpoint_costs[%q] must be > 0", ep))
		}
	}
	for name, g := range c.Groups {
		if name == "" {
			problems = append(problems, "groups contains empty group key")
		}
		if g.RatePerSec <= 0 {
			problems = append(problems, fmt.Sprintf(
				"groups[%q].rate_per_sec must be > 0", name))
		}
		if g.Burst <= 0 {
			problems = append(problems, fmt.Sprintf(
				"groups[%q].burst must be > 0", name))
		}
		if g.RatePerSec > 0 && g.Burst > g.RatePerSec {
			problems = append(problems, fmt.Sprintf(
				"groups[%q].burst (%d) must be <= rate_per_sec (%d)",
				name, g.Burst, g.RatePerSec))
		}
		if g.InFlight <= 0 {
			problems = append(problems, fmt.Sprintf(
				"groups[%q].in_flight must be > 0", name))
		}
	}
	for client, group := range c.ClientGroups {
		if client == "" {
			problems = append(problems, "client_groups contains empty client key")
		}
		if group == "" {
			problems = append(problems, fmt.Sprintf(
				"client_groups[%q] must reference a non-empty group", client))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid traffic config: %v", problems)
	}
	return nil
}

// CostOf 返回入口 endpoint 一次请求消耗的成本；
// 未声明（或 endpoint 为空）时按缺省成本 1 计。
func (c Config) CostOf(endpoint string) int {
	if endpoint != "" {
		if cost, ok := c.EndpointCosts[endpoint]; ok {
			return cost
		}
	}
	return 1
}

// GroupOf 解析 client 归属的分组；返回 "" 表示该客户端不经过分组层
// （未配置映射，或映射指向一个不存在的分组）。
func (c Config) GroupOf(client string) string {
	name, ok := c.ClientGroups[client]
	if !ok {
		return ""
	}
	if _, ok := c.Groups[name]; !ok {
		return ""
	}
	return name
}

// BurstWindow 返回突发窗时长。
func (c Config) BurstWindow() time.Duration {
	return time.Duration(c.BurstWindowMS) * time.Millisecond
}

// OpenCooldown 返回熔断冷却时长。
func (c Config) OpenCooldown() time.Duration {
	return time.Duration(c.OpenCooldownMS) * time.Millisecond
}

// UpstreamTimeout 返回上游调用超时时长。
func (c Config) UpstreamTimeout() time.Duration {
	return time.Duration(c.UpstreamTimeoutMS) * time.Millisecond
}

// RateWindow 是速率窗的固定长度（与 RatePerSec 语义对应）。
const RateWindow = time.Second

// LoadConfig 读取 JSON 配置文件：
//   - 未知字段视为非法（防止把拼错的配置项静默忽略）；
//   - 同一键重复出现视为非法；
//   - 缺失字段套用 DefaultConfig 的缺省值；
//   - 合并后做完整业务校验，非法即返回错误。
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	cfg, err := ParseConfig(data)
	if err != nil {
		return Config{}, fmt.Errorf("load config %q: %w", path, err)
	}
	return cfg, nil
}

// ParseConfig 解析 JSON 配置字节，规则同 LoadConfig。
func ParseConfig(data []byte) (Config, error) {
	if err := checkNoDuplicateKeys(data); err != nil {
		return Config{}, err
	}
	cfg := DefaultConfig()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config json: %w", err)
	}
	// 不允许配置文档里夹带第二个 JSON 值。
	var extra json.RawMessage
	if err := dec.Decode(&extra); err == nil {
		return Config{}, fmt.Errorf("parse config json: unexpected trailing content")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// checkNoDuplicateKeys 递归检查 JSON 对象内是否存在重复键。
func checkNoDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return checkValue(dec, "")
}

func checkValue(dec *json.Decoder, path string) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("parse config json: %w", err)
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return fmt.Errorf("parse config json: %w", err)
				}
				key := k.(string)
				p := key
				if path != "" {
					p = path + "." + key
				}
				if seen[key] {
					return fmt.Errorf("parse config json: duplicate key %q", p)
				}
				seen[key] = true
				if err := checkValue(dec, p); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // 消费 '}'
				return fmt.Errorf("parse config json: %w", err)
			}
		case '[':
			i := 0
			for dec.More() {
				if err := checkValue(dec, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
				i++
			}
			if _, err := dec.Token(); err != nil { // 消费 ']'
				return fmt.Errorf("parse config json: %w", err)
			}
		}
	}
	return nil
}
