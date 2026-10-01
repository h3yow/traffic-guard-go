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
	// RatePerSec 每客户端每速率窗（固定 1s）加权消耗上限。必须 > 0。
	RatePerSec int `json:"rate_per_sec"`
	// Burst 每客户端每突发窗加权消耗上限。必须 > 0 且 <= RatePerSec。
	Burst int `json:"burst"`
	// BurstWindowMS 突发窗长度（毫秒），缺省 100ms，取值 (0,1000]。
	// 同时作为未单独声明突发窗的分组的缺省窗长。
	BurstWindowMS int `json:"burst_window_ms"`
	// GlobalInFlight 全局同时在途加权总量上限。必须 > 0。
	GlobalInFlight int `json:"global_in_flight"`
	// FailureThreshold 连续故障多少次后熔断。必须 > 0。
	FailureThreshold int `json:"failure_threshold"`
	// OpenCooldownMS 熔断打开后冷却多久才允许探测，缺省 5000ms，必须 > 0。
	OpenCooldownMS int `json:"open_cooldown_ms"`
	// UpstreamTimeoutMS 上游调用超时，缺省 2000ms，必须 > 0。
	UpstreamTimeoutMS int `json:"upstream_timeout_ms"`

	// EndpointCosts 声明每个入口（endpoint）单次受理消耗的额度成本。
	// 未声明（或入口名为空）的入口一律按成本 1 计费。
	// 速率、突发、全局/分组在途三类额度都按成本加权累计。
	// 每个成本必须 >= 1，且不得大于任何一层的最小上限，
	// 否则该入口永远无法被受理，属于必报错的非法配置。
	EndpointCosts map[string]int `json:"endpoint_costs,omitempty"`

	// Groups 分组（租户）维度额度，key 为分组名。
	// 只有在本表里显式声明的分组才构成共享租户层：
	// 同组客户端共享该组的速率/突发/在途三层额度。
	// 特殊 key "" 是缺省分组：显式声明时，未命中归属（或显式归属到 ""）
	// 的客户端共享该组额度；不声明时，这些客户端不经过任何分组层，
	// 回到“各客户端独立核算 + 全局在途总闸”的旧版本口径。
	Groups map[string]GroupConfig `json:"groups,omitempty"`

	// ClientGroups 客户端 -> 分组归属映射。
	// key 为客户端标识（不允许空串），value 必须是 Groups 中存在的分组名
	// （缺省分组名 "" 可以显式书写）。未出现在映射中的客户端不归属任何分组。
	ClientGroups map[string]string `json:"client_groups,omitempty"`
}

// GroupConfig 一个分组（租户）的三层额度。零值字段在合并缺省值后必须通过校验。
type GroupConfig struct {
	// RatePerSec 分组内所有客户端合计的每速率窗加权消耗上限。必须 > 0。
	RatePerSec int `json:"rate_per_sec"`
	// Burst 分组每突发窗加权消耗上限。必须 > 0 且 <= RatePerSec。
	Burst int `json:"burst"`
	// InFlight 分组同时在途加权总量上限。必须 > 0。
	InFlight int `json:"in_flight"`
	// BurstWindowMS 分组突发窗毫秒数；取 0 时回落到顶层 BurstWindowMS。
	// 非 0 时取值 (0,1000]。
	BurstWindowMS int `json:"burst_window_ms,omitempty"`
}

// DefaultGroup 是缺省分组的固定名称。
const DefaultGroup = ""

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

	// 入口成本：缺省按 1；显式声明必须 >=1，且不得高于任何一层最小上限，
	// 否则该入口在任何时刻都会被整体拒绝，属于配置错误而非运行时现象。
	minCap := minInt(c.GlobalInFlight, c.RatePerSec, c.Burst)
	for ep, cost := range c.EndpointCosts {
		switch {
		case ep == "":
			problems = append(problems, "endpoint_costs contains an empty endpoint name")
		case cost < 1:
			problems = append(problems, fmt.Sprintf(
				"endpoint_costs[%q] must be >= 1 (undeclared endpoints already cost 1)", ep))
		case cost > minCap:
			problems = append(problems, fmt.Sprintf(
				"endpoint_costs[%q]=%d exceeds the smallest quota cap %d "+
					"(global_in_flight/rate_per_sec/burst); it could never be admitted",
				ep, cost, minCap))
		}
	}

	// 分组额度：字段语义与顶层一致；缺省字段取顶层缺省，取值规则相同。
	for name, g := range c.Groups {
		prefix := fmt.Sprintf("groups[%q]", name)
		rate := g.RatePerSec
		burst := g.Burst
		inflight := g.InFlight
		bw := g.BurstWindowMS
		if rate <= 0 {
			problems = append(problems, prefix+".rate_per_sec must be > 0")
		}
		if burst <= 0 {
			problems = append(problems, prefix+".burst must be > 0")
		}
		if rate > 0 && burst > rate {
			problems = append(problems, fmt.Sprintf(
				"%s.burst (%d) must be <= %s.rate_per_sec (%d)",
				prefix, burst, prefix, rate))
		}
		if inflight <= 0 {
			problems = append(problems, prefix+".in_flight must be > 0")
		}
		if bw < 0 || bw > 1000 {
			problems = append(problems, prefix+".burst_window_ms must be in [0,1000] (0 = inherit)")
		}
		// 分组内成本上限也必须可被受理：成本不得高于该分组任一上限。
		gMin := minPositive(inflight, rate, burst)
		if gMin > 0 {
			for ep, cost := range c.EndpointCosts {
				if cost > gMin {
					problems = append(problems, fmt.Sprintf(
						"endpoint_costs[%q]=%d exceeds smallest cap %d of group %q",
						ep, cost, gMin, groupDisplayName(name)))
				}
			}
		}
	}

	// 客户端归属：客户端名非空，分组必须存在（缺省分组 "" 永远存在）。
	for client, group := range c.ClientGroups {
		if client == "" {
			problems = append(problems, "client_groups contains an empty client id")
		}
		if _, ok := c.Groups[group]; !ok && group != DefaultGroup {
			problems = append(problems, fmt.Sprintf(
				"client_groups[%q] references unknown group %q", client, group))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid traffic config: %v", problems)
	}
	return nil
}

// minInt 返回参数中的最小值（参数均为正整数时使用）。
func minInt(vals ...int) int {
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

// minPositive 只在正数中取最小；若没有正数返回 0（表示“尚无法判定”）。
func minPositive(vals ...int) int {
	m := 0
	for _, v := range vals {
		if v <= 0 {
			continue
		}
		if m == 0 || v < m {
			m = v
		}
	}
	return m
}

// groupDisplayName 让缺省分组在错误信息里可读。
func groupDisplayName(name string) string {
	if name == DefaultGroup {
		return "<default>"
	}
	return name
}

// EndpointCost 返回入口的单次受理成本：未声明或入口名为空时按 1 计费。
func (c Config) EndpointCost(endpoint string) int {
	if endpoint != "" {
		if cost, ok := c.EndpointCosts[endpoint]; ok {
			return cost
		}
	}
	return 1
}

// ClientGroup 返回客户端归属的分组名；未映射或映射缺省时为 DefaultGroup。
// 注意：返回 DefaultGroup 只代表“名义归属”，该客户端是否真正受分组层约束，
// 取决于缺省分组是否被显式配置（见 HasExplicitGroup）。
// Validate 保证映射目标分组存在；对未校验配置的直接调用同样安全回落。
func (c Config) ClientGroup(client string) string {
	if g, ok := c.ClientGroups[client]; ok {
		if _, exists := c.Groups[g]; exists {
			return g
		}
	}
	return DefaultGroup
}

// GroupBurstWindow 返回分组的突发窗：分组未声明或字段为 0 时继承顶层窗长。
func (c Config) GroupBurstWindow(group string) time.Duration {
	if g, ok := c.Groups[group]; ok && g.BurstWindowMS > 0 {
		return time.Duration(g.BurstWindowMS) * time.Millisecond
	}
	return c.BurstWindow()
}

// HasExplicitGroup 报告 group 是否在 groups 中被显式配置。
// 只有显式分组才作为真正的共享租户层参与核算；
// 未显式声明的缺省分组（DefaultGroup）不构成共享层——
// 落入其中的客户端回到“各客户端独立 + 全局在途总闸”的旧版本口径。
func (c Config) HasExplicitGroup(group string) bool {
	_, ok := c.Groups[group]
	return ok
}

// ResolveGroup 返回“显式分组”生效的三层额度与突发窗。
// 仅当 HasExplicitGroup(group) 为真时，返回值才代表一层实际生效的共享约束；
// 缺省分组未显式声明时，返回的继承值（rate_per_sec/burst/global_in_flight）
// 只用于观测展示，不会被准入核算使用——未归属任何显式分组的客户端之间
// 不共享速率/突发/在途额度，保持旧版本口径。
func (c Config) ResolveGroup(group string) GroupConfig {
	if g, ok := c.Groups[group]; ok {
		return g
	}
	return GroupConfig{
		RatePerSec:    c.RatePerSec,
		Burst:         c.Burst,
		InFlight:      c.GlobalInFlight,
		BurstWindowMS: c.BurstWindowMS,
	}
}

// GroupInFlight 返回分组在途加权总量上限（便捷封装）。
func (c Config) GroupInFlight(group string) int {
	return c.ResolveGroup(group).InFlight
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
