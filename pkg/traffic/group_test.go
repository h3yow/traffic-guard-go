package traffic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// groupCfg：两个显式分组 + 缺省分组。
//
//	g1: rate=10 burst=4 in_flight=6（偏紧，专门触发各层拒绝）
//	g2: rate=10 burst=10 in_flight=10
//	default: 宽松
//
// 顶层：rate=20 burst=10 global=15。
func groupCfg() Config {
	cfg := testCfgNoGroupLayer()
	cfg.RatePerSec = 20
	cfg.Burst = 10
	cfg.BurstWindowMS = 100
	cfg.GlobalInFlight = 15
	cfg.Groups = map[string]GroupConfig{
		"g1": {RatePerSec: 10, Burst: 4, InFlight: 6, BurstWindowMS: 100},
		"g2": {RatePerSec: 10, Burst: 10, InFlight: 10, BurstWindowMS: 100},
	}
	cfg.ClientGroups = map[string]string{
		"a": "g1", "b": "g1",
		"x": "g2", "y": "g2",
	}
	return cfg
}

func TestGroupConfigRouting(t *testing.T) {
	cfg := groupCfg()
	cases := map[string]string{
		"a": "g1", "b": "g1", "x": "g2", "y": "g2",
		"nobody": DefaultGroup, // 未映射 -> 缺省分组
	}
	for client, want := range cases {
		if got := cfg.ClientGroup(client); got != want {
			t.Fatalf("ClientGroup(%q)=%q want %q", client, got, want)
		}
	}
	g := cfg.ResolveGroup("g1")
	if g.InFlight != 6 || g.RatePerSec != 10 || g.Burst != 4 {
		t.Fatalf("resolve g1 wrong: %+v", g)
	}
	d := cfg.ResolveGroup(DefaultGroup)
	if d.InFlight != 15 || d.RatePerSec != 20 || d.Burst != 10 {
		t.Fatalf("default group must inherit top-level limits, got %+v", d)
	}
}

func TestGroupThreeLayersAllMustPass(t *testing.T) {
	clk := newFakeClock()
	lg := tlog{t}
	l := NewLimiter(groupCfg(), clk.now)

	// g1 分组突发=4（客户端层用顶层 burst=10，更宽松，所以先由分组层判定）：
	// a 连续 4 个成本 1 占满分组突发窗，同组任意客户端再来都被分组突发拒绝。
	var held []*Admission
	for i := 0; i < 4; i++ {
		held = append(held, mustAdmit(t, l, "a", ""))
	}
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonGroupBurstExceeded {
		t.Fatalf("5th request must hit group burst (client burst=10 is looser), got %v", err)
	}
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupBurstExceeded {
		t.Fatalf("b must be rejected by the shared group burst layer, got %v", err)
	}
	lg.Printf("group burst saturated for both a and b")

	// 分组在途=6：再放 2 个 a（新突发窗，同组累计在途将达 6）。
	clk.advance(150 * time.Millisecond)
	held = append(held, mustAdmit(t, l, "a", "")) // 5
	held = append(held, mustAdmit(t, l, "a", "")) // 6 -> g1 在途打满
	snap, _, err := l.Admit("b", "")
	if denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("b must hit group in-flight cap, got %v (snap=%s)", err, snap.Explain())
	}
	if got := l.GroupInFlight("g1"); got != 6 {
		t.Fatalf("g1 in-flight = %d want 6", got)
	}

	// 归还两个，分组在途回到 4，b 可以进入（突发窗仍可能拒绝，故推进时间）。
	held[4].Release()
	held[5].Release()
	clk.advance(1000 * time.Millisecond)
	badm := mustAdmit(t, l, "b", "")
	badm.Release()

	for i := 0; i < 4; i++ {
		held[i].Release()
	}
	if l.GlobalInFlight() != 0 || l.GroupInFlight("g1") != 0 {
		t.Fatalf("leak after release: global=%d g1=%d", l.GlobalInFlight(), l.GroupInFlight("g1"))
	}
}

func TestGroupIndependence(t *testing.T) {
	clk := newFakeClock()
	// 为纯粹验证分组隔离，这里把全局与分组上限设成同一口径，
	// 避免全局层抢先成为拒绝原因。
	cfg := groupCfg()
	cfg.GlobalInFlight = 100
	for name, g := range cfg.Groups {
		g.Burst, g.RatePerSec = 100, 100
		cfg.Groups[name] = g
	}
	l := NewLimiter(cfg, clk.now)

	var g1 []*Admission
	for i := 0; i < 6; i++ {
		g1 = append(g1, mustAdmit(t, l, "a", ""))
	}
	// g1 已被打满（in_flight=6）。
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g1 must be saturated, got %v", err)
	}
	// g2 完全不受影响，可放到自身上限 10。
	var g2 []*Admission
	for i := 0; i < 10; i++ {
		g2 = append(g2, mustAdmit(t, l, "x", ""))
	}
	if got := l.GroupInFlight("g2"); got != 10 {
		t.Fatalf("g2 in-flight = %d want 10 (independent of g1)", got)
	}
	// g2 满了只影响 g2 自己：g1 仍然维持其拒绝结论。
	if _, _, err := l.Admit("y", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g2 saturation must reject y via group layer, got %v", err)
	}
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g1 must stay saturated independently, got %v", err)
	}
	// 未映射到任何显式分组的客户端不经过分组层：分组计数器一律不增加，
	// 它只受自身窗口与全局总闸约束，额度与 g1/g2 完全隔离。
	before := l.GlobalInFlight()
	d := mustAdmit(t, l, "outsider", "")
	if got := l.GroupInFlight(DefaultGroup); got != 0 {
		t.Fatalf("ungrouped client must not touch the default group, got %d", got)
	}
	if got := l.GlobalInFlight(); got != before+1 {
		t.Fatalf("ungrouped client counts only against global: %d want %d", got, before+1)
	}
	for _, x := range g1 {
		x.Release()
	}
	for _, x := range g2 {
		x.Release()
	}
	d.Release()
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak: %d", l.GlobalInFlight())
	}
}

// TestGroupIndependenceGlobalBoundary 单独固化“分组隔离 + 全局兜底”的确定边界：
// g1 占 6 后，g2 最多再占 9（合计 15 达全局上限），第 10 个由全局层拒绝，
// 且拒绝原因是 global_exceeded 而不是误报给 g2 的分组层；
// g2 自身（<10）和其正常请求并未被误伤。
func TestGroupIndependenceGlobalBoundary(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(groupCfg(), clk.now)
	var g1 []*Admission
	for i := 0; i < 6; i++ {
		if i == 4 {
			clk.advance(150 * time.Millisecond) // 越过 4 个的突发窗
		}
		g1 = append(g1, mustAdmit(t, l, "a", ""))
	}
	var g2 []*Admission
	for i := 0; i < 9; i++ {
		if i > 0 && i%4 == 0 {
			clk.advance(150 * time.Millisecond)
		}
		g2 = append(g2, mustAdmit(t, l, "x", ""))
	}
	if got := l.GlobalInFlight(); got != 15 {
		t.Fatalf("global = %d want 15", got)
	}
	// g2 自己的分组上限是 10，剩余 1 名额；但全局已满 -> global_exceeded。
	_, _, err := l.Admit("x", "")
	if denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("saturated global must be the deciding layer, got %v", err)
	}
	// 缺省分组的正常请求同样被全局层挡住（不是分组误伤）。
	_, _, err = l.Admit("outsider", "")
	if denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("outsider must see global cap, got %v", err)
	}

	// g1 释放 1 后全局出现 1 个名额：g2/缺省分组可立即复用，
	// 证明一个分组的超额不会长期卡死其他分组。
	g1[0].Release()
	if _, _, err := l.Admit("outsider", ""); err != nil {
		t.Fatalf("freed global slot reusable across groups: %v", err)
	}
}

func TestGroupDenyReasonsAreDistinct(t *testing.T) {
	clk := newFakeClock()
	cfg := groupCfg()
	// 放宽客户端层（顶层 rate/burst），让分组速率成为决定性一层；
	// g1: burst=4, rate=10。突发窗 100ms，每 150ms 补一批。
	cfg.RatePerSec, cfg.Burst = 100, 100
	g1 := cfg.Groups["g1"]
	g1.InFlight = 100
	cfg.Groups["g1"] = g1
	l := NewLimiter(cfg, clk.now)

	var held []*Admission
	fill := func(n int) {
		for len(held) < n {
			_, adm, err := l.Admit("a", "")
			if err != nil {
				t.Fatalf("setup fill at %s: %v", clk.now(), err)
			}
			held = append(held, adm)
			if len(held)%4 == 0 {
				clk.advance(150 * time.Millisecond)
			}
		}
	}
	fill(10) // t=300ms，速率窗内 g1 累计恰好 10（4+4+2，均在 1s 内）

	// 客户端层仍有余量（100），分组 g1 速率已满 10：
	// 成本 1 的下一个请求必须由 group_rate_exceeded 拒绝。
	_, _, err := l.Admit("a", "")
	if denyReason(err) != ReasonGroupRateExceeded {
		t.Fatalf("want group_rate_exceeded, got %v", err)
	}
	for _, x := range held {
		x.Release()
	}
}

func TestGroupConfigValidation(t *testing.T) {
	base := groupCfg()
	cases := map[string]func(Config) Config{
		"group zero rate": func(c Config) Config {
			g := c.Groups["g1"]
			g.RatePerSec = 0
			c.Groups["g1"] = g
			return c
		},
		"group burst above rate": func(c Config) Config {
			g := c.Groups["g1"]
			g.Burst = 11
			c.Groups["g1"] = g
			return c
		},
		"group zero in-flight": func(c Config) Config {
			g := c.Groups["g1"]
			g.InFlight = 0
			c.Groups["g1"] = g
			return c
		},
		"bad burst window": func(c Config) Config {
			g := c.Groups["g1"]
			g.BurstWindowMS = 5000
			c.Groups["g1"] = g
			return c
		},
		"unknown group reference": func(c Config) Config {
			c.ClientGroups["a"] = "ghost"
			return c
		},
		"empty client id": func(c Config) Config {
			c.ClientGroups[""] = "g1"
			return c
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if err := mutate(base).Validate(); err == nil {
				t.Fatalf("illegal group config must be rejected: %s", name)
			}
		})
	}
}

// TestGroupConcurrentIsolation 高并发下：每分组在途永不超过自身上限，
// 分组计数器相互独立，全局永不超上限，全部结束后精确归零。
func TestGroupConcurrentIsolation(t *testing.T) {
	clk := newFakeClock()
	cfg := groupCfg()
	// 放宽窗口，只压在途层，便于观察隔离性。
	for name, g := range cfg.Groups {
		g.Burst = 1_000
		g.RatePerSec = 1_000
		cfg.Groups[name] = g
	}
	cfg.Burst, cfg.RatePerSec = 1_000, 1_000
	l := NewLimiter(cfg, clk.now)
	lg := tlog{t}

	var wg sync.WaitGroup
	var g1Max, g2Max int64
	var globalMax int64
	var denied int64

	clients := map[string]string{"a": "g1", "b": "g1", "x": "g2", "y": "g2"}
	const perClient = 40
	for c, gname := range clients {
		for i := 0; i < perClient; i++ {
			wg.Add(1)
			go func(client, group string) {
				defer wg.Done()
				_, adm, err := l.Admit(client, "")
				if err != nil {
					atomic.AddInt64(&denied, 1)
					return
				}
				gf := int64(l.GroupInFlight(group))
				gl := int64(l.GlobalInFlight())
				switch group {
				case "g1":
					for {
						m := atomic.LoadInt64(&g1Max)
						if gf <= m || atomic.CompareAndSwapInt64(&g1Max, m, gf) {
							break
						}
					}
				case "g2":
					for {
						m := atomic.LoadInt64(&g2Max)
						if gf <= m || atomic.CompareAndSwapInt64(&g2Max, m, gf) {
							break
						}
					}
				}
				for {
					m := atomic.LoadInt64(&globalMax)
					if gl <= m || atomic.CompareAndSwapInt64(&globalMax, m, gl) {
						break
					}
				}
				lg.Printf("GROUP-CONC client=%s group=%s g_inflight=%d global=%d",
					client, group, gf, gl)
				adm.Release()
			}(c, gname)
		}
	}
	wg.Wait()

	lg.Printf("GROUP-CONC SUMMARY denied=%d g1_max=%d g2_max=%d global_max=%d",
		denied, g1Max, g2Max, globalMax)
	if g1Max > 6 {
		t.Fatalf("g1 in-flight peaked at %d > cap 6", g1Max)
	}
	if g2Max > 10 {
		t.Fatalf("g2 in-flight peaked at %d > cap 10", g2Max)
	}
	if globalMax > 15 {
		t.Fatalf("global in-flight peaked at %d > cap 15", globalMax)
	}
	if l.GlobalInFlight() != 0 || l.GroupInFlight("g1") != 0 || l.GroupInFlight("g2") != 0 {
		t.Fatalf("leak: global=%d g1=%d g2=%d",
			l.GlobalInFlight(), l.GroupInFlight("g1"), l.GroupInFlight("g2"))
	}
}
