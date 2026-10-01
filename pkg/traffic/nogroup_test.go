package traffic

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件固化“没有分组配置”时的旧口径：
//   - 每个客户端各算各的突发/速率窗，客户端之间互不挤占；
//   - 所有客户端共享的唯一跨客户端约束是全局在途总闸；
//   - 拒绝原因必须能区分层次：窗口层报客户端原因，全局打满报 global_exceeded，
//     绝不允许把全局在途误报成分组在途。

// TestNoGroupClientsDoNotShareWindowQuota：一个客户端把突发/速率用满，
// 不影响其他尚有自身额度的客户端。
func TestNoGroupClientsDoNotShareWindowQuota(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(testCfg(), clk.now) // rate=10 burst=3 window=100ms global=100

	var held []*Admission
	// t=0：a 与 b 各自在自己的突发窗内连放 3 个，互不可见。
	for i := 0; i < 3; i++ {
		held = append(held, mustAdmit(t, l, "a", ""))
		held = append(held, mustAdmit(t, l, "b", ""))
	}
	// 两个客户端的第 4 个请求都撞各自的突发上限——
	// 若是共享额度，第 4 个请求早在合计第 4 次就该被拒，与本断言矛盾。
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("a must hit its own burst cap, got %v", err)
	}
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("b must independently hit its own burst cap, got %v", err)
	}
	// 此时全局在途仅 6，远未触顶：新客户端 c 完全不受 a/b 影响。
	held = append(held, mustAdmit(t, l, "c", ""))

	// 越过完整速率窗：a 把自己的速率窗放满 10，第 11 个撞自身速率上限。
	clk.advance(1100 * time.Millisecond)
	for i := 0; i < 10; i++ {
		held = append(held, mustAdmit(t, l, "a", ""))
		if (i+1)%3 == 0 {
			clk.advance(150 * time.Millisecond) // 滚动突发窗，避免突发层先挡
		}
	}
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonRateExceeded {
		t.Fatalf("a must hit its own rate cap, got %v", err)
	}
	// a 的速率窗已满，但 b 的窗口与 a 完全独立：b 仍能正常突发 3 个。
	clk.advance(150 * time.Millisecond)
	for i := 0; i < 3; i++ {
		held = append(held, mustAdmit(t, l, "b", ""))
	}

	for _, h := range held {
		h.Release()
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("in-flight leak: %d", got)
	}
}

// TestNoGroupDenyReasonsAreLayered：无分组配置时拒绝结论必须落在真实顶到的那一层。
func TestNoGroupDenyReasonsAreLayered(t *testing.T) {
	t.Run("client burst and rate are client-layer reasons", func(t *testing.T) {
		clk := newFakeClock()
		l := NewLimiter(testCfg(), clk.now)
		for i := 0; i < 3; i++ {
			mustAdmit(t, l, "a", "").Release()
		}
		snap, _, err := l.Admit("a", "")
		if denyReason(err) != ReasonBurstExceeded {
			t.Fatalf("want burst_exceeded, got %v", err)
		}
		if snap.GroupEnforced {
			t.Fatalf("ungrouped snapshot must not report an enforced group layer: %s", snap.Explain())
		}
		if !strings.Contains(snap.Explain(), "group_burst=n/a") {
			t.Fatalf("explain must mark group layer n/a: %s", snap.Explain())
		}

		// 全局放得很宽时，b 不受 a 的突发拒绝影响。
		if _, _, err := l.Admit("b", ""); err != nil {
			t.Fatalf("b must stay independent of a's burst denial: %v", err)
		}
	})

	t.Run("global in-flight full reports global layer", func(t *testing.T) {
		clk := newFakeClock()
		cfg := testCfg()
		cfg.RatePerSec, cfg.Burst = 1000, 1000
		cfg.GlobalInFlight = 2
		l := NewLimiter(cfg, clk.now)

		// 两个不同客户端各占 1 个全局名额，它们自身额度都还很宽裕。
		mustAdmit(t, l, "a", "")
		mustAdmit(t, l, "b", "")
		// 第三个客户端被拒：必须是全局层，而不是任何分组层结论。
		snap, _, err := l.Admit("c", "")
		if denyReason(err) != ReasonGlobalExceeded {
			t.Fatalf("full global in-flight must report global_exceeded, got %v (snap=%s)",
				err, snap.Explain())
		}
		if snap.GroupEnforced {
			t.Fatalf("ungrouped deny snapshot must not claim a group layer: %s", snap.Explain())
		}
		// a 自己再请求同样报全局层（而不是 burst/rate）。
		if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonGlobalExceeded {
			t.Fatalf("a must also see global cap, got %v", err)
		}
	})
}

// TestExplicitDefaultGroupIsEnforced：显式配置缺省分组 "" 属于“明确分组”，
// 未映射客户端此时共享该分组三层额度（管理员显式选择的共享口径），
// 拒绝原因正常落在分组层。
func TestExplicitDefaultGroupIsEnforced(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec, cfg.Burst = 100, 100 // 客户端层宽松
	cfg.GlobalInFlight = 100
	cfg.Groups = map[string]GroupConfig{
		DefaultGroup: {RatePerSec: 100, Burst: 2, InFlight: 3, BurstWindowMS: 100},
	}
	l := NewLimiter(cfg, clk.now)

	mustAdmit(t, l, "a", "")
	mustAdmit(t, l, "b", "") // 共享缺省分组突发=2，已打满
	if _, _, err := l.Admit("c", ""); denyReason(err) != ReasonGroupBurstExceeded {
		t.Fatalf("explicit default group burst must be shared, got %v", err)
	}
	if got := l.GroupInFlight(DefaultGroup); got != 2 {
		t.Fatalf("explicit default group in-flight = %d want 2", got)
	}

	clk.advance(150 * time.Millisecond) // 越过突发窗
	mustAdmit(t, l, "a", "")            // 分组在途 3，打满
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("explicit default group in-flight must be shared, got %v", err)
	}
}

// TestMixedUngroupedAndGroupedConcurrent：有/无分组归属的客户端高并发混跑，
// 任一层在途都不超上限，记账精确，最终精确归零。
func TestMixedUngroupedAndGroupedConcurrent(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec, cfg.Burst = 1000, 1000 // 只压在途层
	cfg.GlobalInFlight = 12
	cfg.Groups = map[string]GroupConfig{
		"g1": {RatePerSec: 1000, Burst: 1000, InFlight: 4, BurstWindowMS: 100},
	}
	cfg.ClientGroups = map[string]string{"ga": "g1", "gb": "g1"}
	l := NewLimiter(cfg, clk.now)
	lg := tlog{t}

	var wg sync.WaitGroup
	var globalMax, g1Max int64
	var denied int64
	const clients = 6 // ga/gb 在 g1；u0..u3 无分组归属
	for ci := 0; ci < clients; ci++ {
		client := []string{"ga", "gb", "u0", "u1", "u2", "u3"}[ci]
		for i := 0; i < 80; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, adm, err := l.Admit(client, "")
				if err != nil {
					atomic.AddInt64(&denied, 1)
					return
				}
				g := int64(l.GroupInFlight("g1"))
				gl := int64(l.GlobalInFlight())
				for {
					m := atomic.LoadInt64(&g1Max)
					if g <= m || atomic.CompareAndSwapInt64(&g1Max, m, g) {
						break
					}
				}
				for {
					m := atomic.LoadInt64(&globalMax)
					if gl <= m || atomic.CompareAndSwapInt64(&globalMax, m, gl) {
						break
					}
				}
				lg.Printf("MIXED-CONC client=%s g1=%d/4 global=%d/12", client, g, gl)
				adm.Release()
			}()
		}
	}
	wg.Wait()

	if g1Max > 4 {
		t.Fatalf("g1 in-flight peaked %d > cap 4", g1Max)
	}
	if globalMax > 12 {
		t.Fatalf("global in-flight peaked %d > cap 12", globalMax)
	}
	if denied == 0 {
		t.Fatal("tight caps must produce some denials in this workload")
	}
	if l.GlobalInFlight() != 0 || l.GroupInFlight("g1") != 0 {
		t.Fatalf("leak: global=%d g1=%d", l.GlobalInFlight(), l.GroupInFlight("g1"))
	}
}

// TestMixedGlobalBoundaryReportsGlobal：g1 自己尚有名额，但合计把全局打满时，
// 无论请求有没有分组归属，结论都必须是 global_exceeded（全局兜底，不误伤分组）。
func TestMixedGlobalBoundaryReportsGlobal(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec, cfg.Burst = 1000, 1000
	cfg.GlobalInFlight = 5
	cfg.Groups = map[string]GroupConfig{
		"g1": {RatePerSec: 1000, Burst: 1000, InFlight: 10, BurstWindowMS: 100},
	}
	cfg.ClientGroups = map[string]string{"ga": "g1"}
	l := NewLimiter(cfg, clk.now)

	for i := 0; i < 5; i++ {
		client := "u" // 无分组归属
		if i%2 == 0 {
			client = "ga"
		}
		mustAdmit(t, l, client, "")
	}
	if got := l.GlobalInFlight(); got != 5 {
		t.Fatalf("global = %d want 5", got)
	}
	if _, _, err := l.Admit("ga", ""); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("grouped client must see global backstop, got %v", err)
	}
	if _, _, err := l.Admit("u9", ""); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("ungrouped client must see global backstop, got %v", err)
	}
}
