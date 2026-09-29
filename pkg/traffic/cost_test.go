package traffic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// weightedCfg 构造一个“只看客户端层 + 全局层”的配置，分组层放宽，
// 并声明 heavy=5、bulk=3 两类高成本入口。
func weightedCfg() Config {
	cfg := testCfgNoGroupLayer()
	cfg.RatePerSec = 10
	cfg.Burst = 5
	cfg.BurstWindowMS = 100
	cfg.GlobalInFlight = 8
	cfg.EndpointCosts = map[string]int{
		"heavy": 5,
		"bulk":  3,
	}
	return cfg
}

func TestCostDefaultsToOne(t *testing.T) {
	clk := newFakeClock()
	cfg := weightedCfg()
	cfg.Burst, cfg.RatePerSec = 1_000, 1_000
	l := NewLimiter(cfg, clk.now)
	snap, _, err := l.Admit("c", "unknown-endpoint")
	if err != nil {
		t.Fatalf("undeclared endpoint must be admitted at cost 1: %v", err)
	}
	if snap.Cost != 1 {
		t.Fatalf("cost = %d, want 1 for undeclared endpoint", snap.Cost)
	}
	snap2, _, err := l.Admit("c", "")
	if err != nil {
		t.Fatalf("empty endpoint must be admitted at cost 1: %v", err)
	}
	if snap2.Cost != 1 {
		t.Fatalf("cost = %d, want 1 for empty endpoint", snap2.Cost)
	}
	if h, _, err := l.Admit("c", "heavy"); err != nil || h.Cost != 5 {
		t.Fatalf("heavy cost = %d err=%v, want 5 admitted", h.Cost, err)
	}
}

func TestCostBurstAndRateWeighted(t *testing.T) {
	clk := newFakeClock()
	cfg := weightedCfg() // burst=5, rate=10, window=100ms
	cfg.GlobalInFlight = 1_000
	lg := tlog{t}
	l := NewLimiter(cfg, clk.now)

	var held []*Admission
	admit := func(ep string, want DenyReason) {
		t.Helper()
		snap, adm, err := l.Admit("c", ep)
		lg.Printf("WEIGHTED ep=%s verdict=%s | %s", ep, denyReason(err), snap.Explain())
		if got := denyReason(err); got != want {
			t.Fatalf("ep=%s at=%s reason=%q want %q", ep, clk.now(), got, want)
		}
		if adm != nil {
			held = append(held, adm)
		}
	}
	defer func() {
		for _, a := range held {
			a.Release()
		}
		if l.GlobalInFlight() != 0 {
			t.Fatalf("in-flight leak: %d", l.GlobalInFlight())
		}
	}()

	// t=0：heavy(5) 恰好占满突发窗；再来任何请求都必须整体拒绝，
	// 不允许只占用剩余（此时剩余为 0）。
	admit("heavy", ReasonNone)
	admit("heavy", ReasonBurstExceeded)
	admit("bulk", ReasonBurstExceeded)
	admit("", ReasonBurstExceeded)

	// t=150ms：第一个 heavy 已滑出突发窗，但仍在速率窗内（累计 5）。
	// 新 heavy(5)：突发 5<=5，速率 5+5=10<=10，放行（恰好打满速率窗）。
	clk.advance(150 * time.Millisecond)
	admit("heavy", ReasonNone)
	// 此时突发窗满 5、速率窗满 10：再 heavy 先撞突发。
	admit("heavy", ReasonBurstExceeded)
	// t=300ms：两个 heavy 都已滑出突发窗，但都仍在速率窗（累计 10）。
	// 即使突发窗全空，最小成本 1 也放不下 -> 速率整体拒绝，
	// 且拒绝不扣任何额度（下一秒恢复后可正常受理）。
	clk.advance(150 * time.Millisecond)
	admit("", ReasonRateExceeded)

	// 越过完整速率窗（t=1150ms）：两个旧记录都滑出，额度完全恢复。
	clk.advance(1000 * time.Millisecond)
	admit("heavy", ReasonNone)
	admit("heavy", ReasonBurstExceeded)
}

func TestCostRejectedRequestChargesNothing(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(weightedCfg(), clk.now)

	// heavy=5，全局上限 8：先放一个 heavy，再尝试一个会在突发层被拒的 heavy。
	adm1 := mustAdmit(t, l, "c", "heavy")
	if l.GlobalInFlight() != 5 {
		t.Fatalf("global in-flight = %d, want 5", l.GlobalInFlight())
	}
	_, _, err := l.Admit("c", "heavy")
	if denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("want burst deny, got %v", err)
	}
	// 被整体拒绝的请求不得扣任何额度。
	if l.GlobalInFlight() != 5 {
		t.Fatalf("rejected request changed in-flight: %d", l.GlobalInFlight())
	}
	adm1.Release()
	if l.GlobalInFlight() != 0 {
		t.Fatalf("after release in-flight = %d, want 0", l.GlobalInFlight())
	}
}

func TestCostGlobalAllOrNothing(t *testing.T) {
	clk := newFakeClock()
	cfg := weightedCfg()
	cfg.Burst = 1_000
	cfg.RatePerSec = 1_000
	cfg.GlobalInFlight = 8
	l := NewLimiter(cfg, clk.now)

	// 占 4（bulk=3 + 普通=1），剩余 4 < heavy=5：
	// heavy 必须整体拒绝，全局仍为 4，不能只占用 4。
	a := mustAdmit(t, l, "c1", "bulk")
	b := mustAdmit(t, l, "c2", "")
	if got := l.GlobalInFlight(); got != 4 {
		t.Fatalf("in-flight = %d want 4", got)
	}
	if _, _, err := l.Admit("c3", "heavy"); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("heavy over remaining cap must be global-denied all-or-nothing, got %v", err)
	}
	if got := l.GlobalInFlight(); got != 4 {
		t.Fatalf("partial occupancy forbidden: in-flight = %d want 4", got)
	}
	a.Release() // 仅普通(1)在途，剩余 7 >= 5，heavy 可整体放入。
	d0 := mustAdmit(t, l, "c3", "heavy")
	if got := l.GlobalInFlight(); got != 6 {
		t.Fatalf("in-flight = %d want 6 (1 + 5)", got)
	}
	d0.Release() // 回到 1
	b.Release()  // 全部空闲
	d := mustAdmit(t, l, "c3", "heavy")
	if l.GlobalInFlight() != 5 {
		t.Fatalf("in-flight = %d want 5", l.GlobalInFlight())
	}
	d.Release()
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak: %d", l.GlobalInFlight())
	}
}

func TestCostReleaseExactlyOnce(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(weightedCfg(), clk.now)
	adm := mustAdmit(t, l, "c", "heavy")
	adm.Release()
	adm.Release()
	adm.Release()
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("triple release of cost 5 corrupted in-flight: %d", got)
	}
}

func TestCostConfigValidation(t *testing.T) {
	cases := map[string]func(Config) Config{
		"zero cost": func(c Config) Config {
			c.EndpointCosts = map[string]int{"x": 0}
			return c
		},
		"negative cost": func(c Config) Config {
			c.EndpointCosts = map[string]int{"x": -2}
			return c
		},
		"cost above global": func(c Config) Config {
			c.EndpointCosts = map[string]int{"x": 100}
			return c
		},
		"cost above burst": func(c Config) Config {
			c.EndpointCosts = map[string]int{"x": 6} // burst=5
			return c
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := mutate(weightedCfg())
			if err := cfg.Validate(); err == nil {
				t.Fatalf("illegal cost config must be rejected: %s", name)
			}
		})
	}
}

// TestCostConcurrentNeverOverAdmit 高成本/普通请求混跑并发核算：
// 全局在途加权量任意时刻不得超过上限；所有结束归还后必须归零、不漏不多。
func TestCostConcurrentNeverOverAdmit(t *testing.T) {
	clk := newFakeClock()
	cfg := weightedCfg()
	cfg.Burst = 1_000
	cfg.RatePerSec = 1_000
	cfg.GlobalInFlight = 20
	l := NewLimiter(cfg, clk.now)
	lg := tlog{t}

	endpoints := []string{"heavy", "bulk", ""} // 成本 5 / 3 / 1
	costOf := map[string]int{"heavy": 5, "bulk": 3, "": 1}

	const workers = 16
	const perWorker = 50
	var wg sync.WaitGroup
	var maxObserved int64
	var admitted, denied int64

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ep := endpoints[(id+i)%len(endpoints)]
				snap, adm, err := l.Admit("c", ep)
				if err != nil {
					atomic.AddInt64(&denied, 1)
					continue
				}
				atomic.AddInt64(&admitted, 1)
				inFlight := int64(l.GlobalInFlight())
				for {
					m := atomic.LoadInt64(&maxObserved)
					if inFlight <= m || atomic.CompareAndSwapInt64(&maxObserved, m, inFlight) {
						break
					}
				}
				if inFlight > int64(cfg.GlobalInFlight) {
					t.Errorf("weighted in-flight %d exceeds cap %d | %s",
						inFlight, cfg.GlobalInFlight, snap.Explain())
				}
				lg.Printf("COST-CONCURRENT ep=%q cost=%d in_flight=%d/%d",
					ep, costOf[ep], inFlight, cfg.GlobalInFlight)
				adm.Release()
			}
		}(w)
	}
	wg.Wait()

	lg.Printf("COST-CONCURRENT SUMMARY admitted=%d denied=%d max_in_flight=%d cap=%d",
		admitted, denied, maxObserved, cfg.GlobalInFlight)
	if maxObserved > int64(cfg.GlobalInFlight) {
		t.Fatalf("observed weighted in-flight %d > cap %d", maxObserved, cfg.GlobalInFlight)
	}
	if admitted+denied != workers*perWorker {
		t.Fatalf("every request needs exactly one verdict")
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("weighted in-flight leaked after all releases: %d", got)
	}
}

func mustAdmit(t *testing.T, l *Limiter, client, endpoint string) *Admission {
	t.Helper()
	_, adm, err := l.Admit(client, endpoint)
	if err != nil {
		t.Fatalf("Admit(%q,%q) unexpected deny: %v", client, endpoint, err)
	}
	return adm
}
