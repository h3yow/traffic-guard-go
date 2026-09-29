package traffic

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func groupCfg() Config {
	cfg := testCfg()
	cfg.RatePerSec = 100
	cfg.Burst = 100
	cfg.GlobalInFlight = 100
	// g1 很紧：突发 2、速率 4、在途 3；g2 宽松；窗口取全局 burst=100ms。
	cfg.Groups = map[string]GroupConfig{
		"g1": {RatePerSec: 4, Burst: 2, InFlight: 3},
		"g2": {RatePerSec: 100, Burst: 100, InFlight: 100},
	}
	cfg.ClientGroups = map[string]string{
		"a1": "g1", "a2": "g1",
		"b1": "g2",
	}
	return cfg
}

// TestGroupDenyReasonsAreDistinguishable 组内三道闸各自给出可区分原因。
func TestGroupDenyReasonsAreDistinguishable(t *testing.T) {
	clk := newFakeClock()

	// 闸 A：突发=2，验证 group_burst_exceeded。
	burstCfg := groupCfg()
	burstCfg.Groups["g1"] = GroupConfig{RatePerSec: 100, Burst: 2, InFlight: 100}
	lb := NewLimiter(burstCfg, clk.now)
	_, r1, err := lb.AdmitCost(Request{Client: "a1"})
	if err != nil {
		t.Fatal(err)
	}
	_, r2, err := lb.AdmitCost(Request{Client: "a1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := lb.AdmitCost(Request{Client: "a2"}); denyReason(err) != ReasonGroupBurst {
		t.Fatalf("group burst cap must yield group_burst_exceeded, got %v", err)
	}
	// 组内另一个客户端也必须被同一道闸挡住（额度是组级共享）。
	if _, _, err := lb.AdmitCost(Request{Client: "a2"}); denyReason(err) != ReasonGroupBurst {
		t.Fatalf("group burst is shared across clients, got %v", err)
	}
	r1()
	r2()

	// 闸 B：窗口放宽、在途=3，越窗后验证 group_in_flight_limit。
	clk.advance(150 * time.Millisecond)
	infCfg := groupCfg()
	infCfg.Groups["g1"] = GroupConfig{RatePerSec: 100, Burst: 100, InFlight: 3}
	li := NewLimiter(infCfg, clk.now)
	var rels []func()
	for i := 0; i < 3; i++ {
		_, r, err := li.AdmitCost(Request{Client: "a1"})
		if err != nil {
			t.Fatalf("admit %d under group in-flight 3: %v", i, err)
		}
		rels = append(rels, r)
	}
	if _, _, err := li.AdmitCost(Request{Client: "a2"}); denyReason(err) != ReasonGroupInFlight {
		t.Fatalf("group in-flight cap must yield group_in_flight_limit, got %v", err)
	}
	for _, r := range rels {
		r()
	}
}

// TestGroupRateDenyReason 组速率窗触顶给出 group_rate_exceeded。
func TestGroupRateDenyReason(t *testing.T) {
	clk := newFakeClock()
	cfg := groupCfg()
	cfg.Groups["g1"] = GroupConfig{RatePerSec: 3, Burst: 100, InFlight: 100}
	l := NewLimiter(cfg, clk.now)

	admit := func() error {
		_, r, err := l.AdmitCost(Request{Client: "a1"})
		if err == nil {
			r()
		}
		return err
	}
	if err := admit(); err != nil {
		t.Fatal(err)
	}
	if err := admit(); err != nil {
		t.Fatal(err)
	}
	if err := admit(); err != nil {
		t.Fatal(err)
	}
	if err := admit(); denyReason(err) != ReasonGroupRate {
		t.Fatalf("group rate cap must yield group_rate_exceeded, got %v", err)
	}
}

// TestGroupsAreIndependent 一个分组超额不得影响其他分组，也不得误伤
// 全局内本来正常（无分组）的请求。
func TestGroupsAreIndependent(t *testing.T) {
	cfg := groupCfg()
	// 放宽窗口，只用组在途 3 打满 g1，隔离突发/速率因素。
	cfg.Groups["g1"] = GroupConfig{RatePerSec: 100, Burst: 100, InFlight: 3}
	l := NewLimiter(cfg, SystemClock)

	// 把 g1 打到组在途上限（3），随后 g1 请求一律被组层拒绝。
	var g1rels []func()
	for i := 0; i < 3; i++ {
		_, r, err := l.AdmitCost(Request{Client: "a1"})
		if err != nil {
			t.Fatalf("fill g1: %v", err)
		}
		g1rels = append(g1rels, r)
	}
	if _, _, err := l.AdmitCost(Request{Client: "a2"}); err == nil {
		t.Fatal("g1 must be saturated")
	}

	// g2 完全不受 g1 影响。
	_, br, err := l.AdmitCost(Request{Client: "b1"})
	if err != nil {
		t.Fatalf("g2 must be unaffected by saturated g1: %v", err)
	}
	// 无分组客户端不经过分组层，也不受影响。
	_, nr, err := l.AdmitCost(Request{Client: "nogroup"})
	if err != nil {
		t.Fatalf("ungrouped client must be unaffected: %v", err)
	}
	br()
	nr()
	for _, r := range g1rels {
		r()
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("global in-flight after all release = %d, want 0", got)
	}
}

// TestGroupCostAccounting 组内窗口/在途均按成本累计：一个成本 3 的请求
// 即可占满 g1 的在途（3）与突发（2 会先挡），用 in_flight 口径验证。
func TestGroupCostAccounting(t *testing.T) {
	cfg := groupCfg()
	cfg.Groups["g1"] = GroupConfig{RatePerSec: 10, Burst: 10, InFlight: 5}
	l := NewLimiter(cfg, SystemClock)

	_, r, err := l.AdmitCost(Request{Client: "a1", Cost: 5})
	if err != nil {
		t.Fatalf("cost 5 within group in-flight 5 must pass: %v", err)
	}
	if _, _, err := l.AdmitCost(Request{Client: "a2", Cost: 1}); denyReason(err) != ReasonGroupInFlight {
		t.Fatalf("5+1 > group in-flight 5 must deny, got %v", err)
	}
	r()
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("weighted group release residual=%d", got)
	}
}

// TestGroupConcurrentIndependence 两个分组各自高并发打满自己的桶：
// 每个分组的拒绝都只能归因于自己（原因明细含本组名），全局不超额，最终精确归零。
func TestGroupConcurrentIndependence(t *testing.T) {
	cfg := groupCfg()
	cfg.Groups["g1"] = GroupConfig{RatePerSec: 100000, Burst: 100000, InFlight: 4}
	cfg.Groups["g2"] = GroupConfig{RatePerSec: 100000, Burst: 100000, InFlight: 4}
	l := NewLimiter(cfg, SystemClock)

	var wg sync.WaitGroup
	var maxGlobal int64
	hold := make(chan struct{}, 2000)
	flood := func(client, ownGroup string) {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, r, err := l.AdmitCost(Request{Client: client})
			if err != nil {
				if denyReason(err) != ReasonGroupInFlight {
					t.Errorf("unexpected deny for %s: %v", client, err)
					return
				}
				if !strings.Contains(err.Error(), `group "`+ownGroup+`"`) {
					t.Errorf("deny detail must attribute to %s, got: %v", ownGroup, err)
					return
				}
				continue
			}
			g := int64(l.GlobalInFlight())
			for {
				m := atomic.LoadInt64(&maxGlobal)
				if g <= m || atomic.CompareAndSwapInt64(&maxGlobal, m, g) {
					break
				}
			}
			<-hold
			r()
		}
	}
	wg.Add(2)
	go flood("a1", "g1")
	go flood("b1", "g2")
	go func() {
		for i := 0; i < 2000; i++ {
			hold <- struct{}{}
		}
	}()
	wg.Wait()

	if maxGlobal > int64(cfg.GlobalInFlight) {
		t.Fatalf("global in-flight %d > cap %d", maxGlobal, cfg.GlobalInFlight)
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("residual global in-flight %d", got)
	}
}
