package traffic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiterReloadInvalidKeepsOld(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(weightedCfg(), clk.now)
	adm := mustAdmit(t, l, "c", "heavy") // 成本 5 在途

	bad := weightedCfg()
	bad.GlobalInFlight = 0 // 非法
	if err := l.Reload(bad); err == nil {
		t.Fatal("invalid config must return an error")
	}
	// 旧配置必须原样沿用：成本表、上限、在途都不变。
	if got := l.Config().GlobalInFlight; got != weightedCfg().GlobalInFlight {
		t.Fatalf("old config must remain, got global=%d", got)
	}
	if got := l.GlobalInFlight(); got != 5 {
		t.Fatalf("in-flight must be untouched by failed reload: %d", got)
	}
	// 新受理仍按旧成本 5：突发窗已被占 5，再来必拒。
	if _, _, err := l.Admit("c", "heavy"); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("old cost table must remain in effect, got %v", err)
	}
	adm.Release()
}

func TestLimiterReloadShrinkAndExpand(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfgNoGroupLayer()
	cfg.Burst, cfg.RatePerSec, cfg.GlobalInFlight = 100, 100, 10
	l := NewLimiter(cfg, clk.now)

	var held []*Admission
	for i := 0; i < 5; i++ {
		held = append(held, mustAdmit(t, l, "c", ""))
	}
	if l.GlobalInFlight() != 5 {
		t.Fatalf("setup in-flight %d", l.GlobalInFlight())
	}

	// 缩容到 3：不回收已占用的 5 个名额，但之后的新准入一律按新上限拒绝。
	shrunk := cfg
	shrunk.GlobalInFlight = 3
	if err := l.Reload(shrunk); err != nil {
		t.Fatalf("valid shrink must succeed: %v", err)
	}
	if _, _, err := l.Admit("c", ""); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("post-shrink admit must respect new cap, got %v", err)
	}
	held[0].Release() // 在途 4，仍 > 3
	if _, _, err := l.Admit("c", ""); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("in-flight 4 still over new cap 3, got %v", err)
	}
	held[1].Release() // 在途 3，已满
	if _, _, err := l.Admit("c", ""); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("in-flight 3 equals cap 3, got %v", err)
	}
	held[2].Release()                  // 在途 2，腾出 1 个名额
	mustAdmit(t, l, "c", "").Release() // 按新上限可放行
	held[3].Release()
	held[4].Release()
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak after shrink phase: %d", l.GlobalInFlight())
	}

	// 扩容立即生效。
	expanded := shrunk
	expanded.GlobalInFlight = 50
	if err := l.Reload(expanded); err != nil {
		t.Fatalf("expand reload: %v", err)
	}
	for i := 0; i < 50; i++ {
		held = append(held, mustAdmit(t, l, "c", ""))
	}
	for i := 5; i < len(held); i++ {
		held[i].Release()
	}
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak after expand phase: %d", l.GlobalInFlight())
	}
}

func TestLimiterReloadRateWindowCarriedOver(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfgNoGroupLayer()
	cfg.Burst, cfg.RatePerSec, cfg.GlobalInFlight = 100, 10, 100
	l := NewLimiter(cfg, clk.now)
	for i := 0; i < 3; i++ {
		mustAdmit(t, l, "c", "").Release() // 窗口内累计 3，在途已归零
	}

	// 缩到 burst=2/rate=3：窗口记录保留，已累计 3（>=burst 2），
	// 新准入立刻被额度层拒绝——证明窗口计数跨 Reload 延续、不清零。
	cfg2 := cfg
	cfg2.Burst, cfg2.RatePerSec = 2, 3
	if err := l.Reload(cfg2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Admit("c", ""); denyReason(err) == ReasonNone {
		t.Fatalf("carried-over window usage must count against new caps")
	}
	// 越过完整窗口后按新上限恢复：突发窗先放 2；再等突发窗滑出，
	// 速率窗内已有 2，第 3 个把速率恰好打满，第 4 个由速率层拒绝。
	clk.advance(1100 * time.Millisecond)
	a := mustAdmit(t, l, "c", "")
	b := mustAdmit(t, l, "c", "")
	clk.advance(150 * time.Millisecond)
	_ = mustAdmit(t, l, "c", "")
	if _, _, err := l.Admit("c", ""); denyReason(err) != ReasonRateExceeded {
		t.Fatalf("new rate cap 3 must then reject the 4th, got %v", err)
	}
	a.Release()
	b.Release()
}

func TestLimiterReloadInFlightSettlesAtOldCost(t *testing.T) {
	clk := newFakeClock()
	cfg := weightedCfg() // heavy=5, burst=5, global=8
	l := NewLimiter(cfg, clk.now)

	old := mustAdmit(t, l, "c", "heavy") // 受理时成本 5，在途 5
	if l.GlobalInFlight() != 5 {
		t.Fatalf("setup: %d", l.GlobalInFlight())
	}

	// 运行期把 heavy 成本改为 2（同步调整下限，保证配置合法）。
	cfg2 := cfg
	cfg2.Burst, cfg2.RatePerSec = 2, 2
	cfg2.GlobalInFlight = 8
	cfg2.EndpointCosts = map[string]int{"heavy": 2, "bulk": 2}
	if err := l.Reload(cfg2); err != nil {
		t.Fatalf("reload: %v", err)
	}

	// 已在途请求必须按受理时固化的成本 5 归还，而不是新成本 2。
	old.Release()
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("old-cost release wrong: in-flight=%d want 0 (5-5, not 5-2)", got)
	}
	// 旧的成本 5 窗口条目仍在窗内，越过窗口后新受理才按新成本 2 计费。
	clk.advance(1100 * time.Millisecond)
	snap, adm, err := l.Admit("c", "heavy")
	if err != nil {
		t.Fatalf("new cost 2 must apply after reload: %v", err)
	}
	if snap.Cost != 2 {
		t.Fatalf("post-reload cost = %d want 2", snap.Cost)
	}
	adm.Release()
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak: %d", l.GlobalInFlight())
	}
}

func TestLimiterReloadGroupRemapSettlesOldGroup(t *testing.T) {
	clk := newFakeClock()
	cfg := groupCfg()
	// 放宽窗口/在途，避免其它层干扰对“分组记账归属”的观察。
	for name, g := range cfg.Groups {
		g.Burst, g.RatePerSec, g.InFlight = 100, 100, 100
		cfg.Groups[name] = g
	}
	l := NewLimiter(cfg, clk.now)

	adm := mustAdmit(t, l, "a", "") // 受理时 a 属于 g1
	if l.GroupInFlight("g1") != 1 || l.GroupInFlight("g2") != 0 {
		t.Fatalf("precondition g1=%d g2=%d", l.GroupInFlight("g1"), l.GroupInFlight("g2"))
	}

	// 把 a 改派到 g2。
	cfg2 := cfg
	cfg2.ClientGroups = map[string]string{"a": "g2", "b": "g1", "x": "g2", "y": "g2"}
	if err := l.Reload(cfg2); err != nil {
		t.Fatal(err)
	}
	// 新请求计入 g2。
	newadm := mustAdmit(t, l, "a", "")
	if l.GroupInFlight("g2") != 1 {
		t.Fatalf("post-remap admits must land in g2: g2=%d", l.GroupInFlight("g2"))
	}
	// 旧请求归还必须回到受理时的 g1，不能错误地从 g2 扣两次。
	adm.Release()
	if l.GroupInFlight("g1") != 0 {
		t.Fatalf("old admission must settle against g1: g1=%d", l.GroupInFlight("g1"))
	}
	if l.GroupInFlight("g2") != 1 {
		t.Fatalf("g2 must only reflect the new admission: g2=%d", l.GroupInFlight("g2"))
	}
	newadm.Release()
	if l.GroupInFlight("g2") != 0 || l.GlobalInFlight() != 0 {
		t.Fatalf("leak g2=%d global=%d", l.GroupInFlight("g2"), l.GlobalInFlight())
	}
}

// TestLimiterReloadConcurrentInvariant Reload 与大量并发受理/归还交错：
// 任一时刻在途非负且不超“当时上限”；所有在途请求最终按各自固化成本精确归零。
func TestLimiterReloadConcurrentInvariant(t *testing.T) {
	clk := newFakeClock()
	cfg := weightedCfg()
	cfg.Burst, cfg.RatePerSec = 1000, 1000
	cfg.GlobalInFlight = 20
	l := NewLimiter(cfg, clk.now)
	lg := tlog{t}

	const workers = 12
	const rounds = 60
	var wg sync.WaitGroup
	var violations int64

	stop := make(chan struct{})
	go func() {
		caps := []int{20, 8, 30, 5, 20}
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				c := cfg
				c.GlobalInFlight = caps[i%len(caps)]
				if err := l.Reload(c); err != nil {
					t.Errorf("unexpected reload failure: %v", err)
					return
				}
				i++
			}
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			eps := []string{"heavy", "bulk", ""}
			for i := 0; i < rounds; i++ {
				ep := eps[(id+i)%3]
				snap, adm, err := l.Admit("c", ep)
				if err != nil {
					continue
				}
				// 用受理时刻快照里的上限核对：受理时记账后用量必然 <= 当时上限；
				// 不能用 Reload 之后读到的新上限去要求旧在途立即回落。
				if snap.GlobalUsed < snap.Cost || snap.GlobalUsed > snap.GlobalLimit {
					t.Errorf("admission %d outside [cost=%d, cap=%d]",
						snap.GlobalUsed, snap.Cost, snap.GlobalLimit)
					atomic.AddInt64(&violations, 1)
				}
				if id == 0 && i%20 == 0 {
					lg.Printf("RELOAD-CONC ep=%s admitted_used=%d cap_at_admit=%d",
						ep, snap.GlobalUsed, snap.GlobalLimit)
				}
				adm.Release()
				if got := l.GlobalInFlight(); got < 0 {
					t.Errorf("negative in-flight after release: %d", got)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)

	if violations != 0 {
		t.Fatalf("found %d cap violations during concurrent reloads", violations)
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("all admitted requests must settle exactly: global=%d", got)
	}
}

// gateUpstream 用通道精确控制一次上游调用何时返回，便于夹住“在途”窗口。
type gateUpstream struct {
	gate    chan struct{}
	calls   int64
	mu      sync.Mutex
	blocked []chan struct{}
}

func newGateUpstream() *gateUpstream {
	return &gateUpstream{gate: make(chan struct{})}
}

func (g *gateUpstream) Call(ctx context.Context, _ UpstreamRequest) error {
	atomic.AddInt64(&g.calls, 1)
	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gateUpstream) Calls() int { return int(atomic.LoadInt64(&g.calls)) }

func guardReloadCfg() Config {
	cfg := testCfg()
	cfg.RatePerSec, cfg.Burst, cfg.GlobalInFlight = 100, 100, 10
	cfg.Groups = map[string]GroupConfig{
		DefaultGroup: {RatePerSec: 100, Burst: 100, InFlight: 100, BurstWindowMS: 100},
	}
	cfg.EndpointCosts = map[string]int{"heavy": 5}
	return cfg
}

func TestGuardReloadShrinksAndSettlesInFlight(t *testing.T) {
	up := newGateUpstream()
	cfg := guardReloadCfg()
	guard, err := NewWithClock(cfg, up, tlog{t}, SystemClock)
	if err != nil {
		t.Fatal(err)
	}

	// 一个成本 5 的请求进入并卡在上游调用中（占用 5 个全局名额）。
	done := make(chan Result, 1)
	go func() {
		done <- guard.Serve(context.Background(), "c", "heavy", "req-1")
	}()
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 5 },
		"request must be admitted and hold 5 quota")

	// 在途期间把全局上限缩到 6（不能小于最大入口成本 5，否则该入口永远不可
	// 受理，Validate 会拒绝这类配置）：新请求需要 5+5=10 > 6，被新上限拒绝。
	cfg2 := cfg
	cfg2.GlobalInFlight = 6
	if err := guard.Reload(cfg2); err != nil {
		t.Fatalf("reload: %v", err)
	}
	res := guard.Serve(context.Background(), "c", "heavy", "req-2")
	if res.Reason != ReasonGlobalExceeded {
		t.Fatalf("post-reload request must be denied by new global cap, got %q", res.Reason)
	}
	if up.Calls() != 1 {
		t.Fatalf("denied request must never reach upstream, upstream calls=%d", up.Calls())
	}

	// 非法配置必须报错且沿用旧（已生效的）配置。
	bad := cfg2
	bad.GlobalInFlight = 0
	if err := guard.Reload(bad); err == nil {
		t.Fatal("invalid reload must fail")
	}

	// 放行卡住的上游：已受理请求按旧口径正常收尾，名额精确归还。
	close(up.gate)
	r := <-done
	if !r.Allowed || r.Cost != 5 {
		t.Fatalf("in-flight request must finish normally: allowed=%v cost=%d reason=%q",
			r.Allowed, r.Cost, r.Reason)
	}
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 0 },
		"quota must return exactly to zero after reload-in-flight request settles")

	// 非法 reload 没有破坏服务：当前生效上限仍是 6（既不是原始的 10，
	// 也没有被非法值覆盖），且成本 5 的请求在空闲时可正常受理。
	if got := guard.CurrentConfig().GlobalInFlight; got != 6 {
		t.Fatalf("active global cap must remain 6 after failed reload, got %d", got)
	}
	res2 := guard.Serve(context.Background(), "c", "heavy", "req-3")
	if !res2.Allowed {
		t.Fatalf("service must stay healthy on the last valid config, got reason=%q", res2.Reason)
	}
}

func TestGuardReloadFromJSONValidation(t *testing.T) {
	cfg := guardReloadCfg()
	guard, err := New(cfg, NewScriptedUpstream(0), nil)
	if err != nil {
		t.Fatal(err)
	}
	// JSON 完整校验路径本身必须拒绝非法文档（运维从文件加载时的第一道防线）。
	if _, perr := ParseConfig([]byte(`{"global_in_flight":0}`)); perr == nil {
		t.Fatal("invalid json config must be rejected at parse time")
	}
	// 即便调用方绕过解析直接塞非法结构，Guard.Reload 仍做完整校验并拒绝。
	bad := cfg
	bad.GlobalInFlight = 0
	if err := guard.Reload(bad); err == nil {
		t.Fatal("zero global must fail full validation on reload")
	}
	if got := guard.CurrentConfig().GlobalInFlight; got != 10 {
		t.Fatalf("active config must stay at 10, got %d", got)
	}
}

func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting: " + msg)
}
