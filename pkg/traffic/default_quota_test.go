package traffic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// legacyCfg 构造“没有任何分组配置”的老配置：
// 顶层 rate=10 burst=3 burst_window=100ms global=100。
// 升级后这类配置必须保持旧口径：客户端各算各的 + 全局在途总闸。
func legacyCfg() Config { return testCfg() }

// TestUngroupedClientsDoNotShareWindows 修复核心回归：
// 没有配置分组时，一个客户端把自己的突发/速率用满，
// 不得把其他客户端的额度一起占掉。
func TestUngroupedClientsDoNotShareWindows(t *testing.T) {
	clk := newFakeClock()
	lg := tlog{t}
	l := NewLimiter(legacyCfg(), clk.now)

	var held []*Admission

	// t=0：a 占满自己的突发窗（3 个），第 4 个被“客户端突发”层拒绝。
	for i := 0; i < 3; i++ {
		held = append(held, mustAdmit(t, l, "a", ""))
	}
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("a's 4th must hit its own burst, got %v", err)
	}

	// b 与 a 互不共享：b 的突发窗完全空闲，照样拿满自己的 3 个；
	// 快照必须显示不经过分组层，也不碰任何分组计数器。
	for i := 0; i < 3; i++ {
		snap, adm, err := l.Admit("b", "")
		if err != nil {
			t.Fatalf("b must be independent of a's saturated burst: %v", err)
		}
		if snap.GroupLayer {
			t.Fatalf("ungrouped client must skip group layer: %s", snap.Explain())
		}
		held = append(held, adm)
	}
	if got := l.GroupInFlight(DefaultGroup); got != 0 {
		t.Fatalf("ungrouped admits must not touch a shared default-group counter: %d", got)
	}
	if got := l.ActiveGroups(); got != 0 {
		t.Fatalf("no group actor may exist without explicit groups, got %d", got)
	}
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("b's 4th must hit b's own burst (not a's, not a group), got %v", err)
	}
	lg.Printf("burst windows stay per-client without groups")

	// 越过完整速率窗：窗口清零，干净地验证速率层也互不共享。
	for _, adm := range held {
		adm.Release()
	}
	clk.advance(1100 * time.Millisecond)

	// a 在 1s 速率窗内累计 10 个（分批越过突发窗），第 11 个撞自己的速率层。
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 3; i++ {
			mustAdmit(t, l, "a", "").Release()
		}
		clk.advance(150 * time.Millisecond)
	}
	// 此时 t=1450ms，窗内 9 个；再推进越过最后一批的突发窗（仍在速率窗内）。
	clk.advance(150 * time.Millisecond) // t=1600ms
	mustAdmit(t, l, "a", "").Release()  // 第 10 个，恰好占满 a 的速率窗
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonRateExceeded {
		t.Fatalf("a's 11th must hit its own rate window, got %v", err)
	}
	// b 的速率窗与 a 无关：正常请求必须照放，不动到本来没超量的请求。
	if _, _, err := l.Admit("b", ""); err != nil {
		t.Fatalf("a must not consume b's rate quota: %v", err)
	}
	lg.Printf("rate windows stay per-client without groups")
}

// TestUngroupedGlobalCapIsTheOnlySharedGate 没有分组时，
// 客户端之间唯一共享的是全局在途总闸；打满后结论必须如实落在全局层。
func TestUngroupedGlobalCapIsTheOnlySharedGate(t *testing.T) {
	clk := newFakeClock()
	cfg := legacyCfg()
	cfg.RatePerSec, cfg.Burst = 1000, 1000
	cfg.GlobalInFlight = 3
	l := NewLimiter(cfg, clk.now)

	// a 自己持有全部 3 个全局名额（它自己的突发/速率都还很宽松）。
	var aheld []*Admission
	for i := 0; i < 3; i++ {
		aheld = append(aheld, mustAdmit(t, l, "a", ""))
	}
	// b 自己离上限很远，却因为全局总闸被挡：原因必须是 global_exceeded，
	// 绝不允许报成缺省分组层（group_*）。
	snap, _, err := l.Admit("b", "")
	if denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("global-full must report the global layer for ungrouped clients, got %v", err)
	}
	if snap.GroupLayer {
		t.Fatalf("no group layer may be reported for ungrouped clients: %s", snap.Explain())
	}
	if snap.GroupInFlight != 0 || snap.GroupInLimit != 0 {
		t.Fatalf("group counters must be zeroed when group layer is off: %s", snap.Explain())
	}
	// 被拒不记账：全局仍为 3，且没有产生任何分组在途。
	if got := l.GlobalInFlight(); got != 3 {
		t.Fatalf("denied request changed in-flight: %d", got)
	}
	if got := l.GroupInFlight(DefaultGroup); got != 0 {
		t.Fatalf("deny must not create a default-group counter: %d", got)
	}

	// a 释放 1 个名额：b 立即可用，证明只有全局层在客户端之间共享。
	aheld[0].Release()
	badm := mustAdmit(t, l, "b", "")
	badm.Release()
	for i := 1; i < len(aheld); i++ {
		aheld[i].Release()
	}
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak: %d", l.GlobalInFlight())
	}
}

// TestUngroupedConcurrentWindowIndependence 高并发下无分组口径：
// 冻结时间窗内，每个客户端都应精确拿到自己的突发额度（3 个），
// 与其他客户端的洪峰完全无关；全部归还后精确归零。
func TestUngroupedConcurrentWindowIndependence(t *testing.T) {
	clk := newFakeClock()
	cfg := legacyCfg()
	cfg.RatePerSec, cfg.GlobalInFlight = 1_000_000, 1_000_000
	// burst 仍为 3，时钟全程冻结，所有请求落在同一个突发窗。
	l := NewLimiter(cfg, clk.now)
	lg := tlog{t}

	const clients = 8
	const attempts = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := map[string][]*Admission{}
	var denied int64

	for c := 0; c < clients; c++ {
		client := string(rune('a' + c))
		for i := 0; i < attempts; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, adm, err := l.Admit(client, "")
				if err != nil {
					if denyReason(err) != ReasonBurstExceeded {
						t.Errorf("ungrouped client can only be burst-denied in this setup, got %v", err)
					}
					atomic.AddInt64(&denied, 1)
					return
				}
				mu.Lock()
				admitted[client] = append(admitted[client], adm)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()

	for c := 0; c < clients; c++ {
		client := string(rune('a' + c))
		if got := len(admitted[client]); got != 3 {
			t.Fatalf("client %s admitted %d, want exactly its own burst=3 (shared quota leak?)", client, got)
		}
	}
	if want := int64(clients * (attempts - 3)); denied != want {
		t.Fatalf("denied=%d want %d", denied, want)
	}
	lg.Printf("UNGROUPED-CONC each of %d clients got exactly 3 own-burst slots", clients)

	for _, adms := range admitted {
		for _, adm := range adms {
			adm.Release()
		}
	}
	if l.GlobalInFlight() != 0 || l.ActiveGroups() != 0 {
		t.Fatalf("leak after concurrent ungrouped run: global=%d groups=%d",
			l.GlobalInFlight(), l.ActiveGroups())
	}
}

// TestUngroupedConcurrentGlobalNeverOverAdmit 并发混跑下全局总闸稳定：
// 任意时刻在途不超全局上限，所有归还后精确归零，且不出现分组层拒绝。
func TestUngroupedConcurrentGlobalNeverOverAdmit(t *testing.T) {
	clk := newFakeClock()
	cfg := legacyCfg()
	cfg.RatePerSec, cfg.Burst = 1_000_000, 1_000_000
	cfg.GlobalInFlight = 7
	l := NewLimiter(cfg, clk.now)

	const workers = 12
	const perWorker = 100
	var wg sync.WaitGroup
	var maxObserved int64
	var groupDenied, totalDenied int64

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := string(rune('a' + id%5)) // 5 个独立客户端混跑
			for i := 0; i < perWorker; i++ {
				snap, adm, err := l.Admit(client, "")
				if err != nil {
					switch denyReason(err) {
					case ReasonGlobalExceeded:
						atomic.AddInt64(&totalDenied, 1)
					default:
						t.Errorf("ungrouped mixed run got unexpected reason %v | %s", err, snap.Explain())
						atomic.AddInt64(&groupDenied, 1)
					}
					continue
				}
				inFlight := int64(l.GlobalInFlight())
				for {
					m := atomic.LoadInt64(&maxObserved)
					if inFlight <= m || atomic.CompareAndSwapInt64(&maxObserved, m, inFlight) {
						break
					}
				}
				adm.Release()
			}
		}(w)
	}
	wg.Wait()

	if groupDenied != 0 {
		t.Fatalf("ungrouped clients must never receive group-layer denials")
	}
	if maxObserved > int64(cfg.GlobalInFlight) {
		t.Fatalf("global in-flight peaked at %d > cap %d", maxObserved, cfg.GlobalInFlight)
	}
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leak: %d", l.GlobalInFlight())
	}
}

// TestExplicitDefaultGroupIsARealSharedGroup 显式配置了名为 "" 的缺省分组时，
// 未映射客户端应共享该分组的三层额度（这是新功能的 opt-in 口径，
// 与“完全不配置 groups”的老口径必须区分开）。
func TestExplicitDefaultGroupIsARealSharedGroup(t *testing.T) {
	clk := newFakeClock()
	cfg := legacyCfg()
	cfg.RatePerSec, cfg.Burst = 100, 100
	cfg.GlobalInFlight = 100
	cfg.Groups = map[string]GroupConfig{
		DefaultGroup: {RatePerSec: 100, Burst: 100, InFlight: 2, BurstWindowMS: 100},
	}
	l := NewLimiter(cfg, clk.now)

	u := mustAdmit(t, l, "u", "")
	v1 := mustAdmit(t, l, "v", "")
	if snap := u.Snapshot(); !snap.GroupLayer || snap.Group != DefaultGroup {
		t.Fatalf("explicit default group must engage group layer: %s", snap.Explain())
	}
	if got := l.GroupInFlight(DefaultGroup); got != 2 {
		t.Fatalf("shared default-group in-flight = %d want 2", got)
	}
	// u/v 合计占满缺省分组在途 2：第三个未映射客户端由分组层拒绝。
	if _, _, err := l.Admit("w", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("explicit default group must share in-flight, got %v", err)
	}
	u.Release()
	v1.Release()
	if l.GroupInFlight(DefaultGroup) != 0 || l.GlobalInFlight() != 0 {
		t.Fatalf("leak: group=%d global=%d", l.GroupInFlight(DefaultGroup), l.GlobalInFlight())
	}
}

// TestMixedNamedGroupAndUngroupedCoexist 混跑：显式分组客户端仍受分组三层约束，
// 未分组客户端各算各的；两边互不误伤，全局总闸照旧兜底。
func TestMixedNamedGroupAndUngroupedCoexist(t *testing.T) {
	clk := newFakeClock()
	cfg := groupCfg()         // g1 inflight=6；顶层 burst=10 rate=20
	cfg.GlobalInFlight = 1000 // 本用例只看分组隔离，不看全局边界

	l := NewLimiter(cfg, clk.now)

	// g1 成员 a 占满分组在途 6（中途越过突发窗）。
	var g1held []*Admission
	for i := 0; i < 6; i++ {
		if i == 4 {
			clk.advance(150 * time.Millisecond)
		}
		g1held = append(g1held, mustAdmit(t, l, "a", ""))
	}
	// 同组 b 被“分组在途”挡住。
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("b must hit g1 in-flight, got %v", err)
	}
	// 未分组 outsider 完全不受 g1 打满影响：可拿满自己的 10 个突发。
	var outheld []*Admission
	for i := 0; i < 10; i++ {
		outheld = append(outheld, mustAdmit(t, l, "outsider", ""))
	}
	if snap := outheld[0].Snapshot(); snap.GroupLayer {
		t.Fatalf("outsider must not be group-governed: %s", snap.Explain())
	}
	// outsider 第 11 个撞的是“客户端自己的突发层”，而不是任何分组层。
	if _, _, err := l.Admit("outsider", ""); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("outsider must hit its own burst, got %v", err)
	}
	// g1 仍然饱和（outsider 的活动没有释放/占用 g1 任何额度）。
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g1 must stay saturated independently, got %v", err)
	}
	if got := l.GroupInFlight("g1"); got != 6 {
		t.Fatalf("g1 in-flight = %d want 6", got)
	}
	if got := l.GroupInFlight(DefaultGroup); got != 0 {
		t.Fatalf("ungrouped outsider must not accumulate on default group: %d", got)
	}

	for _, adm := range g1held {
		adm.Release()
	}
	for _, adm := range outheld {
		adm.Release()
	}
	if l.GlobalInFlight() != 0 || l.GroupInFlight("g1") != 0 {
		t.Fatalf("leak: global=%d g1=%d", l.GlobalInFlight(), l.GroupInFlight("g1"))
	}
}

// TestMixedGlobalBackstopReportsGlobalLayer 混跑下全局总闸兜底：
// 显式分组客户端与未分组客户端合计占满全局时，
// 未分组客户端得到的结论必须是 global_exceeded 而不是分组误伤。
func TestMixedGlobalBackstopReportsGlobalLayer(t *testing.T) {
	clk := newFakeClock()
	cfg := groupCfg() // 全局=15，g1 在途=6
	l := NewLimiter(cfg, clk.now)

	// g1 占 6。
	var g1held []*Admission
	for i := 0; i < 6; i++ {
		if i == 4 {
			clk.advance(150 * time.Millisecond)
		}
		g1held = append(g1held, mustAdmit(t, l, "a", ""))
	}
	// 未分组 outsider 占掉剩余 9 个全局名额（自己的突发=10 也恰好够用）。
	var outheld []*Admission
	for i := 0; i < 9; i++ {
		outheld = append(outheld, mustAdmit(t, l, "outsider", ""))
	}
	if got := l.GlobalInFlight(); got != 15 {
		t.Fatalf("global = %d want 15", got)
	}
	// 再来一个未分组客户端：它自身额度充足，挡住它的只能是全局层。
	snap, _, err := l.Admit("outsider-2", "")
	if denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("mixed global-full must report global layer to ungrouped clients, got %v", err)
	}
	if snap.GroupLayer {
		t.Fatalf("ungrouped deny snapshot must have group layer off: %s", snap.Explain())
	}
	// g1 自身在途也已满 6：此时 g1 客户端 b 的决定性一层是分组层，
	// 层次结论必须如实区分（不能因为“其实全局也快满了”就糊成 global）。
	if _, _, err := l.Admit("b", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g1 saturated client must report group layer first, got %v", err)
	}
	// 释放一个 g1 名额：g1=5 恢复可入，但全局仍满（14/15?）——
	// 释放后全局=14，b 成本 1 可入，全局随之回到 15。
	g1held[0].Release() // g1=5，全局=14
	badm := mustAdmit(t, l, "b", "")
	// 现在 g1=6、全局=15：g1 再放一个会先撞分组层（6==cap 6）。
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g1 must be re-saturated at group layer, got %v", err)
	}
	// 释放一个“未分组”名额腾出全局空间，但 g1 仍满：
	// 此时 b 的同组请求由分组层挡（全局不再是决定性一层）。
	outheld[0].Release() // 全局=14
	if _, _, err := l.Admit("a", ""); denyReason(err) != ReasonGroupInFlightExceed {
		t.Fatalf("g1-full must decide at group layer even when global has room, got %v", err)
	}

	badm.Release()
	for i := 1; i < len(g1held); i++ {
		g1held[i].Release()
	}
	for i := 1; i < len(outheld); i++ {
		outheld[i].Release()
	}
	if l.GlobalInFlight() != 0 || l.GroupInFlight("g1") != 0 {
		t.Fatalf("leak: global=%d g1=%d", l.GlobalInFlight(), l.GroupInFlight("g1"))
	}
}

// TestUngroupedReloadFreezesGroupLayer 热加载在“无分组层/有分组层”之间切换时，
// 已受理请求必须按受理时固化的口径归还，不能错记、漏还。
func TestUngroupedReloadFreezesGroupLayer(t *testing.T) {
	clk := newFakeClock()

	// 阶段 1：完全无分组。u 受理时固化 group_layer=off。
	cfg := legacyCfg()
	cfg.RatePerSec, cfg.Burst, cfg.GlobalInFlight = 100, 100, 100
	l := NewLimiter(cfg, clk.now)
	u := mustAdmit(t, l, "u", "")
	if u.Snapshot().GroupLayer {
		t.Fatalf("setup: u must start without group layer")
	}

	// Reload：显式加入缺省分组。v 受理时应进入分组层。
	cfg2 := cfg
	cfg2.Groups = map[string]GroupConfig{
		DefaultGroup: {RatePerSec: 100, Burst: 100, InFlight: 100, BurstWindowMS: 100},
	}
	if err := l.Reload(cfg2); err != nil {
		t.Fatalf("reload: %v", err)
	}
	v := mustAdmit(t, l, "v", "")
	if !v.Snapshot().GroupLayer {
		t.Fatalf("post-reload v must be group-governed")
	}
	if got := l.GroupInFlight(DefaultGroup); got != 1 {
		t.Fatalf("default group in-flight = %d want 1 (only v)", got)
	}
	// u 按旧口径归还：不得碰分组计数器（否则会把 v 的 1 错还成 0 之前先乱）。
	u.Release()
	if got := l.GroupInFlight(DefaultGroup); got != 1 {
		t.Fatalf("old (layer-off) release must not touch group counter, got %d", got)
	}
	v.Release()
	if got := l.GroupInFlight(DefaultGroup); got != 0 || l.GlobalInFlight() != 0 {
		t.Fatalf("after settle: group=%d global=%d", l.GroupInFlight(DefaultGroup), l.GlobalInFlight())
	}

	// 阶段 2（反向）：带显式缺省分组时受理 w，再 Reload 移除分组。
	w := mustAdmit(t, l, "w", "")
	if !w.Snapshot().GroupLayer {
		t.Fatalf("w must be group-governed at admit time")
	}
	if got := l.GroupInFlight(DefaultGroup); got != 1 {
		t.Fatalf("group in-flight = %d want 1", got)
	}
	if err := l.Reload(cfg); err != nil {
		t.Fatalf("reload removing groups: %v", err)
	}
	// 新请求已经不走分组层。
	z := mustAdmit(t, l, "z", "")
	if z.Snapshot().GroupLayer {
		t.Fatalf("after groups removed, new admits must skip group layer")
	}
	// w 仍按受理时口径归还到旧分组计数器，不能漏还、不能出现负数。
	w.Release()
	z.Release()
	if got := l.GroupInFlight(DefaultGroup); got != 0 {
		t.Fatalf("old (layer-on) release must settle the removed group to 0, got %d", got)
	}
	if l.GlobalInFlight() != 0 {
		t.Fatalf("global leak: %d", l.GlobalInFlight())
	}
}

// TestGuardUngroupedGlobalDenyEndToEnd 端到端：无分组配置下全局在途打满时，
// Serve 的结论是全局层，请求不到上游，快照标注 group_layer=off。
func TestGuardUngroupedGlobalDenyEndToEnd(t *testing.T) {
	cfg := legacyCfg()
	cfg.RatePerSec, cfg.Burst = 1000, 1000
	cfg.GlobalInFlight = 2
	up := newGateUpstream()
	guard, err := NewWithClock(cfg, up, tlog{t}, SystemClock)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan Result, 2)
	go func() { done <- guard.Serve(context.Background(), "a", "", "a-1") }()
	go func() { done <- guard.Serve(context.Background(), "a", "", "a-2") }()
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 2 },
		"two requests must hold the full global in-flight cap")

	// b 自己额度充足，却被全局总闸挡住：层次必须报全局。
	res := guard.Serve(context.Background(), "b", "", "b-1")
	if res.Reason != ReasonGlobalExceeded {
		t.Fatalf("want global_exceeded, got %q (group=%q)", res.Reason, res.Group)
	}
	if res.Snapshot.GroupLayer {
		t.Fatalf("ungrouped end-to-end deny must show group_layer=off: %s", res.Snapshot.Explain())
	}
	if up.Calls() != 2 {
		t.Fatalf("globally-denied request must never reach upstream, calls=%d", up.Calls())
	}

	close(up.gate)
	for i := 0; i < 2; i++ {
		r := <-done
		if !r.Allowed {
			t.Fatalf("held requests must finish successfully, got reason=%q", r.Reason)
		}
	}
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 0 },
		"all in-flight quota must return to zero")
}
