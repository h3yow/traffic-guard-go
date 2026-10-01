package traffic

import (
	"context"
	"sync"
	"testing"
)

// latchUpstream 为每个请求（按 ID）提供独立门闩，可精确放行指定的一次
// 上游调用，不会出现“令牌被别的 waiter 抢走”的竞争，保证测试可复现。
type latchUpstream struct {
	mu      sync.Mutex
	latches map[string]chan struct{}
}

func newLatchUpstream() *latchUpstream {
	return &latchUpstream{latches: make(map[string]chan struct{})}
}

func (u *latchUpstream) latch(id string) chan struct{} {
	u.mu.Lock()
	defer u.mu.Unlock()
	ch, ok := u.latches[id]
	if !ok {
		ch = make(chan struct{})
		u.latches[id] = ch
	}
	return ch
}

func (u *latchUpstream) release(t *testing.T, id string) {
	t.Helper()
	u.mu.Lock()
	ch, ok := u.latches[id]
	u.mu.Unlock()
	if !ok {
		t.Fatalf("latchUpstream: no in-flight request %q", id)
	}
	close(ch)
}

func (u *latchUpstream) Call(ctx context.Context, req UpstreamRequest) error {
	select {
	case <-u.latch(req.ID):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestGuardNoGroupLegacySemantics：走完整 Guard.Serve 路径（含熔断/收尾），
// 验证老配置（无任何 groups/client_groups）升级后：
// 1) 多客户端各算各的、互不挤占；2) 全局在途打满时拒绝结论是
// global_exceeded，而不会误报成分组层；3) 释放一个名额后任意其他客户端
// 立即复用；4) 最终在途精确归零。
func TestGuardNoGroupLegacySemantics(t *testing.T) {
	cfg := testCfg()
	cfg.RatePerSec, cfg.Burst = 100, 100
	cfg.GlobalInFlight = 3 // 只压全局在途层
	up := newLatchUpstream()
	guard, err := NewWithClock(cfg, up, tlog{t}, newFakeClock().now)
	if err != nil {
		t.Fatal(err)
	}

	// 3 个不同客户端各占 1 个全局名额，调用挂在上游中（不期待它返回）。
	for _, client := range []string{"a", "b", "c"} {
		go func(c string) { _ = guard.Serve(context.Background(), c, "", c+"-1") }(client)
	}
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 3 },
		"three distinct clients must each hold one global slot")

	// 全局打满：不同客户端都被拒，结论必须是全局层而非分组层。
	for _, client := range []string{"a", "b", "c", "d"} {
		r := guard.Serve(context.Background(), client, "", client+"-denied")
		if r.Reason != ReasonGlobalExceeded {
			t.Fatalf("client %s must be global-denied, got %q", client, r.Reason)
		}
		if r.Snapshot.GroupEnforced {
			t.Fatalf("deny snapshot must not show an enforced group layer: %s",
				r.Snapshot.Explain())
		}
	}

	// 精确放行 a：全局腾出恰好 1 个名额（b、c 仍挂着）。
	up.release(t, "a-1")
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 2 },
		"one slot must return to the global pool")

	// 新客户端 d 复用该名额被受理（在途回到 3），证明跨客户端名额可复用。
	dDone := make(chan Result, 1)
	go func() { dDone <- guard.Serve(context.Background(), "d", "", "d-1") }()
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 3 },
		"the freed slot must be re-admitted to a different client")
	up.release(t, "d-1")
	if r := <-dDone; !r.Allowed {
		t.Fatalf("freed global slot must serve an unrelated client, got reason=%q", r.Reason)
	}

	// 放行 b、c 并优雅收尾，全局在途精确归零。
	up.release(t, "b-1")
	up.release(t, "c-1")
	if err := guard.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 0 },
		"all in-flight must settle exactly to zero")
}

// TestGuardGroupedClientsStillThreeLayered：有分组配置的客户端仍受分组三层约束，
// 与无分组客户端混跑互不干扰（Guard 端到端口径）。
func TestGuardGroupedClientsStillThreeLayered(t *testing.T) {
	cfg := testCfg()
	cfg.RatePerSec, cfg.Burst = 100, 100
	cfg.GlobalInFlight = 100
	cfg.Groups = map[string]GroupConfig{
		"g1": {RatePerSec: 100, Burst: 100, InFlight: 2, BurstWindowMS: 100},
	}
	cfg.ClientGroups = map[string]string{"ga": "g1", "gb": "g1"}

	up := newLatchUpstream()
	guard, err := NewWithClock(cfg, up, tlog{t}, newFakeClock().now)
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = guard.Serve(context.Background(), "ga", "", "ga-1") }()
	go func() { _ = guard.Serve(context.Background(), "gb", "", "gb-1") }()
	waitUntil(t, func() bool { return guard.limiter.GroupInFlight("g1") == 2 },
		"g1 must hold 2 in-flight requests")

	// g1 分组在途打满（2）：ga/gb 被分组层拒绝（全局 100 远未满）。
	if r := guard.Serve(context.Background(), "ga", "", "ga-2"); r.Reason != ReasonGroupInFlightExceed {
		t.Fatalf("grouped client must hit group in-flight cap, got %q", r.Reason)
	}

	// 放行两个挂起的 g1 请求，分组/全局在途回到 0。
	up.release(t, "ga-1")
	up.release(t, "gb-1")
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 0 },
		"g1 requests must settle and release group/global in-flight")

	// 无分组归属的 u 只受全局约束：异步发起，确认已受理且不碰 g1，再放行。
	uDone := make(chan Result, 1)
	go func() { uDone <- guard.Serve(context.Background(), "u", "", "u-1") }()
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 1 },
		"ungrouped client must be admitted against the global pool")
	if got := guard.limiter.GroupInFlight("g1"); got != 0 {
		t.Fatalf("ungrouped client must not touch g1, got %d", got)
	}
	up.release(t, "u-1")
	r := <-uDone
	if !r.Allowed {
		t.Fatalf("ungrouped client must stay unaffected by g1 cap, reason=%q", r.Reason)
	}
	if r.Snapshot.GroupEnforced {
		t.Fatalf("ungrouped snapshot must not be group-enforced: %s", r.Snapshot.Explain())
	}

	if err := guard.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	waitUntil(t, func() bool { return guard.GlobalInFlight() == 0 },
		"global in-flight must settle to zero")

	// 收尾后不再受理。
	if r := guard.Serve(context.Background(), "ga", "", "ga-3"); r.Reason != ReasonClosing {
		t.Fatalf("post-shutdown request must be closing, got %q", r.Reason)
	}
}
