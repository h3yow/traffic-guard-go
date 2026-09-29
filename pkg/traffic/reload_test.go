package traffic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestUpdateConfigInvalidKeepsOldConfig 非法新配置必须报错且旧配置原样生效。
func TestUpdateConfigInvalidKeepsOldConfig(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 10
	cfg.Burst = 3
	g, err := NewWithClock(cfg, NewScriptedUpstream(0), tlog{t}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 旧配置下 3 个突发名额占满，第 4 个 burst_exceeded。
	for i := 0; i < 3; i++ {
		if res := g.Serve(ctx, "c", "id"); !res.Allowed {
			t.Fatalf("request %d must pass under old config: %q", i, res.Reason)
		}
	}
	if res := g.Serve(ctx, "c", "id2"); res.Reason != ReasonBurstExceeded {
		t.Fatalf("4th request must be burst-denied under old cfg, got %q", res.Reason)
	}

	// 非法配置：burst > rate、全局上限为 0，都必须报错。
	bad := cfg
	bad.RatePerSec = 1 // burst=3 > rate=1
	if err := g.UpdateConfig(bad); err == nil {
		t.Fatal("invalid config (burst > rate) must be rejected")
	}
	bad2 := cfg
	bad2.GlobalInFlight = 0
	if err := g.UpdateConfig(bad2); err == nil {
		t.Fatal("zero global cap must be rejected")
	}
	bad3 := cfg
	bad3.EndpointCosts = map[string]int{"heavy": 0}
	if err := g.UpdateConfig(bad3); err == nil {
		t.Fatal("non-positive endpoint cost must be rejected")
	}

	// 旧配置必须继续生效：仍在突发窗内 -> 依旧 burst_exceeded。
	if res := g.Serve(ctx, "c", "id3"); res.Reason != ReasonBurstExceeded {
		t.Fatalf("old config must remain in effect, got %q", res.Reason)
	}
}

// TestUpdateConfigShrinkOnlyAffectsFuture 缩容后的上限只约束换配之后的判定：
// 换配前已占用的窗口成本不被追溯；换配后立即按新上限判定。
func TestUpdateConfigShrinkOnlyAffectsFuture(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 10
	cfg.Burst = 10
	cfg.GlobalInFlight = 100
	g, err := NewWithClock(cfg, NewScriptedUpstream(0), tlog{t}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 旧配置下放 6 个（成本 1），全部结束（在途归零，窗口成本 6）。
	for i := 0; i < 6; i++ {
		if res := g.Serve(ctx, "c", "id"); !res.Allowed {
			t.Fatalf("seed %d must pass", i)
		}
	}
	if g.GlobalInFlight() != 0 {
		t.Fatalf("seeded requests must have settled: %d", g.GlobalInFlight())
	}

	// 推进 150ms：6 条记账滑出突发窗（100ms）但仍在速率窗（1s）内。
	clk.advance(150 * time.Millisecond)

	// 热缩容 rate/burst 10 -> 5：速率窗既有成本 6 已超新上限但不追溯；
	// 突发窗已空，因此下一个新请求必然先在速率层 6+1 > 5 -> rate_exceeded。
	shrunk := cfg
	shrunk.RatePerSec = 5
	shrunk.Burst = 5
	if err := g.UpdateConfig(shrunk); err != nil {
		t.Fatalf("valid shrink must apply: %v", err)
	}
	res := g.Serve(ctx, "c", "id-new")
	if res.Reason != ReasonRateExceeded {
		t.Fatalf("post-shrink admission must see new rate cap, got %q", res.Reason)
	}
	if res.Snapshot.RateUsed != 6 || res.Snapshot.RateLimit != 5 {
		t.Fatalf("snapshot must show old usage 6 against new limit 5: %d/%d",
			res.Snapshot.RateUsed, res.Snapshot.RateLimit)
	}

	// 扩容到 20 后同窗口立即恢复可放（6 < 20）。
	grown := cfg
	grown.RatePerSec = 20
	grown.Burst = 20
	if err := g.UpdateConfig(grown); err != nil {
		t.Fatalf("grow must apply: %v", err)
	}
	if res := g.Serve(ctx, "c", "id-grow"); !res.Allowed {
		t.Fatalf("post-grow request must pass, got %q", res.Reason)
	}
}

// TestUpdateConfigInFlightSettlesUnderOldBasis 换配时已受理未结束的请求：
// 照常按原口径收尾（精确归还、只归还一次），不被重复计费或静默丢弃。
func TestUpdateConfigInFlightSettlesUnderOldBasis(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 100
	cfg.Burst = 100
	cfg.GlobalInFlight = 5

	releaseUp := make(chan struct{})
	var entered int64
	up := &blockingUpstream{release: releaseUp, entered: &entered}
	g, err := NewWithClock(cfg, up, tlog{t}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 受理 3 个成本 1 的在途请求（上游阻塞）。
	var serveWg sync.WaitGroup
	for i := 0; i < 3; i++ {
		serveWg.Add(1)
		go func() {
			defer serveWg.Done()
			if res := g.Serve(ctx, "c", "id"); !res.Allowed {
				t.Errorf("in-flight request must succeed once released: %q", res.Reason)
			}
		}()
	}
	for atomic.LoadInt64(&entered) != 3 {
		time.Sleep(time.Millisecond)
	}
	if got := g.GlobalInFlight(); got != 3 {
		t.Fatalf("want 3 in-flight before reload, got %d", got)
	}

	// 热缩容全局 5 -> 1：不追溯，3 个在途请求不受影响、不被丢弃。
	shrunk := cfg
	shrunk.GlobalInFlight = 1
	if err := g.UpdateConfig(shrunk); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	// 新请求立即受新上限约束：3+1 > 1 -> global_exceeded。
	if res := g.Serve(ctx, "c", "new"); res.Reason != ReasonGlobalExceeded {
		t.Fatalf("new request must see shrunk global cap, got %q", res.Reason)
	}

	// 放行 3 个在途请求收尾：按原成本 1 归还，全局回到 0。
	for i := 0; i < 3; i++ {
		releaseUp <- struct{}{}
	}
	serveWg.Wait()
	if got := g.GlobalInFlight(); got != 0 {
		t.Fatalf("in-flight requests must settle to 0 after reload, got %d", got)
	}

	// 收尾后新上限生效：容量 1 可受理。该请求同样进入阻塞上游，需再放行一次。
	done := make(chan Result, 1)
	go func() { done <- g.Serve(ctx, "c", "after") }()
	for atomic.LoadInt64(&entered) != 4 {
		time.Sleep(time.Millisecond)
	}
	releaseUp <- struct{}{}
	if res := <-done; !res.Allowed {
		t.Fatalf("one request must fit under new cap, got %q", res.Reason)
	}
}

// TestUpdateConfigTakesEffectAtomically 并发换配与准入交替进行：
// 任意受理后的在途成本都不超过“当时生效配置”的上限，全部结束后精确归零。
func TestUpdateConfigTakesEffectAtomically(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 100000
	cfg.Burst = 100000
	cfg.GlobalInFlight = 10
	g, err := NewWithClock(cfg, NewScriptedUpstream(0), tlog{t}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var wg sync.WaitGroup
	var violations int64
	hold := make(chan struct{})
	doneHolding := make(chan struct{})
	go func() {
		for range hold {
		}
		close(doneHolding)
	}()

	// 受理临界区自检：记账后在途成本不得超过本次判定所用配置版本的上限。
	g.limiter.afterAdmit = func(usedCfg Config, globalInFlight int) {
		if globalInFlight > usedCfg.GlobalInFlight {
			atomic.AddInt64(&violations, 1)
		}
	}

	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				cost := 1
				if (id+i)%2 == 0 {
					cost = 2
				}
				res := g.ServeRequest(ctx, RequestInfo{Client: "c", ID: "id", Cost: cost})
				if !res.Allowed {
					continue
				}
				hold <- struct{}{}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		caps := []int{4, 10, 6}
		for i := 0; i < 20; i++ {
			c := cfg
			c.GlobalInFlight = caps[i%len(caps)]
			if err := g.UpdateConfig(c); err != nil {
				t.Errorf("reload: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(hold)
	<-doneHolding

	if violations != 0 {
		t.Fatalf("observed %d admissions exceeding then-effective cap", violations)
	}
	if got := g.GlobalInFlight(); got != 0 {
		t.Fatalf("residual in-flight after concurrent reload = %d", got)
	}
}

// blockingUpstream 阻塞到 release 收到信号后返回成功，用于把请求卡在“在途”。
type blockingUpstream struct {
	release chan struct{}
	entered *int64
}

func (b *blockingUpstream) Call(ctx context.Context, req UpstreamRequest) error {
	atomic.AddInt64(b.entered, 1)
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ErrUpstreamTimeout
	}
}
