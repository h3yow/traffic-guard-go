package traffic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestGuardServeRequestCostAndGroup 端到端：成本声明 + 分组映射 + 三层拒绝原因。
// 使用阻塞上游把首个请求稳定地卡在“在途”状态。
func TestGuardServeRequestCostAndGroup(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 100
	cfg.Burst = 100
	cfg.GlobalInFlight = 10
	cfg.EndpointCosts = map[string]int{"heavy": 6}
	cfg.Groups = map[string]GroupConfig{
		"g": {RatePerSec: 100, Burst: 100, InFlight: 6},
	}
	cfg.ClientGroups = map[string]string{"a": "g", "b": "g"}

	releaseUp := make(chan struct{})
	var entered int64
	up := &blockingUpstream{release: releaseUp, entered: &entered}
	g, err := NewWithClock(cfg, up, tlog{t}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// heavy(6)：命中成本声明与分组映射，受理后阻塞在上游、占 6 在途成本。
	done := make(chan Result, 1)
	go func() {
		done <- g.ServeRequest(ctx, RequestInfo{Client: "a", ID: "1", Endpoint: "heavy"})
	}()
	for atomic.LoadInt64(&entered) != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := g.GlobalInFlight(); got != 6 {
		t.Fatalf("global in-flight cost = %d, want 6", got)
	}
	// 组在途 6 已占满：组内另一客户端任意成本 -> group_in_flight_limit。
	if r := g.ServeRequest(ctx, RequestInfo{Client: "b", ID: "2"}); r.Reason != ReasonGroupInFlight {
		t.Fatalf("group cap must dominate, got %q", r.Reason)
	}
	// 无分组新客户端 heavy(6)：6+6 > 全局 10 -> global_exceeded。
	if r := g.ServeRequest(ctx, RequestInfo{Client: "solo", ID: "3", Endpoint: "heavy"}); r.Reason != ReasonGlobalExceeded {
		t.Fatalf("6+6 > global 10 must be global_exceeded, got %q", r.Reason)
	}
	// 成本 1 的无分组请求：6+1 <= 10，客户端/全局都有空余，正常受理（也会阻塞）。
	plainDone := make(chan Result, 1)
	go func() { plainDone <- g.Serve(ctx, "plain", "4") }()
	for atomic.LoadInt64(&entered) != 2 {
		time.Sleep(time.Millisecond)
	}
	if got := g.GlobalInFlight(); got != 7 {
		t.Fatalf("global in-flight cost = %d, want 7", got)
	}

	releaseUp <- struct{}{} // 放行 heavy
	r1 := <-done
	if !r1.Allowed || r1.Cost != 6 || r1.Group != "g" {
		t.Fatalf("heavy result: allowed=%v cost=%d group=%q", r1.Allowed, r1.Cost, r1.Group)
	}
	releaseUp <- struct{}{} // 放行 plain
	r4 := <-plainDone
	if !r4.Allowed || r4.Cost != 1 || r4.Group != "" {
		t.Fatalf("legacy Serve must behave as cost-1 ungrouped: %+v", r4)
	}
	if got := g.GlobalInFlight(); got != 0 {
		t.Fatalf("all settled, global in-flight = %d want 0", got)
	}
}
