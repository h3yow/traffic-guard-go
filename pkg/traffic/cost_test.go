package traffic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 成本核算专用配置：客户端 burst=4 rate=10，全局在途成本=6。
func costCfg() Config {
	cfg := testCfg()
	cfg.RatePerSec = 10
	cfg.Burst = 4
	cfg.GlobalInFlight = 6
	cfg.EndpointCosts = map[string]int{"heavy": 4, "mid": 2}
	return cfg
}

func TestWeightedAdmitCostsAndRefunds(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(costCfg(), clk.now)

	// 未声明入口按成本 1；声明的 heavy=4、mid=2。
	snap, r1, err := l.AdmitCost(Request{Client: "c", Endpoint: "heavy"})
	if err != nil {
		t.Fatalf("heavy(4) within burst 4 must pass: %v", err)
	}
	if snap.Cost != 4 || snap.BurstUsed != 4 {
		t.Fatalf("snapshot cost accounting wrong: cost=%d burstUsed=%d", snap.Cost, snap.BurstUsed)
	}
	// 已用 4：再来 mid(2) 会超 burst 上限 4 -> 整体拒绝，不扣任何成本。
	_, _, err = l.AdmitCost(Request{Client: "c", Endpoint: "mid"})
	if denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("4+2 > burst 4 must be burst-denied, got %v", err)
	}
	if got := l.GlobalInFlight(); got != 4 {
		t.Fatalf("rejected weighted request must consume nothing, global=%d want 4", got)
	}
	// 成本 1 也放不下（4+1>4），同样整体拒绝。
	if _, _, err := l.AdmitCost(Request{Client: "c"}); denyReason(err) != ReasonBurstExceeded {
		t.Fatalf("4+1 > burst 4 must be burst-denied, got %v", err)
	}
	// 成本比全局剩余额度还大的请求必须整体拒绝（即便客户端窗口全新）。
	r1()
	clk.advance(150 * time.Millisecond)                                  // heavy 滑出突发窗
	_, _, err = l.AdmitCost(Request{Client: "fresh", Endpoint: "heavy"}) // cost 4
	if err != nil {
		t.Fatalf("fresh heavy must pass: %v", err)
	}
	// 全局剩 6-4=2：再来 heavy(4) 超过全局，整体拒绝且不占用。
	if _, _, err := l.AdmitCost(Request{Client: "fresh2", Endpoint: "heavy"}); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("4 > global remaining 2 must be global-denied, got %v", err)
	}
	if got := l.GlobalInFlight(); got != 4 {
		t.Fatalf("global after oversized deny = %d, want 4", got)
	}
}

func TestWeightedRateWindowAndRecovery(t *testing.T) {
	clk := newFakeClock()
	cfg := costCfg()
	cfg.Burst = 100           // 排除突发层干扰，本用例只验证速率窗加权
	cfg.GlobalInFlight = 1000 // 排除全局干扰
	l := NewLimiter(cfg, clk.now)

	rel := func(r func()) { r() }

	// t=0：heavy(4)+mid(2)+cost1 = 7，下一个 mid(2) -> 9 仍 <=10，
	// 再 heavy(4) -> 13 > 10 速率拒绝。
	_, r, _ := l.AdmitCost(Request{Client: "c", Endpoint: "heavy"})
	rel(r)
	_, r, _ = l.AdmitCost(Request{Client: "c", Endpoint: "mid"})
	rel(r)
	snap, r, _ := l.AdmitCost(Request{Client: "c"})
	rel(r)
	if snap.RateUsed != 7 {
		t.Fatalf("rate used = %d, want 7", snap.RateUsed)
	}
	_, r, err := l.AdmitCost(Request{Client: "c", Endpoint: "mid"})
	if err != nil {
		t.Fatalf("7+2=9 <= rate 10 must pass: %v", err)
	}
	rel(r)
	if _, _, err := l.AdmitCost(Request{Client: "c", Endpoint: "heavy"}); denyReason(err) != ReasonRateExceeded {
		t.Fatalf("9+4 > rate 10 must be rate-denied, got %v", err)
	}
	// 在途已全部归还：全局归零，但速率窗成本仍是 9（准入即记账）。
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("in-flight after release = %d, want 0", got)
	}
	// 越过完整速率窗后恢复：heavy(4) 可再次受理。
	clk.advance(1 * time.Second)
	_, r, err = l.AdmitCost(Request{Client: "c", Endpoint: "heavy"})
	if err != nil {
		t.Fatalf("rate window must recover: %v", err)
	}
	rel(r)
}

// TestWeightedConcurrentNeverOverAdmit 混合成本（1 与 4）高并发：
// 任意时刻全局在途成本不超过上限；全部归还后精确归零，不出现“越用越多/漏还”。
func TestWeightedConcurrentNeverOverAdmit(t *testing.T) {
	clk := newFakeClock()
	cfg := costCfg()
	cfg.RatePerSec = 100000
	cfg.Burst = 100000
	cfg.GlobalInFlight = 20
	l := NewLimiter(cfg, clk.now)

	const workers = 16
	const perWorker = 100
	var wg sync.WaitGroup
	var maxObserved int64
	var allowed, denied int64
	var leaks int64

	hold := make(chan struct{}, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				cost := 1
				if (id+i)%3 == 0 {
					cost = 4
				}
				_, rfn, err := l.AdmitCost(Request{Client: "shared", Cost: cost})
				if err != nil {
					atomic.AddInt64(&denied, 1)
					continue
				}
				atomic.AddInt64(&allowed, 1)
				inFlight := int64(l.GlobalInFlight())
				if inFlight > int64(cfg.GlobalInFlight) {
					atomic.AddInt64(&leaks, 1)
				}
				for {
					m := atomic.LoadInt64(&maxObserved)
					if inFlight <= m || atomic.CompareAndSwapInt64(&maxObserved, m, inFlight) {
						break
					}
				}
				<-hold
				rfn()
				if got := int64(l.GlobalInFlight()); got < 0 {
					atomic.AddInt64(&leaks, 1)
				}
			}
		}(w)
	}
	go func() {
		for i := 0; i < workers*perWorker; i++ {
			hold <- struct{}{}
		}
	}()
	wg.Wait()

	if leaks != 0 {
		t.Fatalf("observed over-cap or negative in-flight, leak events=%d", leaks)
	}
	if maxObserved > int64(cfg.GlobalInFlight) {
		t.Fatalf("max in-flight cost %d > cap %d", maxObserved, cfg.GlobalInFlight)
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("weighted refund must return exactly once, residual=%d", got)
	}
	t.Logf("weighted concurrent: allowed=%d denied=%d max_in_flight=%d/%d",
		allowed, denied, maxObserved, cfg.GlobalInFlight)
}

// TestWeightedReleaseIsOnceOnly 高成本请求重复归还不得扣成负数或多次归还。
func TestWeightedReleaseIsOnceOnly(t *testing.T) {
	l := NewLimiter(costCfg(), SystemClock)
	_, rel, err := l.AdmitCost(Request{Client: "c", Endpoint: "heavy"})
	if err != nil {
		t.Fatal(err)
	}
	rel()
	rel()
	rel()
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("triple release of cost-4 corrupted count: %d", got)
	}
}
