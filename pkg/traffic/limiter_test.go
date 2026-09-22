package traffic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock 手动可控时钟，保证窗口判定结论稳定可复现。
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)}
}
func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

func testCfg() Config {
	return Config{
		RatePerSec:        10,
		Burst:             3,
		BurstWindowMS:     100,
		GlobalInFlight:    100,
		FailureThreshold:  5,
		OpenCooldownMS:    5000,
		UpstreamTimeoutMS: 2000,
	}
}

func denyReason(err error) DenyReason {
	if err == nil {
		return ReasonNone
	}
	return err.(*DenyError).Reason
}

func TestLimiterBurstThenRateThenRecovery(t *testing.T) {
	clk := newFakeClock()
	lg := tlog{t}
	l := NewLimiter(testCfg(), clk.now)

	var releases []func()
	admit := func(client string, want DenyReason) {
		t.Helper()
		snap, rel, err := l.Admit(client)
		lg.Printf("ADMIT request{client=%s} verdict=%s | %s", client, denyReason(err), snap.Explain())
		if got := denyReason(err); got != want {
			t.Fatalf("client=%s at=%s: reason = %q, want %q",
				client, clk.now().Format(time.RFC3339Nano), got, want)
		}
		if want == ReasonNone {
			releases = append(releases, rel)
		}
	}

	// t=0：突发窗（100ms）内只允许 3 个，第 4 个必须是 burst_exceeded。
	admit("c1", ReasonNone)
	admit("c1", ReasonNone)
	admit("c1", ReasonNone)
	admit("c1", ReasonBurstExceeded)

	// 其他客户端的额度相互独立，正常请求不得被误拒。
	admit("c2", ReasonNone)

	// t=150ms：最初 3 个已滑出突发窗，突发额度恢复；速率窗累计 3 个。
	clk.advance(150 * time.Millisecond)
	admit("c1", ReasonNone) // 累计 4
	admit("c1", ReasonNone) // 5
	admit("c1", ReasonNone) // 6
	admit("c1", ReasonBurstExceeded)

	// t=450ms：再放 3 个（累计 9），仍未触达速率上限 10/s。
	clk.advance(300 * time.Millisecond)
	admit("c1", ReasonNone) // 7
	admit("c1", ReasonNone) // 8
	admit("c1", ReasonNone) // 9

	// t=600ms：第 10 个恰好占满速率窗，第 11 个为 rate_exceeded
	// （即便此时突发窗有空余，速率口径优先保证任意 1s 窗口不超 10）。
	clk.advance(150 * time.Millisecond)
	admit("c1", ReasonNone) // 10
	admit("c1", ReasonRateExceeded)

	// c2 只有一次准入（t=0），在 t=600ms 已滑出突发窗：3 个全新突发额度可用。
	admit("c2", ReasonNone)
	admit("c2", ReasonNone)
	admit("c2", ReasonNone)
	admit("c2", ReasonBurstExceeded)

	// 越过完整速率窗后 c1 完全恢复，正常请求不得被误拒。
	clk.advance(500 * time.Millisecond) // t=1100ms
	admit("c1", ReasonNone)

	// 全部归还后在途名额必须归零；窗口记录在滑出后被清理。
	for _, r := range releases {
		r()
	}
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("global in flight after release = %d, want 0", got)
	}
	clk.advance(1 * time.Second)
	if _, _, err := l.Admit("cleanup-probe"); err != nil {
		t.Fatalf("fresh client must be admitted: %v", err)
	}
	if got := l.ActiveClients(); got != 1 {
		t.Fatalf("active clients after window expiry = %d, want 1", got)
	}
}

func TestLimiterGlobalCap(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 1000
	cfg.Burst = 1000
	cfg.GlobalInFlight = 2
	l := NewLimiter(cfg, clk.now)

	_, r1, err := l.Admit("a")
	if err != nil {
		t.Fatal(err)
	}
	_, r2, err := l.Admit("b")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Admit("c"); denyReason(err) != ReasonGlobalExceeded {
		t.Fatalf("third concurrent client should hit global cap: %v", err)
	}
	r1()
	_, r3, err := l.Admit("c")
	if err != nil {
		t.Fatalf("freed global slot must be reusable: %v", err)
	}
	r2()
	r3()
	if l.GlobalInFlight() != 0 {
		t.Fatalf("in flight = %d, want 0", l.GlobalInFlight())
	}
}

func TestLimiterRejectedRequestsNotCounted(t *testing.T) {
	clk := newFakeClock()
	lg := tlog{t}
	l := NewLimiter(testCfg(), clk.now)
	for i := 0; i < 50; i++ {
		snap, _, err := l.Admit("c")
		if i < 3 {
			if err != nil {
				t.Fatalf("request %d must pass: %v", i, err)
			}
		} else {
			if denyReason(err) != ReasonBurstExceeded {
				t.Fatalf("request %d must be burst-denied, got %v", i, err)
			}
		}
		if i == 0 || i == 49 {
			lg.Printf("REJECT-NO-COUNT i=%d | %s", i, snap.Explain())
		}
	}
	if l.GlobalInFlight() != 3 {
		t.Fatalf("rejected requests must not consume quota, got in-flight %d", l.GlobalInFlight())
	}
}

func TestLimiterReleaseIsOnceOnly(t *testing.T) {
	clk := newFakeClock()
	l := NewLimiter(testCfg(), clk.now)
	_, rel, err := l.Admit("c")
	if err != nil {
		t.Fatal(err)
	}
	rel()
	rel() // 重复归还不得造成负数/重复计数
	if got := l.GlobalInFlight(); got != 0 {
		t.Fatalf("double release corrupted count: %d", got)
	}
}

// TestLimiterConcurrentNeverOverAdmit 大量并发下核算竞态：
// 任意时刻在途量不得超过全局上限；总通过数不得超过速率上限；
// 每个请求都必须得到稳定一致的结论之一。
func TestLimiterConcurrentNeverOverAdmit(t *testing.T) {
	clk := newFakeClock()
	cfg := testCfg()
	cfg.RatePerSec = 50
	cfg.Burst = 50
	cfg.GlobalInFlight = 10
	l := NewLimiter(cfg, clk.now)
	lg := tlog{t}

	const workers = 40
	const perWorker = 25
	const total = workers * perWorker

	var wg sync.WaitGroup
	var maxObserved int64
	var allowed, deniedBurst, deniedRate, deniedGlobal int64

	hold := make(chan struct{}, total)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				snap, rel, err := l.Admit("shared-client")
				if err != nil {
					switch denyReason(err) {
					case ReasonBurstExceeded:
						atomic.AddInt64(&deniedBurst, 1)
					case ReasonRateExceeded:
						atomic.AddInt64(&deniedRate, 1)
					case ReasonGlobalExceeded:
						atomic.AddInt64(&deniedGlobal, 1)
					}
					continue
				}
				n := atomic.AddInt64(&allowed, 1)
				inFlight := int64(l.GlobalInFlight())
				for {
					m := atomic.LoadInt64(&maxObserved)
					if inFlight <= m || atomic.CompareAndSwapInt64(&maxObserved, m, inFlight) {
						break
					}
				}
				lg.Printf("CONCURRENT ALLOW n=%d in_flight=%d/%d | %s",
					n, inFlight, cfg.GlobalInFlight, snap.Explain())
				<-hold // 持有额度，制造高并发在途洪峰
				rel()
			}
		}()
	}

	// 每次只放行 global cap 个在途请求结束，制造 10 个并发在途的洪峰。
	go func() {
		for i := 0; i < total; i++ {
			hold <- struct{}{}
		}
	}()
	wg.Wait()

	lg.Printf("CONCURRENT SUMMARY allowed=%d burst_denied=%d rate_denied=%d global_denied=%d max_in_flight=%d cap=%d",
		allowed, deniedBurst, deniedRate, deniedGlobal, maxObserved, cfg.GlobalInFlight)

	if allowed != int64(cfg.RatePerSec) {
		t.Fatalf("rate window admitted %d, want exactly %d", allowed, cfg.RatePerSec)
	}
	if maxObserved > int64(cfg.GlobalInFlight) {
		t.Fatalf("observed %d concurrent in-flight > cap %d", maxObserved, cfg.GlobalInFlight)
	}
	if allowed+deniedBurst+deniedRate+deniedGlobal != total {
		t.Fatalf("every request must have exactly one stable verdict")
	}
	if l.GlobalInFlight() != 0 {
		t.Fatalf("leaked in-flight count: %d", l.GlobalInFlight())
	}
}
