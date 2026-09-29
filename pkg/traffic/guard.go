package traffic

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Logger 最小日志接口；测试可接入 t.Log，命令行可接 log/slog。
type Logger interface {
	Printf(format string, args ...any)
}

// RequestInfo 一次入口请求的身份与成本声明。
type RequestInfo struct {
	Client   string
	ID       string
	Endpoint string // 入口标识，用于查 EndpointCosts
	Cost     int    // 显式成本；<=0 时按 Endpoint/配置解析，缺省 1
	Group    string // 显式分组；为空时按 ClientGroups 解析
}

// Result 一次请求经过组件后的归一化结论。
type Result struct {
	Client   string
	Group    string
	Cost     int
	ID       string
	Allowed  bool
	Reason   DenyReason
	Upstream UpstreamClass
	Snapshot ClientSnapshot
	Breaker  BreakerSnapshot
}

// Guard 入口流量防护门面。Serve 的判定顺序固定且可解释：
//  1. 收尾判定（closing -> 直接拒绝，不新开）
//  2. 熔断判定（open -> 快速失败，不占用任何额度）
//  3. 客户端突发窗成本
//  4. 客户端速率窗成本
//  5. 分组突发/速率/在途成本（归属分组的请求才经过）
//  6. 全局在途成本总量
//  7. 调用上游（带超时），归一化错误并更新熔断
//  8. 无论上游成败，按准入时固化的成本精确归还（每个受理请求有且仅有一次）
type Guard struct {
	cfgPtr  atomic.Pointer[Config]
	up      Upstream
	log     Logger
	limiter *Limiter
	breaker *Breaker

	mu      sync.Mutex
	closing bool
	closed  bool
	wg      sync.WaitGroup
}

// config 返回当前生效配置（换配后读到新指针，永不返回 nil）。
func (g *Guard) config() Config { return *g.cfgPtr.Load() }

// New 创建 Guard。cfg 必须已通过 Validate，否则返回错误（不静默兜底）。
func New(cfg Config, up Upstream, log Logger) (*Guard, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if up == nil {
		return nil, fmt.Errorf("traffic: upstream is required")
	}
	g := &Guard{
		up:      up,
		log:     log,
		limiter: NewLimiter(cfg, SystemClock),
		breaker: NewBreaker(cfg.FailureThreshold, cfg.OpenCooldown(), SystemClock),
	}
	g.cfgPtr.Store(&cfg)
	return g, nil
}

// NewWithClock 供测试注入时钟。
func NewWithClock(cfg Config, up Upstream, log Logger, clock Clock) (*Guard, error) {
	g, err := New(cfg, up, log)
	if err != nil {
		return nil, err
	}
	g.limiter = NewLimiter(cfg, clock)
	g.breaker = NewBreaker(cfg.FailureThreshold, cfg.OpenCooldown(), clock)
	return g, nil
}

// Serve 兼容旧调用：以缺省成本 1、不指定入口/分组处理一次请求。
func (g *Guard) Serve(ctx context.Context, client, id string) Result {
	return g.ServeRequest(ctx, RequestInfo{Client: client, ID: id})
}

// UpdateConfig 在不中断服务、不重启进程的前提下换配。
// 新配置先做完整校验：非法时返回错误并继续沿用旧配置（旧服务不受影响）；
// 校验通过后按确定顺序切换——限流器原子换配 -> 熔断器参数更新 -> 发布新配置。
// 已受理但未结束的请求仍按准入时固化的口径收尾，不重复计费、不静默丢弃；
// 缩容后的上限只约束切换之后的准入判定。
func (g *Guard) UpdateConfig(cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// 先底层后发布：限流器换配与其准入判定串行，保证切换边界确定；
	// 即便发布前有并发 ServeRequest，它拿到的也是自洽的一套配置。
	g.limiter.UpdateConfig(cfg)
	g.breaker.UpdateConfig(cfg.FailureThreshold, cfg.OpenCooldown())
	g.cfgPtr.Store(&cfg)
	if g.log != nil {
		g.log.Printf("config updated atomically: rate=%d burst=%d global=%d endpoints=%d groups=%d client_groups=%d",
			cfg.RatePerSec, cfg.Burst, cfg.GlobalInFlight,
			len(cfg.EndpointCosts), len(cfg.Groups), len(cfg.ClientGroups))
	}
	return nil
}

// ServeRequest 处理一次入口请求，返回归一化结论（绝不透出原始上游错误）。
func (g *Guard) ServeRequest(ctx context.Context, info RequestInfo) Result {
	// 收尾检查、受理登记与配置快照读取在同一把锁内完成：
	// 与 UpdateConfig 互斥，保证本次请求的超时口径、成本/分组/上限
	// 是同一版本配置；换配只能发生在本次请求开始之前或结束之后。
	g.mu.Lock()
	cfg := g.config()
	if g.closing {
		g.mu.Unlock()
		res := Result{
			Client:  info.Client,
			Group:   info.ResolvedGroup(cfg),
			ID:      info.ID,
			Reason:  ReasonClosing,
			Breaker: g.breaker.Snapshot(),
		}
		g.logDeny(res, "guard is shutting down, no new request accepted")
		return res
	}
	g.wg.Add(1)
	g.mu.Unlock()
	defer g.wg.Done()

	// 1) 熔断：打开期间快速失败，不触碰限流额度，避免错误计费。
	bs, allowed := g.breaker.Allow()
	if !allowed {
		res := Result{
			Client:  info.Client,
			Group:   info.ResolvedGroup(cfg),
			Cost:    info.ResolvedCost(cfg),
			ID:      info.ID,
			Reason:  ReasonCircuitOpen,
			Breaker: bs,
		}
		g.logDeny(res, "circuit breaker rejects fast")
		return res
	}

	// 2) 三层成本额度核算；任一层拒绝都不产生计数，且熔断状态不变（未调用上游）。
	snap, release, err := g.limiter.AdmitCost(Request{
		Client:   info.Client,
		Endpoint: info.Endpoint,
		Cost:     info.Cost,
		Group:    info.Group,
	})
	if err != nil {
		de := err.(*DenyError)
		res := Result{
			Client:   info.Client,
			Group:    snap.Group,
			Cost:     snap.Cost,
			ID:       info.ID,
			Reason:   de.Reason,
			Snapshot: snap,
			Breaker:  g.breaker.Snapshot(),
		}
		g.logDeny(res, de.Detail)
		return res
	}

	// 3) 已受理：额度有且仅有一次归还；即使下游 panic 也不允许漏计。
	released := false
	doRelease := func() {
		if !released {
			released = true
			release()
		}
	}
	defer doRelease()

	// 4) 带超时调用上游，并归一化为可区分结论。
	callCtx, cancel := context.WithTimeout(ctx, cfg.UpstreamTimeout())
	defer cancel()
	out := CallOutcome(callCtx, g.up, UpstreamRequest{Client: info.Client, ID: info.ID})

	// 5) 依据归一化结论迁移熔断（先记账熔断、再归还额度，顺序确定）。
	state := g.breaker.OnResult(out.Class)

	res := Result{
		Client:   info.Client,
		Group:    snap.Group,
		Cost:     snap.Cost,
		ID:       info.ID,
		Allowed:  out.Class == UpstreamSuccess,
		Upstream: out.Class,
		Snapshot: snap,
		Breaker:  g.breaker.Snapshot(),
	}
	if out.Class != UpstreamSuccess {
		res.Reason = DenyReason(out.Class)
	}
	g.logAdmit(res, out, state)
	return res
}

// ResolvedCost 按 Guard 同一口径解析本次请求成本。
func (r RequestInfo) ResolvedCost(cfg Config) int {
	if r.Cost > 0 {
		return r.Cost
	}
	return cfg.CostOf(r.Endpoint)
}

// ResolvedGroup 按 Guard 同一口径解析分组（"" 表示无分组层）。
func (r RequestInfo) ResolvedGroup(cfg Config) string {
	if r.Group != "" {
		if _, ok := cfg.Groups[r.Group]; ok {
			return r.Group
		}
		return ""
	}
	return cfg.GroupOf(r.Client)
}

// Shutdown 按确定顺序收尾：
//  1. 置位 closing，此后所有新请求以 closing 拒绝；
//  2. 等待所有已受理请求完成（不静默丢弃、不强制中断、不错误计费）；
//  3. 返回后全局在途量必然归零。
//
// 可重复调用，第二次起直接返回 nil。
func (g *Guard) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closing = true
	g.mu.Unlock()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	return nil
}

// GlobalInFlight 暴露当前全局在途量，供测试/观测核对计费口径。
func (g *Guard) GlobalInFlight() int { return g.limiter.GlobalInFlight() }

func (g *Guard) logDeny(r Result, basis string) {
	if g.log == nil {
		return
	}
	g.log.Printf(
		"DENY request{client=%s id=%s} reason=%s | %s | %s | basis: %s",
		r.Client, r.ID, r.Reason, r.Snapshot.Explain(), r.Breaker.explain(), basis,
	)
}

func (g *Guard) logAdmit(r Result, out Outcome, state BreakerState) {
	if g.log == nil {
		return
	}
	verdict := "ALLOW"
	if out.Class != UpstreamSuccess {
		verdict = "UPSTREAM_FAIL"
	}
	g.log.Printf(
		"%s request{client=%s id=%s} upstream=%s latency=%s breaker->%s | %s | detail=%s",
		verdict, r.Client, r.ID, out.Class, out.Latency, state,
		r.Snapshot.Explain(), out.Detail,
	)
}
