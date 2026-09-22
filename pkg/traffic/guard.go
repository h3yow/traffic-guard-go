package traffic

import (
	"context"
	"fmt"
	"sync"
)

// Logger 最小日志接口；测试可接入 t.Log，命令行可接 log/slog。
type Logger interface {
	Printf(format string, args ...any)
}

// Result 一次请求经过组件后的归一化结论。
type Result struct {
	Client   string
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
//  3. 突发窗额度
//  4. 速率窗额度
//  5. 全局在途总量
//  6. 调用上游（带超时），归一化错误并更新熔断
//  7. 无论上游成败，精确归还额度（每个受理请求有且仅有一次）
type Guard struct {
	cfg     Config
	up      Upstream
	log     Logger
	limiter *Limiter
	breaker *Breaker

	mu      sync.Mutex
	closing bool
	closed  bool
	wg      sync.WaitGroup
}

// New 创建 Guard。cfg 必须已通过 Validate，否则返回错误（不静默兜底）。
func New(cfg Config, up Upstream, log Logger) (*Guard, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if up == nil {
		return nil, fmt.Errorf("traffic: upstream is required")
	}
	return &Guard{
		cfg:     cfg,
		up:      up,
		log:     log,
		limiter: NewLimiter(cfg, SystemClock),
		breaker: NewBreaker(cfg.FailureThreshold, cfg.OpenCooldown(), SystemClock),
	}, nil
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

// Serve 处理一次入口请求，返回归一化结论（绝不透出原始上游错误）。
func (g *Guard) Serve(ctx context.Context, client, id string) Result {
	// 收尾检查与“受理登记”在同一把锁内完成，保证：
	// Shutdown 置位 closing 后不会有新请求被登记；
	// 已登记请求一定进入 wg，Shutdown 必须等它完整收尾。
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		res := Result{
			Client:  client,
			ID:      id,
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
			Client:  client,
			ID:      id,
			Reason:  ReasonCircuitOpen,
			Breaker: bs,
		}
		g.logDeny(res, "circuit breaker rejects fast")
		return res
	}

	// 2) 三级额度核算；任一级拒绝都不产生计数，且熔断状态不变（未调用上游）。
	snap, release, err := g.limiter.Admit(client)
	if err != nil {
		de := err.(*DenyError)
		res := Result{
			Client:   client,
			ID:       id,
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
	callCtx, cancel := context.WithTimeout(ctx, g.cfg.UpstreamTimeout())
	defer cancel()
	out := CallOutcome(callCtx, g.up, UpstreamRequest{Client: client, ID: id})

	// 5) 依据归一化结论迁移熔断（先记账熔断、再归还额度，顺序确定）。
	state := g.breaker.OnResult(out.Class)

	res := Result{
		Client:   client,
		ID:       id,
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
