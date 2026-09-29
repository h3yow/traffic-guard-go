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
	Group    string
	Endpoint string
	Cost     int
	ID       string
	Allowed  bool
	Reason   DenyReason
	Upstream UpstreamClass
	Snapshot Snapshot
	Breaker  BreakerSnapshot
}

// Guard 入口流量防护门面。Serve 的判定顺序固定且可解释：
//  1. 收尾判定（closing -> 直接拒绝，不新开）
//  2. 熔断判定（open -> 快速失败，不占用任何额度）
//  3. 客户端突发窗加权额度
//  4. 分组突发窗加权额度
//  5. 客户端速率窗加权额度
//  6. 分组速率窗加权额度
//  7. 分组在途加权总量
//  8. 全局在途加权总量
//  9. 调用上游（带超时），归一化错误并更新熔断
//
// 10. 无论上游成败，精确归还三层在途额度（每个受理请求有且仅有一次）
type Guard struct {
	up  Upstream
	log Logger

	cfgMu   sync.RWMutex
	cfg     Config
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

// CurrentConfig 返回当前生效配置的拷贝（与运行期调整配合使用）。
func (g *Guard) CurrentConfig() Config {
	g.cfgMu.RLock()
	defer g.cfgMu.RUnlock()
	return g.cfg
}

// Reload 在不断服务、不重启进程的前提下让新配置生效。
//
// 生效边界（确定且可复现）：
//   - cfg 先做完整 Validate；非法直接返回错误，线上继续沿用旧配置；
//   - 合法时先更新限流器（窗口/在途状态全部保留，只换额度表），再原子替换
//     Guard 持有的配置快照与熔断阈值/冷却；
//   - 已受理但尚未结束的请求，其成本与分组在 Admission 中已固化，
//     一律按受理时口径归还，不会被重复计费或静默丢弃；
//   - 缩容后的上限只约束 Reload 完成之后新进入的准入判定，
//     不会强行回收已经占用的在途额度（它们照常归还后自然回落到新上限内）。
func (g *Guard) Reload(cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	// 先更新限流器；它内部会再校验一次并在失败时保持旧配置不动。
	if err := g.limiter.Reload(cfg); err != nil {
		return err
	}
	g.breaker.Update(cfg.FailureThreshold, cfg.OpenCooldown())

	g.cfgMu.Lock()
	g.cfg = cfg
	g.cfgMu.Unlock()
	return nil
}

// Serve 处理一次入口请求，返回归一化结论（绝不透出原始上游错误）。
// endpoint 用于查成本表（空串/未声明按成本 1）；client 决定其分组归属。
func (g *Guard) Serve(ctx context.Context, client, endpoint, id string) Result {
	// 收尾检查与“受理登记”在同一把锁内完成，保证：
	// Shutdown 置位 closing 后不会有新请求被登记；
	// 已登记请求一定进入 wg，Shutdown 必须等它完整收尾。
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		res := Result{
			Client:   client,
			Endpoint: endpoint,
			ID:       id,
			Reason:   ReasonClosing,
			Breaker:  g.breaker.Snapshot(),
		}
		g.logDeny(res, "guard is shutting down, no new request accepted")
		return res
	}
	g.wg.Add(1)
	g.mu.Unlock()
	defer g.wg.Done()

	// 受理前固定本次请求使用的配置快照：上游超时在整个调用期间保持不变。
	g.cfgMu.RLock()
	cfg := g.cfg
	g.cfgMu.RUnlock()

	// 1) 熔断：打开期间快速失败，不触碰限流额度，避免错误计费。
	bs, allowed := g.breaker.Allow()
	if !allowed {
		res := Result{
			Client:   client,
			Endpoint: endpoint,
			ID:       id,
			Reason:   ReasonCircuitOpen,
			Breaker:  bs,
		}
		g.logDeny(res, "circuit breaker rejects fast")
		return res
	}

	// 2) 三层六类加权额度核算；任一失败都不产生计数，熔断状态不变（未调用上游）。
	// 分组归属与本次成本在 Admit 内部按当前配置解析，并在 Admission 中固化，
	// 即使 Reload 也不影响其收尾口径。
	snap, adm, err := g.limiter.Admit(client, endpoint)
	if err != nil {
		de := err.(*DenyError)
		res := Result{
			Client:   client,
			Group:    snap.Group,
			Endpoint: endpoint,
			Cost:     snap.Cost,
			ID:       id,
			Reason:   de.Reason,
			Snapshot: snap,
			Breaker:  g.breaker.Snapshot(),
		}
		g.logDeny(res, de.Detail)
		return res
	}

	// 3) 已受理：三层在途额度有且仅有一次归还；即使下游 panic 也不允许漏计。
	defer adm.Release()

	// 4) 带超时调用上游，并归一化为可区分结论。
	callCtx, cancel := context.WithTimeout(ctx, cfg.UpstreamTimeout())
	defer cancel()
	out := CallOutcome(callCtx, g.up, UpstreamRequest{Client: client, Endpoint: endpoint, ID: id})

	// 5) 依据归一化结论迁移熔断（先记账熔断、再归还额度，顺序确定）。
	state := g.breaker.OnResult(out.Class)

	res := Result{
		Client:   client,
		Group:    snap.Group,
		Endpoint: endpoint,
		Cost:     snap.Cost,
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
