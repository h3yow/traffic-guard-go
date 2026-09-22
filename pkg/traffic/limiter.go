package traffic

import (
	"fmt"
	"sync"
	"time"
)

// Clock 用于测试注入时间。
type Clock func() time.Time

// SystemClock 真实系统时钟。
func SystemClock() time.Time { return time.Now() }

// ClientSnapshot 某客户端在判定时刻的额度/窗口视图（用于日志与可解释性）。
type ClientSnapshot struct {
	Client      string
	BurstWindow time.Duration
	BurstUsed   int
	BurstLimit  int
	RateWindow  time.Duration
	RateUsed    int
	RateLimit   int
	GlobalUsed  int
	GlobalLimit int
	InFlight    int
	At          time.Time
}

// Explain 输出判定依据：请求时刻两级窗口的已用量/上限、客户端与全局在途量。
func (s ClientSnapshot) Explain() string {
	return fmt.Sprintf(
		"client=%s at=%s | burst_window=%s admitted=%d/%d | rate_window=%s admitted=%d/%d | client_in_flight=%d global_in_flight=%d/%d",
		s.Client, s.At.Format(time.RFC3339Nano),
		s.BurstWindow, s.BurstUsed, s.BurstLimit,
		s.RateWindow, s.RateUsed, s.RateLimit,
		s.InFlight, s.GlobalUsed, s.GlobalLimit,
	)
}

// clientState 单个客户端的核算状态。
//
// 计费口径（关键）：
//   - admitted 是“准入时间戳”序列，只在滑出速率窗时被裁剪，绝不因请求结束
//     而删除。因此任意 1s / burst 窗口内的放行量都被精确且只计一次，
//     短请求完成不会重复获得速率额度；
//   - inFlight 是该客户端当前尚未结束的已受理请求数，与全局在途名额对应，
//     请求结束（release）时减一。
type clientState struct {
	admitted []time.Time
	inFlight int
}

// prune 删除不晚于 cutoff 的时间戳（调用方持锁）。
func (cs *clientState) prune(cutoff time.Time) {
	drop := 0
	for drop < len(cs.admitted) && !cs.admitted[drop].After(cutoff) {
		drop++
	}
	if drop > 0 {
		cs.admitted = append(cs.admitted[:0], cs.admitted[drop:]...)
	}
}

// Limiter 负责客户端速率/突发额度与全局总量核算。
// 所有方法并发安全；窗口计数只在 Admit 成功时追加一次，
// 全局名额在 Release 时归还，保证不重复计数、不漏计。
type Limiter struct {
	cfg   Config
	clock Clock

	mu             sync.Mutex
	clients        map[string]*clientState
	globalInFlight int
}

// NewLimiter 创建限流器。
func NewLimiter(cfg Config, clock Clock) *Limiter {
	if clock == nil {
		clock = SystemClock
	}
	return &Limiter{
		cfg:     cfg,
		clock:   clock,
		clients: make(map[string]*clientState),
	}
}

// deny 在快照上附带原因构造拒绝错误（拒绝路径不产生任何计数变更）。
func deny(reason DenyReason, snap ClientSnapshot, format string, args ...any) error {
	return &DenyError{
		Reason: reason,
		Detail: fmt.Sprintf(format, args...) + " | " + snap.Explain(),
	}
}

// Admit 尝试为 client 占用一个额度。判定顺序固定为：
// 突发窗 -> 速率窗 -> 全局总量（最紧的额度先判，保证拒绝原因唯一可区分）。
// 成功返回 release：每个受理请求有且仅有一次全局名额归还（重复调用安全）；
// 失败返回 *DenyError，且窗口/全局计数均不发生变化。
func (l *Limiter) Admit(client string) (ClientSnapshot, func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock()
	cs, ok := l.clients[client]
	if !ok {
		cs = &clientState{}
		l.clients[client] = cs
	}
	// 速率窗是最长的一级，只按速率窗裁剪一次过期准入记录；
	// 突发窗是速率窗的子区间，在剩余有序序列上再统计即可。
	cs.prune(now.Add(-RateWindow))

	burstCutoff := now.Add(-l.cfg.BurstWindow())
	burstUsed := 0
	for _, t := range cs.admitted {
		if t.After(burstCutoff) {
			burstUsed++
		}
	}
	rateUsed := len(cs.admitted)

	snap := ClientSnapshot{
		Client:      client,
		BurstWindow: l.cfg.BurstWindow(),
		BurstUsed:   burstUsed,
		BurstLimit:  l.cfg.Burst,
		RateWindow:  RateWindow,
		RateUsed:    rateUsed,
		RateLimit:   l.cfg.RatePerSec,
		GlobalUsed:  l.globalInFlight,
		GlobalLimit: l.cfg.GlobalInFlight,
		InFlight:    cs.inFlight,
		At:          now,
	}

	// 1) 突发额度
	if burstUsed >= l.cfg.Burst {
		return snap, nil, deny(ReasonBurstExceeded, snap,
			"burst quota exhausted: %d requests already admitted within %s",
			burstUsed, l.cfg.BurstWindow())
	}
	// 2) 速率额度
	if rateUsed >= l.cfg.RatePerSec {
		return snap, nil, deny(ReasonRateExceeded, snap,
			"rate quota exhausted: %d requests already admitted within any rolling %s window",
			rateUsed, RateWindow)
	}
	// 3) 全局在途总量
	if l.globalInFlight >= l.cfg.GlobalInFlight {
		return snap, nil, deny(ReasonGlobalExceeded, snap,
			"global in-flight cap reached: %d/%d",
			l.globalInFlight, l.cfg.GlobalInFlight)
	}

	// 全部通过：一次性记账（两级窗口共用同一时间戳）并占用一个全局名额。
	cs.admitted = append(cs.admitted, now)
	cs.inFlight++
	l.globalInFlight++
	snap.GlobalUsed = l.globalInFlight
	snap.InFlight = cs.inFlight
	snap.BurstUsed++
	snap.RateUsed++

	var once sync.Once
	release := func() {
		once.Do(func() { l.release(client) })
	}
	return snap, release, nil
}

// release 归还一个已受理请求的全局在途名额。
// 注意：不移除准入时间戳——窗口放行量是准入即记账的口径，
// 重复调用被 once 拦截，因此绝不会出现负数或重复归还。
func (l *Limiter) release(client string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.globalInFlight--
	cs, ok := l.clients[client]
	if !ok {
		return
	}
	cs.inFlight--
	// 窗口记录全部滑出且无在途请求的空闲客户端才清理，避免 map 无界增长，
	// 同时不能影响仍在窗口语义内的计数。
	cs.prune(l.clock().Add(-RateWindow))
	if cs.inFlight == 0 && len(cs.admitted) == 0 {
		delete(l.clients, client)
	}
}

// GlobalInFlight 返回当前全局在途量。
func (l *Limiter) GlobalInFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.globalInFlight
}

// ActiveClients 返回当前仍有窗口记录或在途请求的客户端数量。
func (l *Limiter) ActiveClients() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.clients)
}
