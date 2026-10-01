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

// Snapshot 一次准入判定时刻的额度视图（按成本加权口径）。
// 所有 Used 字段都是“加权消耗”，Limit 字段是对应口径的上限。
type Snapshot struct {
	Client   string
	Group    string
	Endpoint string
	Cost     int
	At       time.Time

	// GroupEnforced 表示本次判定是否真正施加了分组层约束：
	// 只有客户端归属的分组被显式配置时为 true；无分组归属（缺省分组未显式
	// 配置）时为 false，此时下面的 Group* 字段均为零值（Explain 显示 n/a），
	// 拒绝原因也只会来自客户端层或全局层。
	GroupEnforced bool

	ClientBurstWindow time.Duration
	ClientBurstUsed   int
	ClientBurstLimit  int
	ClientRateWindow  time.Duration
	ClientRateUsed    int
	ClientRateLimit   int

	GroupBurstWindow time.Duration
	GroupBurstUsed   int
	GroupBurstLimit  int
	GroupRateWindow  time.Duration
	GroupRateUsed    int
	GroupRateLimit   int

	ClientInFlight int
	GroupInFlight  int
	GroupInLimit   int
	GlobalUsed     int
	GlobalLimit    int
}

// ClientSnapshot 是 Snapshot 的旧名别名，保持历史日志/测试类型兼容。
type ClientSnapshot = Snapshot

// Explain 输出判定依据：客户端/分组两级窗口的加权已用量/上限，
// 以及客户端、分组、全局三层在途加权量。
// 客户端没有分组归属（分组层未生效）时，分组段输出 n/a，
// 明确告知运维“本次判定不经过分组层”，拒绝原因只可能来自客户端层或全局层。
func (s Snapshot) Explain() string {
	groupSegment := "group_burst=n/a group_rate=n/a group_inflight=n/a"
	if s.GroupEnforced {
		groupSegment = fmt.Sprintf(
			"group_burst=%s used=%d/%d group_rate=%s used=%d/%d | "+
				"in_flight group=%d/%d",
			s.GroupBurstWindow, s.GroupBurstUsed, s.GroupBurstLimit,
			s.GroupRateWindow, s.GroupRateUsed, s.GroupRateLimit,
			s.GroupInFlight, s.GroupInLimit,
		)
	}
	return fmt.Sprintf(
		"client=%s group=%s endpoint=%s cost=%d at=%s | "+
			"client_burst=%s used=%d/%d client_rate=%s used=%d/%d | "+
			"%s | "+
			"in_flight client=%d global=%d/%d",
		s.Client, groupDisplayName(s.Group), s.Endpoint, s.Cost,
		s.At.Format(time.RFC3339Nano),
		s.ClientBurstWindow, s.ClientBurstUsed, s.ClientBurstLimit,
		s.ClientRateWindow, s.ClientRateUsed, s.ClientRateLimit,
		groupSegment,
		s.ClientInFlight, s.GlobalUsed, s.GlobalLimit,
	)
}

// costEntry 是一次准入记账条目：准入时间戳 + 本次成本。
// 窗口按“加权消耗”累计；请求结束不删除窗口条目（准入即记账），
// 只在条目滑出最长窗口时被裁剪。
type costEntry struct {
	at   time.Time
	cost int
}

// actorState 客户端或分组共用的核算状态。
type actorState struct {
	entries  []costEntry // 准入时间窗内的加权条目（按时间有序）
	inFlight int         // 当前在途加权总量
}

// prune 删除不晚于 cutoff 的条目（调用方持锁）。
func (a *actorState) prune(cutoff time.Time) {
	drop := 0
	for drop < len(a.entries) && !a.entries[drop].at.After(cutoff) {
		drop++
	}
	if drop > 0 {
		a.entries = append(a.entries[:0], a.entries[drop:]...)
	}
}

func (a *actorState) windowUsed(cutoff time.Time) int {
	used := 0
	for i := range a.entries {
		if a.entries[i].at.After(cutoff) {
			used += a.entries[i].cost
		}
	}
	return used
}

// Limiter 负责 客户端 -> 分组（仅显式配置时） -> 全局 的成本加权核算。
// 无分组归属的客户端只受自身速率/突发窗与全局在途约束，彼此互不挤占；
// 归属到显式分组的客户端还额外受该分组速率/突发/在途三层约束。
// 所有方法并发安全；窗口消耗只在 Admit 成功时追加一次，在途量在 release 时
// 精确归还且仅一次，拒绝路径不产生任何记账变更。
type Limiter struct {
	clock Clock

	mu             sync.Mutex
	cfg            Config
	clients        map[string]*actorState
	groups         map[string]*actorState
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
		clients: make(map[string]*actorState),
		groups:  make(map[string]*actorState),
	}
}

// Reload 在不中断服务的前提下原子替换额度配置。
// 新配置必须先通过 Validate：非法则原样返回错误并继续沿用旧配置，
// 已在途请求仍按受理时的成本/分组（在 Admission 中固化）收尾，不受影响；
// 新上限只约束 Reload 返回之后的准入判定。
// 既有的客户端/分组窗口与在途状态一律保留（换配置不清账、不重置窗口）。
func (l *Limiter) Reload(cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	l.mu.Lock()
	l.cfg = cfg
	l.mu.Unlock()
	return nil
}

// Config 返回当前生效配置的拷贝。
func (l *Limiter) Config() Config {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cfg
}

func denyErr(reason DenyReason, snap Snapshot, format string, args ...any) error {
	return &DenyError{
		Reason: reason,
		Detail: fmt.Sprintf(format, args...) + " | " + snap.Explain(),
	}
}

// Admit 尝试为 client 的 endpoint 入口受理一次请求。
//
// 客户端没有分组归属（其分组未在 groups 中显式配置）时，判定只有三层：
//  1. 客户端突发窗加权消耗
//  2. 客户端速率窗加权消耗
//  3. 全局在途加权总量
//
// 即升级前“每客户端各算各的 + 全局在途总闸”的口径：客户端之间互不挤占，
// 只有真把全局名额占满才会以 global_exceeded 影响所有人。
//
// 客户端归属到显式配置的分组时，分组层生效，判定顺序固定为
// （先看窗口、后看在途；同口径先客户端后分组，全局最后）：
//  1. 客户端突发窗加权消耗
//  2. 分组突发窗加权消耗
//  3. 客户端速率窗加权消耗
//  4. 分组速率窗加权消耗
//  5. 分组在途加权总量
//  6. 全局在途加权总量
//
// 成本大于任一层剩余额度时必须整体拒绝：要么全部记账、要么一层都不动，
// 绝不只占用一部分。成功返回的 Admission.Release 有且仅有一次归还效力。
func (l *Limiter) Admit(client, endpoint string) (Snapshot, *Admission, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock()
	cfg := l.cfg
	cost := cfg.EndpointCost(endpoint)
	group := cfg.ClientGroup(client)
	groupEnforced := cfg.GroupConfigured(group)
	var gc GroupConfig
	if groupEnforced {
		gc = cfg.ResolveGroup(group)
	}

	// 借准入时机顺带清扫已完全空闲、窗口已滑空的旧 actor，
	// 保证“无人再访问的客户端/分组”也能被回收，map 不无界增长。
	l.sweepLocked(now)

	cs := l.clients[client]
	if cs == nil {
		cs = &actorState{}
		l.clients[client] = cs
	}
	var gs *actorState
	if groupEnforced {
		gs = l.groups[group]
		if gs == nil {
			gs = &actorState{}
			l.groups[group] = gs
		}
		// 速率窗是最长一级，只按它裁剪一次；突发窗在剩余有序序列上再统计。
		gs.prune(now.Add(-RateWindow))
	}
	cs.prune(now.Add(-RateWindow))

	snap := Snapshot{
		Client:   client,
		Group:    group,
		Endpoint: endpoint,
		Cost:     cost,
		At:       now,

		GroupEnforced: groupEnforced,

		ClientBurstWindow: cfg.BurstWindow(),
		ClientBurstUsed:   cs.windowUsed(now.Add(-cfg.BurstWindow())),
		ClientBurstLimit:  cfg.Burst,
		ClientRateWindow:  RateWindow,
		ClientRateUsed:    cs.windowUsed(now.Add(-RateWindow)),
		ClientRateLimit:   cfg.RatePerSec,

		ClientInFlight: cs.inFlight,
		GlobalUsed:     l.globalInFlight,
		GlobalLimit:    cfg.GlobalInFlight,
	}
	if groupEnforced {
		snap.GroupBurstWindow = cfg.GroupBurstWindow(group)
		snap.GroupBurstUsed = gs.windowUsed(now.Add(-cfg.GroupBurstWindow(group)))
		snap.GroupBurstLimit = gc.Burst
		snap.GroupRateWindow = RateWindow
		snap.GroupRateUsed = gs.windowUsed(now.Add(-RateWindow))
		snap.GroupRateLimit = gc.RatePerSec
		snap.GroupInFlight = gs.inFlight
		snap.GroupInLimit = gc.InFlight
	}

	// 1) 客户端突发
	if snap.ClientBurstUsed+cost > cfg.Burst {
		return snap, nil, denyErr(ReasonBurstExceeded, snap,
			"client burst quota exhausted: cost=%d would raise used %d over limit %d within %s",
			cost, snap.ClientBurstUsed, cfg.Burst, cfg.BurstWindow())
	}
	// 2) 分组突发（仅对显式配置的分组生效）
	if groupEnforced && snap.GroupBurstUsed+cost > gc.Burst {
		return snap, nil, denyErr(ReasonGroupBurstExceeded, snap,
			"group burst quota exhausted: cost=%d would raise group %s used %d over limit %d within %s",
			cost, groupDisplayName(group), snap.GroupBurstUsed, gc.Burst, snap.GroupBurstWindow)
	}
	// 3) 客户端速率
	if snap.ClientRateUsed+cost > cfg.RatePerSec {
		return snap, nil, denyErr(ReasonRateExceeded, snap,
			"client rate quota exhausted: cost=%d would raise used %d over limit %d within %s",
			cost, snap.ClientRateUsed, cfg.RatePerSec, RateWindow)
	}
	// 4) 分组速率（仅对显式配置的分组生效）
	if groupEnforced && snap.GroupRateUsed+cost > gc.RatePerSec {
		return snap, nil, denyErr(ReasonGroupRateExceeded, snap,
			"group rate quota exhausted: cost=%d would raise group %s used %d over limit %d within %s",
			cost, groupDisplayName(group), snap.GroupRateUsed, gc.RatePerSec, RateWindow)
	}
	// 5) 分组在途（仅对显式配置的分组生效）
	if groupEnforced && gs.inFlight+cost > gc.InFlight {
		return snap, nil, denyErr(ReasonGroupInFlightExceed, snap,
			"group in-flight cap reached: cost=%d would raise group %s in-flight %d over cap %d",
			cost, groupDisplayName(group), gs.inFlight, gc.InFlight)
	}
	// 6) 全局在途：所有请求共享的总闸，无分组归属的请求打满时如实报全局层。
	if l.globalInFlight+cost > cfg.GlobalInFlight {
		return snap, nil, denyErr(ReasonGlobalExceeded, snap,
			"global in-flight cap reached: cost=%d would raise global in-flight %d over cap %d",
			cost, l.globalInFlight, cfg.GlobalInFlight)
	}

	// 全部通过：一次性记账。窗口追加同一时间戳；在途层按本次实际生效的层次
	// 增加（无分组归属时不加任何分组计数器）。
	e := costEntry{at: now, cost: cost}
	cs.entries = append(cs.entries, e)
	cs.inFlight += cost
	if groupEnforced {
		gs.entries = append(gs.entries, e)
		gs.inFlight += cost
	}
	l.globalInFlight += cost

	snap.ClientBurstUsed += cost
	snap.ClientRateUsed += cost
	snap.ClientInFlight += cost
	snap.GlobalUsed += cost
	if groupEnforced {
		snap.GroupBurstUsed += cost
		snap.GroupRateUsed += cost
		snap.GroupInFlight += cost
	}

	adm := &Admission{
		snap:          snap,
		cost:          cost,
		client:        client,
		group:         group,
		groupEnforced: groupEnforced,
		cs:            cs,
		gs:            gs,
		l:             l,
	}
	return snap, adm, nil
}

// Admission 是一次成功受理的计费句柄。Release 必须且只能生效一次。
type Admission struct {
	snap   Snapshot
	cost   int
	client string
	group  string
	// groupEnforced 在受理时刻固化：归还时只扣当时真正记账的层次。
	groupEnforced bool
	// cs/gs 是受理时刻命中的状态对象指针。即使之后 Reload 改了归属、
	// 或 map 清扫删除了对应键，归还仍精确作用到被记账的计数器上，
	// 不会错记到新归属、也不会漏还。
	cs *actorState
	gs *actorState

	once sync.Once
	l    *Limiter
}

// Snapshot 返回受理时刻的额度视图（受理后口径，含本次成本）。
func (a *Admission) Snapshot() Snapshot { return a.snap }

// Release 归还本次请求占用的在途加权额度；重复调用安全且只生效一次。
// 注意不删除窗口条目——窗口消耗是“准入即记账”，与请求何时结束无关。
func (a *Admission) Release() {
	a.once.Do(func() {
		a.l.mu.Lock()
		defer a.l.mu.Unlock()

		a.l.globalInFlight -= a.cost
		a.cs.inFlight -= a.cost
		if a.groupEnforced {
			a.gs.inFlight -= a.cost
		}

		// 顺手清扫当前时刻已完全空闲且窗口已滑空的 actor，避免 map 无界增长。
		now := a.l.clock()
		a.l.sweepLocked(now)
	})
}

// sweepLocked 删除在途为 0 且窗口内已无条目的客户端/分组状态（调用方持锁）。
func (l *Limiter) sweepLocked(now time.Time) {
	cutoff := now.Add(-RateWindow)
	for name, cs := range l.clients {
		cs.prune(cutoff)
		if cs.inFlight == 0 && len(cs.entries) == 0 {
			delete(l.clients, name)
		}
	}
	for name, gs := range l.groups {
		gs.prune(cutoff)
		if gs.inFlight == 0 && len(gs.entries) == 0 {
			delete(l.groups, name)
		}
	}
}

// GlobalInFlight 返回当前全局在途加权量。
func (l *Limiter) GlobalInFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.globalInFlight
}

// GroupInFlight 返回指定分组当前在途加权量。
func (l *Limiter) GroupInFlight(group string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gs := l.groups[group]; gs != nil {
		return gs.inFlight
	}
	return 0
}

// ActiveClients 返回仍有窗口记录或在途请求的客户端数量。
func (l *Limiter) ActiveClients() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.clients)
}

// ActiveGroups 返回仍有窗口记录或在途请求的分组数量。
func (l *Limiter) ActiveGroups() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.groups)
}
