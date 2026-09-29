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

// ClientSnapshot 判定时刻的额度视图（用于日志与可解释性）。
// 所有 Used/Limit 字段的口径都是“成本”而非请求个数：成本 1 的请求与旧口径一致。
type ClientSnapshot struct {
	Client      string
	Group       string
	Cost        int
	BurstWindow time.Duration
	BurstUsed   int
	BurstLimit  int
	RateWindow  time.Duration
	RateUsed    int
	RateLimit   int
	InFlight    int
	GlobalUsed  int
	GlobalLimit int
	// 分组层视图（Group == "" 时为空）。
	GroupBurstUsed  int
	GroupBurstLimit int
	GroupRateUsed   int
	GroupRateLimit  int
	GroupInFlight   int
	GroupInFlightL  int
	At              time.Time
}

// Explain 输出判定依据：本次成本、客户端/分组两级窗口的已用成本/上限、
// 客户端、分组与全局的在途成本。
func (s ClientSnapshot) Explain() string {
	group := ""
	if s.Group != "" {
		group = fmt.Sprintf(
			" | group=%s burst_admitted=%d/%d rate_admitted=%d/%d in_flight=%d/%d",
			s.Group, s.GroupBurstUsed, s.GroupBurstLimit,
			s.GroupRateUsed, s.GroupRateLimit,
			s.GroupInFlight, s.GroupInFlightL,
		)
	}
	return fmt.Sprintf(
		"client=%s cost=%d at=%s | burst_window=%s admitted=%d/%d | rate_window=%s admitted=%d/%d | client_in_flight=%d global_in_flight=%d/%d%s",
		s.Client, s.Cost, s.At.Format(time.RFC3339Nano),
		s.BurstWindow, s.BurstUsed, s.BurstLimit,
		s.RateWindow, s.RateUsed, s.RateLimit,
		s.InFlight, s.GlobalUsed, s.GlobalLimit, group,
	)
}

// admittedCost 是一次受理的记账条目：准入时间戳与该请求消耗的成本。
type admittedCost struct {
	at   time.Time
	cost int
}

// quotaState 一个核算主体（客户端或分组）的状态。
//
// 计费口径（关键）：
//   - admitted 是“准入时间戳 + 成本”序列，只在滑出速率窗时被裁剪，
//     绝不因请求结束而删除。因此任意 1s / burst 窗口内的累计放行成本都被
//     精确且只计一次，短请求完成不会重复获得速率额度；
//   - inFlight 是该主体当前尚未结束的已受理请求成本总和，与全局在途名额
//     对应，请求结束（release）时按原成本归还，且只归还一次。
type quotaState struct {
	admitted []admittedCost
	inFlight int
}

// prune 删除不晚于 cutoff 的时间戳（调用方持锁）。
func (qs *quotaState) prune(cutoff time.Time) {
	drop := 0
	for drop < len(qs.admitted) && !qs.admitted[drop].at.After(cutoff) {
		drop++
	}
	if drop > 0 {
		qs.admitted = append(qs.admitted[:0], qs.admitted[drop:]...)
	}
}

// burstCost 统计晚于 cutoff 的条目的成本总和（调用方持锁）。
func (qs *quotaState) burstCost(cutoff time.Time) int {
	used := 0
	for _, a := range qs.admitted {
		if a.at.After(cutoff) {
			used += a.cost
		}
	}
	return used
}

// rateCost 是当前序列内全部条目的成本总和（序列已按速率窗裁剪，调用方持锁）。
func (qs *quotaState) rateCost() int {
	used := 0
	for _, a := range qs.admitted {
		used += a.cost
	}
	return used
}

// Limiter 负责客户端速率/突发额度与全局总量核算（均按成本加权）。
// 所有方法并发安全；窗口成本只在 Admit 成功时追加一次，
// 在途成本在 Release 时按原成本归还，保证不重复计数、不漏计。
type Limiter struct {
	cfg   Config
	clock Clock

	mu             sync.Mutex
	clients        map[string]*quotaState
	groups         map[string]*quotaState
	globalInFlight int

	// afterAdmit 仅测试用：记账后、释放锁前回调（受理时刻配置快照, 受理后全局在途成本）。
	afterAdmit func(cfg Config, globalInFlight int)
}

// NewLimiter 创建限流器。
func NewLimiter(cfg Config, clock Clock) *Limiter {
	if clock == nil {
		clock = SystemClock
	}
	return &Limiter{
		cfg:     cfg,
		clock:   clock,
		clients: make(map[string]*quotaState),
		groups:  make(map[string]*quotaState),
	}
}

// deny 在快照上附带原因构造拒绝错误（拒绝路径不产生任何计数变更）。
func deny(reason DenyReason, snap ClientSnapshot, format string, args ...any) error {
	return &DenyError{
		Reason: reason,
		Detail: fmt.Sprintf(format, args...) + " | " + snap.Explain(),
	}
}

// Admit 按缺省成本 1 为 client 核算额度（不经过分组层）。
// 保留它是为了让“无成本/无分组”的旧调用方与旧用例口径不变。
func (l *Limiter) Admit(client string) (ClientSnapshot, func(), error) {
	return l.AdmitCost(Request{Client: client})
}

// Request 一次准入请求的身份与成本声明。
//
//	Cost     显式成本；<=0 时按 Endpoint 查配置，未声明即缺省 1。
//	Endpoint 入口标识，用于查 EndpointCosts；Cost 显式给出时忽略。
//	Group    显式分组；为空时按 ClientGroups[Client] 解析，
//	         解析不到则不经过分组层。
type Request struct {
	Client   string
	Endpoint string
	Cost     int
	Group    string
}

// AdmitCost 尝试为 req 占用成本额度。判定顺序固定为：
// 客户端突发 -> 客户端速率 ->（分组突发 -> 分组速率 -> 分组在途）-> 全局在途。
// 成本比任一层剩余额度大的请求整体拒绝，不产生任何部分占用；
// 成功返回的 release 按同一成本归还，每个受理请求有且仅有一次。
func (l *Limiter) AdmitCost(req Request) (ClientSnapshot, func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock()
	// 整个判定-记账-自检临界区使用同一配置快照；UpdateConfig 也需要本锁，
	// 因此本次请求要么完整按旧配置、要么完整按新配置，边界确定。
	cfg := l.cfg
	cost := req.Cost
	if cost <= 0 {
		cost = cfg.CostOf(req.Endpoint)
	}
	group := req.Group
	if group == "" {
		group = cfg.GroupOf(req.Client)
	}

	cs, ok := l.clients[req.Client]
	if !ok {
		cs = &quotaState{}
		l.clients[req.Client] = cs
	}
	// 速率窗是最长的一级，只按速率窗裁剪一次过期准入记录；
	// 突发窗是速率窗的子区间，在剩余有序序列上再统计即可。
	cs.prune(now.Add(-RateWindow))
	// 顺手清扫“永不回访”的空闲客户端：无在途且最新一条准入记录已滑出
	// 速率窗的状态可以整体删除，避免客户端基数无界增长（当前客户端除外，
	// 它马上要记账）。admitted 按时间有序，最后一条即最新记录。
	l.sweepIdleLocked(now.Add(-RateWindow), cs)

	burstCutoff := now.Add(-cfg.BurstWindow())
	burstUsed := cs.burstCost(burstCutoff)
	rateUsed := cs.rateCost()

	snap := ClientSnapshot{
		Client:      req.Client,
		Group:       group,
		Cost:        cost,
		BurstWindow: cfg.BurstWindow(),
		BurstUsed:   burstUsed,
		BurstLimit:  cfg.Burst,
		RateWindow:  RateWindow,
		RateUsed:    rateUsed,
		RateLimit:   cfg.RatePerSec,
		GlobalUsed:  l.globalInFlight,
		GlobalLimit: cfg.GlobalInFlight,
		InFlight:    cs.inFlight,
		At:          now,
	}

	// 1) 客户端突发额度：已用 + 本次成本 超过上限即整体拒绝。
	if burstUsed+cost > cfg.Burst {
		return snap, nil, deny(ReasonBurstExceeded, snap,
			"burst cost quota exhausted: %d+cost(%d) > %d within %s",
			burstUsed, cost, cfg.Burst, cfg.BurstWindow())
	}
	// 2) 客户端速率额度
	if rateUsed+cost > cfg.RatePerSec {
		return snap, nil, deny(ReasonRateExceeded, snap,
			"rate cost quota exhausted: %d+cost(%d) > %d within any rolling %s window",
			rateUsed, cost, cfg.RatePerSec, RateWindow)
	}

	// 3) 分组层：组内多个客户端共享独立的突发/速率/在途成本上限。
	// 分组与客户端状态互不隶属：本组触顶只影响本组客户端，
	// 其他分组以及无分组的请求照常进入全局判定。
	var gs *quotaState
	if group != "" {
		gc, known := cfg.Groups[group]
		if known {
			gs, ok = l.groups[group]
			if !ok {
				gs = &quotaState{}
				l.groups[group] = gs
			}
			gs.prune(now.Add(-RateWindow))
			l.sweepIdleGroupsLocked(now.Add(-RateWindow), gs)

			gBurstUsed := gs.burstCost(burstCutoff)
			gRateUsed := gs.rateCost()
			snap.GroupBurstUsed = gBurstUsed
			snap.GroupBurstLimit = gc.Burst
			snap.GroupRateUsed = gRateUsed
			snap.GroupRateLimit = gc.RatePerSec
			snap.GroupInFlight = gs.inFlight
			snap.GroupInFlightL = gc.InFlight

			if gBurstUsed+cost > gc.Burst {
				return snap, nil, deny(ReasonGroupBurst, snap,
					"group %q burst cost exhausted: %d+cost(%d) > %d within %s",
					group, gBurstUsed, cost, gc.Burst, cfg.BurstWindow())
			}
			if gRateUsed+cost > gc.RatePerSec {
				return snap, nil, deny(ReasonGroupRate, snap,
					"group %q rate cost exhausted: %d+cost(%d) > %d within any rolling %s window",
					group, gRateUsed, cost, gc.RatePerSec, RateWindow)
			}
			if gs.inFlight+cost > gc.InFlight {
				return snap, nil, deny(ReasonGroupInFlight, snap,
					"group %q in-flight cost cap reached: %d+cost(%d) > %d",
					group, gs.inFlight, cost, gc.InFlight)
			}
		}
	}

	// 4) 全局在途成本总量
	if l.globalInFlight+cost > cfg.GlobalInFlight {
		return snap, nil, deny(ReasonGlobalExceeded, snap,
			"global in-flight cost cap reached: %d+cost(%d) > %d",
			l.globalInFlight, cost, cfg.GlobalInFlight)
	}

	// 全部通过：一次性记账（窗口与在途使用同一成本）。
	cs.admitted = append(cs.admitted, admittedCost{at: now, cost: cost})
	cs.inFlight += cost
	if gs != nil {
		gs.admitted = append(gs.admitted, admittedCost{at: now, cost: cost})
		gs.inFlight += cost
		snap.GroupBurstUsed += cost
		snap.GroupRateUsed += cost
		snap.GroupInFlight += cost
	}
	l.globalInFlight += cost
	snap.GlobalUsed = l.globalInFlight
	snap.InFlight = cs.inFlight
	snap.BurstUsed += cost
	snap.RateUsed += cost

	// 记账后、释放锁前自检：本次受理使用的就是 cfg 这一版本，
	// UpdateConfig 与本临界区互斥，因此这里绝不会跨版本比较。
	// 仅测试注入（生产为 nil），用于并发换配下证明“受理时刻不超当时上限”。
	if l.afterAdmit != nil {
		l.afterAdmit(cfg, l.globalInFlight)
	}

	client := req.Client
	var once sync.Once
	release := func() {
		once.Do(func() { l.release(client, group, cost) })
	}
	return snap, release, nil
}

// release 归还一个已受理请求的全部成本（客户端/分组在途与全局）。
// 注意：不移除准入时间戳——窗口放行成本是准入即记账的口径，
// 重复调用被 once 拦截，因此绝不会出现负数或重复归还。
func (l *Limiter) release(client, group string, cost int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.globalInFlight -= cost
	if cs, ok := l.clients[client]; ok {
		cs.inFlight -= cost
		// 窗口记录全部滑出且无在途成本的空闲客户端才清理，避免 map 无界增长。
		cs.prune(l.clock().Add(-RateWindow))
		if cs.inFlight == 0 && len(cs.admitted) == 0 {
			delete(l.clients, client)
		}
	}
	if gs, ok := l.groups[group]; ok {
		gs.inFlight -= cost
		gs.prune(l.clock().Add(-RateWindow))
		if gs.inFlight == 0 && len(gs.admitted) == 0 {
			delete(l.groups, group)
		}
	}
}

// sweepIdleGroupsLocked 与客户端清扫同口径：删除无在途成本、且全部准入
// 记录都已滑出速率窗的空闲分组（keep 为当前正在记账的分组）。调用方持锁。
func (l *Limiter) sweepIdleGroupsLocked(cutoff time.Time, keep *quotaState) {
	for key, st := range l.groups {
		if st == keep || st.inFlight > 0 || len(st.admitted) == 0 {
			continue
		}
		if !st.admitted[len(st.admitted)-1].at.After(cutoff) {
			delete(l.groups, key)
		}
	}
}

// sweepIdleLocked 删除没有在途成本、且全部准入记录都已滑出速率窗的空闲
// 客户端（keep 为当前正在记账的客户端，永远保留）。调用方持锁。
func (l *Limiter) sweepIdleLocked(cutoff time.Time, keep *quotaState) {
	for key, st := range l.clients {
		if st == keep || st.inFlight > 0 || len(st.admitted) == 0 {
			continue
		}
		// admitted 单调有序，最新一条不晚于 cutoff 即整段记录都已滑出。
		if !st.admitted[len(st.admitted)-1].at.After(cutoff) {
			delete(l.clients, key)
		}
	}
}

// UpdateConfig 在服务不中断的前提下原子换配。
//
// 边界语义（确定且可复现）：
//   - 换配与准入判定共用同一把锁，因此不存在“看到一半新配置”的判定：
//     换配前进入 AdmitCost 的请求全程按旧配置的上限/成本/分组解析核算；
//     换配后的请求全程按新配置核算；
//   - 已受理请求的 lease 在准入时刻已固化 client/group/cost，
//     仍按原口径归还且只归还一次，与配置版本无关；
//   - 上限只在“准入判定”那一刻被读取，因此缩容不追溯在途/窗口内的
//     既有记账，只约束换配之后的判定；
//   - 成本/分组映射同理：历史窗口条目带着准入时的成本，不受后续改动影响；
//   - 新配置中被移除且已无在途成本的分组，其状态立即清除；
//     仍有在途的分组保留状态直到按原口径结算完毕。
func (l *Limiter) UpdateConfig(cfg Config) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cfg = cfg
	for name, gs := range l.groups {
		if _, ok := cfg.Groups[name]; !ok && gs.inFlight == 0 {
			delete(l.groups, name)
		}
	}
}

// GlobalInFlight 返回当前全局在途成本总量。
func (l *Limiter) GlobalInFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.globalInFlight
}

// ActiveClients 返回当前仍有窗口记录或在途成本的客户端数量。
func (l *Limiter) ActiveClients() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.clients)
}

// ActiveGroups 返回当前仍有窗口记录或在途成本的分组数量。
func (l *Limiter) ActiveGroups() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.groups)
}
