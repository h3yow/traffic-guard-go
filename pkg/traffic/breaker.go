package traffic

import (
	"fmt"
	"sync"
	"time"
)

// BreakerState 熔断器状态。
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half_open"
)

// BreakerSnapshot 熔断判定时刻的状态视图（用于日志可解释性）。
type BreakerSnapshot struct {
	State            BreakerState
	ConsecutiveFail  int
	FailureThreshold int
	OpenedAt         time.Time
	Now              time.Time
	CooldownLeft     time.Duration
	ProbeInFlight    bool
}

func (s BreakerSnapshot) explain() string {
	switch s.State {
	case BreakerOpen:
		return fmt.Sprintf("breaker=open fails=%d/%d cooldown_left=%s",
			s.ConsecutiveFail, s.FailureThreshold, s.CooldownLeft)
	case BreakerHalfOpen:
		return fmt.Sprintf("breaker=half_open probe_in_flight=%t fails=%d/%d",
			s.ProbeInFlight, s.ConsecutiveFail, s.FailureThreshold)
	default:
		return fmt.Sprintf("breaker=closed consecutive_fail=%d/%d",
			s.ConsecutiveFail, s.FailureThreshold)
	}
}

// Breaker 上游熔断器。规则：
//   - closed：正常放行；连续失败（超时/连接失败/业务错误均算故障）
//     达到阈值立即 open；任何一次成功都把连续失败清零；
//   - open：全部快速失败（circuit_open），冷却期过后下一个请求成为唯一探测；
//   - half_open：同一时刻只放一个探测请求；探测成功 -> closed 恢复放行，
//     探测失败 -> 立即重新 open 并重置冷却。
type Breaker struct {
	threshold int
	cooldown  time.Duration
	clock     Clock

	mu            sync.Mutex
	state         BreakerState
	fails         int
	openedAt      time.Time
	probeInFlight bool
}

// NewBreaker 创建熔断器。
// 向后兼容：新增配置项缺失时由调用方传入缺省阈值/冷却，构造本身不依赖配置文件版本。
func NewBreaker(threshold int, cooldown time.Duration, clock Clock) *Breaker {
	if clock == nil {
		clock = SystemClock
	}
	return &Breaker{
		threshold: threshold,
		cooldown:  cooldown,
		clock:     clock,
		state:     BreakerClosed,
	}
}

// Allow 是否允许放行（closed）或探测（half-open，同一时刻仅一个探测）。
// open 且未过冷却期、或 half_open 中探测尚未返回时，返回 (snap, false)。
func (b *Breaker) Allow() (BreakerSnapshot, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock()
	switch b.state {
	case BreakerClosed:
		return b.snapshotLocked(now), true

	case BreakerOpen:
		left := b.openedAt.Add(b.cooldown).Sub(now)
		if left > 0 {
			return b.snapshotLocked(now), false
		}
		// 冷却结束：放行唯一探测。
		b.state = BreakerHalfOpen
		b.probeInFlight = true
		return b.snapshotLocked(now), true

	case BreakerHalfOpen:
		// 探测结果未返回前，其余请求一律快速失败。
		return b.snapshotLocked(now), !b.probeInFlight
	}
	return b.snapshotLocked(now), false
}

// OnResult 依据归一化结论更新状态，返回迁移后的状态。
func (b *Breaker) OnResult(class UpstreamClass) BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock()
	ok := class == UpstreamSuccess

	switch b.state {
	case BreakerHalfOpen:
		b.probeInFlight = false
		if ok {
			b.state = BreakerClosed
			b.fails = 0
		} else {
			b.toOpenLocked(now)
		}
	case BreakerClosed:
		if ok {
			b.fails = 0
		} else {
			b.fails++
			if b.fails >= b.threshold {
				b.toOpenLocked(now)
			}
		}
	case BreakerOpen:
		// 正常路径下 open 时不会有请求到达上游；防御性保持打开。
		if !ok {
			b.fails++
		}
	}
	return b.state
}

// toOpenLocked 打开熔断并重置冷却计时（调用方持锁）。
func (b *Breaker) toOpenLocked(now time.Time) {
	b.state = BreakerOpen
	b.openedAt = now
	b.probeInFlight = false
}

// State 返回当前状态（不触发状态迁移）。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Snapshot 返回当前状态视图。
func (b *Breaker) Snapshot() BreakerSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked(b.clock())
}

func (b *Breaker) snapshotLocked(now time.Time) BreakerSnapshot {
	left := time.Duration(0)
	if b.state == BreakerOpen {
		left = b.openedAt.Add(b.cooldown).Sub(now)
		if left < 0 {
			left = 0
		}
	}
	return BreakerSnapshot{
		State:            b.state,
		ConsecutiveFail:  b.fails,
		FailureThreshold: b.threshold,
		OpenedAt:         b.openedAt,
		Now:              now,
		CooldownLeft:     left,
		ProbeInFlight:    b.probeInFlight,
	}
}
