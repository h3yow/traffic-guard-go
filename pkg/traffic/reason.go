package traffic

import "fmt"

// DenyReason 归一化的拒绝/失败原因。
type DenyReason string

const (
	ReasonNone DenyReason = ""
	// 客户端维度：无论客户端是否归属显式分组，这两层都会判定。
	ReasonBurstExceeded  DenyReason = "burst_exceeded"  // 客户端突发窗加权消耗耗尽
	ReasonRateExceeded   DenyReason = "rate_exceeded"   // 客户端速率窗加权消耗超上限
	ReasonGlobalExceeded DenyReason = "global_exceeded" // 全局在途加权总量达上限（所有客户端共享的总闸）
	// 分组（租户）维度：只有归属“显式配置的分组”的客户端才会产生这三类结论；
	// 未配置任何分组的老客户只受客户端两层 + 全局总闸约束，绝不会被报成这些原因。
	ReasonGroupBurstExceeded  DenyReason = "group_burst_exceeded"    // 分组突发窗加权消耗耗尽
	ReasonGroupRateExceeded   DenyReason = "group_rate_exceeded"     // 分组速率窗加权消耗超上限
	ReasonGroupInFlightExceed DenyReason = "group_inflight_exceeded" // 分组在途加权总量达上限
	// 非额度结论（保留旧名）
	ReasonClosing     DenyReason = "closing"      // 进程收尾中，不新开请求
	ReasonCircuitOpen DenyReason = "circuit_open" // 熔断打开，快速失败
)

// UpstreamClass 归一化的上游结论类别。
type UpstreamClass string

const (
	UpstreamSuccess     UpstreamClass = "success"
	UpstreamTimeout     UpstreamClass = "timeout"      // 上游超时
	UpstreamConnFailure UpstreamClass = "conn_failure" // 连接失败
	UpstreamBusiness    UpstreamClass = "business_err" // 业务错误（有响应但不成功）
)

// DenyError 携带可区分原因的拒绝错误。
type DenyError struct {
	Reason DenyReason
	Detail string
}

func (e *DenyError) Error() string {
	return fmt.Sprintf("traffic denied: %s: %s", e.Reason, e.Detail)
}

// Is 支持 errors.Is 按原因匹配。
func (e *DenyError) Is(target error) bool {
	t, ok := target.(*DenyError)
	if !ok {
		return false
	}
	return t.Reason == e.Reason
}
