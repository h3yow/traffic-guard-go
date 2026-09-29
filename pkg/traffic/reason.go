package traffic

import "fmt"

// DenyReason 归一化的拒绝/失败原因。
type DenyReason string

const (
	ReasonNone           DenyReason = ""
	ReasonBurstExceeded  DenyReason = "burst_exceeded"        // 客户端突发窗成本耗尽
	ReasonRateExceeded   DenyReason = "rate_exceeded"         // 客户端速率窗累计成本超上限
	ReasonGlobalExceeded DenyReason = "global_exceeded"       // 全局在途成本总量达上限
	ReasonGroupBurst     DenyReason = "group_burst_exceeded"  // 分组突发窗成本耗尽
	ReasonGroupRate      DenyReason = "group_rate_exceeded"   // 分组速率窗累计成本超上限
	ReasonGroupInFlight  DenyReason = "group_in_flight_limit" // 分组在途成本总量达上限
	ReasonClosing        DenyReason = "closing"               // 进程收尾中，不新开请求
	ReasonCircuitOpen    DenyReason = "circuit_open"          // 熔断打开，快速失败
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
