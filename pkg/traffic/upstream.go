package traffic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// UpstreamRequest 一次上游调用的输入。
type UpstreamRequest struct {
	Client   string
	Endpoint string
	ID       string
}

// Outcome 归一化后的上游调用结论（绝不透出原始错误）。
type Outcome struct {
	Class   UpstreamClass
	Latency time.Duration
	Detail  string
}

// Upstream 上游接口。
type Upstream interface {
	Call(ctx context.Context, req UpstreamRequest) error
}

// 上游错误哨兵：组件内部与测试桩使用，外部只应看到归一化结论。
var (
	ErrUpstreamTimeout  = errors.New("upstream timeout")
	ErrUpstreamConnFail = errors.New("upstream connection failure")
	ErrUpstreamBusiness = errors.New("upstream business error")
)

// BusinessError 携带业务状态码的业务错误（测试桩/HTTP 上游使用）。
type BusinessError struct {
	Code int
	Msg  string
}

func (e *BusinessError) Error() string        { return fmt.Sprintf("business error %d: %s", e.Code, e.Msg) }
func (e *BusinessError) Is(target error) bool { return target == ErrUpstreamBusiness }

// ClassifyError 把任意上游错误归一化为可区分的结论类别：
//   - nil                                   -> success
//   - 超时（context 截止 / net.Error 超时） -> timeout
//   - 连接失败（*net.OpError/*url.Error 等）-> conn_failure
//   - 业务错误（*BusinessError/业务哨兵）   -> business_err
//   - 其余未知错误                          -> business_err（Detail 保留原始信息）
func ClassifyError(err error) UpstreamClass {
	switch {
	case err == nil:
		return UpstreamSuccess
	case IsTimeout(err):
		return UpstreamTimeout
	case IsConnFailure(err):
		return UpstreamConnFailure
	default:
		return UpstreamBusiness
	}
}

// IsTimeout 判断错误是否为超时类。
func IsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrUpstreamTimeout) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
}

// IsConnFailure 判断错误是否为连接失败类（非超时的传输层错误）。
func IsConnFailure(err error) bool {
	if errors.Is(err, ErrUpstreamConnFail) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return !ne.Timeout() // 超时的 net.Error 已在 IsTimeout 中归一
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// CallOutcome 执行上游调用并把结果归一化为 Outcome，延迟一并记录以便日志解释。
// 调用方负责为 ctx 设置超时；这里不吞掉取消语义之外的任何错误。
func CallOutcome(ctx context.Context, up Upstream, req UpstreamRequest) Outcome {
	start := time.Now()
	err := up.Call(ctx, req)
	return Outcome{
		Class:   ClassifyError(err),
		Latency: time.Since(start),
		Detail:  detailOf(err),
	}
}

func detailOf(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

// ScriptedUpstream 可编排上游：按调用顺序返回预设结论，供测试独立复现。
// 脚本耗尽后重复最后一个结论；Delay>0 时尊重 ctx 截止时间，
// 到时返回 ErrUpstreamTimeout，用于复现真实超时。
type ScriptedUpstream struct {
	mu     sync.Mutex
	script []error
	next   int
	delay  time.Duration
	calls  int
}

// NewScriptedUpstream 创建可编排上游。
func NewScriptedUpstream(delay time.Duration, outcomes ...error) *ScriptedUpstream {
	return &ScriptedUpstream{script: outcomes, delay: delay}
}

// Call implements Upstream.
func (f *ScriptedUpstream) Call(ctx context.Context, req UpstreamRequest) error {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrUpstreamTimeout, ctx.Err())
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.script) == 0 {
		return nil
	}
	idx := f.next
	if idx >= len(f.script) {
		idx = len(f.script) - 1
	}
	f.next++
	return f.script[idx]
}

// Calls 返回已发生的调用次数。
func (f *ScriptedUpstream) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// HTTPUpstream 把请求转发给真实 HTTP 上游并归一化传输/响应错误。
type HTTPUpstream struct {
	BaseURL string
	Client  *http.Client
}

// NewHTTPUpstream 创建 HTTP 上游，timeout 为整体调用超时。
func NewHTTPUpstream(baseURL string, timeout time.Duration) *HTTPUpstream {
	return &HTTPUpstream{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Client:  &http.Client{Timeout: timeout},
	}
}

// Call implements Upstream：2xx 为成功；得到响应但非 2xx 为业务错误；
// 传输层失败按超时/连接失败归一化。
func (h *HTTPUpstream) Call(ctx context.Context, req UpstreamRequest) error {
	url := h.BaseURL + "/?req_id=" + req.ID + "&client=" + req.Client
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUpstreamConnFail, err)
	}
	resp, err := h.Client.Do(httpReq)
	if err != nil {
		if IsTimeout(err) {
			return fmt.Errorf("%w: %v", ErrUpstreamTimeout, err)
		}
		return fmt.Errorf("%w: %v", ErrUpstreamConnFail, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &BusinessError{Code: resp.StatusCode, Msg: resp.Status}
	}
	return nil
}
