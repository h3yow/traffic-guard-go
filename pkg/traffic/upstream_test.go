package traffic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

type tlog struct{ t *testing.T }

func (l tlog) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
}

func TestClassifyError(t *testing.T) {
	timeoutNetErr := &net.OpError{Op: "read", Net: "tcp",
		Err: &timeoutErr{}}
	cases := []struct {
		name string
		err  error
		want UpstreamClass
	}{
		{"nil", nil, UpstreamSuccess},
		{"sentinel timeout", fmt.Errorf("%w: x", ErrUpstreamTimeout), UpstreamTimeout},
		{"context deadline", context.DeadlineExceeded, UpstreamTimeout},
		{"wrapped deadline", fmt.Errorf("call: %w", context.DeadlineExceeded), UpstreamTimeout},
		{"net timeout", timeoutNetErr, UpstreamTimeout},
		{"sentinel conn", fmt.Errorf("%w: x", ErrUpstreamConnFail), UpstreamConnFailure},
		{"op error refused", &net.OpError{Op: "dial", Net: "tcp",
			Err: errors.New("connection refused")}, UpstreamConnFailure},
		{"business sentinel", fmt.Errorf("%w: x", ErrUpstreamBusiness), UpstreamBusiness},
		{"business typed", &BusinessError{Code: 503, Msg: "busy"}, UpstreamBusiness},
		{"unknown normalized", errors.New("something weird"), UpstreamBusiness},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyError(tc.err); got != tc.want {
				t.Fatalf("ClassifyError(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

type timeoutErr struct{}

func (*timeoutErr) Error() string   { return "i/o timeout" }
func (*timeoutErr) Timeout() bool   { return true }
func (*timeoutErr) Temporary() bool { return true }

func TestScriptedUpstreamTimeoutHonorsContext(t *testing.T) {
	up := NewScriptedUpstream(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := up.Call(ctx, UpstreamRequest{Client: "c", ID: "1"})
	if !IsTimeout(err) {
		t.Fatalf("expected timeout classification, got %v", err)
	}
	if ClassifyError(err) != UpstreamTimeout {
		t.Fatalf("expected upstream timeout class")
	}
}

func TestDenyErrorReasonMatching(t *testing.T) {
	err := &DenyError{Reason: ReasonRateExceeded, Detail: "x"}
	if !errors.Is(err, &DenyError{Reason: ReasonRateExceeded}) {
		t.Fatal("errors.Is should match by reason")
	}
	if errors.Is(err, &DenyError{Reason: ReasonBurstExceeded}) {
		t.Fatal("different reasons must not match")
	}
}
