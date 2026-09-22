package upstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Kind
	}{
		{"nil", nil, ""},
		{"deadline", context.DeadlineExceeded, KindTimeout},
		{"cancel", context.Canceled, KindCanceled},
		{"timeout type", &TimeoutError{Detail: "slow"}, KindTimeout},
		{"net timeout", &net.OpError{Err: timeoutErr{}}, KindTimeout},
		{"conn refused text", fmt.Errorf("dial tcp: connection refused"), KindConnFailure},
		{"conn type", &ConnError{Detail: "dial tcp 10.0.0.1:80: connect: no route"}, KindConnFailure},
		{"dns", &net.DNSError{Err: "no such host", IsNotFound: true}, KindConnFailure},
		{"business", &BusinessError{Code: 500, Msg: "internal"}, KindBusiness},
		{"plain", errors.New("unexpected payload"), KindBusiness},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := Classify(tc.err)
			if tc.want == "" {
				if f != nil {
					t.Fatalf("nil err must classify to nil, got %+v", f)
				}
				t.Logf("input=<nil> => normalized=<nil>")
				return
			}
			if f == nil || f.Kind != tc.want {
				t.Fatalf("input=%v => got %+v, want kind %q", tc.err, f, tc.want)
			}
			t.Logf("input=%-60q => normalized kind=%q msg=%q", tc.err.Error(), f.Kind, f.Msg)
		})
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

func TestClassifyWrapsNotPassthrough(t *testing.T) {
	// Callers must see a *Failure, not the raw error type.
	f := Classify(&BusinessError{Code: 418, Msg: "teapot"})
	var raw *BusinessError
	if errors.As(f, &raw) {
		t.Fatal("raw upstream error must not pass through unchanged")
	}
	if f.Kind != KindBusiness {
		t.Fatalf("want business, got %s", f.Kind)
	}

	// Sanity: a real dial to a closed port is a connection failure.
	_, err := net.DialTimeout("tcp", "127.0.0.1:1", 100*time.Millisecond)
	if err != nil {
		got := Classify(err)
		t.Logf("real dial error=%q => kind=%q", err.Error(), got.Kind)
		if got.Kind != KindConnFailure {
			t.Fatalf("closed port dial must be conn failure, got %s", got.Kind)
		}
	}
}
