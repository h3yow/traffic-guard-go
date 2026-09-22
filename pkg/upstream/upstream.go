// Package upstream normalizes heterogeneous upstream errors into a small,
// distinguishable set of conclusion kinds instead of letting raw errors
// pass through.
package upstream

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
)

// Kind is the normalized failure classification.
type Kind string

const (
	KindTimeout     Kind = "upstream_timeout"
	KindConnFailure Kind = "connection_failure"
	KindBusiness    Kind = "business_error"
	// KindCanceled is NOT an upstream fault: the caller (or process
	// shutdown) abandoned the request before an answer. It must never be
	// billed to the breaker as an upstream failure.
	KindCanceled Kind = "request_canceled"
)

// Failure is a normalized upstream error; successful calls yield nil.
type Failure struct {
	Kind Kind
	Msg  string
}

func (e *Failure) Error() string { return string(e.Kind) + ": " + e.Msg }

// TimeoutError marks an upstream call that exceeded its deadline.
type TimeoutError struct{ Detail string }

func (e *TimeoutError) Error() string { return "upstream timeout: " + e.Detail }
func (e *TimeoutError) Timeout() bool { return true }

// ConnError marks an upstream connection/dial failure.
type ConnError struct{ Detail string }

func (e *ConnError) Error() string { return "upstream connection failure: " + e.Detail }

// BusinessError marks an upstream that answered with a business-level failure.
type BusinessError struct {
	Code int
	Msg  string
}

func (e *BusinessError) Error() string { return "upstream business error: " + e.Msg }

// Func is an upstream invocation.
type Func func(ctx context.Context) error

// Classify maps a raw upstream error to *Failure (nil when err==nil).
//
// Precedence: caller cancellation -> KindCanceled (not an upstream fault);
// timeout/deadline -> KindTimeout; dial/connection errors ->
// KindConnectionFailure; everything else -> KindBusiness. Raw errors never
// escape callers: the returned *Failure carries Kind + message.
func Classify(err error) *Failure {
	if err == nil {
		return nil
	}
	if IsCanceled(err) {
		return &Failure{Kind: KindCanceled, Msg: err.Error()}
	}
	if IsTimeout(err) {
		return &Failure{Kind: KindTimeout, Msg: err.Error()}
	}
	if IsConnFailure(err) {
		return &Failure{Kind: KindConnFailure, Msg: err.Error()}
	}
	return &Failure{Kind: KindBusiness, Msg: err.Error()}
}

// IsCanceled reports that the caller gave up (context.Canceled), e.g. client
// disconnect or process shutdown. It is distinct from a timeout: no upstream
// fault can be inferred, so the breaker ignores it.
func IsCanceled(err error) bool {
	return err != nil && errors.Is(err, context.Canceled)
}

// IsTimeout reports whether err represents a timeout or deadline exceeded.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var te interface{ Timeout() bool }
	if errors.As(err, &te) && te.Timeout() {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// Defensive: some libraries return unstructured deadline text.
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "timeout") || strings.Contains(s, "deadline exceeded")
}

// IsConnFailure reports a dial/connection-level failure. Recognized via the
// net.OpError/DNS dial error types plus common text markers; a connection
// that was established and then closed (io.EOF) is classified as business,
// because the upstream did answer at the transport level.
func IsConnFailure(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "connection refused"),
		strings.Contains(s, "connection reset"),
		strings.Contains(s, "dial "),
		strings.Contains(s, "no such host"),
		strings.Contains(s, "connection failure"),
		strings.Contains(s, "broken pipe"):
		return true
	}
	return false
}
