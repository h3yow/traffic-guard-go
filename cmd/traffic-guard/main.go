// Command traffic-guard runs a local HTTP server that fronts an upstream
// with the entry-traffic protection component.
//
// Usage:
//
//	go run ./cmd/traffic-guard -config configs/example.json -addr :8080
//
// Every request to GET /guard?client=<id> is protected. Add request header
// X-Upstream-Mode: ok|timeout|conn|biz|slow to script the simulated upstream
// (default ok), which lets you reproduce rate limiting and circuit breaking
// by hand. SIGINT/SIGTERM triggers deterministic graceful shutdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/h3yow/traffic-guard-go/pkg/config"
	"github.com/h3yow/traffic-guard-go/pkg/guard"
	"github.com/h3yow/traffic-guard-go/pkg/upstream"
)

func main() {
	cfgPath := flag.String("config", "configs/example.json", "path to JSON config")
	addr := flag.String("addr", ":8080", "listen address")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "max wait for in-flight drain")
	flag.Parse()

	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatalf("read config %s: %v", *cfgPath, err)
	}
	cfg, err := config.LoadJSON(data)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("config loaded: rate=%d/s burst=%d global=%d breaker(threshold=%d cooldown=%dms probe=%dms)",
		cfg.RateLimit.RatePerSecond, *cfg.RateLimit.Burst, cfg.RateLimit.GlobalMaxInFlight,
		*cfg.Circuit.FailureThreshold, *cfg.Circuit.OpenCooldownMS, *cfg.Circuit.ProbeTimeoutMS)

	g := guard.NewWithLogger(cfg, log.New(os.Stdout, "", log.LstdFlags))

	mux := http.NewServeMux()
	mux.HandleFunc("/guard", func(w http.ResponseWriter, r *http.Request) {
		client := r.URL.Query().Get("client")
		if client == "" {
			client = "anonymous"
		}
		mode := r.Header.Get("X-Upstream-Mode")
		if mode == "" {
			mode = "ok"
		}
		d := g.Serve(r.Context(), client, simulatedUpstream(mode))
		code := http.StatusOK
		switch d.Outcome {
		case guard.OutcomeOK:
			code = http.StatusOK
		case guard.OutcomeRejectedRate, guard.OutcomeRejectedBurst, guard.OutcomeRejectedGlobal,
			guard.OutcomeShuttingDown:
			code = http.StatusTooManyRequests
		case guard.OutcomeCircuitOpen:
			code = http.StatusServiceUnavailable
		case guard.OutcomeCanceled:
			// Non-standard 499: client closed request (nginx convention).
			code = 499
		case guard.OutcomeUpstreamTimeout, guard.OutcomeUpstreamConn, guard.OutcomeUpstreamBiz:
			code = http.StatusBadGateway
		}
		w.Header().Set("X-Guard-Outcome", string(d.Outcome))
		w.Header().Set("X-Guard-Circuit", string(d.Circuit))
		w.WriteHeader(code)
		fmt.Fprintf(w, "client=%s outcome=%s detail=%s\n", client, d.Outcome, d.Detail)
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		st := g.Stats()
		fmt.Fprintf(w, "in_flight=%d limiter=%+v breaker=%+v outcomes=%v\n",
			st.InFlight, st.Limiter, st.Breaker, st.Outcomes)
	})

	srv := &http.Server{Addr: *addr, Handler: mux}

	// Deterministic wind-down on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("traffic-guard listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case sig := <-sigCh:
		log.Printf("signal %s received: graceful shutdown starting", sig)
	case err := <-serverErr:
		log.Fatalf("http server: %v", err)
	}

	// Order: stop accepting new HTTP connections first...
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	httpErr := srv.Shutdown(shutdownCtx)
	// ...then let the guard reject late requests and drain accepted ones.
	guardErr := g.Shutdown(shutdownCtx)
	if httpErr != nil {
		log.Printf("http shutdown: %v", httpErr)
	}
	if guardErr != nil {
		log.Printf("guard shutdown: %v", guardErr)
		os.Exit(1)
	}
	log.Printf("shutdown complete: %+v", g.Stats().Outcomes)
}

// simulatedUpstream returns an upstream.Func that behaves per mode so the
// protection behavior can be exercised without a real dependency.
func simulatedUpstream(mode string) upstream.Func {
	return func(ctx context.Context) error {
		switch mode {
		case "slow":
			// Bounded 300ms delay to keep a request in flight for burst/
			// global cap tests; still honors an early context cancellation.
			select {
			case <-time.After(300 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case "timeout":
			// Unbounded until the context ends; as a half-open probe the
			// guard's ProbeTimeout turns this into the timeout conclusion.
			<-ctx.Done()
			return ctx.Err()
		case "conn":
			return &upstream.ConnError{Detail: "dial tcp: connection refused (simulated)"}
		case "biz":
			return &upstream.BusinessError{Code: 500, Msg: "simulated business failure"}
		default: // "ok"
			return nil
		}
	}
}
