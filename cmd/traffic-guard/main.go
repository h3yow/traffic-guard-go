// Command traffic-guard 是一个本地可运行的入口流量防护示例：
// 在 HTTP 入口上施加 客户端突发/速率 + 全局总量 限流、
// 上游错误归一化与熔断，并在收到停止信号后按确定顺序优雅收尾。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/h3yow/traffic-guard-go/pkg/traffic"
)

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Printf(format string, args ...any) {
	s.l.Info(fmt.Sprintf(format, args...))
}

func main() {
	var (
		configPath = flag.String("config", "", "JSON 配置文件路径（缺省使用内置默认配置）")
		listen     = flag.String("listen", ":8080", "HTTP 监听地址")
		upstream   = flag.String("upstream", "", "上游 BaseURL（缺省使用内置模拟上游）")
		shutdownTO = flag.Duration("shutdown-timeout", 10*time.Second, "优雅收尾最长等待")
	)
	flag.Parse()

	logger := slogLogger{slog.New(slog.NewTextHandler(os.Stdout, nil))}

	cfg := traffic.DefaultConfig()
	if *configPath != "" {
		c, err := traffic.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		cfg = c
	}

	var up traffic.Upstream
	if *upstream != "" {
		up = traffic.NewHTTPUpstream(*upstream, cfg.UpstreamTimeout())
	} else {
		// 本地演示：90% 成功，偶发业务错误/超时，便于观察熔断。
		up = traffic.NewScriptedUpstream(0,
			nil, nil, nil, nil, nil, nil, nil, nil,
			&traffic.BusinessError{Code: 503, Msg: "simulated"},
			fmt.Errorf("%w: slow", traffic.ErrUpstreamTimeout),
		)
	}

	guard, err := traffic.New(cfg, up, logger)
	if err != nil {
		log.Fatalf("init guard: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		client := r.Header.Get("X-Client-ID")
		if client == "" {
			client = r.RemoteAddr
		}
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		res := guard.Serve(r.Context(), client, id)
		switch {
		case res.Reason == traffic.ReasonClosing:
			w.Header().Set("X-Deny-Reason", string(res.Reason))
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		case res.Reason == traffic.ReasonCircuitOpen:
			w.Header().Set("X-Deny-Reason", string(res.Reason))
			http.Error(w, "circuit open", http.StatusServiceUnavailable)
		case res.Reason == traffic.ReasonBurstExceeded ||
			res.Reason == traffic.ReasonRateExceeded ||
			res.Reason == traffic.ReasonGlobalExceeded:
			w.Header().Set("X-Deny-Reason", string(res.Reason))
			http.Error(w, "rate limited: "+string(res.Reason), http.StatusTooManyRequests)
		case res.Upstream != traffic.UpstreamSuccess:
			w.Header().Set("X-Upstream-Class", string(res.Upstream))
			http.Error(w, "upstream failed: "+string(res.Upstream), http.StatusBadGateway)
		default:
			fmt.Fprintln(w, "ok")
		}
	})

	srv := &http.Server{Addr: *listen, Handler: mux}

	// 信号 -> 优雅收尾，顺序确定：
	// 停止接收新连接 -> guard 拒绝新请求并等待在途完成 -> 关闭 HTTP server。
	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		logger.l.Info("traffic-guard listening", "addr", *listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("http server: %v", err)
		}
	case <-stopCtx.Done():
		logger.l.Info("stop signal received, beginning graceful shutdown")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTO)
		defer cancel()

		// 先让 guard 进入收尾：新请求得到 503(closing)，在途请求继续跑完。
		if err := guard.Shutdown(shutdownCtx); err != nil {
			logger.l.Error("guard shutdown timed out", "err", err)
		}
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.l.Error("http server shutdown error", "err", err)
		}
		logger.l.Info("shutdown complete", "in_flight", guard.GlobalInFlight())
	}
}
