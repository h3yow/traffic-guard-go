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

// isQuotaDeny 判定是否为任一额度层（客户端/分组/全局）的拒绝结论。
func isQuotaDeny(r traffic.DenyReason) bool {
	switch r {
	case traffic.ReasonBurstExceeded,
		traffic.ReasonRateExceeded,
		traffic.ReasonGlobalExceeded,
		traffic.ReasonGroupBurstExceeded,
		traffic.ReasonGroupRateExceeded,
		traffic.ReasonGroupInFlightExceed:
		return true
	}
	return false
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
		// 入口名默认取 URL 路径，可由 X-Endpoint 显式声明以匹配成本表；
		// 未在 endpoint_costs 中声明的入口一律按成本 1 计费。
		endpoint := r.Header.Get("X-Endpoint")
		if endpoint == "" {
			endpoint = r.URL.Path
		}
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		res := guard.Serve(r.Context(), client, endpoint, id)
		switch {
		case res.Reason == traffic.ReasonClosing:
			w.Header().Set("X-Deny-Reason", string(res.Reason))
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		case res.Reason == traffic.ReasonCircuitOpen:
			w.Header().Set("X-Deny-Reason", string(res.Reason))
			http.Error(w, "circuit open", http.StatusServiceUnavailable)
		case isQuotaDeny(res.Reason):
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

	// reloadFromFile 重新读取并校验配置文件：
	// 合法 -> 不断服务、不重启进程地让新额度生效；
	// 非法 -> 返回错误，guard 继续沿用旧配置（已在途请求不受影响）。
	reloadFromFile := func(reason string) error {
		if *configPath == "" {
			return fmt.Errorf("no -config file given; runtime reload requires a config path")
		}
		newCfg, err := traffic.LoadConfig(*configPath)
		if err != nil {
			logger.l.Error("config reload rejected, keeping old config",
				"reason", reason, "path", *configPath, "err", err)
			return err
		}
		if err := guard.Reload(newCfg); err != nil {
			logger.l.Error("config reload rejected, keeping old config",
				"reason", reason, "path", *configPath, "err", err)
			return err
		}
		logger.l.Info("config reloaded and applied",
			"reason", reason, "path", *configPath,
			"global_in_flight", newCfg.GlobalInFlight,
			"rate_per_sec", newCfg.RatePerSec, "burst", newCfg.Burst)
		return nil
	}

	// 管理入口：POST /admin/reload 从配置文件热加载，便于脚本化验证
	// （成功 200；读不到/校验失败 400，线上额度保持不变）。
	mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := reloadFromFile("http"); err != nil {
			http.Error(w, "reload rejected, old config still active: "+err.Error(),
				http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "reloaded")
	})

	// SIGHUP -> 热加载配置文件；SIGINT/SIGTERM -> 优雅收尾。
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for range hupCh {
			_ = reloadFromFile("sighup")
		}
	}()

	// 信号 -> 优雅收尾，顺序确定：
	// 停止接收新连接 -> guard 拒绝新请求并等待在途完成 -> 关闭 HTTP server。
	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	defer signal.Stop(hupCh)

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
