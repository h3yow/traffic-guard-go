# traffic-guard-go

本地可运行的**入口流量防护组件**。所有入口（HTTP handler、测试、任何调用方）经由同一个
`guard.Guard.Serve` 管线，获得一致且可解释的：

- 按客户端维度的速率控制（令牌桶）+ 突发/在途额度（per-client burst）+ 全局总量上限；
- 高并发下无竞态、无重复/漏计的额度核算；
- 上游异常归一化（超时 / 连接失败 / 业务错误 / 调用方取消）与三态熔断恢复；
- 进程停止信号下确定顺序的优雅收尾；
- 每个请求一行判定日志：请求维度输入、额度/窗口状态、熔断状态、判定依据。

无任何第三方依赖，仅用 Go 标准库。

## 目录结构

```
pkg/config     配置解析：缺省语义、向后兼容、非法配置显式报错
pkg/ratelimit  令牌桶 + 客户端突发额度 + 全局在途上限（单锁原子核算）
pkg/upstream   原始上游错误 -> 可区分结论（不原样透出）
pkg/circuit    closed/open/half-open 三态熔断，半开单探测
pkg/guard      组装管线、计费结算、优雅收尾、判定日志
cmd/traffic-guard  本地 HTTP 服务 + 模拟上游 + SIGINT/SIGTERM 收尾
configs/example.json
```

## 快速开始

```bash
go run ./cmd/traffic-guard -config configs/example.json -addr :8080
```

另一个终端：

```bash
# 正常请求
curl -i 'http://127.0.0.1:8080/guard?client=alpha'
# 制造上游业务错误（连续 3 次打开熔断，第 4 次被快速失败）
for i in 1 2 3 4; do curl -H 'X-Upstream-Mode: biz' 'http://127.0.0.1:8080/guard?client=beta'; echo; done
# 超时 / 连接失败 / 300ms 慢请求
curl -H 'X-Upstream-Mode: timeout' 'http://127.0.0.1:8080/guard?client=beta'
curl -H 'X-Upstream-Mode: conn'    'http://127.0.0.1:8080/guard?client=beta'
curl -H 'X-Upstream-Mode: slow'    'http://127.0.0.1:8080/guard?client=beta'
# 计数
curl 'http://127.0.0.1:8080/stats'
# 优雅收尾（在途请求会执行完，新请求被拒绝）
kill -TERM <pid>
```

响应头 `X-Guard-Outcome` 给出机读结论；HTTP 状态码映射：200 ok；429
速率/突发/全局/收尾拒绝；503 熔断快速失败；502 上游三类故障；499 调用方取消。

## 限流与计费口径

每接受一个请求，在**同一把互斥锁内原子地**完成三项记账，任何一项不满足则三项都不记：

| 检查（按此固定优先级） | 条件 | 拒绝结论 |
|---|---|---|
| 速率 | 该客户端令牌桶当前令牌 `< 1` | `rejected_rate_exceeded` |
| 突发 | 该客户端在途数 `>= burst` | `rejected_burst_exceeded` |
| 全局 | 全进程在途数 `>= global_max_in_flight` | `rejected_global_cap` |

- **令牌桶**：容量 = `rate_per_second`（突发的速率语义由 burst 单独表达），按
  `rate_per_second` 令牌/秒连续补充，新建桶初始为满。令牌只在请求**被接受**时扣减 1 个；
  被拒请求不扣令牌。
- **突发额度**：`burst` 是单个客户端允许同时在途的请求数。请求结束（无论成功、上游故障、
  熔断放行后失败、panic、调用方取消）都恰好归还 1 个客户端槽位和 1 个全局槽位。
- **全局总量**：所有客户端共享的并发在途硬上限。
- 固定优先级保证多重超限时原因唯一、确定、可复现。
- 被熔断快速失败的请求：**在熔断检查之后、上游调用之前**立即归还刚预订的额度，
  不执行上游、不产生上游计费；因此响应体/日志里的 `globalInFlight` 对快速失败恒为其占用前值。
- 归还函数幂等（`sync.Once`），重复调用不会产生负数或重复计费。
- 并发安全：所有桶、全局计数、统计计数器只在 `Limiter` 的单把锁下变更，
  `go test -race` 验证 200 goroutine 竞争下全局峰值绝不超过上限、账实相符
  （accepted + 三类 rejected == 总请求数）。

## 上游异常归一化与熔断恢复

### 归一化（原始错误不透出）

`upstream.Classify` 把异构错误收敛为四种可区分结论：

| 结论 kind | 来源 | 是否计为上游故障 |
|---|---|---|
| `upstream_timeout` | `context.DeadlineExceeded`、实现 `Timeout() bool` 的错误、i/o timeout、超时文本 | 是 |
| `connection_failure` | `net.OpError`、`net.DNSError`、connection refused/reset、dial 失败等 | 是 |
| `business_error` | 上游有应答但业务失败，以及其他未归类错误、上游 panic | 是 |
| `request_canceled` | `context.Canceled`（客户端断开、进程收尾主动放弃） | **否** |

调用方取消说明不了上游健康度：请求照常结算归还额度，但熔断器用 `Ignore()` 处理——
closed 下既不增加也不清零连续故障计数；若它恰好是 half-open 的探测请求，
因健康未经验证，保守地重新 open（而不是凭未知结果全量放行）。
上游 panic 被 recover 并归一为 `business_error`，请求不会被静默丢弃。

### 熔断状态机

- `closed`：全部放行；连续失败计数，**成功立即清零**；连续失败达到
  `failure_threshold` 时转 `open`。健康（成功或成功穿插的少量失败）永不误熔断。
- `open`：在 `open_cooldown_ms` 内所有请求快速失败（`circuit_open_fast_fail`），
  不调用上游、不占额度。冷却结束后的第一个请求把状态置为 `half-open`，它是**唯一探测请求**；
  探测在途期间的并发到达一律快速失败。
- `half-open`：探测成功 -> `closed`（计数清零、恢复全量放行）；探测失败 -> 重新
  `open` 并重新计时冷却。探测调用由 guard 附加 `probe_timeout_ms` 超时，超时即探测失败。

## 优雅收尾（确定顺序）

`Shutdown(ctx)`：

1. 在准入锁内置 `shutting=true` 并立即释放锁——此后任何 `Serve` 在第一步即得到
   `rejected_shutting_down`，**绝不新开请求**，且拒绝不预订任何额度；
2. 等待所有**已受理**请求结算（`sync.WaitGroup`；受理登记在解锁前完成，因此不会漏等）；
3. 已受理请求必定走到终态（成功/三类上游故障/取消/panic），其额度恰好归还一次、
   结果恰好计数一次，不会被静默丢弃或错误计费；
4. `ctx` 到期仍有在途请求时返回带剩余数量的错误，绝不假装收尾成功。

`cmd/traffic-guard` 收到 SIGINT/SIGTERM 后先 `http.Server.Shutdown`（停止接收新连接），
再 `Guard.Shutdown`（拒绝迟到请求并排空在途）。

## 配置兼容范围与验证

配置为 JSON 解析，`DisallowUnknownFields` 打开：**新增的未知键直接报错**，
拼错的键不会被静默忽略。

| 键 | 必填 | 缺省 | 非法值 |
|---|---|---|---|
| `rate_limit.rate_per_second` | 是 | — | `<= 0` 报错 |
| `rate_limit.burst` | 否 | = `rate_per_second` | 显式 `<= 0` 报错（不会回退缺省） |
| `rate_limit.global_max_in_flight` | 是 | — | `<= 0` 报错 |
| `circuit.failure_threshold` | 否 | `5` | 显式 `0` 报错 |
| `circuit.open_cooldown_ms` | 否 | `5000` | 显式 `<= 0` 报错 |
| `circuit.probe_timeout_ms` | 否 | `2000` | 显式 `<= 0` 报错 |

兼容规则：**键缺失 = 采用确定缺省；键存在但非法 = 启动失败**。未来新增配置项只要保持
可选 + 缺省，即为向后兼容；旧配置文件可直接加载。

校验入口：`config.LoadJSON`（解析+补默认+`Validate`），非法配置绝不静默忽略。

## 判定日志

每个请求一行，包含输入维度、额度/窗口状态与判定依据，例如：

```
decision t=2026-09-22T17:25:20.652Z client="beta" outcome=circuit_open_fast_fail accepted=false | quota: tokens=2.059 cap=5 clientInFlight=0/2 globalInFlight=0/10 | circuit: state=open consecFailures=3 failKind="" | basis=circuit open: fast-fail, consecutive failures=3
```

## 测试与独立复现

```bash
# 全量单测（含竞态检测）
go test ./... -race -v
# 只看结论
go test ./... -race
```

覆盖场景与对应测试：

| 场景 | 测试 |
|---|---|
| 三类拒绝原因可区分、正常请求不误拒 | `pkg/guard` `TestRejectionReasonsDistinguishable`；`pkg/ratelimit` `TestBurstAndGlobalReasons` |
| 令牌补充/容量上限 | `pkg/ratelimit` `TestRateRefill` |
| 200 并发下不超全局/客户上限、无重复漏计、释放幂等 | `TestConcurrentNoOverAdmission`、`TestReleaseIdempotent` |
| 上游超时/连接/业务归一化、原样不透出 | `pkg/upstream` `TestClassify*` |
| 达阈值熔断、冷却快速失败、单探测、探测成功恢复、失败重开、健康不误熔断 | `pkg/circuit` 四个测试 |
| 探测超时有界 | `pkg/guard` `TestProbeTimeoutBound` |
| 调用方取消不计故障、不占额度 | `TestCanceledNotBilledAsFailure` |
| 上游 panic 不丢请求 | `TestPanicRecoveredNotDropped` |
| 收尾期间拒绝新请求、在途全部完成且账实相符、收尾超时报错 | `TestGracefulShutdown`、`TestShutdownTimeoutReportsRemaining` |
| 配置缺省/非法/未知键 | `pkg/config` `TestApplyDefaultsAndValidate`、`TestLoadJSON` |

单测日志用 `-v` 查看：每个判定都打印客户端、令牌/桶容量、客户端在途/突发、
全局在途/上限、熔断状态、连续故障数与判定依据。
