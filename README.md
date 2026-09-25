# 多上游请求编排网关（Python / asyncio）

面向多上游依赖的请求编排网关：**对外保持稳定且可解释的响应语义，对内对多个本地模拟
上游做选择、容错与降级**。所有上游均为本地可控的模拟实现，不依赖任何外部服务；时间
通过 `Clock` 依赖注入，故障注入场景可在零真实耗时下确定复现。

## 能力总览

- **多上游选择与负载分发**：按分片（shard）组织副本；选择时先过熔断器，再按
  “最少在途调用数”分流，平局采用确定性轮询；定选即预订在途名额，保证并发压力下
  承载立即可见、计数一致，单个慢上游不会阻塞整体。
- **熔断 / 限流 / 带预算重试**：
  - 熔断器 CLOSED / OPEN / HALF_OPEN 三态，冷却后受控探测，非探测流量快速失败；
  - 每副本一个令牌桶限流；重试受“全局重试预算令牌桶 + 单请求截止时间 + 单分片
    最大尝试次数”三重约束，恢复抖动不会放大为重试风暴或跨上游放大；
  - 请求内重试优先避开刚失败的副本（落到其他健康副本）。
- **降级与可区分失败语义**：部分分片成功返回 `degraded` 与成功数据；全部失败返回
  `unavailable` 并给出确定性的分类原因；底层异常一律归一化，绝不原样透出。
- **可观测**：每次选择、熔断判定、超时、重试、降级聚合都记录日志，标明判定依据。

## 目录结构

```
gateway/
  models.py        # FailureKind / BreakerState / Completeness / 结果结构
  config.py        # 网关、熔断、限流、重试配置项
  clock.py         # RealClock 与事件驱动 FakeClock（确定性测试）
  upstream.py      # MockUpstream：行为脚本化、调用计数/在途峰值统计
  breaker.py       # 三态熔断器（peek/reserve 两阶段，冷却后受控探测）
  ratelimit.py     # 令牌桶（上游限流与全局重试预算共用）
  retry.py         # 指数退避（可注入抖动/种子）+ 全局重试预算
  selector.py      # 熔断过滤 + 最少在途分流 + 平局轮询 + 请求内失败排除
  orchestrator.py  # 编排核心：扇出、超时、重试、熔断反馈、降级聚合
  app.py           # FastAPI 装配：/query、/health、/admin/stats、/admin/scenario
tests/             # 单元、端到端、并发与故障注入测试
main.py            # uvicorn 启动入口
```

## 上游选择策略

1. 上游按 `shard` 分组，一次请求对所有分片做**有界并发扇出**
   （`fanout_concurrency`），分片之间互不阻塞。
2. 对一个分片的每次尝试，`UpstreamSelector`：
   - 用熔断器 `peek()` 过滤 OPEN、以及 HALF_OPEN 中探测名额已满的副本；
   - CLOSED 普通副本优先于 HALF_OPEN 探测副本；
   - 在候选中选**在途调用数最少**者；在途数相同则按每选择器的确定性轮询游标轮换；
   - 同一请求内已失败副本在后续重试中优先排除（无其他副本时才回退到同一副本）；
   - 定选后才 `reserve()` 探测名额并 `reserve_slot()` 预订在途名额，避免遍历询问
     泄漏探测名额、也避免“任务尚未开跑导致全部读到在途 0”的负载倾斜。

## 熔断策略（`BreakerConfig`）

| 状态 | 行为 |
| --- | --- |
| CLOSED | 正常放行；连续成功会清零连续失败计数 |
| OPEN | 连续失败达到 `failure_threshold` 后跳闸，期间流量**快速失败**（不触达上游） |
| HALF_OPEN | OPEN 冷却 `cooldown_seconds` 后进入；仅放行 `half_open_probe_max` 个探测请求，其余快速失败 |

- 探测成功累计达到 `half_open_success_threshold` → 回到 CLOSED；
- 探测失败 → 立即重新 OPEN 并重置冷却（`trip_count` 累加，便于观察抖动）。

## 限流与重试

- **上游限流**（`RateLimitConfig`）：每副本一个令牌桶（容量 `capacity`、
  补充速率 `refill_per_second`），令牌不足时该次尝试快速失败并归类为
  `rate_limited`，在途名额立即释放。
- **重试**（`RetryConfig`）：
  - 单分片最多 `max_attempts_per_shard` 次（含首次）；
  - 退避 `min(max_backoff, base * 2**attempt)`；`jitter_ratio>0` 时全抖动，
    随机源可设种子，保证测试可复现；
  - 每次额外重试先申请**全局重试预算**令牌（`budget_capacity` /
    `budget_refill_per_second`），预算不足立即停止重试并归类为
    `retry_budget_exhausted`；
  - 若退避后会超过请求截止时间，则不再重试，归类为 `deadline_exceeded`。
- **超时**：单次调用超时 `call_timeout_seconds`，与“请求剩余截止”取较小值；
  超时后在途上游调用被取消，慢上游及时止损、不会拖垮整体。

## 降级语义与失败分类

响应 `completeness`：

- `full`：所有分片成功，`failure_reason=null`；
- `degraded`：部分分片成功，`data` 仅含成功分片，HTTP 仍为 200；
- `unavailable`：全部分片失败，HTTP 为 503，`failure_reason` 为分类原因。

`FailureKind`（底层异常**不会**原样透出，统一归一化）：

`timeout` / `upstream_error` / `rate_limited` / `circuit_open` /
`no_healthy_upstream` / `deadline_exceeded` / `retry_budget_exhausted`。

全失败时多分片原因按**确定性优先级**汇总为单个 `failure_reason`（越偏系统性、
越无恢复手段越优先）：

```
no_healthy_upstream > deadline_exceeded > rate_limited >
retry_budget_exhausted > timeout > upstream_error > circuit_open
```

每个分片在响应 `shards[].trace` 中给出每次尝试的副本、是否探测、失败分类与耗时，
响应因此可解释、可追溯；同一故障输入在不同并发时序下结论保持一致。

## 配置项（`gateway/config.py`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `call_timeout_seconds` | 1.0 | 单次上游调用超时 |
| `request_timeout_seconds` | 3.0 | 单请求总截止时间 |
| `fanout_concurrency` | 32 | 分片级扇出并发上限 |
| `breaker.failure_threshold` | 3 | 连续失败多少次跳闸 |
| `breaker.cooldown_seconds` | 5.0 | OPEN 冷却时长 |
| `breaker.half_open_probe_max` | 1 | HALF_OPEN 探测并发上限 |
| `breaker.half_open_success_threshold` | 1 | 探测成功多少次恢复 |
| `rate_limit.capacity` | 100 | 每副本令牌桶容量 |
| `rate_limit.refill_per_second` | 100 | 每副本令牌补充速率 |
| `retry.max_attempts_per_shard` | 3 | 单分片最大尝试次数 |
| `retry.base/max_backoff_seconds` | 0.1 / 1.0 | 退避区间 |
| `retry.jitter_ratio` | 0.0 | 抖动强度（0 为完全确定） |
| `retry.budget_capacity` | 50 | 全局重试预算容量 |
| `retry.budget_refill_per_second` | 20 | 全局重试预算补充速率 |

## 本地验证

依赖与环境（使用 uv）：

```bash
uv sync --group dev
```

运行全部测试（单测日志会打印“注入场景”与“判定依据”）：

```bash
uv run pytest                      # 全部
uv run pytest -o log_cli=true      # 同时在终端查看场景/判定日志
uv run pytest tests/test_concurrency.py   # 仅并发与故障注入
```

测试使用 `FakeClock`（事件驱动虚拟时钟），覆盖：连续失败跳闸与冷却探测、
探测成功恢复 / 失败重开、超时取消慢上游、跨副本重试、限流快速失败、
全局重试预算封顶（防风暴）、100 并发计数一致性、缓慢恢复、状态抖动，
以及“同故障输入在单请求与 32 并发下结论一致”。

启动 HTTP 服务并手动注入故障：

```bash
uv run uvicorn main:app --reload
# 正常查询（默认全健康）
curl -s -X POST localhost:8000/query -H 'content-type: application/json' \
  -d '{"payload":{"user":1},"request_id":"demo-1"}'
# 注入 payments 分片故障
curl -s -X POST localhost:8000/admin/scenario -H 'content-type: application/json' \
  -d '{"target":"payments","ok":false,"error":"db down"}'
# 再次查询 -> degraded；观察各副本计数与熔断状态
curl -s -X POST localhost:8000/query -d '{}' -H 'content-type: application/json'
curl -s localhost:8000/admin/stats
```

`/admin/scenario` 的 `target` 支持 `all`、分片名（如 `orders`）或具体副本名
（如 `orders-a`），可用 `delay` 注入慢上游、`ok:false` 注入报错。
`POST /admin/reset` 可把所有副本恢复为健康行为并清零计数、熔断器与重试预算
（无需重启服务即可重新开始一轮故障演练）；`GET /admin/stats` 查看各副本调用计数、
在途数、峰值并发、错误数与熔断快照。
