# Resilient Upstream Gateway（多上游请求编排网关）

一个面向多上游依赖的请求编排网关：**对外保持稳定、可解释的三态响应语义**，
内部对多个本地模拟上游做**选择、容错与降级**。上游超时、报错、限流、
熔断以及逐步恢复时的行为都确定性、可复现，全部由本进程内的模拟上游驱动，
不依赖任何外部服务。

## 1. 总体语义

数据按**分片（shard）**组织，每个分片有多个**副本上游（replica）**。
默认拓扑为 3 个分片 × 2 个副本：

```
users      : users-0, users-1
orders     : orders-0, orders-1
inventory  : inventory-0, inventory-1
```

一次请求的各分片**并发**处理，最终聚合为三态响应：

| status     | 含义                                   | 附带信息 |
| ---------- | -------------------------------------- | -------- |
| `success`  | 所有分片成功                           | 完整数据，`completeness = 1.0` |
| `degraded` | 部分分片失败，其余成功                 | 成功分片数据 + `completeness = 成功分片数/总分片数` + 每分片判定轨迹 |
| `failed`   | 所有分片失败                           | 确定性的 `failure_reason` 分类 |

`failure_reason` 取值（与并发时序无关，只由各分片结论集合决定）：

- `all_timeout`：失败分片全部因超时；
- `all_upstream_error`：失败分片全部因上游错误/熔断；
- `no_capacity`：失败分片全部因副本停用或无承载能力；
- `mixed`：失败分片中出现多种原因；
- `request_deadline_exceeded`：整体请求截止时间到达。

**底层异常不会透出**：上游抛出的任何异常在编排层被翻译成受控枚举
（`success / timeout / error / rejected_circuit_open / rate_limited /
retry_budget_exhausted / request_deadline`）与固定文案，响应中只出现这些值。

## 2. 上游选择与负载分发

`gateway/selector.py` 在每次尝试时重新计算候选副本：

1. 管理面未停用（`admin_state != disabled`）；
2. 熔断器放行（闭合，或拿到半开探测名额）——熔断打开的副本被直接跳过，
   实现"非探测流量快速失败"；
3. 合格副本按 `(健康状态优先级, 在途调用数, 权重随机)` 排序，
   - 半开探测副本最优先（探测名额由熔断器严格限制为 1），
   - 然后是健康副本；在途调用更少的副本优先，实现按承载能力分流；
   - 加权随机使用**固定种子**，相同输入下候选顺序可复现。

**承载能力（令牌桶）**：`gateway/limiter.py` 的单上游令牌桶。选择阶段只做
健康/熔断筛选；真正发起调用前 `try_acquire()` 取一个令牌，调用结束
（无论成败）立即 `refund()` 归还——因此令牌数等价于该副本的实时在途容量。
取不到令牌时立即尝试下一个副本，**绝不阻塞等待**。

**慢副本不拖垮整体**：分片在线程池中并发执行；单次调用有独立超时
（`per_attempt_timeout`），整个请求有总截止时间
（`retry.request_timeout_seconds`），最坏耗时被总截止时间封顶。

所有上游计数（成功/失败/拒绝/在途）都在该上游的锁内更新，
`GET /upstreams` 或 `registry.snapshot()` 可导出一致快照供断言。

## 3. 熔断策略

`gateway/breaker.py` 实现 CLOSED → OPEN → HALF_OPEN → CLOSED 状态机：

- **CLOSED（healthy）**：连续失败达到 `failure_threshold` 转为 OPEN；
- **OPEN**：非探测流量一律快速失败（不发起调用）；冷却
  `cooldown_seconds` 后允许进入 HALF_OPEN；
- **HALF_OPEN**：至多 `half_open_max_probe` 个受控探测请求通过；
  连续成功 `success_to_close` 次后闭合；任一探测失败**立即重新 OPEN 并
  重置冷却**，避免抖动恢复期间探测流量放大。

所有状态迁移在单个互斥锁内完成，并发下不会出现重复探测或状态翻转竞争。

## 4. 限流与带预算的重试

- **单上游限流**：令牌桶（容量 `capacity`、补充速率 `refill_per_second`），
  默认无限流；容量取空代表该副本暂时不具备承载能力。
- **带预算的重试**（`RetryBudget` + `Gateway._handle_shard`）：
  - 单请求最多 `max_attempts` 次尝试（含首次）；首次尝试不消费预算令牌；
  - 每次重试都要从**全局共享**的重试令牌桶取令牌
    （`total_budget_tokens` 容量、`budget_refill_per_second` 匀速恢复）。
    恢复抖动导致大量重试时，令牌被取空后后续重试被立即放弃，
    **防止重试风暴与跨上游放大**；
  - 重试前做指数退避（`backoff_base_seconds × backoff_factor^n`），
    退避时间被剩余截止时间截断；
  - 同一请求内优先尝试**尚未试过的副本**（跨副本故障转移），
    不会对同一个坏副本重复放大；
  - 单请求截止时间与整体请求截止时间双重约束，到点即停。

## 5. 降级语义

- 部分分片成功 → `degraded`：`data` 中**只包含成功分片**的数据，
  `completeness` 标明数据完整比例，`failed_shards` 标明缺口，
  `trace` 给出每个分片每个副本每次尝试的 `outcome/detail/耗时`，
  可直接用于解释"为什么缺数据"。
- 全部分片失败 → `failed`：不返回任何业务数据，只给可分类的
  `failure_reason`。
- 分片结论优先级：出现过超时 → `timeout`；否则出现过上游错误 →
  `upstream_error`；否则（全部停用/熔断/限流）→ `no_capacity`。

## 6. 运行期策略调整（不重启）

`UpstreamRegistry` 提供管理面接口，所有修改在目标副本自己的锁内完成，
**不触碰计数器**；与在途请求并发时语义为：

- **在途请求**：按进入时的判断正常跑完，成功/失败照常计入该副本计数，
  不被中途打断；
- **新请求**：立即按新策略选择副本；
- 成功/失败/拒绝/在途计数在任意并发翻转下保持一致，
  `GET /upstreams` 的计数可与响应 `trace` 里的判定一一对上。

| 操作 | Python API | HTTP |
| --- | --- | --- |
| 改权重 | `registry.update_policy(name, weight=...)` | `POST /admin/upstreams/{name}/policy {"weight": 500}` |
| 改承载能力 | `registry.update_policy(name, capacity=..., refill_per_second=...)` | `POST /admin/upstreams/{name}/policy {"capacity": 8}` |
| 临时停用 | `registry.disable(name)` | `POST /admin/upstreams/{name}/state {"state":"disabled"}` |
| 重新启用 | `registry.enable(name)` | `POST /admin/upstreams/{name}/state {"state":"healthy"}` |

承载能力调小时：在途调用不受影响（结束时 `refund` 只补到新容量上限），
桶内余量立即截断到新容量，新请求立刻按新容量放行或 `rate_limited` 拒绝。

## 7. 灰度流量接管（新副本平滑上线）

`gateway/traffic.py` 的 `TrafficShiftManager` 把某分片流量从旧副本
（source）平滑切到新副本（target）：

- target 先分 `start_share` 的权重份额，每过 `step_seconds` 增加
  `step_share`，直到 100%——此时 source 权重乘子归零（排空），接管完成；
- 加码期间持续观察 target：新增失败（含超时）达到
  `rollback_failure_threshold` 或 target 熔断打开，**立即撤回**——
  target 乘子归零、source 保持全量，整体可用性不受影响；
- 推进是惰性、时钟驱动的（与熔断器同风格），无需后台线程；
- 进度随时可查：阶段（`ramping/complete/rolled_back`）、当前份额、
  两个副本的生效权重、加码期失败数、推进历史。

```python
from gateway.traffic import TrafficShiftPlan
gateway.traffic.start_shift(TrafficShiftPlan(
    shard="users", source="users-0", target="users-1",
    start_share=0.1, step_share=0.15, step_seconds=0.5,
    rollback_failure_threshold=1,
))
gateway.traffic.status("users")   # phase/current_share/effective_weights/...
```

HTTP：`POST /admin/traffic/shift`（同参数 JSON）启动，
`GET /admin/traffic/status?shard=users` 查询进度。

## 8. 恢复路径的并发保证

- 选择器筛选候选时只做**只读窥视**（`probe_available`），不占用半开
  探测名额；编排器在真正发起调用前才 `acquire_probe` 占用，
  "选中但未调用"（如被限流拦截、并发竞争落选）时名额被归还或不占用，
  **不会泄漏**——任何副本都不会因名额泄漏而永远停在 HALF_OPEN；
- 半开探测并发度恒受 `half_open_max_probe` 上界约束，请求洪峰不会
  放大探测流量、不会把刚恢复的副本打回去；
- 同一分片多个副本同时熔断、同时到冷却期时，每个副本都会被实际
  探测并独立走回 CLOSED；彻底恢复后行为与从未故障一致。

## 9. 兼容性

- 响应三态语义、`failure_reason` 分类、`trace` 结构完全不变；
- 既有静态配置（`UpstreamConfig.weight`、`RateLimiterConfig`）作为
  初始值生效，运行期修改覆盖之，不重启不丢失计数；
- 未启动任何接管计划时，流量乘子恒为 1.0，选择行为与旧版一致；
- `CircuitBreaker.allow_request()` 保留（等价于 `acquire_probe`），
  旧的直接调用方不受影响。

## 10. 配置项

| 配置 | 位置 | 默认 | 说明 |
| --- | --- | --- | --- |
| `failure_threshold` | `BreakerConfig` | 5 | 连续失败多少次熔断 |
| `cooldown_seconds` | `BreakerConfig` | 2.0 | 熔断冷却时间 |
| `half_open_max_probe` | `BreakerConfig` | 1 | 半开并发探测名额 |
| `success_to_close` | `BreakerConfig` | 2 | 半开连续成功闭合次数 |
| `capacity` / `refill_per_second` | `RateLimiterConfig` | ∞ | 单副本承载令牌桶 |
| `max_attempts` | `RetryConfig` | 3 | 单请求最大尝试次数 |
| `backoff_base_seconds` / `backoff_factor` | `RetryConfig` | 0.01 / 2.0 | 指数退避 |
| `total_budget_tokens` | `RetryConfig` | 64 | 全局重试令牌总量 |
| `budget_refill_per_second` | `RetryConfig` | 16 | 全局重试令牌恢复速率 |
| `request_timeout_seconds` | `RetryConfig` | 1.0 | 整个请求总截止时间 |
| `per_attempt_timeout` | `GatewayConfig` | 0.2 | 单次上游调用超时 |
| `max_workers` | `GatewayConfig` | 16 | 分片并发线程池大小 |
| `start_share` / `step_share` / `step_seconds` | `TrafficShiftPlan` | 0.1 / 0.15 / 0.05 | 灰度初始份额、每步加码、阶梯间隔 |
| `rollback_failure_threshold` | `TrafficShiftPlan` | 1 | 加码期允许的目标失败次数上限 |

模拟上游行为表（`behaviors`，键为上游名）支持：`latency`（固定延迟）、
`jitter`（随机抖动）、`fail_nth`（按调用序号报错）、
`timeout_nth`（按调用序号超时）、`fail_every`（每 N 次报错一次）、
`weight`、`capacity`、`refill_per_second`、`seed`。

## 11. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测（含熔断/重试/超时/缓慢恢复/状态抖动/并发计数一致性、
# 运行期策略调整、灰度接管与撤回、多副本并发恢复）
uv run pytest -s          # -s 可查看每个用例打印的[场景]与[判定]依据

# 只跑新增能力
uv run pytest tests/test_admin_policy.py tests/test_traffic_shift.py tests/test_recovery_concurrency.py -s

# 启动 HTTP 服务（默认带演示故障注入场景）
uv run python main.py     # http://127.0.0.1:8000
```

HTTP 接口：

```bash
# 发起编排（默认场景中 orders 会降级或故障转移，可多调用观察恢复）
curl -s -X POST localhost:8000/orchestrate \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo","shards":["users","orders","inventory"]}'

curl -s localhost:8000/upstreams                      # 状态与调用计数
curl -s localhost:8000/scenario                       # 当前注入场景
curl -s -X POST localhost:8000/admin/upstreams/orders-0/state \
  -H 'content-type: application/json' -d '{"state":"disabled"}'  # 手动停用
curl -s -X POST localhost:8000/admin/upstreams/orders-0/policy \
  -H 'content-type: application/json' -d '{"weight":500,"capacity":8}'  # 运行期改策略
curl -s -X POST localhost:8000/admin/traffic/shift \
  -H 'content-type: application/json' \
  -d '{"shard":"users","source":"users-0","target":"users-1","step_seconds":0.5}'  # 灰度接管
curl -s 'localhost:8000/admin/traffic/status?shard=users'          # 接管进度
```

## 12. 代码结构

```
gateway/
  types.py       枚举、配置、请求/响应/尝试记录等稳定数据结构
  errors.py      内部异常层次（不对外透出）
  clock.py       单调时钟抽象
  breaker.py     熔断器状态机（探测名额窥视/占用/归还分离）
  limiter.py     单上游令牌桶（运行期可调）+ 全局重试预算桶
  upstream.py    脚本化模拟上游 + 并发安全的注册表/计数/策略调整
  selector.py    健康/承载/灰度乘子感知的候选选择与负载排序
  traffic.py     灰度流量接管：阶梯加码、健康观察、自动撤回
  gateway.py     编排器：并发、超时、重试、降级聚合（核心）
  scenarios.py   可复现故障注入场景工厂
  app.py         FastAPI 装配与观测/管理端点
tests/           熔断器、限流器、选择器、编排集成、HTTP 层、
                 运行期策略、灰度接管、并发恢复测试
```
