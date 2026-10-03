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
3. 合格副本按 `(健康状态优先级, 在途调用数, 有效权重随机)` 排序，
   - 半开探测副本最优先（探测名额由熔断器严格限制），
   - 然后是健康副本；在途调用更少的副本优先，实现按承载能力分流；
   - 有效权重 = 运行期 `weight × traffic_scale`（灰度比例为 0 的副本
     不参与选择）；加权随机使用**固定种子**，相同输入下候选顺序可复现。

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
探测名额的获取分两个阶段（筛选时窥视、调用前准入），详见第 8 节。

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

每个副本除了静态 `UpstreamConfig`，还有一份**运行期可变策略**
`RuntimePolicy`（`gateway/types.py`），注册时从静态配置初始化，之后可在线调整：

| 字段 | 含义 | 调整效果 |
| --- | --- | --- |
| `weight` | 选择权重 | 立即影响新请求的候选排序 |
| `capacity` / `refill_per_second` | 并发承载令牌桶 | 调大立即补发差额令牌；调小截断多余令牌，但在途请求已取走的令牌不受影响 |
| `traffic_scale` | 灰度流量比例 0~1 | 由灰度接管控制器驱动（见第 7 节），一般不需要手改 |

语义保证：

- **在途请求不被打断**：策略只影响变更之后的选择与准入判断，已经在跑的
  请求按它进入时拿到的名额正常跑完；
- **计数不乱**：成功/失败/拒绝绝/在途计数与策略变更无关，全部仍在那
  个副本自己的锁内更新，`registry.snapshot()` / `GET /upstreams`
  返回里带 `policy` 字段，判定信息可和计数对账；
- 停用/启用走 `registry.disable()/enable()`（锁内完成），HTTP 层
  `POST /admin/upstreams/{name}/state` 语义不变。

```python
registry.update_policy("orders-0", weight=0)                 # 摘立即摘流
registry.update_policy("orders-0", capacity=4, refill_per_second=2)
registry.disable("orders-0"); registry.enable("orders-0")    # 临时停用/恢复
```

## 7. 灰度接管（流量平滑切换到新副本）

`gateway/rollout.py` 的 `TrafficShiftManager` 把流量按节奏从旧副本
转移到新上线副本：

```python
mgr = TrafficShiftManager(registry)
gateway.shift_manager = mgr          # 网关每请求惰性推进一次评估
shift_id = mgr.begin_shift(
    "orders", "orders-1", ["orders-0"],
    steps=5, step_interval_seconds=0.2,
)
mgr.status()  # 档位、状态、各副本当前实际流量比例
```

- **加码**：`begin_shift` 后新副本立即进入第 1 档
  （`traffic_scale = 1/steps`），每过 `step_interval_seconds` 升一档，
  旧副本比例同步下调，直到 `steps` 档走完（新副本 1.0、旧副本 0.0，
  完全替下）。选择器按 `weight × traffic_scale` 分流。
- **自动撤回**：加码期间新副本熔断器 OPEN（变慢/报错触发）或被停用，
  下一次评估立即撤回——新副本比例归零、旧副本恢复 1.0，状态记为
  `rolled_back` 并附原因。分片内未参与接管的副本全程不受影响，
  整体可用性不被拖下水。
- **可观测**：`mgr.status()` / `GET /admin/shifts` 返回每次接管的
  `state`（`ramping/completed/rolled_back`）、`current_step/steps`、
  `rollback_reason` 和分片内各副本的**实际流量比例**
  （按有效权重计算），不是一句"已切换"。
- 评估是**惰性**的（不起后台线程）：由 `Gateway.handle` 每请求调用
  一次 `refresh()`，按注入的时钟计算档位，结论可复现。

## 8. 并发恢复保证（无饿死、探测有上界）

熔断器采用**两阶段探测名额**语义，保证多副本同时故障、同时到恢复
观察点时没有死角：

- 选择器筛选候选时只**窥视**（`breaker.would_allow()`），不消耗
  探测名额——候选最终没被调用也不会泄漏名额，杜绝"某副本永远
  占着探测名额进不了恢复观察"的死角；
- 编排器在真正发起调用前才 `allow_request()` 准入，HALF_OPEN 下
  占用一个探测名额，调用结束由 `record_success/record_failure`
  释放。准入与结果记录严格一一对应；
- 探测上界为 `half_open_max_probe`：无论多少请求同时涌入，每个
  恢复 episode 准入的探测数不超过配置值；
  `breaker.probes_admitted_total` 累计准入探测数，可直接断言上界；
- 探测失败立即重新 OPEN 并重置冷却，下一个冷却期仍会被再次探测，
  直到稳定恢复；彻底恢复（CLOSED）后计数与行为与从未故障一致。

## 9. 配置项

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
| `weight` / `capacity` / `refill_per_second` | `RuntimePolicy`（运行期） | 继承静态配置 | 在线调整，立即对新请求生效 |
| `traffic_scale` | `RuntimePolicy`（运行期） | 1.0 | 灰度流量比例，由接管控制器驱动 |
| `steps` / `step_interval_seconds` | `begin_shift` 参数 | 5 / 0.2 | 灰度加码档数与每档间隔 |

模拟上游行为表（`behaviors`，键为上游名）支持：`latency`（固定延迟）、
`jitter`（随机抖动）、`fail_nth`（按调用序号报错）、
`timeout_nth`（按调用序号超时）、`fail_every`（每 N 次报错一次）、
`weight`、`capacity`、`refill_per_second`、`seed`。

### 兼容性范围

- **响应语义完全不变**：三态、`completeness`、`failure_reason`、
  `trace` 结构与受控 outcome 枚举都没有新增或修改对外字段；
- **静态配置不变**：`UpstreamConfig`/`BreakerConfig`/`RetryConfig` 等
  老配置原样有效，运行期策略只是在注册时从静态配置初始化的一份
  可变副本，不调整时行为与之前完全一致；
- **老管理端点不变**：`POST /admin/upstreams/{name}/state` 语义保持，
  只是内部改为锁内更新；新增端点（`/policy`、`/admin/shifts`）
  均为纯增量；
- **选择器行为变化只有一处**：筛选阶段不再消耗半开探测名额
  （改为调用前准入），这修复了多副本同时恢复时探测名额泄漏
  导致的饿死，单副本场景的恢复节奏不受影响。

## 10. 本地验证

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测（熔断/重试/超时/缓慢恢复/状态抖动/并发计数一致性/
# 运行期策略调整/灰度接管与撤回/多副本并发恢复）
uv run pytest -s          # -s 可查看每个用例打印的[场景]与[判定]依据

# 只跑新增能力的用例
uv run pytest tests/test_runtime_policy.py tests/test_rollout.py \
  tests/test_concurrent_recovery.py -s

# 启动 HTTP 服务（默认带演示故障注入场景）
uv run python main.py     # http://127.0.0.1:8000
```

HTTP 接口：

```bash
# 发起编排（默认场景中 orders 会降级或故障转移，可多调用观察恢复）
curl -s -X POST localhost:8000/orchestrate \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo","shards":["users","orders","inventory"]}'

curl -s localhost:8000/upstreams                      # 状态、运行期策略与调用计数
curl -s localhost:8000/scenario                       # 当前注入场景
curl -s -X POST localhost:8000/admin/upstreams/orders-0/state \
  -H 'content-type: application/json' -d '{"state":"disabled"}'  # 手动停用

# 运行期调策略（不重启）：权重调 0 摘流 / 并发承载调为 4
curl -s -X POST localhost:8000/admin/upstreams/orders-0/policy \
  -H 'content-type: application/json' -d '{"weight":0}'
curl -s -X POST localhost:8000/admin/upstreams/orders-0/policy \
  -H 'content-type: application/json' -d '{"capacity":4,"refill_per_second":2}'

# 灰度接管：把 orders 流量从 orders-0 平滑切到 orders-1，并轮询进度
curl -s -X POST localhost:8000/admin/shifts \
  -H 'content-type: application/json' \
  -d '{"shard":"orders","new_replica":"orders-1","old_replicas":["orders-0"],
       "steps":5,"step_interval_seconds":1.0}'
curl -s localhost:8000/admin/shifts    # 档位/状态/各副本实际流量比例
```

## 11. 代码结构

```
gateway/
  types.py       枚举、配置、运行期策略、请求/响应/尝试记录等稳定数据结构
  errors.py      内部异常层次（不对外透出）
  clock.py       单调时钟抽象
  breaker.py     熔断器状态机（两阶段探测：窥视筛选 + 调用前准入）
  limiter.py     单上游令牌桶（支持运行期 resize）+ 全局重试预算桶
  upstream.py    脚本化模拟上游 + 并发安全的注册表/计数/运行期策略
  selector.py    健康/承载/灰度比例感知的候选选择与负载排序
  rollout.py     灰度接管控制器：按节奏加码、异常自动撤回、进度可观测
  gateway.py     编排器：并发、超时、重试、降级聚合（核心）
  scenarios.py   可复现故障注入场景工厂
  app.py         FastAPI 装配与观测/管理端点
tests/           熔断器、限流器、选择器、编排集成、HTTP 层、
                 运行期策略、灰度接管、并发恢复测试
```
