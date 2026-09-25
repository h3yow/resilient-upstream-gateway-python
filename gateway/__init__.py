"""多上游请求编排网关。

对外提供稳定、可解释的响应语义；内部完成上游选择、熔断、限流、
带预算重试与降级聚合。所有时间均可通过 Clock 注入，便于确定性测试。
"""

from gateway.orchestrator import GatewayOrchestrator
from gateway.config import GatewayConfig

__all__ = ["GatewayOrchestrator", "GatewayConfig"]
