"""内部异常层次。

这些异常只在网关内部流转，最终由编排层统一翻译成
``ShardStatus`` / ``FailureReason``，绝不原样透出给调用方。
"""

from __future__ import annotations


class GatewayError(Exception):
    """所有网关内部异常的基类。"""


class UpstreamTimeout(GatewayError):
    """上游在截止时间内没有返回。"""


class UpstreamCallError(GatewayError):
    """上游返回了错误（5xx、异常等）。"""


class CircuitOpenError(GatewayError):
    """熔断打开期间的非探测流量被快速拒绝。"""


class RateLimitedError(GatewayError):
    """上游令牌桶无可用令牌（超出承载能力）。"""


class UpstreamDisabledError(GatewayError):
    """上游被管理面停用。"""


class RetryBudgetExceeded(GatewayError):
    """全局重试令牌预算耗尽。"""


class RequestDeadlineExceeded(GatewayError):
    """整个请求的总截止时间已到。"""
