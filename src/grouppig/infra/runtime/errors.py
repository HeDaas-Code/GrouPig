"""grouppig.infra.runtime.errors —— infra 异常类型。

normify id: ``grouppig.infra.runtime.errors``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from typing import Any


class GrouPigError(Exception):
    """本项目所有自定义异常的基类。"""


class ConfigError(GrouPigError):
    """配置加载 / 校验失败。"""

    def __init__(self, message: str, *, issues: list[Any] | None = None) -> None:
        super().__init__(message)
        self.issues = list(issues or [])


class ContractError(GrouPigError):
    """违反 normify 契约（未登记的名字、未知主题等）。"""


class UnknownNameError(ContractError):
    """使用了 api-index.json 里不存在的 ``rpc:`` / ``kafka:`` 名字。"""


class UnknownTopicError(UnknownNameError):
    """发布了 api-index.json 里不存在的事件主题。"""


class HandlerNotRegistered(GrouPigError, KeyError):
    """按名字调用时处理器未注册。"""


class TransportError(GrouPigError):
    """模型 HTTP 传输层失败（连接失败、非 2xx、超时）。"""

    def __init__(self, message: str, *, status: int | None = None, retryable: bool = True, body: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.retryable = retryable
        self.body = body


class ModelCallError(GrouPigError):
    """模型调用在重试与降级后仍然失败。"""

    def __init__(self, message: str, *, attempts: int = 0, last_error: BaseException | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


class ModelTimeoutError(ModelCallError):
    """模型调用超时。"""


class TokenBudgetExceeded(GrouPigError):
    """token 预算不足且策略要求拒绝（``on_exceed = "raise"``）。"""


__all__ = [
    "ConfigError",
    "ContractError",
    "GrouPigError",
    "HandlerNotRegistered",
    "ModelCallError",
    "ModelTimeoutError",
    "TokenBudgetExceeded",
    "TransportError",
    "UnknownNameError",
    "UnknownTopicError",
]
