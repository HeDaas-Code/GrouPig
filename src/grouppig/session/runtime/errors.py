"""grouppig.session.runtime.errors —— session 域异常类型。

normify id: ``grouppig.session.runtime.errors``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from grouppig.infra.runtime.errors import GrouPigError


class SessionError(GrouPigError):
    """会话层失败（状态转移非法、会话不存在、跨域调用缺参）。"""


class SessionNotFound(SessionError, KeyError):
    """按 session_id 取会话时不存在。"""


class InvalidTransition(SessionError):
    """非法的会话状态转移。"""


class WakeError(SessionError):
    """跨会话唤醒失败（档案缺失、缓冲已满、TTL 非法）。"""


class DependencyMissing(SessionError):
    """需要跨域 ``rpc:`` 调用但未注入 caller（或 caller 未注册该名字）。"""


__all__ = ["DependencyMissing", "InvalidTransition", "SessionError", "SessionNotFound", "WakeError"]
