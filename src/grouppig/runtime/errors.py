"""grouppig.runtime.errors —— 集成层的错误类型。"""

from __future__ import annotations


class IntegrationError(RuntimeError):
    """单进程装配 / 闭环驱动失败（缺域、顺序错、契约缺口）。"""


__all__ = ["IntegrationError"]
