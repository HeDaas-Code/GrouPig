"""grouppig.perception.runtime.errors —— 感知层异常类型。

normify id: ``grouppig.perception.runtime.errors``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from grouppig.infra.runtime.errors import GrouPigError


class PerceptionError(GrouPigError):
    """感知层失败（载荷非法、窗口不可用、下游调用失败等）。"""


class PayloadError(PerceptionError):
    """上游事件 / 消息载荷不合法，无法归一化。"""


class WindowUnavailable(PerceptionError):
    """既没有内存滚动窗、也没有可用的 ``rpc:chat.window`` 数据源。"""


__all__ = ["PayloadError", "PerceptionError", "WindowUnavailable"]
