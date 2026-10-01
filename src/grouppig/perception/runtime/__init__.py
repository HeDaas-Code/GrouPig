"""grouppig.perception.runtime —— 感知层运行时补充包。

设计树里的感知层是 17 个叶子（observer 2 + normalizer 3 + classifier 4 + flood 3 +
rhythm 2 + interrupt 3），本包提供它们共用的运行时胶水：

* :mod:`~grouppig.perception.runtime.config` —— 阈值与开关（代码里有默认值，配置可覆盖）
* :mod:`~grouppig.perception.runtime.text` —— 文本工具（分词 / 指纹 / 相似度 / 表情 / 情感）
* :mod:`~grouppig.perception.runtime.messages` —— QQEvent → 感知内部消息的归一化与统计
* :mod:`~grouppig.perception.runtime.calls` —— 下游 ``rpc:`` 的最佳努力调用与节流
* :mod:`~grouppig.perception.runtime.di` —— 21 个 ``rpc:`` 处理器 + 2 个主题的装配入口

normify id: ``grouppig.perception.runtime``（运行时补充包，设计树暂无；与
``grouppig.infra.runtime`` / ``grouppig.memory.runtime`` 同例）。
"""

from __future__ import annotations

from grouppig.perception.runtime import calls, config, messages, text
from grouppig.perception.runtime.errors import PayloadError, PerceptionError, WindowUnavailable

__all__ = [
    "PayloadError",
    "PerceptionError",
    "WindowUnavailable",
    "calls",
    "config",
    "messages",
    "text",
]
