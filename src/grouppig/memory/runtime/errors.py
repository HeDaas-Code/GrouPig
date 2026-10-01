"""grouppig.memory.runtime.errors —— memory 域异常类型。

normify id: ``grouppig.memory.runtime.errors``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from grouppig.infra.runtime.errors import GrouPigError


class StoreError(GrouPigError):
    """存储层失败（连接、事务、约束、序列化）。"""


class SchemaError(StoreError):
    """表结构 / 元数据与 normify 设计契约不一致。"""


class RecordNotFound(StoreError, KeyError):
    """按主键读取的记录不存在。"""


__all__ = ["RecordNotFound", "SchemaError", "StoreError"]
