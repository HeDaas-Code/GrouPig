"""grouppig.memory.runtime.meta —— memory 全库共享的 :class:`~sqlalchemy.MetaData`。

六类存储的 schema 叶子模块都把表登记到这一个 MetaData 上，好处：

* 迁移脚本一次 ``create_all`` 就能建全库；
* 重名表（例如两处都定义 ``chat_messages``）会立刻抛错，防止表名漂移；
* 索引/约束命名有统一约定（``ix_`` / ``uq_`` / ``pk_``），SQLite 与 MySQL 一致。

normify id: ``grouppig.memory.runtime.meta``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from sqlalchemy import MetaData

#: 索引 / 唯一约束 / 主键的命名约定；显式命名的对象不受影响。
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "pk": "pk_%(table_name)s",
}

#: 全部 memory 表共用的元数据对象。
metadata = MetaData(naming_convention=NAMING_CONVENTION)


__all__ = ["NAMING_CONVENTION", "metadata"]
