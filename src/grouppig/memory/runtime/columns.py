"""grouppig.memory.runtime.columns —— 跨存储复用的列类型与建表参数。

目标：同一套 DDL 在开发期 SQLite（aiosqlite）与生产期 MySQL（aiomysql）上都成立：

* 自增主键用 :data:`BIGINT_PK`（``BigInteger`` 在 SQLite 上退化为 ``INTEGER``，
  否则 SQLite 不会把 ``BIGINT PRIMARY KEY`` 当作 rowid 别名，自增会失效）；
* 事件时间统一用 :data:`EPOCH`（Float 秒级 epoch，排序/切片简单，跨库无时区坑）；
* 审计时间用 :func:`created_column` / :func:`updated_column`（无时区 ``DateTime``，Python 侧默认值）；
* 半结构化数据用 :data:`JSON_COL`（MySQL ``JSON`` / SQLite ``JSON``，自动序列化）；
* 建表参数固定 utf8mb4 + InnoDB，MySQL 侧不会因默认字符集丢表情。

normify id: ``grouppig.memory.runtime.columns``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, Column, DateTime, Float, Integer

#: 自增主键列类型（SQLite 用 INTEGER，MySQL 用 BIGINT）。
BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")

#: 秒级 epoch 事件时间列类型。
EPOCH = Float()

#: 半结构化数据列类型。
JSON_COL = JSON()

#: MySQL 建表参数（其他方言忽略 ``mysql_*`` 前缀参数）。
TABLE_KWARGS: dict[str, Any] = {"mysql_charset": "utf8mb4", "mysql_engine": "InnoDB"}


def utcnow() -> datetime:
    """当前 UTC 时间（无时区，避免 SQLite/MySQL 时区差异）。"""

    return datetime.now(UTC).replace(tzinfo=None)


def created_column() -> Column[datetime]:
    """``created_at``：插入时自动填 UTC 时间。"""

    return Column("created_at", DateTime(timezone=False), default=utcnow, nullable=False)


def updated_column() -> Column[datetime]:
    """``updated_at``：插入与每次更新自动填 UTC 时间。"""

    return Column("updated_at", DateTime(timezone=False), default=utcnow, onupdate=utcnow, nullable=False)


__all__ = [
    "BIGINT_PK",
    "EPOCH",
    "JSON_COL",
    "TABLE_KWARGS",
    "created_column",
    "updated_column",
    "utcnow",
]
