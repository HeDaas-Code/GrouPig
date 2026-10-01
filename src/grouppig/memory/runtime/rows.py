"""grouppig.memory.runtime.rows —— 结果行 → 可 JSON 序列化字典。

DAO 对外（``rpc:`` 处理器返回值）一律给纯字典，方便直接塞进事件总线 payload
或模型提示词；这里统一处理 ``datetime`` / ``Decimal`` / ``bytes`` 等类型的转换。

normify id: ``grouppig.memory.runtime.rows``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.engine import Row


def jsonable(value: Any) -> Any:
    """把单个值转成可 JSON 序列化的形式。"""

    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [jsonable(v) for v in value]
    return value


def row_to_dict(row: Row | Any | None) -> dict[str, Any] | None:
    """一行 → 字典；``None`` 原样返回。"""

    if row is None:
        return None
    mapping = getattr(row, "_mapping", row)
    if isinstance(mapping, dict):
        return {str(k): jsonable(v) for k, v in mapping.items()}
    return {str(k): jsonable(mapping[k]) for k in mapping.keys()}


def rows_to_dicts(rows: Any) -> list[dict[str, Any]]:
    """多行 → 字典列表。"""

    out: list[dict[str, Any]] = []
    for row in rows:
        item = row_to_dict(row)
        if item is not None:
            out.append(item)
    return out


__all__ = ["jsonable", "row_to_dict", "rows_to_dicts"]
