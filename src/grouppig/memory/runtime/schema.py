"""grouppig.memory.runtime.schema —— 六类存储的表结构汇总与契约校验。

把六类存储（chat / thread / profile / social / session-archive / slang）在
:mod:`grouppig.memory.runtime.meta` 上登记的表汇总起来，供迁移脚本、健康检查与
契约测试使用：


* :data:`CONTRACT_TABLES` —— ``api-index.json`` 里属于 ``grouppig.memory`` 的 10 张 ``mysql:`` 表（逐字对齐）；
* :func:`check_contract` —— 比对「库里/元数据里有的表」与契约表，返回 ``missing`` / ``unknown``；
  契约**零容忍**：出现契约外的表说明表名没进设计树，:func:`assert_contract` 会直接报错。


normify id: ``grouppig.memory.runtime.schema``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import Table

from grouppig.infra.runtime import contract
from grouppig.memory.runtime.db import Database
from grouppig.memory.runtime.errors import SchemaError
from grouppig.memory.runtime.meta import metadata

#: memory 域的模块前缀。
MEMORY_SCOPE = "grouppig.memory"

#: 设计树登记、必须逐字存在的 10 张契约表（表名抄自 ``api-index.json``）。
CONTRACT_TABLES: tuple[str, ...] = (
    "chat_messages",
    "chat_window_index",
    "chat_threads",
    "chat_thread_edges",
    "member_profiles",
    "profile_facts",
    "social_edges",
    "relationship_scores",
    "session_archives",
    "slang_entries",
)

_LOADED = False


def load_tables() -> None:
    """导入全部 schema / 表定义模块，把表登记到共享 MetaData（幂等）。"""

    global _LOADED
    if _LOADED:
        return
    from grouppig.memory.chat_store import schema as chat_schema
    from grouppig.memory.profile_store import schema as profile_schema
    from grouppig.memory.session_archive import dao as archive_dao
    from grouppig.memory.slang_kb import dictionary as slang_dictionary
    from grouppig.memory.social_store import schema as social_schema
    from grouppig.memory.thread_store import schema as thread_schema

    del chat_schema, profile_schema, archive_dao, slang_dictionary, social_schema, thread_schema
    _LOADED = True


def tables() -> dict[str, Table]:
    """全部 memory 表（表名 → Table）。"""

    load_tables()
    return dict(metadata.tables)


def table(name: str) -> Table:
    """按表名取 Table；不存在抛 :class:`~grouppig.memory.runtime.errors.SchemaError`。"""

    load_tables()
    try:
        return metadata.tables[name]
    except KeyError as exc:
        raise SchemaError(f"未知的 memory 表：{name!r}（已定义：{sorted(metadata.tables)}）") from exc


def table_names() -> tuple[str, ...]:
    return tuple(sorted(tables()))


def contract_tables() -> dict[str, Table]:
    """10 张契约表（设计树登记的表名）。"""

    return {name: table(name) for name in CONTRACT_TABLES}


def unknown_tables(known: Iterable[str] | None = None) -> tuple[str, ...]:
    """契约外的表名（正常为空：memory 的表必须先在 ``api-index.json`` 登记 ``mysql:`` 名字）。"""

    return tuple(check_contract(known)["unknown"])


def index_names(name: str) -> tuple[str, ...]:
    """某张表上的索引名（迁移校验用）。"""

    return tuple(sorted(i.name for i in table(name).indexes if i.name))


def indexes() -> dict[str, tuple[str, ...]]:
    return {name: index_names(name) for name in table_names()}


def check_contract(known: Iterable[str] | None = None) -> dict[str, list[str]]:
    """比对表名与设计契约。

    ``known`` 传「库里实际存在的表名」时，``missing`` 表示库里缺表；
    不传时用元数据里已登记的表（即「代码里定义的表」）。
    """

    defined = set(known) if known is not None else set(table_names())
    expected = set(CONTRACT_TABLES)
    return {
        "missing": sorted(expected - defined),
        "unknown": sorted(defined - expected),
    }


def assert_contract() -> None:
    """元数据自检：10 张契约表必须都在，且不得出现契约外的表。"""

    load_tables()
    check = check_contract()
    if check["missing"]:
        raise SchemaError(f"缺少契约表：{check['missing']}")
    if check["unknown"]:
        raise SchemaError(f"出现契约外的 memory 表：{check['unknown']}（请先在 api-index.json 登记 mysql: 名字）")
    for name in CONTRACT_TABLES:
        assert contract.is_known_name(f"mysql:{name}"), name


def summary() -> dict[str, Any]:
    """概览（启动日志 / 健康检查用）。"""

    load_tables()
    return {
        "tables": len(metadata.tables),
        "contract_tables": list(CONTRACT_TABLES),
        "unknown_tables": list(check_contract()["unknown"]),
        "indexes": {name: list(names) for name, names in indexes().items()},
    }


async def create_all(db: Database) -> dict[str, Any]:
    """建全库（幂等：``create_all`` 只建缺失的表与索引）。"""

    load_tables()
    assert_contract()
    await db.create_all(metadata)
    return {"created": table_names(), "count": len(metadata.tables)}


async def drop_all(db: Database) -> dict[str, Any]:
    """删全库（仅开发/测试用）。"""

    load_tables()
    await db.drop_all(metadata)
    return {"dropped": table_names()}


__all__ = [
    "CONTRACT_TABLES",
    "MEMORY_SCOPE",
    "assert_contract",
    "check_contract",
    "contract_tables",
    "create_all",
    "drop_all",
    "index_names",
    "indexes",
    "load_tables",
    "summary",
    "table",
    "table_names",
    "tables",
    "unknown_tables",
]
