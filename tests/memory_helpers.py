"""memory 域测试辅助：临时库夹具、样例消息与契约断言工具。"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime.di import MEMORY_RPC, MEMORY_TABLES, register_memory_handlers
from grouppig.memory.runtime.stores import MemoryStores

#: 测试默认用 SQLite 内存库（StaticPool，多事务共享同一连接）。
MEMORY_DSN = "sqlite+aiosqlite:///:memory:"

#: 测试用群号。
GROUP_ID = 100200300

#: 测试用群友。
SENDER_IDS = (1001, 1002, 1003)

#: 测试基准时间（固定值，保证断言可复现）。
BASE_TS = 1_700_000_000.0


def message(index: int, **overrides: Any) -> dict[str, Any]:
    """造一条归一化后的聊天消息（默认落在 BASE_TS 之后）。"""

    payload: dict[str, Any] = {
        "message_id": f"msg-{index}",
        "group_id": GROUP_ID,
        "sender_id": SENDER_IDS[index % len(SENDER_IDS)],
        "sender_name": f"群友{index % len(SENDER_IDS)}",
        "role": "member",
        "msg_type": "text",
        "content": f"今晚打本吗（{index}）",
        "ts": BASE_TS + index * 3.0,
    }
    payload.update(overrides)
    return payload


def chat_burst(
    count: int = 6,
    *,
    content: str = "打本打本，缺一个奶妈",
    base_ts: float = BASE_TS,
    step: float = 3.0,
    **overrides: Any,
) -> list[dict[str, Any]]:
    """造一段刷屏消息（同一内容重复，时间按 ``step`` 递进，供 flood/window 断言）。"""

    return [message(index, content=content, ts=base_ts + index * step, **overrides) for index in range(count)]


def register_isolated(stores: MemoryStores) -> Registry:
    """把 memory 处理器注册进独立注册表（不碰全局注册表）。"""

    registry = Registry()
    register_memory_handlers(registry, stores)
    return registry


def contract_coverage(registry: Registry, *, scope: str = "grouppig.memory") -> dict[str, list[str]]:
    """memory 域契约覆盖度：``rpc:`` 名字看注册表，``mysql:`` 表名看 schema 定义。

    注册表只装 rpc/kafka 处理器，表名由 schema 叶子定义，所以两者分开比对：
    ``missing`` = 契约里没注册的 rpc 名字；``tables_missing`` = 契约里有但 schema 没定义的表。
    """

    from grouppig.memory.runtime import schema as schema_module

    check = contract.check_registry(set(registry.names()), scope=scope)
    expected_tables = {
        name.split(":", 1)[1]
        for name, module in contract.api_index().items()
        if name.startswith("mysql:") and (module == scope or module.startswith(scope + "."))
    }
    defined = set(schema_module.table_names())
    return {
        "missing": [name for name in check["missing"] if not name.startswith("mysql:")],
        "unknown": check["unknown"],
        "tables_missing": sorted(expected_tables - defined),
    }


def expected_memory_names() -> dict[str, set[str]]:
    """api-index.json 里归属 grouppig.memory 的名字（rpc / mysql 分开）。"""

    names = {name for name, module in contract.api_index().items() if module.startswith("grouppig.memory")}
    return {
        "rpc": {n for n in names if n.startswith("rpc:")},
        "mysql": {n for n in names if n.startswith("mysql:")},
        "all": names,
    }


def fake_llm(text: str = "模型摘要：今晚群里在约打本。") -> Any:
    """假的模型摘要回调（同步返回，验证 ``llm`` 钩子）。"""

    async def _llm(prompt: str) -> str:
        assert "群聊会话" in prompt
        return text

    return _llm


def now() -> float:
    return time.time()


def ids(rows: Sequence[dict[str, Any]], key: str = "message_id") -> list[Any]:
    return [row[key] for row in rows]


__all__ = [
    "BASE_TS",
    "GROUP_ID",
    "MEMORY_DSN",
    "MEMORY_RPC",
    "MEMORY_TABLES",
    "SENDER_IDS",
    "chat_burst",
    "contract_coverage",
    "expected_memory_names",
    "fake_llm",
    "ids",
    "message",
    "now",
    "register_isolated",
]
