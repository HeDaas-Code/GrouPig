"""grouppig.panel.snapshot —— 面板快照：面板唯一的数据来源。

把运行时可观测面收敛成一份**可序列化、只读、不抛错**的 JSON：

* ``app`` —— 运行时长、装配顺序、选项、泵状态；
* ``contract`` —— 契约自检（registered / missing / unknown）；
* ``domains`` / ``domain_health`` —— 各域是否装配与健康；
* ``registry`` —— 注册名清单（按前缀分组计数）；
* ``tables`` —— 契约表的行数（读不到就标 ``error``，不影响其它部分）；
* ``events`` —— 事件总线最近若干条（内存环形缓冲，可选）。

**刻意不做的**：不直接 import 任何域实现（避免循环依赖），只通过传入的 ``app``、
``container`` 与 memory 层暴露的查询接口取数；任何一部分失败都降级成 ``{"error": ...}``。
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from grouppig.memory.runtime.schema import CONTRACT_TABLES

#: 快照格式版本，前端可据此判断兼容性。
SNAPSHOT_VERSION = 1

#: 同步读表的默认上限（秒）。面板宁可报「读不到」，也不能把进程挂死。
DEFAULT_TABLE_TIMEOUT = 5.0

#: 事件流在内存里最多保留多少条。
EVENT_BUFFER_SIZE = 200


@dataclass
class SnapshotOptions:
    """快照选项。"""

    include_health: bool = True
    include_tables: bool = True
    event_limit: int = 50
    table_limit: int = 20
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "include_health": self.include_health,
            "include_tables": self.include_tables,
            "event_limit": self.event_limit,
            "table_limit": self.table_limit,
        }


class EventRing:
    """进程内事件环形缓冲：面板显示的「最近发生了什么」。

    只保留结构化摘要（主题 + 少量字段），不落库、不重放。
    """

    def __init__(self, size: int = EVENT_BUFFER_SIZE) -> None:
        self._items: deque[dict[str, Any]] = deque(maxlen=size)

    def record(self, topic: str, payload: Any = None, *, source: str = "") -> dict[str, Any]:
        item = {
            "ts": time.time(),
            "time": time.strftime("%H:%M:%S"),
            "topic": str(topic),
            "source": str(source),
            "summary": _summarize(payload),
        }
        self._items.append(item)
        return item

    def tail(self, limit: int = 50) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        items = list(self._items)
        return items[-limit:]

    def clear(self) -> None:
        self._items.clear()

    def __len__(self) -> int:
        return len(self._items)


#: 进程级事件缓冲（Web / TUI 共用）。
EVENTS = EventRing()

#: 事件负载里装**用户原话 / 模型原文**的键。
#:
#: 面板是运维观测面，不是聊天记录阅读器：总线把每条发布的事件都塞进环形缓冲，
#: 而聊天事件的 payload 里就带着群消息正文。默认把这类键脱敏，避免「打开面板
#: 就等于把群聊内容回显到浏览器」—— 尤其在没有鉴权、或绑到非本机地址的时候。
FREE_TEXT_KEYS = frozenset(
    {
        "text",
        "content",
        "message",
        "msg",
        "raw",
        "raw_message",
        "body",
        "reply",
        "prompt",
        "answer",
        "completion",
    }
)

#: 超过这个长度的自由文本才脱敏。
#:
#: 保留短串是为了不把观测面变成盲盒：`text=hi` 这类短标记（测试、心跳、状态词）
#: 仍然可读，而真正的群消息正文一律只给长度。
FREE_TEXT_KEEP = 12


def _redact(value: str) -> str:
    """自由文本的默认呈现：只给长度，不给内容。"""

    if len(value) <= FREE_TEXT_KEEP:
        return value
    return f"<已脱敏 {len(value)} 字>"


def _summarize(payload: Any, *, width: int = 120) -> str:
    """把事件负载压成一行摘要（自由文本按键脱敏）。"""

    if payload is None:
        return ""
    if isinstance(payload, dict):
        parts = []
        for key, value in list(payload.items())[:6]:
            shown = _redact(value) if key in FREE_TEXT_KEYS and isinstance(value, str) else _short(value)
            parts.append(f"{key}={shown}")
        text = " ".join(parts)
    elif isinstance(payload, str):
        text = _redact(payload)
    else:
        text = _short(payload)
    return text[:width]


def _short(value: Any) -> str:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return str(value)
    if isinstance(value, (list, tuple)):
        return f"[{len(value)}]"
    if isinstance(value, dict):
        return f"{{{len(value)}}}"
    return type(value).__name__


def table_counts(app: Any = None, *, limit: int = 20, timeout: float = DEFAULT_TABLE_TIMEOUT) -> dict[str, Any]:
    """契约表行数（走 memory 层；任何一张表读不到都单独记错）。

    **必须有界**：同步入口在没有运行中的 loop 时会自己开一个（``asyncio.run``），
    而 memory 的 ``SerializedConnection`` 闸门是按 loop 绑定的。若闸门正被另一个
    （已经停掉的）loop 上的任务攥着 —— 例如某个泵的即时首拍被留下半途 ——
    这个新 loop 就会**永远**等下去。面板是运维排查用的东西，卡死比读不到表更糟：
    超时后降级成 ``available: False``，把「读不到」如实报出来。
    """

    database = _database(app)
    if database is None:
        return {"available": False, "reason": "memory 域未装配", "tables": {}}
    import asyncio

    async def collect() -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name in CONTRACT_TABLES[: max(0, limit)]:
            try:
                rows = await database.fetch_all(text(f"SELECT COUNT(*) AS n FROM {name}"))
                result[name] = int(rows[0]["n"]) if rows else 0
            except Exception as error:  # noqa: BLE001 - 面板不许因为一张表而整体失败
                result[name] = {"error": f"{type(error).__name__}: {error}"}
        return result

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        return {"available": False, "reason": "同步快照不能在有事件循环的线程里读表", "tables": {}}

    bounded = max(0.0, float(timeout))
    try:
        tables = asyncio.run(asyncio.wait_for(collect(), bounded if bounded else None))
    except TimeoutError:
        return {
            "available": False,
            "reason": f"读表超时（{bounded:g}s）：数据库连接可能被另一个事件循环上的任务占着",
            "tables": {},
        }
    return {"available": True, "tables": tables}


def _database(app: Any) -> Any:
    """从 app 上找到 memory 的 Database（``app.memory`` 是 MemoryStores，``.db`` 才是 Database）。"""

    if app is None:
        return None
    candidates: list[Any] = []
    memory = getattr(app, "memory", None)
    if memory is not None:
        candidates.extend([getattr(memory, "db", None), memory])
    container = getattr(app, "container", None)
    if container is not None:
        candidates.extend([getattr(container, "memory", None), getattr(container, "database", None)])
    for candidate in candidates:
        db = getattr(candidate, "db", candidate)
        if db is not None and hasattr(db, "fetch_all"):
            return db
    return None


def _contract_of(app: Any) -> dict[str, Any]:
    """契约自检：app 有 ``contract_check`` 就用它；容器则按注册表与契约名比对。"""

    probe = getattr(app, "contract_check", None)
    if callable(probe):
        try:
            return dict(probe())
        except Exception as error:  # noqa: BLE001
            return {"error": f"{type(error).__name__}: {error}"}
    container = getattr(app, "container", app)
    try:
        from grouppig.infra.runtime import contract as contract_module

        registry = getattr(container, "registry", None)
        registered = set(registry.names()) if hasattr(registry, "names") else set(registry or [])
        expected = set(contract_module.rpc_names())
        return {
            "registered": len(registered),
            "missing": sorted(expected - registered),
            "unknown": sorted(registered - expected - set(contract_module.topic_names())),
        }
    except Exception as error:  # noqa: BLE001
        return {"error": f"{type(error).__name__}: {error}"}


def registry_summary(container: Any) -> dict[str, Any]:
    """注册表摘要：总数 + 按前缀分组计数（不吐全部名字，避免快照过大）。"""

    registry = getattr(container, "registry", None)
    if hasattr(registry, "names"):
        raw = registry.names()
    else:
        raw = registry or []
    names = sorted(str(name) for name in raw)
    groups: dict[str, int] = {}
    for name in names:
        prefix = name.split(":", 1)[0] if ":" in name else "other"
        groups[prefix] = groups.get(prefix, 0) + 1
    return {"total": len(names), "by_prefix": groups, "names": names}


def build_snapshot(app: Any = None, options: SnapshotOptions | None = None) -> dict[str, Any]:
    """构造一份只读快照；``app`` 为 None 时返回最小的空壳而不是报错。"""

    options = options or SnapshotOptions()
    snapshot: dict[str, Any] = {
        "version": SNAPSHOT_VERSION,
        "generated_at": time.time(),
        "options": options.as_dict(),
        "app": {},
        "contract": {},
        "registry": {},
        "domains": {},
        "pumps": [],
        "tables": {},
        "events": EVENTS.tail(options.event_limit),
    }
    if app is None:
        snapshot["app"] = {"started": False, "reason": "未提供 app"}
        return snapshot

    try:
        snapshot["app"] = dict(app.status())
    except Exception as error:  # noqa: BLE001
        snapshot["app"] = {"error": f"{type(error).__name__}: {error}"}
    snapshot["contract"] = _contract_of(app)
    try:
        snapshot["registry"] = registry_summary(getattr(app, "container", app))
    except Exception as error:  # noqa: BLE001
        snapshot["registry"] = {"error": f"{type(error).__name__}: {error}"}
    snapshot["pumps"] = [pump.status() for pump in getattr(app, "pumps", [])]
    snapshot["domains"] = {name: domain is not None for name, domain in (getattr(app, "domains", {}) or {}).items()}
    if options.include_tables:
        snapshot["tables"] = table_counts(app, limit=options.table_limit)
    return snapshot


__all__ = [
    "EVENTS",
    "EVENT_BUFFER_SIZE",
    "FREE_TEXT_KEEP",
    "FREE_TEXT_KEYS",
    "SNAPSHOT_VERSION",
    "EventRing",
    "SnapshotOptions",
    "build_snapshot",
    "registry_summary",
    "table_counts",
]
