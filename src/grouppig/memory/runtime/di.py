"""grouppig.memory.runtime.di —— memory 域装配入口（20 个 ``rpc:`` 处理器注册）。

用法（集成入口 t10 只需两行）::

    from grouppig.memory.runtime.di import attach_memory

    stores = await attach_memory(container)          # 按 config.storage.dsn 建库 + 建表 + 注册处理器
    await container.call("rpc:chat.append", message) # 之后即可按契约名调用

注册的 20 个名字逐字来自 ``normify-grouppig/api-index.json``，
且每个处理器都登记为设计树里该名字的归属模块（``contract.owner(name)``），
便于 :func:`grouppig.infra.runtime.contract.check_registry` 按域校验。

normify id: ``grouppig.memory.runtime.di``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.registry import Registry
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.runtime.stores import MemoryStores

#: memory 域注册的 20 个 ``rpc:`` 名字（= api-index.json 中归属 grouppig.memory 的全部 rpc 名字）。
MEMORY_RPC: tuple[str, ...] = tuple(
    sorted(n for n, m in contract.api_index().items() if m.startswith("grouppig.memory") and n.startswith("rpc:"))
)

#: memory 域契约表名（10 张，``mysql:`` 协议）。
MEMORY_TABLES: tuple[str, ...] = tuple(
    sorted(
        n.split(":", 1)[1]
        for n, m in contract.api_index().items()
        if m.startswith("grouppig.memory") and n.startswith("mysql:")
    )
)


def _registry_of(target: Any) -> Registry:
    """接受 ``Container`` 或 ``Registry``。"""

    if isinstance(target, Registry):
        return target
    registry = getattr(target, "registry", None)
    if not isinstance(registry, Registry):
        raise StoreError(f"attach/register 需要 Container 或 Registry，得到 {type(target).__name__}")
    return registry


def memory_handlers(stores: MemoryStores) -> dict[str, Any]:
    """名字 → 处理器（全部为协程函数，入参与返回均为可 JSON 序列化的值）。"""

    # grouppig.memory.chat-store.dao → rpc:chat.append / rpc:chat.query / rpc:chat.window
    async def chat_append(message: Mapping[str, Any]) -> dict[str, Any]:
        return await stores.chat.append(message)

    async def chat_query(criteria: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        params = {**(criteria or {}), **kwargs}
        messages = await stores.chat.query(params)
        return {"messages": messages, "count": len(messages), "criteria": {k: v for k, v in params.items()}}

    async def chat_window(group_id: int, **kwargs: Any) -> dict[str, Any]:
        return await stores.chat.window(int(group_id), **kwargs)

    # grouppig.memory.chat-store.window-index → rpc:chat.window.advance / rpc:chat.window.prune
    async def chat_window_advance(group_id: int, **kwargs: Any) -> dict[str, Any]:
        window = await stores.chat_window.advance(int(group_id), **kwargs)
        return {"window": window}

    async def chat_window_prune(**kwargs: Any) -> dict[str, Any]:
        return {"pruned": await stores.chat_window.prune(**kwargs)}

    # grouppig.memory.thread-store.dao → rpc:thread.save / rpc:thread.load / rpc:thread.find-cross
    async def thread_save(thread: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
        return await stores.threads.save(thread, **kwargs)

    async def thread_load(session_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        threads = await stores.threads.load(session_id, **kwargs)
        return {"threads": threads, "count": len(threads)}

    async def thread_find_cross(**kwargs: Any) -> dict[str, Any]:
        threads = await stores.threads.find_cross(**kwargs)
        return {"threads": threads, "count": len(threads)}

    # grouppig.memory.profile-store.dao → rpc:profile-store.get / rpc:profile-store.put
    async def profile_get(user_id: int, **kwargs: Any) -> dict[str, Any]:
        profile = await stores.profiles.get(int(user_id), **kwargs)
        return {"profile": profile, "facts": (profile or {}).get("facts", [])}

    async def profile_put(profile: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
        return await stores.profiles.put(profile, **kwargs)

    # grouppig.memory.social-store.dao → rpc:social-store.get-edges / rpc:social-store.put-edge
    async def social_get_edges(**kwargs: Any) -> dict[str, Any]:
        edges = await stores.social.get_edges(**kwargs)
        return {"edges": edges, "count": len(edges)}

    async def social_put_edge(edge: Mapping[str, Any]) -> dict[str, Any]:
        return await stores.social.put_edge(edge)

    # grouppig.memory.session-archive.dao → rpc:archive.save / rpc:archive.load
    async def archive_save(archive: Mapping[str, Any]) -> dict[str, Any]:
        return {"archive": await stores.archives.save(archive)}

    async def archive_load(session_id: str) -> dict[str, Any]:
        return {"archive": await stores.archives.load(str(session_id))}

    # grouppig.memory.session-archive.summary-index → rpc:archive.summarize / rpc:archive.find
    async def archive_summarize(session_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        return await stores.summaries.summarize(session_id, **kwargs)

    async def archive_find(**kwargs: Any) -> dict[str, Any]:
        archives = await stores.summaries.find(**kwargs)
        return {"archives": archives, "count": len(archives)}

    # grouppig.memory.slang-kb.dictionary → rpc:slang.lookup / rpc:slang.upsert
    async def slang_lookup(term: str | None = None, **kwargs: Any) -> dict[str, Any]:
        entries = await stores.slang.lookup(term, **kwargs)
        return {"entries": entries, "count": len(entries)}

    async def slang_upsert(entry: Mapping[str, Any]) -> dict[str, Any]:
        return {"entry": await stores.slang.upsert(entry)}

    # grouppig.memory.slang-kb.freshness → rpc:slang.decay / rpc:slang.refresh
    async def slang_decay(**kwargs: Any) -> dict[str, Any]:
        return await stores.slang_freshness.decay(**kwargs)

    async def slang_refresh(term: str | None = None, **kwargs: Any) -> dict[str, Any]:
        return await stores.slang_freshness.refresh(term, **kwargs)

    return {
        "rpc:archive.find": archive_find,
        "rpc:archive.load": archive_load,
        "rpc:archive.save": archive_save,
        "rpc:archive.summarize": archive_summarize,
        "rpc:chat.append": chat_append,
        "rpc:chat.query": chat_query,
        "rpc:chat.window": chat_window,
        "rpc:chat.window.advance": chat_window_advance,
        "rpc:chat.window.prune": chat_window_prune,
        "rpc:profile-store.get": profile_get,
        "rpc:profile-store.put": profile_put,
        "rpc:slang.decay": slang_decay,
        "rpc:slang.lookup": slang_lookup,
        "rpc:slang.refresh": slang_refresh,
        "rpc:slang.upsert": slang_upsert,
        "rpc:social-store.get-edges": social_get_edges,
        "rpc:social-store.put-edge": social_put_edge,
        "rpc:thread.find-cross": thread_find_cross,
        "rpc:thread.load": thread_load,
        "rpc:thread.save": thread_save,
    }


def register_memory_handlers(target: Any, stores: MemoryStores, *, replace: bool = True) -> Registry:
    """把 memory 的 20 个 ``rpc:`` 处理器注册进容器 / 注册表。"""

    registry = _registry_of(target)
    handlers = memory_handlers(stores)
    missing = set(MEMORY_RPC) - set(handlers)
    if missing:  # pragma: no cover - 静态自查
        raise StoreError(f"memory 处理器未覆盖契约名字：{sorted(missing)}")
    for name, handler in handlers.items():
        registry.register(name, handler, module=contract.owner(name), replace=replace)
    return registry


async def attach_memory(
    container: Any,
    *,
    dsn: str | None = None,
    config: Any | None = None,
    migrate: bool = True,
    echo: bool = False,
    window_seconds: int | None = None,
    **kwargs: Any,
) -> MemoryStores:
    """按容器配置建库 → 建表 → 注册处理器 → 挂到 ``container.memory``。"""

    cfg = config if config is not None else getattr(container, "config", None)
    if dsn:
        stores = MemoryStores.from_dsn(dsn, echo=echo, **kwargs)
    elif cfg is not None:
        stores = MemoryStores.from_config(cfg, echo=echo, **kwargs)
    else:
        raise StoreError("attach_memory 需要 dsn 或带 config 的容器")
    if window_seconds is not None:
        stores.window_seconds = int(window_seconds)
    if migrate:
        await stores.migrate()
    register_memory_handlers(container, stores)
    if container is not None:
        container.memory = stores
    logger = getattr(container, "logger", None)
    if logger is not None:
        logger.info(
            "memory.attached",
            handlers=len(MEMORY_RPC),
            tables=len(MEMORY_TABLES),
            dsn=stores.db.dsn,
        )
    return stores


def get_memory(container: Any) -> MemoryStores:
    """取容器上已挂载的 memory 存储；未挂载抛错。"""

    stores = getattr(container, "memory", None)
    if stores is None:
        raise StoreError("容器未挂载 memory 存储：请先 await attach_memory(container)")
    return stores


__all__ = [
    "MEMORY_RPC",
    "MEMORY_TABLES",
    "attach_memory",
    "get_memory",
    "memory_handlers",
    "register_memory_handlers",
]
