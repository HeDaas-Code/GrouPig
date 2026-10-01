"""保留策略泵：把三个「写好了但没人调」的清理接口接上时钟。

这三个名字在设计树里都注册了处理器，却**没有任何生产调用方**：

* ``rpc:chat.window.prune`` —— 没人调它，``chat_window_index`` 只增不减；
* ``rpc:slang.decay`` —— 没人调它，黑话库只读不衰；
* ``rpc:relationship.decay`` —— 没人调它，「最近」这个语义不存在。

本文件钉住：泵真的会按时敲这三个门，而且敲完**数据真的变少了**。
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from grouppig.infra.runtime.errors import HandlerNotRegistered
from grouppig.runtime.pumps import (
    RPC_CHAT_WINDOW_PRUNE,
    RPC_RELATIONSHIP_DECAY,
    RPC_SLANG_DECAY,
    MaintenancePump,
)


class _Recorder:
    """按名字返回预设结果的假容器（记录调用顺序）。"""

    def __init__(self, results: dict[str, Any] | None = None, missing: bool = False) -> None:
        self.results = dict(results or {})
        self.missing = missing
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((name, kwargs))
        if self.missing:
            raise HandlerNotRegistered(name)
        return self.results.get(name)

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


async def test_maintenance_pump_drives_all_three_cleanup_interfaces():
    """一轮巡检必须敲满三个门（顺序无关，但三个都要到）。"""

    container = _Recorder(
        {
            RPC_CHAT_WINDOW_PRUNE: 7,
            RPC_SLANG_DECAY: {"counts": {"active": 1, "stale": 2, "retired": 3}},
            RPC_RELATIONSHIP_DECAY: {"decayed": 5},
        }
    )
    pump = MaintenancePump(container, interval=3600.0)

    result = await pump.maintain_once()

    assert sorted(container.names()) == sorted([RPC_CHAT_WINDOW_PRUNE, RPC_SLANG_DECAY, RPC_RELATIONSHIP_DECAY])
    assert result["pruned"] == 7
    assert result["slang_retired"] == 3
    assert result["relationships_decayed"] == 5
    assert result["calls"] == {"ok": 3, "skipped": 0, "failed": 0, "errors": []}
    assert pump.stats["missing"] == 0


async def test_maintenance_pump_forwards_the_configured_limits():
    """保留时长与单轮上限必须真的传下去，否则配置是装饰品。"""

    container = _Recorder()
    pump = MaintenancePump(container, keep_seconds=3600.0, decay_limit=42)

    await pump.maintain_once()

    by_name = dict(container.calls)
    assert by_name[RPC_CHAT_WINDOW_PRUNE]["keep_seconds"] == 3600.0
    assert by_name[RPC_SLANG_DECAY]["limit"] == 42
    assert by_name[RPC_RELATIONSHIP_DECAY]["limit"] == 42


async def test_maintenance_pump_omits_keep_seconds_when_unset():
    """未配置保留时长时不传该参数，让记忆层用自己的默认值。"""

    container = _Recorder()
    pump = MaintenancePump(container, keep_seconds=None)

    await pump.maintain_once()

    assert "keep_seconds" not in dict(container.calls)[RPC_CHAT_WINDOW_PRUNE]


async def test_missing_handlers_degrade_without_failing_the_round():
    """任一名字未注册只记 skipped，不影响其余清理动作，也不抛错。"""

    pump = MaintenancePump(_Recorder(missing=True))

    result = await pump.maintain_once()

    assert result["calls"]["skipped"] == 3
    assert result["calls"]["failed"] == 0
    assert result["pruned"] == 0
    assert pump.stats["missing"] == 3


async def test_maintenance_pump_does_not_run_a_first_tick_on_start():
    """保留策略是慢活，不该在启动路径上先删一遍库、把就绪时间往后推。"""

    container = _Recorder()
    pump = MaintenancePump(container, interval=3600.0)
    pump.start(immediate=False)
    try:
        assert pump.running is True
        assert container.calls == [], "首拍必须被跳过"
    finally:
        await pump.stop()


# ---- 生产后果：真的会少东西 ------------------------------------------------
async def test_prune_actually_removes_expired_window_buckets(stores):
    """端到端：过期窗口桶真的被删掉（而不只是「调用了这个方法」）。

    这是本泵存在的理由——没有它，``chat_window_index`` 只增不减。
    """

    from grouppig.memory.chat_store.window_index import WindowIndex

    index = WindowIndex(stores.db)
    now = time.time()
    # 两个相隔很远的窗口桶：一个早已过期，一个是当前窗口
    await index.advance(1, message={"message_id": 1, "ts": now - 100000, "text": "很久以前"}, now=now - 100000)
    await index.advance(1, message={"message_id": 2, "ts": now, "text": "刚刚"}, now=now)

    before = await index.count(group_id=1)
    assert before >= 2

    removed = await index.prune(keep_seconds=3600.0, now=now)
    assert removed >= 1

    after = await index.count(group_id=1)
    assert after == before - removed
    assert after >= 1, "当前窗口的桶不该被删掉"


async def test_app_exposes_the_maintenance_pump_and_can_disable_it():
    """集成层：泵出现在 ``app.pumps`` 里，且可以被配置关掉。"""

    from grouppig.runtime.app import build_app

    app = build_app(
        config_path=None,
        dsn="sqlite+aiosqlite:///:memory:",
        connect=False,
        registry=_isolated_registry(),
    )
    try:
        await app.start()
        assert MaintenancePump.name in [pump.name for pump in app.pumps]
    finally:
        await app.aclose()

    off = build_app(
        config_path=None,
        dsn="sqlite+aiosqlite:///:memory:",
        connect=False,
        maintenance=False,
        registry=_isolated_registry(),
    )
    try:
        await off.start()
        assert MaintenancePump.name not in [pump.name for pump in off.pumps]
    finally:
        await off.aclose()


def _isolated_registry() -> Any:
    """独立注册表：装配会注册整棵树的名字，别污染全局 default_registry。"""

    from grouppig.infra.runtime.registry import Registry

    return Registry()


@pytest.fixture
async def stores():
    """建好表的 SQLite 内存库（六类存储全装配）。"""

    from grouppig.memory.runtime.stores import MemoryStores

    instance = MemoryStores.from_dsn("sqlite+aiosqlite:///:memory:")
    await instance.migrate()
    try:
        yield instance
    finally:
        await instance.aclose()
