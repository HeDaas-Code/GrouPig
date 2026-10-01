"""memory 单连接引擎的并发回归测试（t15 竞态根因）。

背景：``sqlite+aiosqlite:///:memory:`` 走 ``StaticPool``，全进程共用**一条**
``AsyncConnection``。一条连接上同时开两个事务时，后开的 ``BEGIN`` 会落在前一个事务
中间，谁的 ``COMMIT`` / ``ROLLBACK`` 先到，谁就把对方未提交的写一起提交或回滚掉——
表现为「写返回成功，但查不到这一行」。

t15 的端到端偶发失败就是这个：测试任务走 ``rpc:threads.save``（chat_threads 事务），
网关投递泵任务同时走 ``perception.ingest`` → ``rpc:chat.append``（chat_messages 事务），
两条事务在同一条连接上交错，``chat_messages`` 里就会少行，用例在 10s 轮询里超时。

这里的用例不依赖墙钟：用 ``asyncio.sleep(0)``（纯让出点，无时长）把两个任务卡在
「都在事务里」的位置，直接断言**事务不会交错**。
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from sqlalchemy import select

from grouppig.memory.chat_store.schema import chat_messages
from grouppig.memory.runtime.stores import MemoryStores
from memory_helpers import MEMORY_DSN


def _row(message_id: str, *, sender_id: int) -> dict[str, object]:
    return {
        "message_id": message_id,
        "group_id": 100,
        "sender_id": sender_id,
        "sender_name": f"member-{sender_id}",
        "role": "member",
        "msg_type": "text",
        "content": f"hello {message_id}",
        "ts": 1700000000.0,
    }


async def test_concurrent_transactions_never_interleave_on_one_connection() -> None:
    """两个任务同时开事务时，必须一个完整跑完再进下一个（否则会互相吞掉写入）。"""

    stores = MemoryStores.from_dsn(MEMORY_DSN)
    await stores.migrate()
    try:
        trace: list[str] = []

        async def worker(tag: str, sender_id: int) -> None:
            async with stores.db.begin() as conn:
                trace.append(f"{tag}:enter")
                # 纯让出点：给另一个任务「插进同一个事务」的机会。
                await asyncio.sleep(0)
                await conn.execute(chat_messages.insert().values(**_row(tag, sender_id=sender_id)))
                await asyncio.sleep(0)
                trace.append(f"{tag}:exit")

        await asyncio.gather(worker("a", 1), worker("b", 2))

        # 因果：任何交错都会让 trace 变成 a:enter, b:enter, ... —— 那就是两条事务
        # 共用了同一条连接。串行化之后只有两种合法顺序（谁先拿到锁谁先跑完）。
        assert trace in (["a:enter", "a:exit", "b:enter", "b:exit"], ["b:enter", "b:exit", "a:enter", "a:exit"]), (
            f"两条事务在同一条连接上交错了：{trace}"
        )

        rows = await stores.db.fetch_all(select(chat_messages).order_by(chat_messages.c.id))
        assert sorted(row["message_id"] for row in rows) == ["a", "b"], f"有写入被另一个事务吞掉：{rows}"
    finally:
        await stores.aclose()


async def test_concurrent_dao_writes_all_survive() -> None:
    """DAO 层并发写：每条都必须留在库里（原来会静默丢行）。"""

    stores = MemoryStores.from_dsn(MEMORY_DSN)
    await stores.migrate()
    try:
        total = 12

        async def append(index: int) -> None:
            await stores.chat.append(_row(f"m{index}", sender_id=200 + index % 2))

        async def bump(index: int) -> None:
            # 另一张表上的事务，制造「两条不同事务抢同一条连接」。
            await stores.db.upsert(
                chat_messages,
                _row(f"u{index}", sender_id=300),
                index_elements=("message_id",),
            )

        await asyncio.gather(*(append(index) for index in range(total)), *(bump(index) for index in range(total)))

        rows = await stores.db.fetch_all(select(chat_messages))
        assert len(rows) == total * 2, f"并发写丢了行：期望 {total * 2}，实得 {len(rows)}"
    finally:
        await stores.aclose()


def test_gate_is_enabled_only_for_single_connection_engines() -> None:
    """只有「全进程一条连接」的引擎才需要串行化。

    因果：MySQL 与 SQLite 文件库走真实连接池，每个事务各拿一条连接，两条事务物理上
    不可能落在同一条连接上，也就不会互相提交/回滚。若在那里也开锁，只是白白引入
    全局串行化（还会掩盖真正的连接池配置问题）。所以门控必须按引擎判定，而不是
    「只要是 Database 就锁」。
    """

    from grouppig.memory.runtime.db import Database, is_single_connection_engine

    memory = Database.from_dsn(MEMORY_DSN)
    assert is_single_connection_engine(memory.engine) is True
    assert memory._gate.enabled is True

    with tempfile.TemporaryDirectory() as tmp:
        file_db = Database.from_dsn(f"sqlite+aiosqlite:///{Path(tmp) / 'probe.db'}")
        assert is_single_connection_engine(file_db.engine) is False
        assert file_db._gate.enabled is False


async def test_disabled_gate_does_not_serialize() -> None:
    """门控关闭时是直通：不排队、不改变行为。"""

    from contextlib import asynccontextmanager as _acm

    from grouppig.memory.runtime.db import _ConnectionGate

    gate = _ConnectionGate(enabled=False)
    order: list[str] = []

    @_acm
    async def no_connection():
        yield object()

    async def worker(tag: str) -> None:
        async with gate.hold(no_connection):
            order.append(f"{tag}:enter")
            await asyncio.sleep(0)
            order.append(f"{tag}:exit")

    await asyncio.gather(worker("a"), worker("b"))
    assert order == ["a:enter", "b:enter", "a:exit", "b:exit"], order


async def test_gate_is_reentrant_within_one_task() -> None:
    """同一个任务在事务里再调数据库方法不能自锁死。

    因果：``Database.insert`` / ``upsert`` 会在自己开的事务里回调 ``_row_by_pk`` /
    ``_row_by_index``，DAO 也可能在 ``begin()`` 里再调 ``fetch_one``。若门控对同一任务
    也重新取锁，就会自己等自己 —— 那是比原来的竞态更严重的故障。这里用
    ``wait_for`` 把它钉死在「必须很快返回」。
    """

    stores = MemoryStores.from_dsn(MEMORY_DSN)
    await stores.migrate()
    try:

        async def nested() -> None:
            async with stores.db.begin() as conn:
                await conn.execute(chat_messages.insert().values(**_row("nested", sender_id=1)))
                # 同一任务、同一条连接上的再次取用（重入路径）。
                found = await stores.db.fetch_one(select(chat_messages).where(chat_messages.c.message_id == "nested"))
                assert found is not None
                # DAO 自己开事务的路径也要能重入。
                await stores.chat.append(_row("nested-2", sender_id=2))

        await asyncio.wait_for(nested(), timeout=5.0)

        rows = await stores.db.fetch_all(select(chat_messages).order_by(chat_messages.c.id))
        assert [row["message_id"] for row in rows] == ["nested", "nested-2"]
    finally:
        await stores.aclose()
