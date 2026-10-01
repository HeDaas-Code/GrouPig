"""memory 域验收测试：20 个契约 rpc 全走一遍，验证六类存储落库与可检索。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from grouppig.infra.logger import RuntimeLogger
from grouppig.infra.model_gateway.router import ModelRouter
from grouppig.infra.runtime import contract
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.di import Container
from grouppig.infra.runtime.registry import Registry
from grouppig.infra.token_budget.meter import TokenMeter
from grouppig.memory.runtime.di import MEMORY_RPC, attach_memory, get_memory
from grouppig.memory.runtime.errors import StoreError
from memory_helpers import (
    BASE_TS,
    GROUP_ID,
    SENDER_IDS,
    chat_burst,
    message,
)


#: 一次会话回放：6 条消息（含刷屏）、1 条聊天线、2 个档案、2 条社交边、1 条黑话。
def _session_messages() -> list[dict]:
    rows = chat_burst(4, content="打本打本，缺一个奶妈", base_ts=BASE_TS)
    rows.append(message(4, content="我奶妈，带我", base_ts=BASE_TS, ts=BASE_TS + 12, sender_id=SENDER_IDS[2]))
    rows.append(message(5, content="那今晚八点", base_ts=BASE_TS, ts=BASE_TS + 15, sender_id=SENDER_IDS[0]))
    return rows


@pytest.fixture
async def wired(tmp_path: Path, config):
    """容器 + 文件库 SQLite（走真实 DSN 路径），memory 已挂载。"""

    registry = Registry()
    logger = RuntimeLogger()
    container = Container(
        config=config,
        registry=registry,
        logger=logger,
        bus=EventBus(logger=logger, strict_topics=True),
        meter=TokenMeter(logger=logger, config=config),
        router=ModelRouter(config, logger=logger, meter=TokenMeter(logger=logger, config=config)),
    )
    await container.start()
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'acceptance.db'}"
    stores = await attach_memory(container, dsn=dsn)
    try:
        yield container, stores, tmp_path / "acceptance.db"
    finally:
        await container.aclose()


async def test_all_twenty_contract_rpcs_are_callable(wired):
    memory_container, _stores, _path = wired
    for name in MEMORY_RPC:
        assert memory_container.registry.has(name), name
        assert contract.owner(name).startswith("grouppig.memory")
    check = contract.check_registry(set(memory_container.registry.names()), scope="grouppig.memory")
    assert [name for name in check["missing"] if not name.startswith("mysql:")] == []
    assert check["unknown"] == []


async def test_full_memory_loop_end_to_end(wired):
    memory_container, stores, db_path = wired
    messages = _session_messages()

    # 1) 消息进 → 聊天流水落库（去重）
    for row in messages:
        assert (await memory_container.call("rpc:chat.append", row))["created"] is True
    assert (await memory_container.call("rpc:chat.append", messages[0]))["duplicate"] is True
    assert (await memory_container.call("rpc:chat.query", {"group_id": GROUP_ID}))["count"] == 6

    # 2) 时间窗索引：刷屏可被快速切片
    window = await memory_container.call("rpc:chat.window", GROUP_ID, seconds=30, now=BASE_TS + 20, limit=20)
    assert window["count"] == 6
    assert window["window"]["message_count"] == 6
    assert window["window"]["repeat_max"] == 4
    assert window["window"]["top_content"] == "打本打本，缺一个奶妈"
    assert (await memory_container.call("rpc:chat.window.advance", GROUP_ID, now=BASE_TS + 20))["window"][
        "message_count"
    ] >= 0
    assert (await memory_container.call("rpc:chat.window.prune", now=BASE_TS + 20, keep_seconds=10**6))["pruned"] == 0

    # 3) 消息挂到会话/话题/聊天线
    message_ids = [row["message_id"] for row in messages]
    assert await stores.chat.link(message_ids, session_id="s-1", topic_id="tp-1", thread_id="th-1") == 6

    # 4) 聊天线保存 + 跨会话检索
    await memory_container.call(
        "rpc:thread.save",
        {
            "thread_id": "th-1",
            "group_id": GROUP_ID,
            "session_id": "s-1",
            "topic_id": "tp-1",
            "title": "今晚打本",
            "summary": "约打本，缺奶妈",
            "participants": list(SENDER_IDS),
            "message_ids": message_ids,
            "first_ts": BASE_TS,
            "last_ts": BASE_TS + 15,
            "embedding": [1.0, 0.0],
        },
        edges=[{"parent_id": "th-0", "edge_type": "branch"}],
    )
    loaded = await memory_container.call("rpc:thread.load", "s-1", with_edges=True)
    assert loaded["count"] == 1
    assert loaded["threads"][0]["edges"][0]["parent_id"] == "th-0"
    cross = await memory_container.call(
        "rpc:thread.find-cross", keywords=["打本"], query_embedding=[1.0, 0.0], recency_weight=0.0
    )
    assert cross["threads"][0]["thread_id"] == "th-1"

    # 5) 画像与事实：新事实顶替旧事实
    await memory_container.call(
        "rpc:profile-store.put",
        {"user_id": SENDER_IDS[2], "nickname": "奶妈", "group_ids": [GROUP_ID], "last_seen": BASE_TS},
        facts=[
            {"fact_key": "role", "fact_value": "辅助", "category": "skill", "confidence": 0.6},
            {"fact_key": "city", "fact_value": "杭州", "category": "identity", "confidence": 0.7},
        ],
    )
    updated = await memory_container.call(
        "rpc:profile-store.put",
        {"user_id": SENDER_IDS[2], "nickname": "奶妈", "last_seen": BASE_TS + 15},
        facts=[{"fact_key": "role", "fact_value": "奶妈", "category": "skill", "confidence": 0.9}],
    )
    assert updated["superseded"] == 1
    profile = await memory_container.call("rpc:profile-store.get", SENDER_IDS[2])
    assert profile["profile"]["version"] == 2
    assert {fact["fact_key"] for fact in profile["facts"]} == {"role", "city"}
    assert {fact["fact_value"] for fact in profile["facts"] if fact["fact_key"] == "role"} == {"奶妈"}

    # 6) 社交网与关系分
    edge = await memory_container.call(
        "rpc:social-store.put-edge",
        {
            "src_id": 0,
            "dst_id": SENDER_IDS[2],
            "group_id": GROUP_ID,
            "edge_type": "mention",
            "weight": 1.5,
            "score_delta": 25.0,
            "factors": {"affinity": 2.0},
        },
    )
    assert edge["score"]["tier"] == "acquaintance"
    edges = await memory_container.call("rpc:social-store.get-edges", src_id=0)
    assert edges["count"] == 1
    assert edges["edges"][0]["weight"] == 1.5

    # 7) 会话结束 → 反思摘要落档 + 可检索
    summary = await memory_container.call(
        "rpc:archive.summarize",
        "s-1",
        messages=messages,
        group_id=GROUP_ID,
        conclusion="约好今晚八点打本",
        review={"metrics": {"message_count": 6}},
        embedding=[1.0, 0.0],
    )
    assert summary["saved"] is True
    assert summary["summary"]["message_count"] == 6
    assert summary["summary"]["participants"] == list(SENDER_IDS)
    archive = await memory_container.call("rpc:archive.load", "s-1")
    assert archive["archive"]["conclusion"] == "约好今晚八点打本"
    found = await memory_container.call("rpc:archive.find", keywords=["打本"], recency_weight=0.0)
    assert found["archives"][0]["session_id"] == "s-1"

    # 8) 黑话库：学习 → 查询 → 使用刷新 → 长期不用衰减
    entry = await memory_container.call(
        "rpc:slang.upsert",
        {
            "term": "打本",
            "meaning": "组队打副本",
            "usage_context": "约游戏",
            "group_id": GROUP_ID,
            "examples": ["今晚打本吗"],
        },
    )
    assert entry["entry"]["freshness"] == 1.0
    assert (await memory_container.call("rpc:slang.lookup", "打本", group_id=GROUP_ID))["count"] == 1
    refreshed = await memory_container.call("rpc:slang.refresh", "打本", group_id=GROUP_ID, now=BASE_TS + 10 * 86400)
    assert refreshed["refreshed"] == 1
    assert refreshed["entries"][0]["use_count"] == 1
    decayed = await memory_container.call("rpc:slang.decay", now=BASE_TS + 200 * 86400, group_id=GROUP_ID)
    assert decayed["counts"]["retired"] == 1
    assert (await memory_container.call("rpc:slang.lookup", group_id=GROUP_ID, status=None))["count"] == 1

    # 9) 落盘与健康检查
    assert db_path.is_file()
    health = await stores.health()
    assert health["contract"]["missing"] == []
    assert health["contract"]["unknown"] == []
    assert health["rows"] == {
        "chat_messages": 6,
        "chat_threads": 1,
        "member_profiles": 1,
        "social_edges": 1,
        "slang_entries": 1,
    }
    assert json.loads(json.dumps(health, ensure_ascii=False))  # 可 JSON 序列化（事件总线 payload 要求）


async def test_memory_survives_container_restart(tmp_path: Path, config):
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'restart.db'}"

    def make_container() -> Container:
        logger = RuntimeLogger()
        return Container(
            config=config,
            registry=Registry(),
            logger=logger,
            bus=EventBus(logger=logger, strict_topics=True),
            meter=TokenMeter(logger=logger, config=config),
            router=ModelRouter(config, logger=logger, meter=TokenMeter(logger=logger, config=config)),
        )

    first = make_container()
    await first.start()
    await attach_memory(first, dsn=dsn)
    await first.call("rpc:chat.append", message(0))
    await first.call("rpc:slang.upsert", {"term": "打本", "group_id": GROUP_ID})
    await first.aclose()

    second = make_container()
    await second.start()
    reopened = await attach_memory(second, dsn=dsn, migrate=False)
    assert (await second.call("rpc:chat.query", {"group_id": GROUP_ID}))["count"] == 1
    assert (await second.call("rpc:slang.lookup", "打本", group_id=GROUP_ID))["count"] == 1
    assert (await reopened.health())["contract"]["missing"] == []
    await second.aclose()


async def test_get_memory_requires_attach(config):
    logger = RuntimeLogger()
    container = Container(
        config=config,
        registry=Registry(),
        logger=logger,
        bus=EventBus(logger=logger, strict_topics=True),
        meter=TokenMeter(logger=logger, config=config),
        router=ModelRouter(config, logger=logger, meter=TokenMeter(logger=logger, config=config)),
    )
    with pytest.raises(StoreError):
        get_memory(container)
    stores = await attach_memory(container, dsn="sqlite+aiosqlite:///:memory:")
    assert get_memory(container) is stores
    await container.aclose()


async def test_infra_container_still_reports_memory_handlers(wired):
    memory_container, _stores, _path = wired
    health = memory_container.health()
    # infra 子树的 rpc 名字数从契约推导：以后 infra 子树 api_add/api_remove 都不必手改这里。
    infra_rpc = {
        name
        for name in contract.api_index()
        if name.startswith("rpc:") and (contract.owner(name) or "").startswith("grouppig.infra")
    }
    registered_rpc = {name for name in memory_container.registry.names() if name.startswith("rpc:")}
    assert health["registry"]["rpc"] == len(infra_rpc) + len(MEMORY_RPC)
    # 原意未削弱：infra 子树 + 已 attach 的 memory 处理器 = 注册表里的全部 rpc 名字（精确相等，不是下界）。
    assert registered_rpc == infra_rpc | set(MEMORY_RPC)
    assert health["registry"]["unknown"] == []
    assert health["contract"]["mysql"] == 10


async def test_archive_and_slang_rpc_payloads_are_json_serializable(memory_container):
    """两张表相关的 rpc 返回必须能原样 JSON 往返（事件总线 payload 要求）。"""

    await memory_container.call(
        "rpc:archive.save",
        {"session_id": "s-json", "group_id": GROUP_ID, "keywords": ["打本"], "summary": "约打本", "ended_at": BASE_TS},
    )
    await memory_container.call(
        "rpc:slang.upsert",
        {"term": "打本", "group_id": GROUP_ID, "meaning": "组队打副本", "examples": ["今晚打本吗"]},
    )
    payloads = [
        await memory_container.call("rpc:archive.save", {"session_id": "s-json-2", "group_id": GROUP_ID}),
        await memory_container.call("rpc:archive.load", "s-json"),
        await memory_container.call("rpc:archive.summarize", "s-json"),
        await memory_container.call("rpc:archive.find", keywords=["打本"]),
        await memory_container.call("rpc:slang.lookup", "打本", group_id=GROUP_ID),
        await memory_container.call("rpc:slang.upsert", {"term": "开黑", "group_id": GROUP_ID}),
        await memory_container.call("rpc:slang.refresh", "打本", group_id=GROUP_ID),
        await memory_container.call("rpc:slang.decay", group_id=GROUP_ID),
    ]
    for payload in payloads:
        assert json.loads(json.dumps(payload, ensure_ascii=False)) == payload
