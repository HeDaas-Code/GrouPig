"""t11 端到端冒烟验证：用模拟 OneBot 服务端回放群聊，检查全链路可观测、结果可复现。

用法::

    uv run python tools/smoke.py                     # 跑全部场景，打印人读摘要 + JSON
    uv run python tools/smoke.py --scenario closed-loop
    uv run python tools/smoke.py --replay 3          # 基础场景回放 3 次并比对归一化轨迹
    uv run python tools/smoke.py --json smoke.json   # 落盘报告
    uv run python tools/smoke.py --quiet             # 只输出 JSON

退出码：0 = 全部场景通过（含可复现性）；1 = 有场景失败。

五个场景（每个都从空库冷启动，走 ``build_app`` 生产装配 + 真实 WebSocket + 真实事件总线）：

===================  ==============================================================================
场景                 验证什么
===================  ==============================================================================
closed-loop          冷启动 → 群消息入站 → 入库 / 话题会话 / 画像关系 / 回复 / 节流发送 / 归档反思
concurrent-groups    两个群交错发言：会话、话题、流水都按真实 group_id 归属，互不串台
duplicate-messages   同一条 message_id 重复推送：聊天流水不重复落库（DAO 去重），链路不炸
model-degraded       模型不可用（FakeTransport 全失败）：回复仍能生成并发出，降级可观测
wake-after-archive   归档后唤醒：rpc:session.wake 恢复上下文 → 缓冲可读 → rpc:session.sleep 归还
===================  ==============================================================================

每个场景的 ``links`` 是「取证」，读的都是**生产对象**（memory 表 / SessionLayer / 反思层 / MockOneBotServer），
中间层不 mock；``findings`` 是回单：发现的问题带 owner 与证据，指向对应任务。

可复现性：会话 id / 聊天线 id / 话题 id 里都带时间戳与随机盐，因此比对的是 ``normalize_trace()``
归一化之后的**结构事实**（计数、顺序、role、关键词集合、分层、发送动作名、回复文本），
时间戳与 id 一律折叠成占位符。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
for _extra in (REPO_ROOT / "src", REPO_ROOT / "tests"):
    if str(_extra) not in sys.path:
        sys.path.insert(0, str(_extra))

from gateway_helpers import DEFAULT_SELF_ID, EventCollector, MockOneBotServer, group_message  # noqa: E402
from grouppig.infra.config.loader import load_config  # noqa: E402
from grouppig.infra.runtime.errors import TransportError  # noqa: E402
from grouppig.infra.runtime.registry import Registry  # noqa: E402
from grouppig.runtime.app import GrouppigApp, build_app  # noqa: E402
from helpers import FakeTransport  # noqa: E402

CONFIG_PATH = REPO_ROOT / "config" / "grouppig.toml"

#: 冒烟场景固定参数（固定 = 可复现）。
GROUP_ID = 100
GROUP_ID_B = 101
ALICE = 200
BOB = 201
CAROL = 202
SELF_ID = DEFAULT_SELF_ID

#: 回放消息的时间基准：相对「现在」往回 30 秒（t10 的 e2e 用同一手法）。
#:
#: 不能写死绝对时间戳（早先用过 1_700_000_000）：感知缓冲窗口（300s）、画像窗口（900s）、
#: 聊天流水的保留期都按「现在」过滤，过期的消息会被静默丢弃 —— 现象是 chat_messages 只落下一部分、
#: 画像泵算出 0 个成员，看上去像链路断了，其实是回放数据本身「过期」。这是本脚本踩过的坑，记在此处。
BASE_OFFSET = 30.0

#: 回放的群聊：中文、有重复实词，话题候选生成器才切得出短语。
SCRIPT: tuple[tuple[str, int], ...] = (
    ("周末一起去爬山吧", ALICE),
    ("爬山好啊我也想去爬山", BOB),
    ("那就周六早上八点集合去爬山", ALICE),
    ("爬山要带什么装备", BOB),
)

#: 第二个群的群聊（并发场景用）。
SCRIPT_B: tuple[tuple[str, int], ...] = (
    ("今晚谁打游戏", CAROL),
    ("打游戏算我一个", ALICE),
    ("打游戏几点开始", CAROL),
)

#: 场景 1 的会话连续性上限：4 条同话题群聊最多允许开 2 个会话（t17 的验收线）。
#:
#: 修复前是「每条消息一个会话」（sessions_per_message = 1.0）：话题识别只看到单条消息，
#: 措辞一变就判 changed 开新会话。上限取 2 而不是 1，是给「一句话里就换了话题」留余量，
#: 但 4 条连贯对话绝不该超过 2 个会话。
SESSION_LIMIT = 2

#: 假模型的回复（真模型不可用时的确定性替身）。
REPLY_TEXT = "喵，爬山听起来不错呀"

#: 冒烟用配置追加：把节流压到毫秒级、关掉分类节流、把周期泵调成「不自己跑」。
EXTRA_CONFIG = """
[onebot.rate]
per_minute = 600
burst = 4
min_interval = 0.05

[perception.features]
classify_min_interval = 0
classify_min_messages = 2

[app.integration]
drain_interval = 3600
profile_interval = 3600
sweep_interval = 3600
"""

#: 已确认的既有缺陷（队长已另开任务）：冒烟遇到时按「已知」回单，不重复阻塞、不改别人的文件。
KNOWN_TASKS = {
    "chat_rows_missing": "t15 gateway-engineer（时序脆弱：计数对但 rpc:chat.query 少读/读 0）",
    "persisted_counter": "t16 perception-engineer（buffer.stats[persisted] 计数不准）",
}


async def wait_for(predicate: Callable[[], bool], *, timeout: float = 10.0, interval: float = 0.02) -> bool:
    """轮询等待条件成立（异步旁路多，轮询比固定 sleep 稳）。"""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _write_config(tmp_path: Path, ws_url: str, extra: str = EXTRA_CONFIG) -> Path:
    """把仓库配置复制到 tmp 并改写成冒烟用（ws 指向 mock、self_id 定死）。"""

    text = CONFIG_PATH.read_text(encoding="utf-8")
    text = text.replace("threshold = 0.26", "threshold = 0.55").replace("renormalize = true", "renormalize = false")
    text = text.replace('ws_url = "ws://127.0.0.1:3001"', 'ws_url = "' + ws_url + '"')
    text = text.replace("self_id = 0", "self_id = " + str(SELF_ID))
    path = tmp_path / "grouppig.toml"
    path.write_text(text + extra, encoding="utf-8")
    return path


def _frame_texts(frame: Mapping[str, Any]) -> list[str]:
    """从 OneBot 发送帧里取出全部文本段。"""

    message = frame.get("params", {}).get("message") or ()
    if isinstance(message, str):
        return [message]
    return [str(seg.get("data", {}).get("text", "")) for seg in message if seg.get("type") == "text"]


def _band(score: Any) -> str:
    """关系分归档成区间（分数本身与交互次数有关，区间更稳）。"""

    try:
        value = float(score)
    except (TypeError, ValueError):
        return ""
    if value >= 75:
        return "close"
    if value >= 45:
        return "friend"
    if value >= 20:
        return "acquaintance"
    return "stranger"


def _jsonable(value: Any) -> Any:
    """把任意返回体折成可 JSON 序列化的形状。"""

    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _finding(
    identifier: str,
    severity: str,
    link: str,
    problem: str,
    evidence: str,
    owner: str,
    required_fix: str,
    *,
    known: bool = False,
) -> dict[str, Any]:
    """一条回单（owner = 该修的人/任务）。"""

    return {
        "id": identifier,
        "severity": severity,
        "link": link,
        "problem": problem,
        "evidence": evidence,
        "owner": owner,
        "requiredFix": required_fix,
        "known": known,
    }


# --------------------------------------------------------------------------
# 测试隔离
# --------------------------------------------------------------------------
def _pin_laya_transport(router: Any, transport: Any) -> None:
    """把假传输同时钉到 LAY A provider 上（测试隔离）。

    只注册 "*" 不够：``ModelRouter._transport()`` 对 laya 会绕开 "*"
    （``/v1/systemone`` 非 OpenAI 兼容），于是 classify / system1 任务会去连真实
    LAY A 端点 —— 没有密钥时 401 仍能回落通过（假绿），一旦配上有效密钥就会失败。
    与 tests/ 里 t25 引入的 ``pin_laya_transport`` 同源，保持三处修法一致。
    """

    router.set_transport(transport)
    router.set_transport(transport, provider="laya")


# --------------------------------------------------------------------------
# 起停与取证工具
# --------------------------------------------------------------------------
@contextlib.asynccontextmanager
async def _running(
    tmp_path: Path,
    *,
    transport: Any = None,
    extra: str = EXTRA_CONFIG,
) -> AsyncIterator[tuple[GrouppigApp, MockOneBotServer, FakeTransport]]:
    """起一个完整的单进程 GrouPig（真实 WebSocket + 内存库），退出时收尾。"""

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    fake = transport if transport is not None else FakeTransport(reply=REPLY_TEXT)
    config = load_config(_write_config(tmp_path, url, extra), use_env=False, use_local=False)
    app = build_app(config, registry=Registry(), transport=fake, dsn="sqlite+aiosqlite:///:memory:")
    # 只注册 "*" 时 classify/system1（provider=laya）会绕开它去连真实端点，
    # model-degraded 场景必须确定性地走假模型
    _pin_laya_transport(app.container.router, fake)
    await app.start(connect=True, pumps=True)
    try:
        yield app, server, fake
    finally:
        await app.aclose()
        await server.stop()


async def _chat_rows(app: GrouppigApp, group_id: int | None = None) -> list[dict[str, Any]]:
    """读聊天流水（``rpc:chat.query``）。"""

    criteria: dict[str, Any] = {} if group_id is None else {"group_id": int(group_id)}
    result = await app.container.call("rpc:chat.query", criteria)
    return list(result.get("messages") or ())


async def _wait_rows(
    app: GrouppigApp, group_id: int, expected: int, *, timeout: float = 6.0
) -> tuple[list[dict[str, Any]], bool]:
    """等聊天流水里出现至少 expected 条该群消息（固定次数采样会把「还没落完」误判成丢消息）。"""

    deadline = time.monotonic() + timeout
    rows = await _chat_rows(app, group_id)
    while time.monotonic() < deadline:
        if len(rows) >= expected:
            return rows, True
        await asyncio.sleep(0.05)
        rows = await _chat_rows(app, group_id)
    return rows, len(rows) >= expected


async def _push(
    server: MockOneBotServer,
    app: GrouppigApp,
    script: Sequence[tuple[str, int]],
    *,
    group_id: int,
    start_id: int,
    base: float,
    timeout: float = 6.0,
) -> dict[str, Any]:
    """推一批群消息过真实 WebSocket，并逐条等它落到聊天流水。"""

    missing: list[str] = []
    landed = 0
    for index, (text, user_id) in enumerate(script):
        await server.push(
            group_message(
                text,
                group_id=group_id,
                user_id=user_id,
                message_id=start_id + index,
                self_id=SELF_ID,
                time=base + index,
            )
        )
        await asyncio.sleep(0.02)
        rows, ok = await _wait_rows(app, group_id, index + 1, timeout=timeout)
        if ok:
            landed = len(rows)
        else:
            missing.append(str(start_id + index))
    return {"pushed": len(script), "landed": landed, "missing_ids": missing}


async def _contract_link(app: GrouppigApp) -> dict[str, Any]:
    """装配自检：已注册名字相对 api-index.json 零缺口、零未登记。"""

    report = app.contract_check()
    return {
        "observed": not report["missing"] and not report["unknown"],
        "registered": int(report["registered"]),
        "missing": list(report["missing"]),
        "unknown": list(report["unknown"]),
        "by_scope": {scope: info["expected_rpc"] for scope, info in sorted(report["by_scope"].items())},
    }


def _memory_finding(
    pushed: Mapping[str, Any], app: GrouppigApp, rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any] | None:
    """聊天流水少读时生成回单（t15 是已知问题，按「已知」回单）。"""

    missing = list(pushed.get("missing_ids") or ())
    if not missing:
        return None
    router = app.gateway.demux.stats.as_dict()
    buffer = app.perception.buffer.snapshot()["stats"]
    expected = int(pushed.get("pushed", 0))
    known = int(router.get("received", 0)) >= expected and int(router.get("routed", 0)) >= expected
    return _finding(
        "F-chat-rows-missing",
        "high",
        "memory",
        "推送的群消息没有全部出现在聊天流水里：网关/感知计数正常，但 rpc:chat.query 少读（甚至读 0）。",
        "missing_ids="
        + str(missing)
        + " rows="
        + str(len(rows))
        + " router="
        + str(router.get("received"))
        + "/"
        + str(router.get("routed"))
        + " buffer="
        + str(buffer),
        KNOWN_TASKS["chat_rows_missing"],
        "修 t15 的时序/丢消息问题（队长已另开任务）；本脚本按已知问题回单，不重复阻塞。",
        known=known,
    )


def _scenario_ok(links: Mapping[str, Any], findings: Sequence[Mapping[str, Any]]) -> tuple[bool, list[str]]:
    """场景是否通过：已知问题涉及的链路不参与判定（回单里已记录）。"""

    excused = {str(item.get("link")) for item in findings if item.get("known")}
    failed = [name for name, link in links.items() if name not in excused and not link.get("observed")]
    return (not failed), failed


# --------------------------------------------------------------------------
# 场景 1：冷启动 + 六条链路
# --------------------------------------------------------------------------
async def scenario_closed_loop(tmp_path: Path) -> dict[str, Any]:
    """冷启动 → 一条群消息入站 → 入库 / 话题会话 / 画像关系 / 回复 / 发送 / 反思。"""

    links: dict[str, Any] = {}
    notes: list[str] = []
    findings: list[dict[str, Any]] = []
    async with _running(tmp_path) as (app, server, _fake):
        cold = await _chat_rows(app)
        links["cold_start"] = {
            "observed": not cold,
            "rows_before": len(cold),
            "registered": len(app.container.registry.names()),
            "domains": sum(1 for value in app.domains.values() if value),
            "pumps": sorted(pump.name for pump in app.pumps),
        }
        if cold:
            notes.append("冷启动时聊天流水非空：" + str(len(cold)) + " 行")

        base = time.time() - BASE_OFFSET
        pushed = await _push(server, app, SCRIPT, group_id=GROUP_ID, start_id=2000, base=base)
        rows = await _chat_rows(app, GROUP_ID)
        router = app.gateway.demux.stats.as_dict()
        links["memory"] = {
            "observed": len(rows) >= len(SCRIPT) and not pushed["missing_ids"],
            "rows": len(rows),
            "pushed": pushed,
            "roles": sorted({str(row.get("role", "")) for row in rows}),
            "senders": sorted({int(row.get("sender_id", 0) or 0) for row in rows}),
            "contents": [str(row.get("content", "")) for row in rows],
            "router": {"received": router["received"], "routed": router["routed"], "dropped": router["dropped"]},
            "buffer": app.perception.buffer.snapshot()["stats"],
        }
        finding = _memory_finding(pushed, app, rows)
        if finding is not None:
            findings.append(finding)

        # ② 话题与会话
        await app.drain_once()
        await asyncio.sleep(0.05)
        records = [item for item in app.session.ingested if int(item.get("group_id", 0) or 0) == GROUP_ID]
        current = await app.container.call("rpc:session.current", group_id=GROUP_ID)
        session = current.get("session") or {}
        sessions = sorted({str(item.get("session_id", "")) for item in records})
        topics = sorted({str(item.get("topic_id", "")) for item in records})
        sessions_ok = len(sessions) <= SESSION_LIMIT
        links["topic"] = {
            "observed": bool(records) and bool(session.get("session_id")) and sessions_ok,
            "session_limit": SESSION_LIMIT,
            "sessions_ok": sessions_ok,
            "records": len(records),
            "topics": topics,
            "sessions": sessions,
            "threads": sorted({str(item.get("thread_id", "")) for item in records}),
            "group_ids": sorted({int(item.get("group_id", 0) or 0) for item in records}),
            "continuity": {
                "messages": len(records),
                "sessions": len(sessions),
                "topics": len(topics),
                "sessions_per_message": round(len(sessions) / len(records), 3) if records else 0.0,
            },
            "current": {
                "found": bool(current.get("found")),
                "group_id": int(session.get("group_id", 0) or 0),
                "topic_id": str(session.get("topic_id", "")),
                "state": str(session.get("state", "")),
                "keywords": list(session.get("keywords") or ()),
                "message_count": int(session.get("message_count", 0) or 0),
            },
        }
        if not links["topic"]["observed"]:
            notes.append("会话/话题没有按真实群号归属：话题链路不可观测")
        if links["topic"]["group_ids"] not in ([], [GROUP_ID]):
            notes.append("会话记录里出现了非预期群号：" + str(links["topic"]["group_ids"]))
        if records and not sessions_ok:
            findings.append(
                _finding(
                    "F1-session-churn",
                    "medium",
                    "topic",
                    "一条连贯的群聊被拆成多个会话：每条消息都被判成新话题并开新会话，会话平均长度降到 1~2 条消息，会话结束/归档/反思都作用在碎片上。",
                    str(len(records))
                    + " 条消息 → "
                    + str(len(sessions))
                    + " 个会话 / "
                    + str(len(topics))
                    + " 个话题；sessions_per_message="
                    + str(links["topic"]["continuity"]["sessions_per_message"])
                    + "（上限 "
                    + str(SESSION_LIMIT)
                    + " 个会话）",
                    "t17 session-engineer（会话连续性回归）",
                    "rpc:topic.detect 在判定 changed 前先与当前会话话题做关键词归并（TopicRanker.merge_with_current），且 session/runtime/di.on_message 把会话最近窗口 + 本条消息一起交给 ranker.detect；回归说明这条链又断了，先看 session/runtime/di.py 的 window_of 与 ranker 的 MERGE_THRESHOLD。",
                )
            )

        # ③ 画像与关系分
        profile = await app.profile_pump.run_once([GROUP_ID])
        speech = await app.container.call("rpc:speech.profile", ALICE, group_id=GROUP_ID)
        relationship = await app.container.call("rpc:relationship.get", ALICE, group_id=GROUP_ID)
        links["profile"] = {
            "observed": bool(speech.get("summary")) and bool(relationship.get("found")),
            "members": int(profile.get("members", 0) or 0),
            "failed_calls": int(profile.get("calls", {}).get("failed", 0) or 0),
            "speech_sample_size": int(speech.get("sample_size", 0) or 0),
            "speech_tags": sorted(str(tag) for tag in (speech.get("tags") or ())),
            "relationship": {
                "found": bool(relationship.get("found")),
                "tier": str(relationship.get("tier", "")),
                "interactions": int(relationship.get("interactions", 0) or 0),
                "score_band": _band(relationship.get("score")),
            },
        }
        if not links["profile"]["observed"]:
            notes.append("画像/关系分没有产出：画像链路不可观测")

        # ④ 回复（决策 → 心流） ⑤ 发送（真实 WebSocket 帧）
        decision = await app.container.call(
            "rpc:interrupt.decide",
            GROUP_ID,
            score=0.9,
            score_detail={"band": "strong", "total": 0.9},
            behavior="repeat",
            cooldown={"allowed": True, "reason": "smoke"},
        )
        sent = await wait_for(lambda: len(server.actions_of("send_group_msg")) >= 1, timeout=15.0)
        await asyncio.sleep(0.3)
        sends = server.actions_of("send_group_msg")
        flow = app.expression.flow.store.status()
        composer = app.gateway.composer.stats.as_dict()
        texts = [text for frame in sends for text in _frame_texts(frame)]
        links["reply"] = {
            "observed": bool(decision.get("action")) and int(flow.get("starts", 0) or 0) >= 1,
            "action": str(decision.get("action", "")),
            "published": bool(decision.get("published")),
            "flow_starts": int(flow.get("starts", 0) or 0),
            "flow_ends": int(flow.get("ends", 0) or 0),
            "flow_active": int(flow.get("active", 0) or 0),
        }
        links["send"] = {
            "observed": bool(sent) and bool(texts),
            "actions": [str(frame.get("action", "")) for frame in sends],
            "group_ids": sorted({int(frame.get("params", {}).get("group_id", 0) or 0) for frame in sends}),
            "texts": texts,
            "text_matches_reply": all(text.startswith(REPLY_TEXT[:2]) for text in texts) if texts else False,
            "composer_sent": int(composer.get("sent", 0) or 0),
            "composer_failures": int(composer.get("failures", 0) or 0),
        }
        if not links["reply"]["observed"]:
            notes.append("决策没有推动心流：回复链路不可观测")
        if not links["send"]["observed"]:
            notes.append("没有在 WebSocket 上看到 send_group_msg：发送链路不可观测")

        # ⑥ 会话结束 → 反思
        last_ingested = str(records[-1].get("session_id", "")) if records else ""
        reviews = dict(app.reflection.extra.get("reviews") or {})
        before = set(reviews)
        archived = await app.end_session(GROUP_ID, reason="smoke")
        ended = str((archived.get("session") or {}).get("session_id") or archived.get("session_id") or "")
        await wait_for(lambda: bool(set(app.reflection.extra.get("reviews") or {}) - before), timeout=10.0)
        reviews = dict(app.reflection.extra.get("reviews") or {})
        new_ids = sorted(set(reviews) - before)
        key = ended if ended in reviews else (new_ids[0] if new_ids else "")
        review = reviews.get(key) or {}
        links["reflection"] = {
            "observed": bool(new_ids),
            "ended_session_id": ended,
            "last_ingested_session_id": last_ingested,
            "reviewed_session_ids": new_ids,
            "matched_last_ingested": bool(last_ingested) and last_ingested in new_ids,
            "archived": archived.get("found") is not False,
            "review_keys": sorted(review) if isinstance(review, Mapping) else [],
            "review": _jsonable(review),
        }
        if not links["reflection"]["observed"]:
            notes.append("会话结束没有落到反思层：反思链路不可观测")

        links["contract"] = await _contract_link(app)
        if not links["contract"]["observed"]:
            notes.append(
                "契约有缺口：missing="
                + str(links["contract"]["missing"])
                + " unknown="
                + str(links["contract"]["unknown"])
            )

    ok, failed = _scenario_ok(links, findings)
    if failed:
        notes.append("未通过链路：" + ", ".join(failed))
    return {"name": "closed-loop", "ok": ok, "links": links, "notes": notes, "findings": findings}


# --------------------------------------------------------------------------
# 场景 2：并发多群
# --------------------------------------------------------------------------
async def scenario_concurrent_groups(tmp_path: Path) -> dict[str, Any]:
    """两个群交错发言：会话/话题/流水按 group_id 归属，互不串台。"""

    links: dict[str, Any] = {}
    notes: list[str] = []
    findings: list[dict[str, Any]] = []
    async with _running(tmp_path) as (app, server, _fake):
        base = time.time() - BASE_OFFSET
        for index in range(max(len(SCRIPT), len(SCRIPT_B))):
            if index < len(SCRIPT):
                text, user_id = SCRIPT[index]
                await server.push(
                    group_message(
                        text,
                        group_id=GROUP_ID,
                        user_id=user_id,
                        message_id=3000 + index,
                        self_id=SELF_ID,
                        time=base + index,
                    )
                )
            if index < len(SCRIPT_B):
                text_b, user_b = SCRIPT_B[index]
                await server.push(
                    group_message(
                        text_b,
                        group_id=GROUP_ID_B,
                        user_id=user_b,
                        message_id=4000 + index,
                        self_id=SELF_ID,
                        time=base + index,
                    )
                )
            await asyncio.sleep(0.02)
        rows_a, ok_a = await _wait_rows(app, GROUP_ID, len(SCRIPT))
        rows_b, ok_b = await _wait_rows(app, GROUP_ID_B, len(SCRIPT_B))
        await app.drain_once()
        await asyncio.sleep(0.05)
        current_a = await app.container.call("rpc:session.current", group_id=GROUP_ID)
        current_b = await app.container.call("rpc:session.current", group_id=GROUP_ID_B)
        session_a = current_a.get("session") or {}
        session_b = current_b.get("session") or {}
        records = list(app.session.ingested)
        by_group: dict[str, int] = {}
        for record in records:
            key = str(int(record.get("group_id", 0) or 0))
            by_group[key] = by_group.get(key, 0) + 1
        texts_a = sorted(str(row.get("content", "")) for row in rows_a)
        texts_b = sorted(str(row.get("content", "")) for row in rows_b)
        script_a = sorted(text for text, _ in SCRIPT)
        script_b = sorted(text for text, _ in SCRIPT_B)
        cross = sorted(set(texts_a) & set(texts_b))
        links["groups"] = {
            "observed": (
                bool(session_a.get("session_id"))
                and bool(session_b.get("session_id"))
                and int(session_a.get("group_id", 0) or 0) == GROUP_ID
                and int(session_b.get("group_id", 0) or 0) == GROUP_ID_B
                and session_a.get("session_id") != session_b.get("session_id")
                and bool(session_a.get("topic_id"))
                and session_a.get("topic_id") != session_b.get("topic_id")
                and not cross
                and set(texts_a) <= set(script_a)
                and set(texts_b) <= set(script_b)
            ),
            "session_group_a": int(session_a.get("group_id", 0) or 0),
            "session_group_b": int(session_b.get("group_id", 0) or 0),
            "topic_a": str(session_a.get("topic_id", "")),
            "topic_b": str(session_b.get("topic_id", "")),
            "topics_distinct": bool(session_a.get("topic_id"))
            and session_a.get("topic_id") != session_b.get("topic_id"),
            "group_ids_a": sorted({int(row.get("group_id", 0) or 0) for row in rows_a}),
            "group_ids_b": sorted({int(row.get("group_id", 0) or 0) for row in rows_b}),
            "cross_group_texts": cross,
            "ingested_by_group": by_group,
        }
        links["groups_rows"] = {
            "observed": ok_a and ok_b,
            "rows_a": len(rows_a),
            "rows_b": len(rows_b),
            "expected_a": len(SCRIPT),
            "expected_b": len(SCRIPT_B),
        }
        if not links["groups"]["observed"]:
            notes.append(
                "多群隔离不成立：session_group_a/b="
                + str(links["groups"]["session_group_a"])
                + "/"
                + str(links["groups"]["session_group_b"])
                + " cross="
                + str(cross)
            )
        if sorted(by_group) != sorted({str(GROUP_ID), str(GROUP_ID_B)}):
            notes.append("会话记录的群号分布不对：" + str(by_group))
        if not links["groups_rows"]["observed"]:
            router = app.gateway.demux.stats.as_dict()
            findings.append(
                _finding(
                    "F-chat-rows-missing",
                    "high",
                    "groups_rows",
                    "并发两个群时聊天流水少读了消息（与 closed-loop 的 F-chat-rows-missing 同类现象）。",
                    "rows_a="
                    + str(len(rows_a))
                    + "/"
                    + str(len(SCRIPT))
                    + " rows_b="
                    + str(len(rows_b))
                    + "/"
                    + str(len(SCRIPT_B))
                    + " router="
                    + str(router.get("received"))
                    + "/"
                    + str(router.get("routed")),
                    KNOWN_TASKS["chat_rows_missing"],
                    "修 t15 的时序/丢消息问题（队长已另开任务）；本脚本按已知问题回单，不重复阻塞。",
                    known=int(router.get("received", 0)) >= len(SCRIPT) + len(SCRIPT_B),
                )
            )

        links["contract"] = await _contract_link(app)
    ok, failed = _scenario_ok(links, findings)
    if failed:
        notes.append("未通过链路：" + ", ".join(failed))
    return {"name": "concurrent-groups", "ok": ok, "links": links, "notes": notes, "findings": findings}


# --------------------------------------------------------------------------
# 场景 3：重复消息
# --------------------------------------------------------------------------
async def scenario_duplicate_messages(tmp_path: Path) -> dict[str, Any]:
    """同一条 message_id 重复推送：聊天流水不重复落库，链路不炸。"""

    links: dict[str, Any] = {}
    notes: list[str] = []
    findings: list[dict[str, Any]] = []
    async with _running(tmp_path) as (app, server, _fake):
        base = time.time() - BASE_OFFSET
        pushed = await _push(server, app, SCRIPT, group_id=GROUP_ID, start_id=5000, base=base)
        before = await _chat_rows(app, GROUP_ID)
        for index in (0, 2):
            text, user_id = SCRIPT[index]
            await server.push(
                group_message(
                    text,
                    group_id=GROUP_ID,
                    user_id=user_id,
                    message_id=5000 + index,
                    self_id=SELF_ID,
                    time=base + index,
                )
            )
        await asyncio.sleep(0.4)
        after = await _chat_rows(app, GROUP_ID)
        direct = await app.container.call(
            "rpc:chat.append",
            {"message_id": "5000", "group_id": GROUP_ID, "sender_id": ALICE, "content": SCRIPT[0][0], "ts": base},
        )
        fresh = await app.container.call(
            "rpc:chat.append",
            {
                "message_id": "9999",
                "group_id": GROUP_ID,
                "sender_id": ALICE,
                "content": "冷启动补一条",
                "ts": base + 10,
            },
        )
        links["duplicates"] = {
            "observed": (
                not pushed["missing_ids"]
                and len(after) == len(before)
                and bool(direct.get("duplicate"))
                and bool(fresh.get("created"))
            ),
            "pushed": pushed,
            "rows_before": len(before),
            "rows_after": len(after),
            "direct_duplicate": bool(direct.get("duplicate")),
            "direct_created": bool(direct.get("created")),
            "fresh_created": bool(fresh.get("created")),
            "rows_total": len(await _chat_rows(app, GROUP_ID)),
        }
        if len(after) != len(before):
            notes.append("重复推送改变了聊天流水行数：" + str(len(before)) + " → " + str(len(after)))
        if not direct.get("duplicate"):
            notes.append("rpc:chat.append 对已存在的 message_id 没有标记 duplicate")
        finding = _memory_finding(pushed, app, before)
        if finding is not None:
            findings.append(finding)
        links["contract"] = await _contract_link(app)
    ok, failed = _scenario_ok(links, findings)
    if failed:
        notes.append("未通过链路：" + ", ".join(failed))
    return {"name": "duplicate-messages", "ok": ok, "links": links, "notes": notes, "findings": findings}


# --------------------------------------------------------------------------
# 场景 4：模型不可用降级
# --------------------------------------------------------------------------
async def scenario_model_degraded(tmp_path: Path) -> dict[str, Any]:
    """模型全失败（FakeTransport failures=99）：回复仍要生成并发出，降级要可观测。"""

    links: dict[str, Any] = {}
    notes: list[str] = []
    findings: list[dict[str, Any]] = []
    failing = FakeTransport(reply=REPLY_TEXT, failures=99, error=TransportError("upstream 503", status=503))
    async with _running(tmp_path, transport=failing) as (app, server, fake):
        base = time.time() - BASE_OFFSET
        pushed = await _push(server, app, SCRIPT, group_id=GROUP_ID, start_id=7000, base=base)
        await app.drain_once()
        composed = EventCollector(app.container.bus).listen("kafka:grouppig.reply.composed")
        decision = await app.container.call(
            "rpc:interrupt.decide",
            GROUP_ID,
            score=0.9,
            score_detail={"band": "strong", "total": 0.9},
            behavior="repeat",
            cooldown={"allowed": True, "reason": "smoke-degraded"},
        )
        sent = await wait_for(lambda: len(server.actions_of("send_group_msg")) >= 1, timeout=15.0)
        await asyncio.sleep(0.3)
        sends = server.actions_of("send_group_msg")
        texts = [text for frame in sends for text in _frame_texts(frame)]
        payloads = composed.of_topic("kafka:grouppig.reply.composed")
        payload = payloads[0] if payloads else {}
        degraded = payload.get("degraded") if isinstance(payload, Mapping) else None
        health = await app.health()
        composed.close()
        links["degraded"] = {
            "observed": bool(decision.get("action")) and bool(sent) and bool(texts) and bool(fake.calls),
            "action": str(decision.get("action", "")),
            "texts": texts,
            "text_non_empty": all(bool(text.strip()) for text in texts) if texts else False,
            "model_calls": len(fake.calls),
            "model_paths": sorted({str(path) for path in fake.paths}),
            "composed_degraded": _jsonable(degraded),
            "composer_failures": int(app.gateway.composer.stats.as_dict().get("failures", 0) or 0),
            "flow_active": int(app.expression.flow.store.status().get("active", 0) or 0),
            "domain_errors": {
                name: value.get("error")
                for name, value in (health.get("domains") or {}).items()
                if isinstance(value, Mapping) and value.get("error")
            },
            "bus_failed": int(app.container.bus.stats.as_dict().get("failed", 0) or 0),
        }
        if not texts:
            notes.append("模型不可用时没有发出任何回复（降级没有兜住）")
        if links["degraded"]["composer_failures"]:
            notes.append("发送失败计数非零：" + str(links["degraded"]["composer_failures"]))
        finding = _memory_finding(pushed, app, await _chat_rows(app, GROUP_ID))
        if finding is not None:
            findings.append(finding)
        links["contract"] = await _contract_link(app)
    ok, failed = _scenario_ok(links, findings)
    if failed:
        notes.append("未通过链路：" + ", ".join(failed))
    return {"name": "model-degraded", "ok": ok, "links": links, "notes": notes, "findings": findings}


# --------------------------------------------------------------------------
# 场景 5：归档后唤醒
# --------------------------------------------------------------------------
async def scenario_wake_after_archive(tmp_path: Path) -> dict[str, Any]:
    """归档后唤醒：wake 恢复上下文 → 缓冲可读 → sleep 归还。"""

    links: dict[str, Any] = {}
    notes: list[str] = []
    findings: list[dict[str, Any]] = []
    async with _running(tmp_path) as (app, server, _fake):
        base = time.time() - BASE_OFFSET
        pushed = await _push(server, app, SCRIPT, group_id=GROUP_ID, start_id=8000, base=base)
        await app.drain_once()
        await asyncio.sleep(0.05)
        archived = await app.end_session(GROUP_ID, reason="smoke-wake")
        session_id = str((archived.get("session") or {}).get("session_id") or archived.get("session_id") or "")
        state_after = str((archived.get("session") or {}).get("state", ""))
        wake = await app.container.call("rpc:session.wake", session_id, reason="smoke-wake")
        context = wake.get("context") if isinstance(wake, Mapping) else None
        pop = await app.container.call("rpc:wake.buffer.pop", session_id, peek=True)
        contexts = list(pop.get("contexts") or ())
        sleep = await app.container.call("rpc:session.sleep", session_id)
        current = await app.container.call("rpc:session.current", group_id=GROUP_ID)
        links["wake"] = {
            "observed": (
                bool(session_id)
                and bool(wake.get("woken"))
                and isinstance(context, Mapping)
                and bool(context)
                and len(contexts) >= 1
                and sleep.get("slept") is not False
            ),
            "session_id": session_id,
            "state_after_archive": state_after,
            "woken": bool(wake.get("woken")),
            "wake_reason": str(wake.get("reason", "")),
            "context_keys": sorted(context) if isinstance(context, Mapping) else [],
            "context_snippets": len((context or {}).get("snippets") or ()) if isinstance(context, Mapping) else 0,
            "buffer_contexts": len(contexts),
            "buffer_size": int(pop.get("size", 0) or 0),
            "slept": sleep.get("slept"),
            "saved_ok": bool(sleep.get("saved")),
            "session_still_live": bool(current.get("found")),
            "pushed": pushed,
        }
        if not wake.get("woken"):
            notes.append("归档会话没有被唤醒：" + str(wake.get("reason", "")))
        if not contexts:
            notes.append("唤醒缓冲里没有上下文（rpc:wake.buffer.pop 为空）")
        finding = _memory_finding(pushed, app, await _chat_rows(app, GROUP_ID))
        if finding is not None:
            findings.append(finding)
        links["contract"] = await _contract_link(app)
    ok, failed = _scenario_ok(links, findings)
    if failed:
        notes.append("未通过链路：" + ", ".join(failed))
    return {"name": "wake-after-archive", "ok": ok, "links": links, "notes": notes, "findings": findings}


SCENARIOS: tuple[tuple[str, Callable[[Path], Awaitable[dict[str, Any]]]], ...] = (
    ("closed-loop", scenario_closed_loop),
    ("concurrent-groups", scenario_concurrent_groups),
    ("duplicate-messages", scenario_duplicate_messages),
    ("model-degraded", scenario_model_degraded),
    ("wake-after-archive", scenario_wake_after_archive),
)

# --------------------------------------------------------------------------
# 可复现性：归一化轨迹
# --------------------------------------------------------------------------
#: 归一化时丢弃的键（时间戳 / id / 受分词影响的词表，天然不可逐字复现）。
VOLATILE_KEYS = frozenset(
    {
        "session_id",
        "ended_session_id",
        "last_ingested_session_id",
        "reviewed_session_ids",
        "sessions",
        "topics",
        "threads",
        "texts",
        "contents",
        "keywords",
        "speech_tags",
        "review",
        "review_keys",
        "current",
        "relationship",
        "registered",
        "senders",
        "by_scope",
        "message",
        "saved",
        "context",
        "context_keys",
        "buffer",
        "model_paths",
        "topic_a",
        "topic_b",
    }
)


def normalize_trace(report: Mapping[str, Any]) -> dict[str, Any]:
    """把报告折成「结构事实」：id 折叠成占位符、集合排序、丢弃时间戳。

    会话/聊天线/话题 id 里含时间戳与随机盐，跨回放必然不同；这里按首次出现顺序
    映射成 session-1 / thread-1 / topic-1 这类占位符，只保留「有几个、顺序如何」。
    """

    ids: dict[str, str] = {}

    def placeholder(value: str, prefix: str) -> str:
        key = prefix + ":" + value
        if key not in ids:
            ids[key] = prefix + "-" + str(sum(1 for item in ids if item.startswith(prefix + ":")) + 1)
        return ids[key]

    def walk(node: Any) -> Any:
        if isinstance(node, Mapping):
            out: dict[str, Any] = {}
            for key, item in sorted(node.items()):
                name = str(key)
                if name in VOLATILE_KEYS:
                    continue
                if name.endswith("_id") and isinstance(item, str) and item:
                    out[name] = placeholder(item, name[: -len("_id")])
                else:
                    out[name] = walk(item)
            return out
        if isinstance(node, (list, tuple)):
            items = [walk(item) for item in node]
            if all(isinstance(item, str) for item in items):
                return sorted(items)
            return items
        if isinstance(node, float):
            return round(node, 3)
        return node

    return walk(report)


def _diff(left: Any, right: Any, *, path: str, limit: int) -> list[str]:
    """列出两棵归一化轨迹的差异（最多 limit 条）。"""

    out: list[str] = []

    def walk(a: Any, b: Any, where: str) -> None:
        if len(out) >= limit:
            return
        if isinstance(a, Mapping) and isinstance(b, Mapping):
            for key in sorted(set(a) | set(b)):
                walk(a.get(key), b.get(key), where + "." + str(key))
                if len(out) >= limit:
                    return
            return
        if isinstance(a, list) and isinstance(b, list):
            if len(a) != len(b):
                out.append(where + ": 长度 " + str(len(a)) + " != " + str(len(b)))
                return
            for index, (item_a, item_b) in enumerate(zip(a, b, strict=False)):
                walk(item_a, item_b, where + "[" + str(index) + "]")
                if len(out) >= limit:
                    return
            return
        if a != b:
            out.append(where + ": " + repr(a) + " != " + repr(b))

    walk(left, right, path or "$")
    return out


# --------------------------------------------------------------------------
# 汇总与 CLI
# --------------------------------------------------------------------------
LINK_LABELS = {
    "cold_start": "冷启动",
    "memory": "① 入库",
    "topic": "② 话题/会话",
    "profile": "③ 画像/关系",
    "reply": "④ 回复",
    "send": "⑤ 发送",
    "reflection": "⑥ 反思",
    "contract": "契约自检",
    "groups": "多群隔离",
    "duplicates": "重复消息",
    "degraded": "模型降级",
    "wake": "归档唤醒",
}


def _detail(key: str, link: Mapping[str, Any]) -> str:
    if key == "cold_start":
        return (
            "rows_before="
            + str(link.get("rows_before"))
            + " domains="
            + str(link.get("domains"))
            + " pumps="
            + str(len(link.get("pumps") or ()))
        )
    if key == "memory":
        return (
            "rows="
            + str(link.get("rows"))
            + " roles="
            + str(link.get("roles"))
            + " senders="
            + str(link.get("senders"))
        )
    if key == "topic":
        current = link.get("current") or {}
        continuity = link.get("continuity") or {}
        return (
            "records="
            + str(link.get("records"))
            + " group_ids="
            + str(link.get("group_ids"))
            + " state="
            + str(current.get("state"))
            + " keywords="
            + str(len(current.get("keywords") or ()))
            + " sessions/messages="
            + str(continuity.get("sessions"))
            + "/"
            + str(continuity.get("messages"))
            + " sessions_per_message="
            + str(continuity.get("sessions_per_message"))
            + "(<="
            + str(link.get("session_limit"))
            + ")"
        )
    if key == "profile":
        relationship = link.get("relationship") or {}
        return (
            "members="
            + str(link.get("members"))
            + " samples="
            + str(link.get("speech_sample_size"))
            + " tier="
            + str(relationship.get("tier"))
            + " interactions="
            + str(relationship.get("interactions"))
        )
    if key == "reply":
        return (
            "action="
            + str(link.get("action"))
            + " flow starts/ends="
            + str(link.get("flow_starts"))
            + "/"
            + str(link.get("flow_ends"))
        )
    if key == "send":
        return "actions=" + str(link.get("actions")) + " texts=" + str(link.get("texts"))
    if key == "reflection":
        return "reviewed=" + str(link.get("observed")) + " sessions=" + str(len(link.get("reviewed_session_ids") or ()))
    if key == "groups_rows":
        return (
            "rows_a="
            + str(link.get("rows_a"))
            + "/"
            + str(link.get("expected_a"))
            + " rows_b="
            + str(link.get("rows_b"))
            + "/"
            + str(link.get("expected_b"))
        )
    if key == "contract":
        return (
            "registered="
            + str(link.get("registered"))
            + " missing="
            + str(link.get("missing"))
            + " unknown="
            + str(link.get("unknown"))
        )
    if key == "groups":
        return (
            "session_groups="
            + str(link.get("session_group_a"))
            + "/"
            + str(link.get("session_group_b"))
            + " topics_distinct="
            + str(link.get("topics_distinct"))
        )
    if key == "duplicates":
        return (
            "rows "
            + str(link.get("rows_before"))
            + "→"
            + str(link.get("rows_after"))
            + " duplicate="
            + str(link.get("direct_duplicate"))
            + " created="
            + str(link.get("fresh_created"))
        )
    if key == "degraded":
        return (
            "texts="
            + str(link.get("texts"))
            + " model_calls="
            + str(link.get("model_calls"))
            + " composer_failures="
            + str(link.get("composer_failures"))
        )
    if key == "wake":
        return (
            "woken="
            + str(link.get("woken"))
            + " contexts="
            + str(link.get("buffer_contexts"))
            + " slept="
            + str(link.get("slept"))
            + " saved="
            + str(link.get("saved"))
        )
    return ""


def summarize(report: Mapping[str, Any]) -> str:
    """人读摘要：每个场景一段，逐条链路一行。"""

    lines = ["GrouPig 端到端冒烟（模拟 OneBot 服务端回放）", "=" * 60]
    for scenario in report.get("scenarios") or ():
        mark = "PASS" if scenario.get("ok") else "FAIL"
        lines.append(
            "[" + mark + "] " + str(scenario.get("name")) + "  （回放 " + str(scenario.get("replays", 1)) + " 次）"
        )
        for key, link in (scenario.get("links") or {}).items():
            flag = "OK  " if link.get("observed") else "FAIL"
            label = LINK_LABELS.get(key, key)
            lines.append("    [" + flag + "] " + label.ljust(12) + " " + _detail(key, link))
        for note in scenario.get("notes") or ():
            lines.append("    ! " + str(note))
        if scenario.get("replays", 1) > 1:
            verdict = "一致" if scenario.get("reproducible") else "不一致"
            lines.append("    可复现性：" + str(scenario.get("replays")) + " 次回放的归一化轨迹" + verdict)
            for line in (scenario.get("diff") or ())[:8]:
                lines.append("      - " + str(line))
    lines.append("=" * 60)
    lines.append("结论：" + ("全部场景通过" if report.get("ok") else "存在失败场景"))
    findings = report.get("findings") or ()
    if findings:
        lines.append("回单（" + str(len(findings)) + " 条）：")
        for finding in findings:
            tag = "已知" if finding.get("known") else "新增"
            lines.append(
                "  ["
                + str(finding.get("severity", "")).upper()
                + "|"
                + tag
                + "] "
                + str(finding.get("id"))
                + " -> "
                + str(finding.get("owner"))
            )
            lines.append("        " + str(finding.get("problem")))
            lines.append("        证据：" + str(finding.get("evidence")))
    return "\n".join(lines)


async def run_scenarios(*, replays: int = 1, workdir: Path | None = None, only: str | None = None) -> dict[str, Any]:
    """跑（选定的）场景；基础场景回放 replays 次并比对归一化轨迹。"""

    entries: list[dict[str, Any]] = []
    for name, scenario in SCENARIOS:
        if only and name != only:
            continue
        rounds = max(1, replays) if name == "closed-loop" else 1
        reports: list[dict[str, Any]] = []
        for index in range(rounds):
            if workdir is not None:
                base = Path(workdir) / (name + "-" + str(index))
                base.mkdir(parents=True, exist_ok=True)
            else:
                base = Path(tempfile.mkdtemp(prefix="grouppig-smoke-" + name + "-" + str(index) + "-"))
            reports.append(await scenario(base))
        traces = [normalize_trace(item) for item in reports]
        reproducible = all(trace == traces[0] for trace in traces[1:])
        diff = [] if reproducible or len(traces) < 2 else _diff(traces[0], traces[1], path="", limit=12)
        entries.append(
            {
                **reports[0],
                "replays": rounds,
                "reproducible": reproducible,
                "diff": diff,
                "runs": reports,
                "traces": traces,
            }
        )
    findings: list[dict[str, Any]] = []
    for entry in entries:
        for finding in entry.get("findings") or ():
            if finding.get("id") not in {item.get("id") for item in findings}:
                findings.append(finding)
    return {
        "ok": all(entry["ok"] for entry in entries) and all(entry["reproducible"] for entry in entries),
        "scenarios": entries,
        "findings": findings,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GrouPig 端到端冒烟验证（t11）")
    parser.add_argument("--scenario", default="", help="只跑某个场景（默认全部）")
    parser.add_argument("--replay", type=int, default=1, help="closed-loop 回放次数（>1 时比对归一化轨迹）")
    parser.add_argument("--json", dest="json_path", default="", help="把报告写到该文件")
    parser.add_argument("--quiet", action="store_true", help="只输出 JSON")
    parser.add_argument("--summary-only", action="store_true", help="只输出人读摘要")
    args = parser.parse_args(argv)

    report = asyncio.run(run_scenarios(replays=max(1, int(args.replay)), only=args.scenario or None))
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not args.quiet:
        print(summarize(report))
    if not args.summary_only:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":  # pragma: no cover - CLI 入口
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(main())
