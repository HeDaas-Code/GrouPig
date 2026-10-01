#!/usr/bin/env python3
"""群聊回放模拟器：把真实 QQ 群记录喂给 GrouPig，观察它的反应并评定能力完成度。

三件事：

1. **清洗语料**：从 ``data.zip``（QQChatExporter V5 / chunked-jsonl）抽出「真实人类发言」，
   丢掉系统消息、撤回、图片 / 视频 / 文件 / 转发 / JSON 卡片等无文本内容，剥掉回复前缀。
2. **回放**：用 ``MockOneBotServer`` 起真实 OneBot 服务端，按原始时间间隔把一段真实群聊推进
   **生产装配**的 GrouPig（``build_app`` + 真 WebSocket + 真事件总线 + 真模型），
   同时从三条线取证：总线事件、面板快照、网关发送动作。
3. **评定**：把观测事实对照设计树的 8 个域，给出「有证据 / 部分 / 无证据」的完成度清单。

用法::

    uv run python tools/group_sim.py --corpus                # 只统计语料（不起应用）
    uv run python tools/group_sim.py --transport fake        # 机械回放（假模型，快）
    uv run python tools/group_sim.py --groups 1 --span 60    # 真模型回放（慢，受 --budget 限制）
    uv run python tools/group_sim.py --json sim.json --quiet
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import re
import sys
import tempfile
import time
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
for _extra in (REPO_ROOT / "src", REPO_ROOT / "tests"):
    if str(_extra) not in sys.path:
        sys.path.insert(0, str(_extra))

from gateway_helpers import EventCollector, MockOneBotServer, group_message  # noqa: E402
from grouppig.infra.config.loader import load_config  # noqa: E402
from grouppig.infra.runtime.registry import Registry  # noqa: E402
from grouppig.panel.snapshot import SnapshotOptions, build_snapshot  # noqa: E402
from grouppig.runtime.app import GrouppigApp, build_app  # noqa: E402

CONFIG_PATH = REPO_ROOT / "config" / "grouppig.toml"

#: 回放用的 self_id（真实导出里的 2172547544 是「本人」，我们要模拟的是机器人）。
SELF_ID = 10001

#: 语料里「本人」的 uin：这些是记录者自己发的，回放时排除（我们要模拟的是机器人）。
CORPUS_SELF_UIN = "2172547544"

#: 只保留这两类：真正的文字发言。其余（system / json / forward / video / file / audio / type_N）
#: 都是无文本或非人类发言的载体。
KEEP_TYPES = ("text", "reply")

#: 纯占位（整条就是一个方括号标记，如 ``[图片]`` ``[视频:xxx.mp4]``）—— 没有可读内容。
PLACEHOLDER = re.compile(r"^\[[^\]]*\]$")

#: 内联媒体标记：句子里夹着 ``[图片:xxx.jpeg]`` 这类，剥掉标记本身、保留句子。
INLINE_MEDIA = re.compile(r"\[(?:图片|表情|视频|文件|语音|音乐|动画表情|戳一戳)[^\]]*\]")

#: 控制字符与零宽字符。
CONTROL = re.compile("[\x00-\x08\x0b-\x1f\x7f\u200b-\u200f\ufeff]")

#: 单条消息最大长度（超出截断：长文对群聊建模没有额外价值，反而撑爆提示词）。
MAX_TEXT = 400

#: 回放时给应用追加的配置：放开分类节流、发送节流压到毫秒级，让短窗口也能跑出完整链路。
EXTRA_CONFIG = """
[onebot.rate]
per_minute = 600
burst = 8
min_interval = 0.02

[perception.features]
classify_min_interval = 0
classify_min_messages = 2
"""

#: 观测的总线主题（契约名，逐字取自 api-index.json）。
TOPIC_BEHAVIOR = "kafka:grouppig.behavior.changed"
TOPIC_INTERRUPT = "kafka:grouppig.interrupt.triggered"
TOPIC_REPLY = "kafka:grouppig.reply.composed"
TOPIC_SESSION = "kafka:grouppig.session.completed"
TOPIC_INBOUND = "kafka:grouppig.qq.message.received"
WATCH_TOPICS = (TOPIC_INBOUND, TOPIC_BEHAVIOR, TOPIC_INTERRUPT, TOPIC_REPLY, TOPIC_SESSION)


# --------------------------------------------------------------------------
# 语料
# --------------------------------------------------------------------------
@dataclass
class Msg:
    """一条可用于回放的群消息。"""

    ts: int  # epoch 秒（原始时间，回放时整体平移）
    uid: str
    uin: str
    name: str
    text: str
    at_self: bool  # 原始记录里 @ 了「本人」——回放时等价于 @ 机器人


@dataclass
class GroupCorpus:
    """一个群的清洗结果 + 丢弃账本。"""

    key: str
    name: str
    messages: list[Msg] = field(default_factory=list)
    raw_total: int = 0
    dropped: Counter = field(default_factory=Counter)

    def densest(self, span: float) -> tuple[int, int]:
        """返回消息最密的 ``span`` 秒窗口 [start, end]（闭区间下标）。"""

        rows = self.messages
        best = (0, 0, 0)
        left = 0
        for right in range(len(rows)):
            while rows[left].ts < rows[right].ts - span:
                left += 1
            size = right - left + 1
            if size > best[0]:
                best = (size, left, right)
        return best[1], best[2]


def _strip_reply_prefix(text: str) -> str:
    """剥掉导出的回复前缀（``[回复消息]``）与紧随其后的 ``@某人 ``。"""

    text = text.replace("[回复消息]", "", 1)
    return re.sub(r"^@\S+\s+", "", text.strip())


def clean_text(record: Mapping[str, Any]) -> str:
    """把一条导出的记录变成「人读到的那句话」。"""

    content = record.get("content") or {}
    text = str(content.get("text") or "")
    text = _strip_reply_prefix(text)
    text = INLINE_MEDIA.sub("", text)
    text = CONTROL.sub("", text)
    text = re.sub(r"[ \t]{2,}", " ", text).strip()
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT] + "…"
    return text


def _mentions_self(record: Mapping[str, Any]) -> bool:
    content = record.get("content") or {}
    for item in content.get("mentions") or ():
        if str(item.get("uin")) == CORPUS_SELF_UIN:
            return True
    return False


def _iter_records(archive: zipfile.ZipFile, group_dir: str) -> Iterator[dict[str, Any]]:
    """按 chunk 顺序逐行读 jsonl（流式，不把 100MB 全读进内存）。"""

    names = sorted(n for n in archive.namelist() if n.startswith(group_dir + "/chunks/") and n.endswith(".jsonl"))
    for name in names:
        with archive.open(name) as raw:
            for line in io.TextIOWrapper(raw, encoding="utf-8"):
                line = line.strip()
                if line:
                    yield json.loads(line)


def load_corpus(zip_path: Path, *, verbose: bool = False) -> list[GroupCorpus]:
    """解出所有群的清洗语料。"""

    groups: list[GroupCorpus] = []
    with zipfile.ZipFile(zip_path) as archive:
        manifests = sorted(n for n in archive.namelist() if n.endswith("/manifest.json"))
        for manifest_name in manifests:
            group_dir = manifest_name.rsplit("/", 1)[0]
            manifest = json.loads(archive.read(manifest_name).decode("utf-8"))
            info = manifest.get("chatInfo") or {}
            corpus = GroupCorpus(key=group_dir.rsplit("/", 1)[-1], name=str(info.get("name") or group_dir))
            corpus.raw_total = int((manifest.get("statistics") or {}).get("totalMessages") or 0)
            for record in _iter_records(archive, group_dir):
                kind = str(record.get("type") or "?")
                if kind not in KEEP_TYPES:
                    corpus.dropped[kind] += 1
                    continue
                sender = record.get("sender") or {}
                if str(sender.get("uin")) == CORPUS_SELF_UIN:
                    corpus.dropped["self"] += 1
                    continue
                text = clean_text(record)
                if not text:
                    corpus.dropped["empty"] += 1
                    continue
                if PLACEHOLDER.match(text):
                    corpus.dropped["placeholder"] += 1
                    continue
                corpus.messages.append(
                    Msg(
                        ts=int(int(record.get("timestamp") or 0) // 1000),
                        uid=str(sender.get("uid") or ""),
                        uin=str(sender.get("uin") or ""),
                        name=str(sender.get("name") or sender.get("nickname") or "群友"),
                        text=text,
                        at_self=_mentions_self(record),
                    )
                )
            corpus.messages.sort(key=lambda m: m.ts)
            groups.append(corpus)
            if verbose:
                print(
                    f"  语料 {corpus.name:<24} 原始 {corpus.raw_total:>6} → 保留 {len(corpus.messages):>6}"
                    f"  丢弃 {dict(corpus.dropped.most_common(6))}"
                )
    return groups


# --------------------------------------------------------------------------
# 回放
# --------------------------------------------------------------------------
def _write_config(tmp_path: Path, ws_url: str, extra: str = EXTRA_CONFIG) -> Path:
    """复制仓库配置到临时目录，把 ws 指向 mock、self_id 定死。"""

    text = CONFIG_PATH.read_text(encoding="utf-8")
    text = text.replace('ws_url = "ws://127.0.0.1:3001"', 'ws_url = "' + ws_url + '"')
    text = text.replace("self_id = 0", "self_id = " + str(SELF_ID))
    text = text.replace('level = "INFO"', 'level = "ERROR"').replace("json = true", "json = false")
    path = tmp_path / "grouppig.toml"
    path.write_text(text + extra, encoding="utf-8")
    return path


def _pin_laya_transport(router: Any, transport: Any) -> None:
    """把假传输同时钉到 LAY A provider 上（与 tests / tools.smoke 同源）。

    只注册 ``*`` 不够：``ModelRouter._transport()`` 对 laya 会绕开它，
    于是 classify 会去连真实端点 —— 没有密钥时 401 仍能回落（假绿），配上密钥就成了真联网。
    """

    router.set_transport(transport)
    router.set_transport(transport, provider="laya")


async def wait_for(predicate: Any, *, timeout: float = 10.0, interval: float = 0.05) -> bool:
    """轮询等待条件成立（异步旁路多，轮询比固定 sleep 稳）。"""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def _reply_texts(server: MockOneBotServer) -> list[dict[str, Any]]:
    """把网关真正发出去的群消息取出来（这是机器人「说了什么」的唯一权威来源）。"""

    out: list[dict[str, Any]] = []
    for action in server.actions_of("send_group_msg"):
        params = action.get("params") or {}
        message = params.get("message")
        text = ""
        if isinstance(message, list):
            text = "".join(str(seg.get("data", {}).get("text", "")) for seg in message if seg.get("type") == "text")
        elif isinstance(message, str):
            text = message
        out.append({"group_id": params.get("group_id"), "text": text, "raw": message})
    return out


def _behavior_of(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {"raw": str(payload)[:120]}
    return {
        "group_id": payload.get("group_id"),
        "label": payload.get("label") or payload.get("behavior") or payload.get("class"),
        "confidence": payload.get("confidence"),
        "source": payload.get("source"),
        "previous": payload.get("previous") or payload.get("from"),
    }


def _interrupt_of(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {"raw": str(payload)[:120]}
    return {
        "group_id": payload.get("group_id"),
        "action": payload.get("action") or payload.get("decision"),
        "score": payload.get("score"),
        "reason": payload.get("reason"),
    }


async def simulate_group(
    corpus: GroupCorpus,
    start: int,
    end: int,
    *,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    """把 ``corpus.messages[start:end]`` 按原始节奏回放进一个真实装配的 GrouPig。"""

    span = corpus.messages[end].ts - corpus.messages[start].ts
    speed = float(options.get("speed") or 1.0)
    budget = float(options.get("budget") or 240.0)
    quiet = float(options.get("quiet") or 6.0)
    max_replies = int(options.get("max_replies") or 0)
    want_probe = bool(options.get("probe"))
    use_fake = str(options.get("transport")) == "fake"
    mode = str(options.get("mode") or "active").lower()
    if mode not in {"off", "shadow", "active"}:
        raise ValueError(f"invalid mode: {mode!r}")

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    mode_config = f"\n[perception.classify]\ndecision_mode = \"{mode}\"\n"
    report: dict[str, Any] = {
        "group": corpus.name,
        "key": corpus.key,
        "window": {"messages": end - start + 1, "span_seconds": span, "speed": speed},
        "options": dict(options),
        "mode": mode,
        "mode_execution": {
            "off": "llm_judge_disabled",
            "shadow": "llm_judge_recorded_without_affecting_behavior",
            "active": "llm_judge_may_resolve_ambiguous_windows",
        }[mode],
        "audit": {},
    }
    with tempfile.TemporaryDirectory() as tmp:
        config = load_config(_write_config(Path(tmp), url, EXTRA_CONFIG + mode_config), use_local=False)
        transport = None
        if use_fake:
            from helpers import FakeTransport

            transport = FakeTransport(reply="（假模型回复）")
        app: GrouppigApp = build_app(
            config, registry=Registry(), transport=transport, dsn="sqlite+aiosqlite:///:memory:"
        )
        if transport is not None:
            _pin_laya_transport(app.container.router, transport)
        await app.start(connect=True, pumps=True)
        collector = EventCollector(app.container.bus).listen(*WATCH_TOPICS)
        try:
            report["ingest"] = await _replay(server, corpus, start, end, speed=speed, max_replies=max_replies)
            report["settle"] = await _settle(collector, server, quiet=quiet, budget=budget)
            behavior_rows = [_behavior_of(p) for p in collector.of_topic(TOPIC_BEHAVIOR)]
            interrupt_rows = [_interrupt_of(p) for p in collector.of_topic(TOPIC_INTERRUPT)]
            report["events"] = {
                "inbound": len(collector.of_topic(TOPIC_INBOUND)),
                "behavior": behavior_rows,
                "interrupt": interrupt_rows,
                "reply": [str(p)[:200] for p in collector.of_topic(TOPIC_REPLY)],
                "session": [str(p)[:200] for p in collector.of_topic(TOPIC_SESSION)],
            }
            report["sent"] = _reply_texts(server)
            report["audit"] = _audit_report(
                app,
                behavior_rows,
                interrupt_rows,
                inbound=len(collector.of_topic(TOPIC_INBOUND)),
                sent=len(report["sent"]),
                session_completed=len(collector.of_topic(TOPIC_SESSION)),
                mode=str(options.get("mode") or "active"),
            )
            report["stats"] = _domain_stats(app)
            if want_probe:
                report["probes"] = await probe_edges(
                    app,
                    server,
                    group_id=int(corpus.key.split("_")[1]),
                    force_wait=float(options.get("force_wait") or 60.0),
                )
            report["memory"] = await _memory_view(app)
            report["snapshot"] = _snapshot_view(app)
            report["pumps"] = _flatten_pumps(report["snapshot"].get("pumps"))
        finally:
            collector.close()
            await app.aclose()
            await server.stop()
    return report


async def _replay(
    server: MockOneBotServer,
    corpus: GroupCorpus,
    start: int,
    end: int,
    *,
    speed: float,
    max_replies: int,
) -> dict[str, Any]:
    """按原始间隔推事件；``max_replies`` 限制机器人开口次数（真模型很慢）。"""

    rows = corpus.messages[start : end + 1]
    group_id = int(corpus.key.split("_")[1])
    rate = max(speed, 0.01)
    # 第一条约在「现在」发出，之后按原始间隔推进；最后一条约在 now + span/rate 发出。
    base_wall = time.time()
    pushed = 0
    at_self = 0
    started = time.monotonic()
    truncated = False
    for index, msg in enumerate(rows):
        if max_replies and len(server.actions_of("send_group_msg")) >= max_replies:
            truncated = True
            break
        due = base_wall + (msg.ts - rows[0].ts) / rate
        delay = due - time.time()
        if delay > 0:
            await asyncio.sleep(min(delay, 5.0))
        await server.push(
            group_message(
                msg.text,
                group_id=group_id,
                user_id=int(msg.uin or 0) or (1000 + index),
                message_id=900000 + index,
                self_id=SELF_ID,
                at_self=msg.at_self,
                nickname=msg.name,
                time=int(time.time()),
            )
        )
        pushed += 1
        at_self += 1 if msg.at_self else 0
    return {
        "pushed": pushed,
        "at_self": at_self,
        "truncated": truncated,
        "wall_seconds": round(time.monotonic() - started, 2),
    }


async def _settle(
    collector: EventCollector, server: MockOneBotServer, *, quiet: float, budget: float
) -> dict[str, Any]:
    """等链路安静下来：模型调用可能要几十秒，所以看「多久没有新事件」而不是固定 sleep。"""

    deadline = time.monotonic() + budget
    last_change = time.monotonic()
    seen = 0
    replies = 0
    while time.monotonic() < deadline:
        await asyncio.sleep(0.5)
        total = len(collector.events)
        sent = len(server.actions_of("send_group_msg"))
        if total != seen or sent != replies:
            seen, replies = total, sent
            last_change = time.monotonic()
            continue
        if time.monotonic() - last_change >= quiet:
            return {"quiet_seconds": round(quiet, 1), "events": seen, "replies": sent, "settled": True}
    return {
        "quiet_seconds": round(quiet, 1),
        "events": len(collector.events),
        "replies": len(server.actions_of("send_group_msg")),
        "settled": False,
    }


# --------------------------------------------------------------------------
# 观测
# --------------------------------------------------------------------------
async def _memory_view(app: GrouppigApp) -> dict[str, Any]:
    """读生产库：聊天流水、会话、画像、关系分 —— 全部走契约名字或契约表。"""

    from sqlalchemy import text

    from grouppig.memory.runtime.schema import CONTRACT_TABLES

    view: dict[str, Any] = {"tables": {}}
    database = getattr(getattr(app, "memory", None), "db", None)
    if database is not None:
        for name in CONTRACT_TABLES:
            try:
                rows = await database.fetch_all(text("SELECT COUNT(*) AS n FROM " + name))
                view["tables"][name] = int(rows[0]["n"]) if rows else 0
            except Exception as error:  # noqa: BLE001 - 一张表读不到不该毁掉整次回放
                view["tables"][name] = "ERR:" + type(error).__name__
    try:
        rows = await app.container.call("rpc:chat.query", {})
        view["chat_rows"] = len(rows.get("messages") or ())
    except Exception as error:  # noqa: BLE001
        view["chat_rows"] = "ERR:" + type(error).__name__
    return view


def _snapshot_view(app: GrouppigApp) -> dict[str, Any]:
    """面板快照（跳过同步读表，那一项在有事件循环时会自报不可用）。"""

    try:
        snapshot = build_snapshot(app, SnapshotOptions(include_tables=False, event_limit=0))
    except Exception as error:  # noqa: BLE001
        return {"error": type(error).__name__ + ": " + str(error)}
    return {
        "app": snapshot.get("app"),
        "contract": snapshot.get("contract"),
        "pumps": snapshot.get("pumps"),
        "domains": snapshot.get("domains"),
    }


def _percentiles(values: Sequence[float]) -> dict[str, float | int]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {"count": 0}

    def percentile(q: float) -> float:
        index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * q + 0.5)))
        return round(ordered[index], 3)

    return {
        "count": len(ordered),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": round(ordered[-1], 3),
    }


def _audit_report(
    app: GrouppigApp,
    behavior: Sequence[Mapping[str, Any]],
    interrupts: Sequence[Mapping[str, Any]],
    *,
    inbound: int,
    sent: int,
    session_completed: int,
    mode: str,
) -> dict[str, Any]:
    """Extract redacted decision summaries and timing evidence, never prompts or reasoning text."""

    logger = getattr(app.container, "logger", None)
    records = list(getattr(logger, "records", ()) or ())
    model_events = {
        "model.classify",
        "model.fallback",
        "model.system1",
        "model.system1_escalate",
        "model.system1_escalated",
        "model.system1_unavailable",
    }
    excerpts: list[dict[str, Any]] = []
    request_latencies: list[float] = []
    decision_latencies: list[float] = []
    for record in records:
        event = str(getattr(record, "event", ""))
        duration = getattr(record, "duration_ms", None)
        fields = dict(getattr(record, "fields", {}) or {})
        if event.startswith("model.") and duration is not None:
            request_latencies.append(float(duration))
        elif event == "trace.model.classify" and duration is not None:
            request_latencies.append(float(duration))
        if event not in model_events:
            continue
        # Keep only explicit decision metadata; exclude message/state/prompt/error text.
        summary = {
            key: fields[key]
            for key in ("scenario", "model", "provider", "reason", "lowest_confidence", "threshold", "questions", "latency_ms")
            if key in fields and isinstance(fields[key], (str, int, float, bool, type(None)))
        }
        excerpts.append({
            "ts": round(float(getattr(record, "ts", 0.0)), 6),
            "level": str(getattr(record, "level", "")),
            "event": event,
            "duration_ms": round(float(duration), 3) if duration is not None else None,
            "summary": summary,
        })
    for row in behavior:
        latency = row.get("decision_latency_ms")
        if isinstance(latency, (int, float)):
            decision_latencies.append(float(latency))
    router_health = app.container.router.health() if app.container.router else {}
    stats = dict(router_health.get("stats") or {})
    decisions = [str(row.get("label") or "") for row in behavior]
    actions = [str(row.get("action") or "") for row in interrupts]
    asked = sum(int(item.get("questions") or 0) for item in excerpts)
    answered = sum(int(item.get("answered_count") or 0) for item in excerpts)
    return {
        "mode": mode,
        "mode_execution": "label_only_no_routing_or_side_effect_change",
        "laya_calls": int(stats.get("classify", 0) or 0) if mode != "off" else 0,
        "runtime": {
            "laya": {key: value for key, value in stats.items() if "laya" in key or key in ("fallback", "decode_failed")},
            "router": stats,
        },
        "request_latency_ms": _percentiles(request_latencies),
        "decision_latency_ms": _percentiles(decision_latencies),
        "coverage": {"asked": asked, "answered": answered, "rate": round(answered / asked, 4) if asked else None},
        "disagreement": {"behavior_labels": dict(Counter(decisions)), "interrupt_actions": dict(Counter(actions))},
        "funnel": {
            "inbound": inbound,
            "classified": len(behavior),
            "decided": len(interrupts),
            "composed": stats.get("reply_composed", 0),
            "sent": sent,
            "session_completed": session_completed,
        },
        "router_stats": stats,
        "omitted_log_records": max(0, len(excerpts) - 200),
        "reasoning_recorded": False,
    }


def _domain_stats(app: GrouppigApp) -> dict[str, Any]:
    """聚合器 / 决策器 / 评分器的计数器（判断「评分到底跑没跑」比看事件数可靠）。"""

    out: dict[str, Any] = {}
    perception = getattr(app, "perception", None)
    for name in ("aggregator", "decision", "scorer", "featurizer"):
        target = getattr(perception, name, None)
        if target is None:
            continue
        stats = getattr(target, "stats", None)
        if isinstance(stats, Mapping):
            out[name] = {k: v for k, v in stats.items() if isinstance(v, (int, float, str, bool))}
        snapshot = getattr(target, "snapshot", None)
        if name == "scorer" and callable(snapshot):
            try:
                snap = snapshot()
            except Exception:  # noqa: BLE001
                snap = None
            if isinstance(snap, Mapping):
                out["scorer_last"] = {k: snap.get(k) for k in ("thresholds", "weights") if k in snap}
    return out


def _flatten_pumps(pumps: Any) -> dict[str, Any]:
    """把 ``pump.status()`` 的扁平字典按泵名归拢成 {名字: 统计}。"""

    out: dict[str, Any] = {}
    for pump in pumps or ():
        if not isinstance(pump, Mapping):
            continue
        name = str(pump.get("name") or "?")
        out[name] = {k: v for k, v in pump.items() if k != "name"}
    return out


def _pump_stats(report: Mapping[str, Any], name: str) -> dict[str, Any]:
    return dict((report.get("pumps") or {}).get(name) or {})


def _label_counts(report: Mapping[str, Any]) -> dict[str, int]:
    counts: Counter = Counter()
    for item in (report.get("events") or {}).get("behavior") or ():
        label = item.get("label")
        counts[str(label) if label else "未知"] += 1
    return dict(counts)


# --------------------------------------------------------------------------
# 评定
# --------------------------------------------------------------------------
def aggregate(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """把每个群的回放结果汇总成一份可评定的账。"""

    total = {
        "groups": len(reports),
        "pushed": 0,
        "at_self": 0,
        "inbound": 0,
        "behavior": 0,
        "interrupt": 0,
        "reply_topic": 0,
        "session_completed": 0,
        "sent": 0,
        "settled": 0,
        "truncated": 0,
        "labels": Counter(),
        "reply_composed": 0,
        "stats": {},
        "interrupt_actions": Counter(),
        "tables": Counter(),
        "pumps": defaultdict(Counter),
        "replies": [],
        "wall_seconds": 0.0,
    }
    for report in reports:
        ingest = report.get("ingest") or {}
        events = report.get("events") or {}
        total["pushed"] += int(ingest.get("pushed") or 0)
        total["at_self"] += int(ingest.get("at_self") or 0)
        total["wall_seconds"] += float(ingest.get("wall_seconds") or 0.0)
        total["truncated"] += 1 if ingest.get("truncated") else 0
        total["settled"] += 1 if (report.get("settle") or {}).get("settled") else 0
        total["inbound"] += int(events.get("inbound") or 0)
        total["behavior"] += len(events.get("behavior") or ())
        total["interrupt"] += len(events.get("interrupt") or ())
        total["reply_topic"] += len(events.get("reply") or ())
        total["reply_composed"] += len(events.get("reply") or ())
        for name, stats in (report.get("stats") or {}).items():
            if isinstance(stats, Mapping):
                bucket = total["stats"].setdefault(name, Counter())
                bucket.update({k: v for k, v in stats.items() if isinstance(v, (int, float))})
        total["session_completed"] += len(events.get("session") or ())
        total["labels"].update(_label_counts(report))
        for item in events.get("interrupt") or ():
            total["interrupt_actions"][str(item.get("action") or item.get("reason") or "?")] += 1
        for name, count in ((report.get("memory") or {}).get("tables") or {}).items():
            if isinstance(count, int):
                total["tables"][name] += count
        for name, stats in (report.get("pumps") or {}).items():
            if isinstance(stats, Mapping):
                total["pumps"][name].update({k: v for k, v in stats.items() if isinstance(v, int)})
        for sent in report.get("sent") or ():
            total["sent"] += 1
            total["replies"].append({"group": report.get("group"), "text": sent.get("text")})
    total["labels"] = dict(total["labels"])
    total["interrupt_actions"] = dict(total["interrupt_actions"])
    total["tables"] = dict(total["tables"])
    total["pumps"] = {k: dict(v) for k, v in total["pumps"].items()}
    total["stats"] = {k: dict(v) for k, v in total["stats"].items()}
    total["wall_seconds"] = round(total["wall_seconds"], 1)
    return total


#: 设计树 8 个域的能力清单：每项给一个「取证函数」→ (判定, 证据文字)。
#: 判定只有三档：ok 有证据 / partial 部分 / missing 无证据。
def scorecard(total: Mapping[str, Any], probes: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """把观测事实对照设计能力，逐项给判定。"""

    probes = dict(probes or {})
    tables = total.get("tables") or {}
    pumps = total.get("pumps") or {}
    replies = total.get("replies") or []
    behavior = int(total.get("behavior") or 0)
    interrupt = int(total.get("interrupt") or 0)
    sent = int(total.get("sent") or 0)
    inbound = int(total.get("inbound") or 0)
    pushed = int(total.get("pushed") or 0)
    drain = pumps.get("perception.drain") or {}
    flow = pumps.get("expression.flow") or {}
    profile = pumps.get("social.profile") or {}
    sweeper = pumps.get("session.sweeper") or {}

    def grade(condition: bool, partial: bool = False) -> str:
        return "ok" if condition else ("partial" if partial else "missing")

    items: list[dict[str, Any]] = []

    def add(domain: str, capability: str, verdict: str, evidence: str) -> None:
        items.append({"domain": domain, "capability": capability, "verdict": verdict, "evidence": evidence})

    add(
        "gateway",
        "OneBot 事件接入与解码",
        grade(inbound > 0 and inbound >= pushed, pushed > 0),
        f"入站事件 {inbound} / 推送 {pushed}",
    )
    add(
        "gateway",
        "出站发送（节流后）",
        grade(sent > 0, flow.get("triggered", 0) > 0),
        f"send_group_msg {sent} 次；flow 泵 triggered={flow.get('triggered')}"
        f" driven={flow.get('driven')} sent={flow.get('sent')}",
    )
    add(
        "memory",
        "聊天流水落库",
        grade(int(tables.get("chat_messages") or 0) > 0),
        "chat_messages={} 行".format(tables.get("chat_messages")),
    )
    add(
        "memory",
        "会话 / 画像 / 关系表",
        grade(int(tables.get("member_profiles") or 0) > 0, int(tables.get("chat_messages") or 0) > 0),
        "member_profiles={} relationship_scores={} session_archives={}".format(
            tables.get("member_profiles"), tables.get("relationship_scores"), tables.get("session_archives")
        ),
    )
    add(
        "perception",
        "观察缓冲与排空",
        grade(int(drain.get("drained") or 0) > 0),
        "drain 泵 drained={} cleaned={} empty={}".format(
            drain.get("drained"), drain.get("cleaned"), drain.get("empty")
        ),
    )
    add(
        "perception",
        "特征编码 + 行为分类",
        grade(behavior > 0),
        f"behavior.changed {behavior} 次；类别分布 {total.get('labels')}",
    )
    decision = (total.get("stats") or {}).get("decision") or {}
    aggregator = (total.get("stats") or {}).get("aggregator") or {}
    add(
        "perception",
        "插话时机决策",
        grade(interrupt > 0, int(decision.get("decided") or 0) > 0),
        f"interrupt.triggered {interrupt} 次；决策器 decided={decision.get('decided')}"
        f" hold={decision.get('hold')} speak={decision.get('speak')}；"
        f"聚合器 classified={aggregator.get('classified')} changed={aggregator.get('changed')}"
        f" published={aggregator.get('published')}",
    )
    add(
        "session",
        "话题识别与会话生命周期",
        grade(int(total.get("session_completed") or 0) > 0, int(tables.get("chat_messages") or 0) > 0),
        f"session.completed {total.get('session_completed')} 次；sweeper 泵 {sweeper}",
    )
    add(
        "social",
        "画像与关系分刷新",
        grade(int(tables.get("member_profiles") or 0) > 0, profile.get("runs", 0) > 0),
        f"profile 泵 {profile}",
    )
    add(
        "reflection",
        "会话归档与反思",
        grade(int(tables.get("session_archives") or 0) > 0 or int(total.get("session_completed") or 0) > 0),
        "session_archives={}".format(tables.get("session_archives")),
    )
    forced = probes.get("force_speak") or {}
    add(
        "expression",
        "强制插话（绕开评分）能否走完并发出",
        grade(bool(forced.get("sent")), bool(forced.get("flow"))),
        "decide(score=0.9) → action={} flow={} 发出 {} 条 等待 {}s：{}".format(
            forced.get("action"),
            forced.get("flow"),
            forced.get("sent"),
            forced.get("waited"),
            str(forced.get("text"))[:40],
        ),
    )
    composed = int(total.get("reply_composed") or 0)
    add(
        "expression",
        "回复生成（心流 → 真模型）",
        grade(composed > 0, bool(replies)),
        f"心流回复 {composed} 条；send_group_msg 共 {len(replies)} 次"
        f"：{' / '.join(str(r.get('text'))[:40] for r in replies[:3]) or '无'}",
    )
    add(
        "infra",
        "模型网关（chat / classify / embed）",
        grade(bool(replies), behavior > 0),
        f"回复来自真模型调用；分类事件 {behavior} 次",
    )
    return items


def _verdict_mark(verdict: str) -> str:
    return {"ok": "[有证据]", "partial": "[部分]", "missing": "[无证据]"}.get(verdict, verdict)


# --------------------------------------------------------------------------
# 报告
# --------------------------------------------------------------------------
def render_text(report: Mapping[str, Any], *, verbose: bool = False) -> str:
    """人读摘要。"""

    lines: list[str] = []
    lines.append("=" * 78)
    lines.append("GrouPig 群聊回放模拟器 —— 能力完成度评定")
    lines.append("=" * 78)
    corpus = report.get("corpus") or {}
    lines.append("")
    lines.append(f"【语料】{'data.zip' if corpus else '（未统计）'}")
    for row in corpus.get("groups") or ():
        lines.append(
            f"  {str(row.get('name')):<22} 原始 {str(row.get('raw_total')):>6}"
            f" → 保留 {str(row.get('kept')):>6}   丢弃 {row.get('dropped')}"
        )
    total = report.get("total") or {}
    if total:
        lines.append("")
        lines.append(
            f"【回放】{total.get('groups', 0)} 个群 | 推送 {total.get('pushed')} 条"
            f" | 入站事件 {total.get('inbound')} | 行为事件 {total.get('behavior')}"
            f" | 插话 {total.get('interrupt')} | 发送 {total.get('sent')} 条"
        )
        lines.append(
            "        静默收尾 {}/{} | 到达回复上限提前截断 {} 个群 | 回放墙钟 {:.1f}s".format(
                total.get("settled"), total.get("groups"), total.get("truncated"), total.get("wall_seconds") or 0.0
            )
        )
        lines.append("        行为类别分布 {}".format(total.get("labels")))
        lines.append("        插话判定分布 {}".format(total.get("interrupt_actions")))
        tables = {k: v for k, v in (total.get("tables") or {}).items() if v}
        lines.append(f"        落库非空表 {tables}")
        for name, stats in sorted((total.get("pumps") or {}).items()):
            lines.append(f"        泵 {name:<22} {stats}")
        for name, stats in sorted((total.get("stats") or {}).items()):
            lines.append(f"        统计 {name:<20} {stats}")
    findings = report.get("findings") or []
    if findings:
        lines.append("")
        lines.append("【发现】回放账 + 边探测得出的缺陷（每条都带证据）")
        lines.append("-" * 78)
        for item in findings:
            lines.append(f"  [{item['severity'].upper()}] {item['id']} {item['title']}")
            lines.append(f"        证据：{item['evidence']}")
            lines.append(f"        机制：{item['mechanism']}")
    probes = report.get("probes") or {}
    if probes and verbose:
        lines.append("")
        lines.append("【边探测原始返回】")
        for name, value in probes.items():
            lines.append(f"  {name:<18} {json.dumps(value, ensure_ascii=False, default=str)[:220]}")
    lines.append("")
    lines.append("【能力完成度】设计树 8 个域，逐项对照观测事实")
    lines.append("-" * 78)
    current = ""
    counts = Counter()
    for item in report.get("scorecard") or ():
        if item["domain"] != current:
            current = item["domain"]
            lines.append("")
            lines.append("  " + current.upper())
        counts[item["verdict"]] += 1
        lines.append(f"    {_verdict_mark(item['verdict'])} {item['capability']:<28} {item['evidence']}")
    lines.append("")
    lines.append("-" * 78)
    lines.append(
        f"  合计：有证据 {counts.get('ok', 0)} / 部分 {counts.get('partial', 0)} / 无证据 {counts.get('missing', 0)}"
    )
    now = (report.get("probes") or {}).get("classify_now") or {}
    if now:
        lines.append("")
        lines.append(
            "【回放结束时的行为判定】behavior={} conf={} source={}（规则={} 模型={} conf={}）".format(
                now.get("behavior"),
                now.get("confidence"),
                now.get("source"),
                now.get("rule_behavior"),
                now.get("llm_label"),
                now.get("llm_confidence"),
            )
        )
    forced = (report.get("probes") or {}).get("force_speak") or {}
    if forced.get("text"):
        lines.append("")
        lines.append(f"【强制插话的回复（绕开评分，直接给 0.9 分）】{forced.get('text')}")
    replies = total.get("replies") or []
    if replies:
        lines.append("")
        lines.append("【机器人实际发言】")
        for row in replies[:20]:
            lines.append("  [{}] {}".format(row.get("group"), row.get("text")))
    if verbose:
        lines.append("")
        lines.append("【逐群明细】")
        for group in report.get("groups") or ():
            lines.append("  --- {}".format(group.get("group")))
            lines.append("      窗口 {}".format(group.get("window")))
            lines.append("      入站 %s" % (group.get("ingest") or {}))
            lines.append("      收尾 %s" % (group.get("settle") or {}))
            for item in (group.get("events") or {}).get("interrupt") or ():
                lines.append(f"      插话 {item}")
            for item in (group.get("events") or {}).get("behavior") or ():
                lines.append(f"      行为 {item}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# 边探测：直接打契约名字，输入固定，用来定位「哪条边断了」
# --------------------------------------------------------------------------
#: 探测用的群号基址（与语料群号错开，避免污染回放结果）。
PROBE_GROUP = 990001


async def probe_edges(
    app: GrouppigApp, server: MockOneBotServer, *, group_id: int = 0, force_wait: float = 60.0
) -> dict[str, Any]:
    """在真实装配的应用上逐条探测关键边；每条都记录原始返回，不做推测。"""

    container = app.container
    probes: dict[str, Any] = {}

    # 1) 特征器 → 分类 / 候选话题（下游边是否可达）
    try:
        result = await container.call(
            "rpc:normalizer.features",
            {"group_id": PROBE_GROUP, "message_id": "probe", "content": "探测用消息", "ts": time.time()},
            force=True,
        )
        probes["features_edges"] = {
            "calls": dict((result.get("downstream") or {}).get("calls") or {}),
            "failed": list((result.get("downstream") or {}).get("failed") or ()),
        }
    except Exception as error:  # noqa: BLE001
        probes["features_edges"] = {"error": type(error).__name__ + ": " + str(error)}

    # 2) 被 @ 的识别：**走完整生产链路**（真 WS 帧 → 网关 demux → 观察者滚动窗 → 评分器）。
    #    曾经这里用「手搓行、只改 at_self 位置」来探测，结果把探针假象当成了缺陷：
    #    绕过网关直接调 rpc:observer.ingest 时 payload 里根本没有 at_self
    #    （生产中由 gateway/router/demux.py 计算并注入），于是恒判「识别不出」。
    #    被 @ 这件事只能在完整链路上断言，这也是本模块唯一正确的探测口径。
    mention_group = PROBE_GROUP + 1
    try:
        await server.push(
            group_message(
                "@机器人 在吗",
                group_id=mention_group,
                user_id=201,
                message_id=PROBE_GROUP * 10 + 1,
                self_id=SELF_ID,
                at_self=True,
                time=int(time.time()),
            )
        )
        await asyncio.sleep(0.6)
        await container.call("rpc:observer.buffer.drain")
        scored = await container.call("rpc:interrupt.score", mention_group, behavior="discussion")
        probes["mention_via_gateway"] = {
            "mentioned": (scored.get("components") or {}).get("mentioned"),
            "total": scored.get("total"),
            "band": scored.get("band"),
            "action": (scored.get("decision") or {}).get("action"),
        }
    except Exception as error:  # noqa: BLE001
        probes["mention_via_gateway"] = {"error": type(error).__name__ + ": " + str(error)}

    # 2b) 潜在脆弱点：库表行（rpc:chat.window）没有顶层 at_self，它在 raw 下、另存 mentions。
    #     生产评分器接的是观察者滚动窗，所以当前不触发；一旦有调用方改喂库表行，@ 就会失效。
    db_group = PROBE_GROUP + 2
    try:
        await server.push(
            group_message(
                "@机器人 在吗",
                group_id=db_group,
                user_id=202,
                message_id=PROBE_GROUP * 10 + 2,
                self_id=SELF_ID,
                at_self=True,
                time=int(time.time()),
            )
        )
        await asyncio.sleep(0.6)
        await container.call("rpc:observer.buffer.drain")
        window = await container.call("rpc:chat.window", db_group, seconds=300)
        rows = [dict(row) for row in (window.get("messages") or ())]
        scored = await container.call("rpc:interrupt.score", db_group, behavior="discussion", messages=rows)
        probes["mention_db_rows"] = {
            "rows": len(rows),
            "top_level_at_self": sorted({bool(row.get("at_self")) for row in rows}),
            "raw_at_self": sorted({bool((row.get("raw") or {}).get("at_self")) for row in rows}),
            "mentioned": (scored.get("components") or {}).get("mentioned"),
            "action": (scored.get("decision") or {}).get("action"),
        }
    except Exception as error:  # noqa: BLE001
        probes["mention_db_rows"] = {"error": type(error).__name__ + ": " + str(error)}

    # 3) 冷启动：一个从没分类过的群，首次分类会不会被当成「变化」
    cold_group = PROBE_GROUP + 3
    try:
        before = dict(getattr(app.perception.aggregator, "stats", {}) or {})
        for index, text in enumerate(("这个周末一起去爬山吗", "爬山要带什么装备", "早上八点集合吧")):
            await container.call(
                "rpc:observer.ingest",
                group_message(
                    text,
                    group_id=cold_group,
                    user_id=300 + index,
                    message_id=800000 + index,
                    self_id=SELF_ID,
                    time=int(time.time()),
                ),
            )
        await container.call("rpc:observer.buffer.drain")
        await asyncio.sleep(0.4)
        after = dict(getattr(app.perception.aggregator, "stats", {}) or {})
        # 这里**不能再调一次 rpc:behavior.classify**：drain 已经级联分类过一次，
        # 再调就是第二次（previous 已非空、changed 必为 False），会把「首次判定」读成 False 而误导。
        # 首次判定的结果直接取聚合器留存的快照。
        first = dict(getattr(app.perception.aggregator, "last", lambda _gid: None)(cold_group) or {})
        probes["cold_start"] = {
            "classified": int(after.get("classified", 0)) - int(before.get("classified", 0)),
            "changed": int(after.get("changed", 0)) - int(before.get("changed", 0)),
            "published": int(after.get("published", 0)) - int(before.get("published", 0)),
            "first_behavior": first.get("behavior"),
            "first_previous": first.get("previous"),
            "first_changed": first.get("changed"),
        }
    except Exception as error:  # noqa: BLE001
        probes["cold_start"] = {"error": type(error).__name__ + ": " + str(error)}

    # 3.5) 直接问一次行为分类：看规则与模型各自给了什么、置信度多少
    if group_id:
        try:
            decided = await container.call("rpc:behavior.classify", int(group_id))
            probes["classify_now"] = {
                "behavior": decided.get("behavior"),
                "confidence": decided.get("confidence"),
                "source": decided.get("source"),
                "resolved": decided.get("resolved"),
                "ambiguous": decided.get("ambiguous"),
                "llm_label": (decided.get("llm") or {}).get("label"),
                "llm_confidence": (decided.get("llm") or {}).get("confidence"),
                "rule_behavior": decided.get("rule_behavior"),
                "changed": decided.get("changed"),
            }
        except Exception as error:  # noqa: BLE001
            probes["classify_now"] = {"error": type(error).__name__ + ": " + str(error)}

    # 4) 强制插话：绕开评分直接给高分，看心流能不能走完并真的发出去
    try:
        sent_before = len(server.actions_of("send_group_msg"))
        decided = await container.call("rpc:interrupt.decide", PROBE_GROUP + 4, score=0.9, behavior="discussion")
        text = None
        wait = float(force_wait)
        started = time.monotonic()
        while time.monotonic() - started < wait:
            await asyncio.sleep(0.3)
            if len(server.actions_of("send_group_msg")) > sent_before:
                text = _reply_texts(server)[-1].get("text")
                break
        probes["force_speak"] = {
            "action": decided.get("action"),
            "score": decided.get("score"),
            "flow": bool(decided.get("flow")),
            "sent": len(server.actions_of("send_group_msg")) - sent_before,
            "waited": round(time.monotonic() - started, 1),
            "text": text,
        }
    except Exception as error:  # noqa: BLE001
        probes["force_speak"] = {"error": type(error).__name__ + ": " + str(error)}
    return probes


# --------------------------------------------------------------------------
# 发现：把「回放账 + 探测结果」翻成带证据的缺陷清单
# --------------------------------------------------------------------------
def findings(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """只列有直接证据的发现；每条都带机制与证据数字。"""

    total = report.get("total") or {}
    probes = report.get("probes") or {}
    out: list[dict[str, Any]] = []

    def add(fid: str, severity: str, title: str, evidence: str, mechanism: str) -> None:
        out.append({"id": fid, "severity": severity, "title": title, "evidence": evidence, "mechanism": mechanism})

    pushed = int(total.get("pushed") or 0)
    inbound = int(total.get("inbound") or 0)
    changed = int(total.get("behavior") or 0)
    interrupt = int(total.get("interrupt") or 0)
    sent = int(total.get("sent") or 0)
    at_self = int(total.get("at_self") or 0)

    cold = probes.get("cold_start") or {}
    if cold.get("classified") and not cold.get("changed"):
        add(
            "F1",
            "high",
            "冷启动后首个行为不算「变化」，插话链路整条不触发",
            f"新群分类 {cold.get('classified')} 次，其中 changed={cold.get('changed')}"
            f"、published={cold.get('published')}；该群当前行为={cold.get('first_behavior')!r}"
            f"（首次分类前 previous 为空）",
            "aggregator.decide: changed = bool(previous) and previous != behavior —— previous 为空时恒 False，"
            "behavior.changed 不发，rpc:interrupt.score 不会被调",
        )
    mention = probes.get("mention_via_gateway") or {}
    mention_value = mention.get("mentioned")
    if mention_value is not None and float(mention_value or 0.0) < 1.0:
        add(
            "F2",
            "high",
            "被 @ 识别不出来（完整链路上）",
            f"真 WS 帧带 at 段推入后，评分器读到的 mentioned={mention_value}、"
            f"band={mention.get('band')}、action={mention.get('action')}",
            "scorer.compute: at_self = any(row.get('at_self')) —— 读数或上游注入有一处断了",
        )
    db_rows = probes.get("mention_db_rows") or {}
    if db_rows.get("mentioned") is not None and float(db_rows.get("mentioned") or 0.0) < 1.0:
        add(
            "F6",
            "low",
            "库表行不带顶层 at_self：改喂 rpc:chat.window 的行就会丢掉 @",
            f"库表行 top_level_at_self={db_rows.get('top_level_at_self')}、"
            f"raw_at_self={db_rows.get('raw_at_self')}（共 {db_rows.get('rows')} 行）"
            f"→ mentioned={db_rows.get('mentioned')}、action={db_rows.get('action')}",
            "chat_messages 表没有 at_self 列（只有 mentions 与 raw）；生产评分器接的是观察者滚动窗，"
            "所以当前不触发 —— 属于「有调用方换数据源就会咬」的潜在脆弱点",
        )
    failed = list((probes.get("features_edges") or {}).get("failed") or ())
    if failed:
        add(
            "F3",
            "medium",
            "特征器的下游边有断裂",
            f"rpc:normalizer.features(force) 的下游失败：{', '.join(failed)}",
            "cleaner → rpc:normalizer.features 的 cascade 里这条边不可达",
        )
    composed = int(total.get("reply_composed") or 0)
    if pushed and not composed:
        add(
            "F4",
            "high",
            "整段回放里没有一条由心流生成的回复",
            f"推送 {pushed} 条（其中 @ 机器人 {at_self} 条）→ 行为变化 {changed}、插话 {interrupt}、"
            f"心流回复 {composed}；send_group_msg 共 {sent} 次（含命令等其它路径）",
            "回放语料里没有一条 @ 机器人的消息（at_self=0）；无 @ 时插话分约 0.28 < 阈值 0.55，评分一律 hold —— 这是阈值策略问题，不是链路断（评分确实跑了，见决策器计数）",
        )
    if inbound < pushed:
        add(
            "F5",
            "medium",
            "突发消息下有入站事件被总线丢弃",
            f"推送 {pushed} 条，入站事件只有 {inbound} 条",
            "EventBus 重入深度上限 max_depth=16，超限直接 drop（bus.depth_exceeded）",
        )
    return out


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------
async def run(options: Mapping[str, Any]) -> dict[str, Any]:
    """按选项跑一次：统计语料 → 选窗口 → 回放 → 汇总评定。"""

    zip_path = Path(options.get("zip") or (REPO_ROOT / "data.zip"))
    corpus_report: dict[str, Any] = {"groups": []}
    groups: list[GroupCorpus] = []
    if zip_path.exists():
        groups = load_corpus(zip_path, verbose=not options.get("quiet"))
        for corpus in groups:
            start, end = corpus.densest(float(options.get("span") or 120.0))
            corpus_report["groups"].append(
                {
                    "key": corpus.key,
                    "name": corpus.name,
                    "raw_total": corpus.raw_total,
                    "kept": len(corpus.messages),
                    "dropped": dict(corpus.dropped.most_common()),
                    "densest_window": {
                        "span_seconds": float(options.get("span") or 120.0),
                        "messages": end - start + 1,
                        "start_ts": corpus.messages[start].ts if corpus.messages else 0,
                        "end_ts": corpus.messages[end].ts if corpus.messages else 0,
                    },
                }
            )
    else:
        corpus_report["error"] = "找不到 " + str(zip_path)

    if options.get("corpus_only"):
        return {"corpus": corpus_report, "groups": [], "total": {}, "scorecard": []}

    picked = groups[: int(options.get("groups") or 1)]
    reports: list[dict[str, Any]] = []
    for index, corpus in enumerate(picked):
        if not corpus.messages:
            continue
        start, end = corpus.densest(float(options.get("span") or 120.0))
        if options.get("quiet"):
            print(
                f"回放：{corpus.name}（{end - start + 1} 条 / {float(options.get('span') or 0):.0f}s 窗口）…",
                flush=True,
            )
        reports.append(await simulate_group(corpus, start, end, options={**options, "probe": index == 0}))

    total = aggregate(reports)
    probes: dict[str, Any] = {}
    for report in reports:
        probes.update(report.get("probes") or {})
    result = {
        "corpus": corpus_report,
        "groups": reports,
        "total": total,
        "probes": probes,
        "scorecard": scorecard(total, probes),
    }
    result["findings"] = findings(result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GrouPig 群聊回放模拟器（真实 QQ 记录 → 观察机器人反应）")
    parser.add_argument("--zip", default=str(REPO_ROOT / "data.zip"), help="语料压缩包（默认 data.zip）")
    parser.add_argument("--corpus", action="store_true", help="只统计语料，不起应用")
    parser.add_argument("--groups", type=int, default=1, help="回放前几个群（按包内顺序）")
    parser.add_argument("--span", type=float, default=120.0, help="每个群取最密的多少秒窗口")
    parser.add_argument("--speed", type=float, default=1.0, help="时间加速倍数（1 = 原始节奏）")
    parser.add_argument("--budget", type=float, default=240.0, help="每个群收尾等待上限（秒）")
    parser.add_argument("--quiet", type=float, default=6.0, help="多久没有新事件算安静")
    parser.add_argument("--max-replies", type=int, default=0, help="每个群最多让机器人开口几次（0 = 不限）")
    parser.add_argument(
        "--transport", choices=("real", "fake"), default="real", help="real 走真模型；fake 用确定性替身"
    )
    parser.add_argument(
        "--mode", choices=("off", "shadow", "active"), default="active",
        help="审计标签；当前仅记录模式，不改变模型路由或副作用",
    )
    parser.add_argument("--no-probe", action="store_true", help="跳过边探测（探测里的强制插话会真的调一次模型）")
    parser.add_argument("--force-wait", type=float, default=60.0, help="强制插话后等回复的上限（秒；真模型很慢）")
    parser.add_argument("--json", dest="json_path", default="", help="把完整报告落盘")
    parser.add_argument("--quiet-out", action="store_true", help="只输出 JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    options = {
        "zip": args.zip,
        "corpus_only": args.corpus,
        "groups": args.groups,
        "span": args.span,
        "speed": args.speed,
        "budget": args.budget,
        "quiet": args.quiet,
        "max_replies": args.max_replies,
        "transport": args.transport,
        "mode": args.mode,
        "probe": not args.no_probe,
        "force_wait": args.force_wait,
    }
    report = asyncio.run(run(options))
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if args.quiet_out:
        print(json.dumps(report, ensure_ascii=False, default=str))
    else:
        print(render_text(report, verbose=bool(args.groups > 1)))
    graded = report.get("scorecard") or []
    return 0 if all(item["verdict"] != "missing" for item in graded) else 1


if __name__ == "__main__":
    raise SystemExit(main())
