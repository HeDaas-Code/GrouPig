"""长流程语料测验的混合记录仪 —— 把思维链、调用链、Bot 行为模式写进一条时间轴。

与 ``group_sim.py`` 的分工：``group_sim.py`` 产出**评定报告**（能力完成度打分），
本模块产出**记录日志**（一次运行的完整可读轨迹）。两者可独立使用。

三条轨道各自回答一个问题：

* ``TRACE`` **思维链** —— 「为什么」：每一步内部状态（特征、规则信号、候选、
  行为判定、评分分量、阈值对比、冷却），是各组件**自己算出来的值**，不是反推的。
* ``CALL``  **调用链** —— 「怎么走的」：注册表叶子调用与总线事件，形成
  消息 → 清洗 → 去重 → 特征 → 分类 → 评分 → 决策 → 心流 的因果链。
* ``BOT``   **行为模式** —— 「它做了什么」：完整行为状态机（切换/保持/沉默窗口/
  决策动作/开口内容），由 TRACE+CALL 的原始事实归纳。

时间轴以**回放虚拟时间**（源语料时间戳）为主，墙钟为辅；同一秒内用序号稳定排序。
``--observe-only`` 时不下载任何传输层（不替换 ModelRouter 的 transport），
模型调用一律走真实端点，适用于「观察真模型行为」的场景。

用法::

    uv run python tools/trace_replay.py --groups 2 --span 900 --speed 8 \
        --out /tmp/trace.log --json /tmp/trace.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
for _extra in (REPO_ROOT / "src", REPO_ROOT / "tests", REPO_ROOT / "tools"):
    if str(_extra) not in sys.path:
        sys.path.insert(0, str(_extra))

from group_sim import (  # noqa: E402
    SELF_ID,
    GroupCorpus,
    _pin_laya_transport,
    _write_config,
    load_corpus,
)

from gateway_helpers import MockOneBotServer, group_message  # noqa: E402
from grouppig.infra.runtime.registry import Registry  # noqa: E402
from grouppig.runtime.app import GrouppigApp, build_app  # noqa: E402

TOPIC_INBOUND = "kafka:grouppig.qq.message.received"
TOPIC_BEHAVIOR = "kafka:grouppig.behavior.changed"
TOPIC_INTERRUPT = "kafka:grouppig.interrupt.triggered"
TOPIC_REPLY = "kafka:grouppig.reply.composed"
TOPIC_SESSION = "kafka:grouppig.session.completed"
WATCH_TOPICS = (TOPIC_INBOUND, TOPIC_BEHAVIOR, TOPIC_INTERRUPT, TOPIC_REPLY, TOPIC_SESSION)

TRACE = "TRACE"
CALL = "CALL"
BOT = "BOT"

WIDTH = 108


@dataclass
class Frame:
    """混合记录里的一帧：三轨共用同一结构，便于统一排序与渲染。"""

    seq: int
    track: str
    sim_ts: float
    wall_ts: float
    group_id: int
    actor: str
    action: str
    detail: dict[str, Any] = field(default_factory=dict)
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "track": self.track,
            "sim_ts": self.sim_ts,
            "sim_time": time.strftime("%H:%M:%S", time.gmtime(self.sim_ts)) if self.sim_ts else "",
            "wall_ts": round(self.wall_ts, 3),
            "group_id": self.group_id,
            "actor": self.actor,
            "action": self.action,
            "detail": self.detail,
            "note": self.note,
        }


class Recorder:
    """收集三种轨道并做「源语料时间戳回填」。"""

    def __init__(self, *, max_frames: int = 200000) -> None:
        self.frames: list[Frame] = []
        self.max_frames = max_frames
        self.overflow = 0
        self._seq = 0
        # 群 → 该群已回放到的**源语料时间戳**（回放推进时更新）。
        # 三轨里只有「入站消息」自带源时间；其它帧发生在两次 push 之间，
        # 所以回填到当前已回放到的源时间戳上，日志时间轴才对齐真实语料。
        self.cursor: dict[int, float] = {}

    def set_cursor(self, group_id: int, ts: float) -> None:
        self.cursor[int(group_id)] = float(ts)

    def add(
        self,
        track: str,
        group_id: int,
        actor: str,
        action: str,
        detail: Mapping[str, Any] | None = None,
        *,
        sim_ts: float | None = None,
        note: str = "",
    ) -> Frame | None:
        if len(self.frames) >= self.max_frames:
            self.overflow += 1
            return None
        self._seq += 1
        gid = int(group_id or 0)
        frame = Frame(
            seq=self._seq,
            track=track,
            sim_ts=float(sim_ts if sim_ts is not None else self.cursor.get(gid, 0.0)),
            wall_ts=time.time(),
            group_id=gid,
            actor=actor,
            action=action,
            detail=dict(detail or {}),
            note=note,
        )
        self.frames.append(frame)
        return frame

    def of_track(self, track: str) -> list[Frame]:
        return [f for f in self.frames if f.track == track]


def _short(value: Any, limit: int = 42) -> str:
    """把任意值压成一行短串（渲染用）。"""

    if value is None:
        return "-"
    if isinstance(value, float):
        text = f"{value:.4g}"
    elif isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (list, tuple)):
        text = "[" + ",".join(_short(v, 12) for v in list(value)[:4]) + (",…]" if len(value) > 4 else "]")
    elif isinstance(value, Mapping):
        text = "{" + ",".join(f"{k}={_short(v, 12)}" for k, v in list(value.items())[:4]) + "}"
    else:
        text = str(value)
    text = text.replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _pad(text: str, width: int) -> str:
    text = text if len(text) <= width else text[: width - 1] + "…"
    return text + " " * (width - len(text))


def render(recorder: Recorder, *, meta: Mapping[str, Any]) -> str:
    """把帧流渲染成人类可读的混合日志（三轨交织、按虚拟时间排序）。"""

    lines: list[str] = []
    lines.append("=" * WIDTH)
    lines.append("GrouPig 长流程语料测验 —— 混合记录日志（TRACE 思维链 / CALL 调用链 / BOT 行为模式）")
    lines.append("=" * WIDTH)
    for key, value in meta.items():
        lines.append("  " + _pad(str(key), 14) + str(value))
    lines.append("")
    lines.append("  图例： TRACE=为什么（内部状态）  CALL=怎么走的（叶子调用与总线事件）  BOT=它做了什么（行为状态机）")
    lines.append("  时间： sim=源语料时间戳（回放虚拟时间 HH:MM:SS）  序号=全局稳定排序键")
    lines.append("=" * WIDTH)
    lines.append("")
    lines.append("  " + _pad("序号", 6) + _pad("sim", 9) + _pad("轨道", 7) + _pad("环节", 22) + "事件与状态")
    lines.append("  " + "-" * (WIDTH - 4))

    last_group = None
    for frame in recorder.frames:
        if frame.group_id != last_group:
            last_group = frame.group_id
            lines.append("")
            lines.append("  +-- 群 " + str(frame.group_id) + " " + "-" * 40)
        head = (
            "  "
            + _pad("#" + str(frame.seq), 6)
            + _pad(frame.as_dict().get("sim_time") or "-", 9)
            + _pad(frame.track, 7)
            + _pad(frame.actor + "." + frame.action, 22)
        )
        body = " ".join(k + "=" + _short(v) for k, v in frame.detail.items())
        lines.append(head + body + (("  // " + frame.note) if frame.note else ""))
    lines.append("")
    lines.append("=" * WIDTH)
    counts = {track: len(recorder.of_track(track)) for track in (TRACE, CALL, BOT)}
    tail = f"  合计：TRACE {counts[TRACE]} 帧 · CALL {counts[CALL]} 帧 · BOT {counts[BOT]} 帧"
    if recorder.overflow:
        tail += " · 超限丢弃 " + str(recorder.overflow)
    lines.append(tail)
    lines.append("=" * WIDTH)
    return "\n".join(lines)


def _wrap_leaf_calls(app: GrouppigApp, recorder: Recorder, group_getter: Any) -> Any:
    """给注册表装一层「调用旁路」，把叶子调用记进 CALL 轨（不改动任何叶子实现）。"""

    registry = app.container.registry
    original = registry.acall

    async def traced(name: str, *args: Any, **kwargs: Any) -> Any:
        started = time.monotonic()
        ok = True
        error = ""
        try:
            return await original(name, *args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - 记录后原样抛出
            ok = False
            error = type(exc).__name__ + ": " + str(exc)
            raise
        finally:
            recorder.add(
                CALL,
                group_getter(),
                "registry",
                name.replace("rpc:", "").replace(".", "_"),
                {
                    "leaf": registry.get(name).module if registry.has(name) else "?",
                    "status": "ok" if ok else "error",
                    "ms": round((time.monotonic() - started) * 1000, 1),
                    "args": len(args),
                    "err": error,
                },
            )

    registry.acall = traced  # type: ignore[method-assign]
    return original


def _wrap_aggregator(app: GrouppigApp, recorder: Recorder, group_getter: Any) -> None:
    """包一层行为分类器：把 decide() 的**完整内部态**记进 TRACE 轨。"""

    aggregator = app.perception.aggregator
    original_decide = aggregator.decide
    original_classify = aggregator.classify
    # decide 拿不到 group_id（签名里没有），用 classify 进入时的群号兜底。
    self_active: dict[str, Any] = {"group_id": 0}

    # ``decide`` 是**同步纯函数、全关键字参数**（见 aggregator.py:110），没有 group_id 也没有 features。
    # 曾经按「印象」写成 async decide(group_id, features, **kwargs)，结果真调用一进来就
    # TypeError: missing 2 required positional arguments —— 与刚修过的 F3 是同一类错误
    # （凭印象猜参数形状，没读契约）。分组归属只能由 classify 的上下文给出。
    def traced_decide(**kwargs: Any) -> dict[str, Any]:
        rules = dict(kwargs.get("rules") or {})
        signals = dict(rules.get("signals") or {})
        encoded = dict(kwargs.get("encoded") or {})
        group_id = int(encoded.get("group_id") or self_active["group_id"])
        decided = original_decide(**kwargs)
        recorder.add(
            TRACE,
            int(group_id),
            "behavior.decide",
            "判定",
            {
                "behavior": decided.get("behavior"),
                "confidence": decided.get("confidence"),
                "source": decided.get("source"),
                "rule": decided.get("rule_behavior"),
                "llm": decided.get("llm_behavior"),
                "changed": decided.get("changed"),
                "previous": decided.get("previous"),
            },
            note="规则与模型各自给值，resolved=" + str(decided.get("resolved")),
        )
        if signals:
            recorder.add(
                TRACE,
                int(group_id),
                "rules.signals",
                "信号",
                {k: signals.get(k) for k in sorted(signals)[:8]},
                note="规则引擎的原始判据（行为=" + str(decided.get("rule_behavior")) + "）",
            )
        return decided

    async def traced_classify(group_id: int, **kwargs: Any) -> dict[str, Any]:
        self_active["group_id"] = int(group_id)
        out = await original_classify(group_id, **kwargs)
        features = dict(out.get("features") or {})
        if features:
            recorder.add(
                TRACE,
                int(group_id),
                "features",
                "特征向量",
                {k: features.get(k) for k in ("message_count", "sender_count", "rate", "repeat_ratio")},
                note="窗口形态（分类的输入）",
            )
        return out

    aggregator.decide = traced_decide  # type: ignore[method-assign]
    aggregator.classify = traced_classify  # type: ignore[method-assign]


def _wrap_interrupt(app: GrouppigApp, recorder: Recorder) -> None:
    """包一层评分器与决策器：把**评分分量、阈值对比、冷却**记进 TRACE 轨。"""

    scorer = app.perception.scorer
    original_score = scorer.score

    async def traced_score(group_id: int, **kwargs: Any) -> dict[str, Any]:
        out = await original_score(group_id, **kwargs)
        components = dict(out.get("components") or {})
        decision = dict(out.get("decision") or {})
        penalties = dict(out.get("penalties") or {})
        recorder.add(
            TRACE,
            int(group_id),
            "interrupt.score",
            "插话评分",
            {
                "total": out.get("total"),
                "band": out.get("band"),
                "threshold": decision.get("threshold"),
                "action": decision.get("action"),
                "reason": decision.get("reason"),
            },
            note="分量 " + _short(components) + " 罚分 " + _short(penalties),
        )
        return out

    scorer.score = traced_score  # type: ignore[method-assign]


class BotModel:
    """从 TRACE/CALL 的原始事实归纳**行为模式**（不新增任何取值，只做归纳）。"""

    def __init__(self, recorder: Recorder) -> None:
        self.recorder = recorder
        self.current: dict[int, str] = {}
        self.since: dict[int, float] = {}
        self.speaks: list[dict[str, Any]] = []
        self.silence: dict[int, int] = {}

    def on_behavior(self, group_id: int, payload: Mapping[str, Any], sim_ts: float) -> None:
        gid = int(group_id)
        label = str(payload.get("behavior") or payload.get("label") or "?")
        previous = str(payload.get("previous") or "")
        self.current[gid] = label
        self.since[gid] = sim_ts
        self.recorder.add(
            BOT,
            gid,
            "behavior",
            "切换",
            {
                "from": previous or "(cold)",
                "to": label,
                "conf": payload.get("confidence"),
                "source": payload.get("source"),
            },
            note="行为状态机：" + (previous or "冷启动") + " → " + label,
        )

    def on_message(self, group_id: int, label: str, text: str, source: str) -> None:
        """记一条消息，并统计「当前行为下的连续条数」。"""

        gid = int(group_id)
        current = self.current.get(gid, "")
        if current == label:
            self.silence[gid] = self.silence.get(gid, 0) + 1
        else:
            self.silence[gid] = 1

    def on_decision(self, group_id: int, payload: Mapping[str, Any]) -> None:
        gid = int(group_id)
        action = str(payload.get("action") or payload.get("decision") or "?")
        self.recorder.add(
            BOT,
            gid,
            "interrupt",
            "决策 " + action,
            {"score": payload.get("score"), "reason": payload.get("reason"), "behavior": self.current.get(gid, "")},
            note="阈值以上的话才会 speak；hold 表示继续沉默",
        )

    def on_sent(self, group_id: int, text: str) -> None:
        """出站帧（权威的「它到底说了什么」）。与 on_speak 分开：composed 只是组好了。"""

        gid = int(group_id)
        self.speaks.append({"group_id": gid, "text": text, "source": "onebot.send"})
        self.recorder.add(
            BOT,
            gid,
            "send",
            "发出",
            {"chars": len(text), "text": text[:80]},
            note="网关已发出（OneBot 动作）",
        )

    def on_speak(self, group_id: int, text: str, source: str) -> None:
        gid = int(group_id)
        self.speaks.append({"group_id": gid, "text": text, "source": source})
        self.recorder.add(BOT, gid, "speak", "开口", {"chars": len(text), "text": text[:80]}, note="来源 " + source)


class HybridRecorder:
    """把 Recorder + BotModel + 总线订阅绑在一起，供回放过程驱动。"""

    def __init__(self, app: GrouppigApp, *, observe_only: bool, max_frames: int) -> None:
        self.app = app
        self.observe_only = observe_only
        self.recorder = Recorder(max_frames=max_frames)
        self.bot = BotModel(self.recorder)
        self.active_group = 0
        self.outbound_seen = 0
        self._limiter_last: dict[str, Any] = {}
        self._subs: list[Any] = []

    def cursor(self, group_id: int, ts: float) -> None:
        self.active_group = int(group_id)
        self.recorder.set_cursor(group_id, ts)

    def _gid(self) -> int:
        return self.active_group

    def snapshot_limiter(self) -> None:
        """把限流器的统计快照记一笔（checks/allowed/delayed/denied/waited）。

        出站帧与「被节流拦下」是两件事：只记 send_group_msg 会看不到被限流吞掉的回复，
        于是「它没说话」与「它想说但被拦了」在日志里长得一样 —— 快照用来消歧。
        """

        gateway = getattr(self.app, "gateway", None)
        limiter = getattr(gateway, "limiter", None) or getattr(getattr(gateway, "sender", None), "limiter", None)
        stats = getattr(limiter, "stats", None)
        if stats is None:
            return
        as_dict = getattr(stats, "as_dict", None)
        detail = (
            dict(as_dict()) if callable(as_dict) else {k: v for k, v in vars(stats).items() if not k.startswith("_")}
        )
        if detail and detail == self._limiter_last:
            return
        self._limiter_last = detail
        self.recorder.add(
            CALL,
            self.active_group,
            "rate_limiter",
            "节流账本",
            detail,
            note="checks=总校验 allowed=放行 delayed=延迟 denied=拒绝",
        )

    def install(self) -> None:
        bus = self.app.container.bus
        self._subs.append(bus.subscribe(TOPIC_INBOUND, self._on_inbound, name="trace:inbound"))
        self._subs.append(bus.subscribe(TOPIC_BEHAVIOR, self._on_behavior, name="trace:behavior"))
        self._subs.append(bus.subscribe(TOPIC_INTERRUPT, self._on_interrupt, name="trace:interrupt"))
        self._subs.append(bus.subscribe(TOPIC_REPLY, self._on_reply, name="trace:reply"))
        self._subs.append(bus.subscribe(TOPIC_SESSION, self._on_session, name="trace:session"))
        _wrap_leaf_calls(self.app, self.recorder, self._gid)
        _wrap_aggregator(self.app, self.recorder, self._gid)
        _wrap_interrupt(self.app, self.recorder)

    def close(self) -> None:
        for sub in self._subs:
            try:
                self.app.container.bus.unsubscribe(sub)
            except Exception:  # noqa: BLE001 - 关闭阶段不因退订失败而中断
                pass
        self._subs = []

    async def _on_inbound(self, event: Any) -> dict[str, Any]:
        payload = getattr(event, "payload", event)
        if not isinstance(payload, Mapping):
            return {"ok": True}
        gid = int(payload.get("group_id") or 0)
        text = str(payload.get("content") or payload.get("text") or "")
        sim_ts = float(payload.get("ts") or self.recorder.cursor.get(gid, 0.0))
        self.recorder.add(
            CALL,
            gid,
            "bus",
            "inbound",
            {"from": payload.get("sender_id"), "at_self": payload.get("at_self"), "chars": len(text)},
            sim_ts=sim_ts,
            note="入站：" + _short(text, 54),
        )
        self.bot.on_message(gid, str(payload.get("content") or ""), text, "inbound")
        return {"ok": True}

    async def _on_behavior(self, event: Any) -> dict[str, Any]:
        payload = getattr(event, "payload", event)
        if isinstance(payload, Mapping):
            gid = int(payload.get("group_id") or 0)
            self.recorder.add(
                CALL,
                gid,
                "bus",
                "behavior.changed",
                {"behavior": payload.get("behavior"), "previous": payload.get("previous")},
                note="总线事件：行为切换驱动下游插话评分",
            )
            self.bot.on_behavior(gid, payload, self.recorder.cursor.get(gid, 0.0))
        return {"ok": True}

    async def _on_interrupt(self, event: Any) -> dict[str, Any]:
        payload = getattr(event, "payload", event)
        if isinstance(payload, Mapping):
            self.bot.on_decision(int(payload.get("group_id") or 0), payload)
        return {"ok": True}

    async def _on_reply(self, event: Any) -> dict[str, Any]:
        payload = getattr(event, "payload", event)
        if isinstance(payload, Mapping):
            gid = int(payload.get("group_id") or 0)
            text = str(payload.get("text") or payload.get("content") or "")
            self.recorder.add(CALL, gid, "bus", "reply.composed", {"chars": len(text)}, note=_short(text, 54))
            self.bot.on_speak(gid, text, "flow")
        return {"ok": True}

    async def _on_session(self, event: Any) -> dict[str, Any]:
        payload = getattr(event, "payload", event)
        if isinstance(payload, Mapping):
            self.recorder.add(
                CALL,
                int(payload.get("group_id") or 0),
                "bus",
                "session.completed",
                {"topic": payload.get("topic"), "messages": payload.get("message_count")},
                note="会话收束（长窗口才跑得到）",
            )
        return {"ok": True}


async def run_traced(corpus: GroupCorpus, start: int, end: int, *, options: Mapping[str, Any]) -> dict[str, Any]:
    """回放一个群的窗口，把三轨记录写进文件。"""

    speed = float(options.get("speed") or 1.0)
    budget = float(options.get("budget") or 120.0)
    quiet = float(options.get("quiet") or 10.0)
    observe_only = bool(options.get("observe_only"))
    out_path = Path(str(options.get("out") or "/tmp/trace.log"))
    rows = corpus.messages[start : end + 1]
    group_id = int(corpus.key.split("_")[1])

    server = MockOneBotServer(self_id=SELF_ID)
    url = await server.start()
    summary: dict[str, Any] = {"group": corpus.name, "group_id": group_id, "messages": len(rows)}
    with tempfile.TemporaryDirectory() as tmp:
        config = load_config_for(url, tmp)
        transport = None
        if not observe_only:
            from helpers import FakeTransport

            transport = FakeTransport(reply="（假模型回复）")
        app: GrouppigApp = build_app(
            config, registry=Registry(), transport=transport, dsn="sqlite+aiosqlite:///:memory:"
        )
        if transport is not None:
            _pin_laya_transport(app.container.router, transport)
        # **必须先 start 再装探针**：``app.perception`` 在 start 时才由 install_perception 建出来，
        # 早装会包到一个 None 上（曾经因此 TRACE/BOT 全 0 帧而 CALL 正常——CALL 包的是
        # registry.acall，那个在容器构造时就存在，所以掩盖了顺序错误）。
        await app.start(connect=True, pumps=True)
        recorder = HybridRecorder(app, observe_only=observe_only, max_frames=int(options.get("max_frames") or 200000))
        recorder.install()
        recorder.cursor(group_id, rows[0].ts)
        rate = max(speed, 0.01)
        base_wall = time.time()
        started = time.monotonic()
        try:
            for index, msg in enumerate(rows):
                recorder.cursor(group_id, msg.ts)
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
                        time=int(msg.ts),
                    )
                )
                await _drain_outbound(recorder, server)
            await _settle(recorder, server, quiet=quiet, budget=budget)
        finally:
            summary["wall_seconds"] = round(time.monotonic() - started, 2)
            summary["trace_frames"] = len(recorder.recorder.frames)
            summary["outbound_frames"] = recorder.outbound_seen
            summary["speaks"] = list(recorder.bot.speaks)
            meta = {
                "语料群": corpus.name,
                "群号": group_id,
                "窗口": f"{len(rows)} 条 / {rows[-1].ts - rows[0].ts}s",
                "加速": f"{speed}x",
                "传输": "真实端点（observe-only）" if observe_only else "假传输",
                "墙钟": f"{summary['wall_seconds']:.1f}s",
            }
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(render(recorder.recorder, meta=meta), encoding="utf-8")
            summary["out"] = str(out_path)
            summary["frames"] = [f.as_dict() for f in recorder.recorder.frames]
            recorder.close()
            await app.aclose()
    return summary


async def _drain_outbound(recorder: HybridRecorder, server: MockOneBotServer) -> int:
    """把 MockOneBot 上**真正发出去的**动作帧记进出站轨。

    为什么必须这样记：网关出站走的是 OneBot 动作（``rpc:onebot.send`` → ``send_group_msg``），
    **不发总线事件** —— 只订阅 ``kafka:grouppig.reply.composed`` 只能看到「表达层组好了」，
    看不到「有没有真的发出去、内容是什么、节流是否拦下」。曾经因此出站轨恒为空，
    让「确实没说话」与「工具漏记」两种结论混在一起（用户先发现）。
    """

    seen = recorder.outbound_seen
    frames = list(server.actions)
    for index in range(seen, len(frames)):
        frame = frames[index]
        action = str(frame.get("action") or "?")
        params = dict(frame.get("params") or {})
        text = ""
        message = params.get("message")
        if isinstance(message, list):
            text = "".join(str(seg.get("data", {}).get("text", "")) for seg in message if seg.get("type") == "text")
        elif isinstance(message, str):
            text = message
        gid = int(params.get("group_id") or 0)
        recorder.recorder.add(
            CALL,
            gid,
            "onebot",
            action,
            {"chars": len(text), "echo": frame.get("echo"), "retcode": frame.get("retcode")},
            note="出站帧（真发到 WS 上）：" + _short(text, 54),
        )
        if action == "send_group_msg":
            recorder.bot.on_sent(gid, text)
    recorder.outbound_seen = len(frames)
    return len(frames)


async def _settle(recorder: HybridRecorder, server: MockOneBotServer, *, quiet: float, budget: float) -> None:
    """等链路安静下来（与 group_sim 同口径：看「多久没有新事件」而非固定 sleep）。"""

    deadline = time.monotonic() + float(budget)
    last = time.monotonic()
    seen = 0
    while time.monotonic() < deadline:
        await asyncio.sleep(0.5)
        await _drain_outbound(recorder, server)
        recorder.snapshot_limiter()
        now = len(recorder.recorder.frames)
        if now != seen:
            seen = now
            last = time.monotonic()
        elif time.monotonic() - last >= float(quiet):
            break


def load_config_for(ws_url: str, tmp: str) -> Any:
    """把仓库配置指向 mock 服务端的临时副本。"""

    from grouppig.infra.config.loader import load_config

    return load_config(_write_config(Path(tmp), ws_url), use_local=False)


async def run(options: Mapping[str, Any]) -> dict[str, Any]:
    """按选项跑：载语料 → 选窗口 → 逐群回放并写三轨日志。"""

    zip_path = Path(str(options.get("zip") or (REPO_ROOT / "data.zip")))
    groups = load_corpus(zip_path, verbose=bool(options.get("verbose")))
    picked = groups[: int(options.get("groups") or 1)]
    reports: list[dict[str, Any]] = []
    for corpus in picked:
        if not corpus.messages:
            continue
        start, end = corpus.densest(float(options.get("span") or 900.0))
        print(f"记录：{corpus.name}（{end - start + 1} 条 / {int(options.get('span') or 0)}s 窗口）…", flush=True)
        report = await run_traced(corpus, start, end, options=options)
        reports.append(report)
        print(
            f"  完成：{report['trace_frames']} 帧，墙钟 {report['wall_seconds']:.1f}s，"
            f"开口 {len(report['speaks'])} 次 -> {report['out']}",
            flush=True,
        )
    return {"reports": reports}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GrouPig 长流程语料测验 —— 混合记录仪（思维链 / 调用链 / 行为模式）")
    parser.add_argument("--zip", default=str(REPO_ROOT / "data.zip"), help="语料压缩包（默认 data.zip）")
    parser.add_argument("--groups", type=int, default=1, help="记录前几个群")
    parser.add_argument("--span", type=float, default=900.0, help="每群取最密的多少秒窗口")
    parser.add_argument("--speed", type=float, default=8.0, help="时间加速倍数")
    parser.add_argument("--budget", type=float, default=120.0, help="收尾等待上限（秒）")
    parser.add_argument("--quiet", type=float, default=10.0, help="多久没有新帧算安静")
    parser.add_argument("--out", default="/tmp/trace.log", help="混合日志输出路径")
    parser.add_argument("--json", dest="json_path", default="", help="帧流 JSON 输出路径")
    parser.add_argument("--max-frames", type=int, default=200000, help="帧数上限（防跑飞占满磁盘）")
    parser.add_argument("--observe-only", action="store_true", help="不注入假传输：模型走真实端点（只观察）")
    parser.add_argument("--verbose", action="store_true", help="打印语料统计")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    options = vars(args)
    result = asyncio.run(run(options))
    if options.get("json_path"):
        Path(str(options["json_path"])).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    total = sum(int(r["trace_frames"]) for r in result["reports"])
    print(f"合计 {total} 帧")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
