"""grouppig.reflection.session-review.metrics —— 行为指标计算器（`rpc:review.metrics`）。

职责（对应设计 `grouppig.reflection.session-review.metrics`
「计算本段会话行为指标：插话率、回应率、冷场时长、预设命中率」）：

拿时间线，算出**描述「这段会话里我怎么表现」的一组确定性数字**，供结论生成器与策略综合器使用。
设计依赖 `rpc:review.metrics` → `rpc:review.timeline`（已在实现中生效：调用方既可以传
`timeline=...`，也可以只传 `session_id`，本叶子自己回读时间线）。

**核心指标**（全部 0-1，除时长/条数外）::

    interrupt_rate   插话率：我方抢在他人话头前（<3s）说话的占比
    reply_rate       回应率：我方发言后 60s 内有人接话的比例
    ignore_rate      被无视率：我方发言后没人接话的比例（= 1 - reply_rate）
    miss_rate        漏接率：出现插话机会（连续他人对话且我方缺席）但没接的比例
    hit_rate         命中率：他人明确指向我方（回复/@/点名）时我方是否接上
    silence_ratio    冷场占比：>5 分钟无消息的时长占会话总时长
    repeat_rate      复读率：我方重复同一内容的比例
    presence_rate    发言占比：我方消息占全部消息的比例
    peak_rate        峰值密度：最密 60 秒里的消息数 / 60

**结论口径**（`insights` 键，与 `strategy.synthesizer` 的输入一一对应）::

    noise      = interrupt_rate            （插话过早的比例，高了要收敛）
    missed     = miss_rate                 （该接没接，高了要更主动）
    ignored    = ignore_rate               （说了没人理，要降频/换语气）
    positive   = hit_rate              （被点名时接上的比例，高了保持）
    coverage   = 1 - silence_ratio         （会话覆盖度）

设计：`grouppig.reflection.session-review.metrics`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.session-review.metrics"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:review.metrics",)

#: 设计依赖：读时间线（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_TIMELINE = "rpc:review.timeline"

#: 回应窗口（秒）：我方发言后多久内算被接话。
REPLY_WINDOW = 60.0

#: 抢话窗口（秒）：他人发言后我方在这么短时间内说话算插话。
INTERRUPT_WINDOW = 3.0

#: 冷场阈值（秒）：超过就算一段冷场。
SILENCE_GAP = 300.0

#: 判定复读的重复次数。
REPEAT_LIMIT = 3

#: 指标键（顺序即报表顺序）。
METRIC_KEYS: tuple[str, ...] = (
    "interrupt_rate",
    "reply_rate",
    "ignore_rate",
    "miss_rate",
    "hit_rate",
    "silence_ratio",
    "repeat_rate",
    "presence_rate",
    "peak_rate",
)


def clamp01(value: Any, default: float = 0.0) -> float:
    """把任意值收进 [0, 1]。"""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, number))


def ratio(part: float, whole: float, default: float = 0.0) -> float:
    """`part / whole`，`whole` 为 0 时返回 `default`。"""

    return clamp01(part / whole) if whole else default


def _ts(row: Mapping[str, Any], fallback: float = 0.0) -> float:
    try:
        return float(row.get("ts", fallback) or fallback)
    except (TypeError, ValueError):
        return fallback


def silence_spans(entries: Sequence[Mapping[str, Any]], *, gap: float = SILENCE_GAP) -> list[dict[str, Any]]:
    """找出所有冷场区间（相邻消息间隔 > `gap`）。"""

    spans: list[dict[str, Any]] = []
    for previous, entry in zip(entries, entries[1:], strict=False):
        delta = _ts(entry) - _ts(previous)
        if delta > gap:
            spans.append(
                {
                    "start": _ts(previous),
                    "end": _ts(entry),
                    "duration": round(delta, 3),
                    "after_message_id": str(previous.get("message_id") or ""),
                    "before_message_id": str(entry.get("message_id") or ""),
                }
            )
    return spans


def peak_density(entries: Sequence[Mapping[str, Any]], *, window: float = 60.0) -> int:
    """最密 `window` 秒内的消息数。"""

    stamps = sorted(_ts(entry) for entry in entries)
    best = 0
    start = 0
    for end, stamp in enumerate(stamps):
        while stamp - stamps[start] > window:
            start += 1
        best = max(best, end - start + 1)
    return best


def compute_metrics(
    timeline: Mapping[str, Any],
    *,
    self_id: int | None = None,
    reply_window: float = REPLY_WINDOW,
    interrupt_window: float = INTERRUPT_WINDOW,
    silence_gap: float = SILENCE_GAP,
) -> dict[str, Any]:
    """从时间线算行为指标（纯函数，便于回放与单测）。"""

    entries = [dict(item) for item in (timeline.get("timeline") or timeline.get("entries") or ())]
    archive = timeline.get("archive") if isinstance(timeline.get("archive"), Mapping) else {}
    me = int(self_id if self_id is not None else (timeline.get("self_id") or 0) or 0)
    mine = [entry for entry in entries if entry.get("is_self") or int(entry.get("sender_id", 0) or 0) == me]
    others = [entry for entry in entries if entry not in mine]
    mine_ids = {str(entry.get("message_id") or "") for entry in mine}

    # 插话率：他人发言后 interrupt_window 秒内我方抢话
    premature = 0
    chances = 0
    for previous, entry in zip(entries, entries[1:], strict=False):
        if previous.get("is_self"):
            continue
        chances += 1
        if entry.get("is_self") and _ts(entry) - _ts(previous) < interrupt_window:
            premature += 1
    interrupt_rate = ratio(premature, chances)

    # 回应率 / 被无视率：我方发言后 reply_window 秒内有人接
    replied = 0
    for entry in mine:
        stamp = _ts(entry)
        if any(stamp <= _ts(other) <= stamp + reply_window for other in others):
            replied += 1
    reply_rate = ratio(replied, len(mine))
    ignore_rate = (1.0 - reply_rate) if mine else 0.0

    # 漏接率：插话机会里没接上的比例
    opportunities = [
        item for item in (timeline.get("opportunities") if timeline.get("opportunities") is not None else [])
    ]
    if timeline.get("opportunities") is None:
        # 未预计算时，用简单口径近似：他人连续对话段且我方完全缺席
        opportunities = []
        streak = 0
        for entry in entries:
            if entry.get("is_self"):
                if streak >= 2:
                    opportunities.append({"messages": streak})
                streak = 0
            else:
                streak += 1
        if streak >= 2:
            opportunities.append({"messages": streak})
    missed_opportunities = sum(1 for item in opportunities if not item.get("mentioned_self"))
    miss_rate = ratio(missed_opportunities, len(opportunities))

    # 命中率：他人指向我方（@ 或回复）时我方是否接上
    directed = [entry for entry in others if entry.get("mentions_self") or str(entry.get("reply_to") or "") in mine_ids]
    hit = 0
    for entry in directed:
        stamp = _ts(entry)
        if any(stamp <= _ts(candidate) <= stamp + reply_window for candidate in mine):
            hit += 1
    hit_rate = ratio(hit, len(directed))

    # 冷场占比
    spans = silence_spans(entries, gap=silence_gap)
    silence_total = sum(span["duration"] for span in spans)
    duration = float(timeline.get("span", {}).get("duration", 0.0) or 0.0)
    if not duration:
        duration = float(archive.get("duration", 0.0) or 0.0)
    if not duration and entries:
        duration = max(0.0, _ts(entries[-1]) - _ts(entries[0]))
    silence_ratio = clamp01(silence_total / duration) if duration else 0.0

    # 复读率
    seen: dict[str, int] = {}
    repeats = 0
    for entry in mine:
        content = str(entry.get("content") or "").strip()
        if not content:
            continue
        seen[content] = seen.get(content, 0) + 1
        if seen[content] >= REPEAT_LIMIT:
            repeats += 1
    repeat_rate = ratio(repeats, len(mine))

    presence_rate = ratio(len(mine), len(entries))
    peak = peak_density(entries)

    metrics = {
        "interrupt_rate": round(interrupt_rate, 4),
        "reply_rate": round(reply_rate, 4),
        "ignore_rate": round(ignore_rate, 4),
        "miss_rate": round(miss_rate, 4),
        "hit_rate": round(hit_rate, 4),
        "silence_ratio": round(silence_ratio, 4),
        "repeat_rate": round(repeat_rate, 4),
        "presence_rate": round(presence_rate, 4),
        "peak_rate": round(clamp01(peak / 60.0), 4),
    }
    insights = {
        "noise": metrics["interrupt_rate"],
        "missed": metrics["miss_rate"],
        "miss": metrics["miss_rate"],
        "ignored": metrics["ignore_rate"],
        "positive": metrics["hit_rate"],
        "coverage": round(1.0 - metrics["silence_ratio"], 4),
        "repeat": metrics["repeat_rate"],
        "presence": metrics["presence_rate"],
    }
    counts = {
        "messages": len(entries),
        "self_messages": len(mine),
        "others": len(others),
        "chances": chances,
        "premature": premature,
        "replied": replied,
        "opportunities": len(opportunities),
        "missed_opportunities": missed_opportunities,
        "directed": len(directed),
        "hit": hit,
        "repeats": repeats,
        "silences": len(spans),
        "peak_messages": peak,
    }
    return {
        "metrics": metrics,
        "insights": insights,
        "counts": counts,
        "silences": spans,
        "duration": round(duration, 3),
        "session_id": str(timeline.get("session_id") or ""),
        "self_id": me,
    }


class BehaviorMetrics:
    """行为指标计算（`rpc:review.metrics`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        self_id: int = 0,
        reply_window: float = REPLY_WINDOW,
        interrupt_window: float = INTERRUPT_WINDOW,
        silence_gap: float = SILENCE_GAP,
    ) -> None:
        self.ctx = ctx
        self.self_id = int(self_id)
        self.reply_window = float(reply_window)
        self.interrupt_window = float(interrupt_window)
        self.silence_gap = float(silence_gap)
        self.computed = 0
        self.last: dict[str, Any] = {}

    # ---- 工具 ----------------------------------------------------------
    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else time.time()

    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """跨域调用；下游没挂时返回 `None`（复盘绝不因为某个下游缺失而整体失败）。"""

        caller = getattr(self.ctx, "call", None)
        if not callable(caller):
            return None
        try:
            return await caller(name, *args, **kwargs)
        except Exception as error:  # noqa: BLE001 - 下游缺失/失败都退化为本地计算
            self._log("warning", "reflection.dependency_unavailable", name=name, error=str(error))
            return None

    async def metrics(
        self,
        timeline: Mapping[str, Any] | None = None,
        *,
        session_id: str = "",
        group_id: int = 0,
        self_id: int | None = None,
        with_silences: bool = True,
    ) -> dict[str, Any]:
        """`rpc:review.metrics` —— 计算行为指标（无时间线时按 `session_id` 回读）。"""

        self.computed += 1
        built = dict(timeline) if isinstance(timeline, Mapping) else None
        if built is None:
            sid = str(session_id or "")
            built = await self._call(DEP_TIMELINE, sid, group_id=group_id)
            if built is None:
                built = {
                    "session_id": sid,
                    "self_id": self.self_id if self_id is None else int(self_id),
                    "timeline": [],
                    "opportunities": [],
                    "span": {},
                }
        me = int(self_id if self_id is not None else (built.get("self_id") or self.self_id) or 0)
        measured = compute_metrics(
            built,
            self_id=me,
            reply_window=self.reply_window,
            interrupt_window=self.interrupt_window,
            silence_gap=self.silence_gap,
        )
        result = {
            **measured,
            "group_id": int(group_id or built.get("group_id", 0) or 0),
            "now": self._now(),
        }
        if not with_silences:
            result.pop("silences", None)
        self.last = result
        self._log(
            "debug",
            "review.metrics_computed",
            session_id=result["session_id"],
            messages=result["counts"]["messages"],
            interrupt_rate=result["metrics"]["interrupt_rate"],
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "computed": self.computed,
            "self_id": self.self_id,
            "reply_window": self.reply_window,
            "interrupt_window": self.interrupt_window,
            "silence_gap": self.silence_gap,
        }


def make_handlers(metrics: BehaviorMetrics) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:review.metrics": metrics.metrics}


def register(target: Any, instance: BehaviorMetrics | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or BehaviorMetrics()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "DEP_TIMELINE",
    "INTERRUPT_WINDOW",
    "METRIC_KEYS",
    "MODULE",
    "NAMES",
    "REPLY_WINDOW",
    "REPEAT_LIMIT",
    "SILENCE_GAP",
    "BehaviorMetrics",
    "clamp01",
    "compute_metrics",
    "make_handlers",
    "peak_density",
    "ratio",
    "register",
    "silence_spans",
]
