"""grouppig.reflection.evaluator.scoring —— 策略评分器（`rpc:strategy.score`）。

职责（对应设计 `grouppig.reflection.evaluator.scoring`「给策略打分并输出保留/回滚建议」）：

在策略上线后的**后续会话**里，读那段时间的聊天流水，算「政策生效前后」的行为差异，
给策略一个 0-1 分，并给出 `keep`/`rollback` 建议。两条设计依赖都在实现中生效：

* `rpc:strategy.score` → `rpc:chat.query`（读后续消息，按 `since`/`until` 切片）；
* `rpc:strategy.score` → `rpc:presets.match`（对照预设，判断实际落点场景）。

**打分口径**（确定性、可解释；全部落在 0-1）::

    score = Σ(weight_i × component_i) / Σ(weight_i)

    response  回应率：我方发言后被他人回应的比例（越高越好）
    continuity 延续率：有效回应里对方继续接话（>=2 轮）的比例
    engagement 参与度：消息里指向我方（mention/reply_to 我方）的占比
    rhythm    节奏：我方发言间隔的合理度（过密扣分，过疏也扣分）
    restraint 克制：刷屏/复读占比越低越好
    safety    安全：命中敏感/{AI 自曝}/@全体 的比例越低越好

**会话内 vs 会话后**：调用方可以传 `baseline_messages=`（策略启用前的对照窗口），
本模块会算两组的差值 `delta`，作为 A/B 的原料；没传就只有绝对分。

设计：`grouppig.reflection.evaluator.scoring`（叶子模块）。
"""

from __future__ import annotations

import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.evaluator.scoring"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:strategy.score",)

#: 设计依赖（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_CHAT_QUERY = "rpc:chat.query"
DEP_PRESETS_MATCH = "rpc:presets.match"

#: 各分项权重（和 = 1.0）。
COMPONENT_WEIGHTS: dict[str, float] = {
    "response": 0.25,
    "continuity": 0.20,
    "engagement": 0.20,
    "rhythm": 0.15,
    "restraint": 0.12,
    "safety": 0.08,
}

#: 建议阈值：>= keep 保留，<= rollback 回滚，中间观察。
KEEP_THRESHOLD = 0.6
ROLLBACK_THRESHOLD = 0.45

#: 我方发言间隔的合理区间（秒）。低于下限算过密，高于上限算过疏。
MIN_INTERVAL = 4.0
MAX_INTERVAL = 180.0

#: 判定为刷屏的重复次数。
REPEAT_LIMIT = 3

#: 安全扫描（与 `strategy.validator` 同口径，避免两处定义漂移）。
AI_PATTERNS: tuple[str, ...] = (r"\bAI\b", r"人工智能", r"机器人", r"助手", r"语言模型", r"提示词", r"prompt")
SENSITIVE_PATTERNS: tuple[str, ...] = (r"傻逼", r"妈的", r"操你", r"去死", r"智障", r"脑残", r"你妈")
AT_ALL_PATTERNS: tuple[str, ...] = (r"@\s*全体成员", r"@\s*all\b", r"@\s*everyone\b")

_AI_RE = re.compile("|".join(AI_PATTERNS), re.I)
_SENSITIVE_RE = re.compile("|".join(SENSITIVE_PATTERNS), re.I)
_AT_ALL_RE = re.compile("|".join(AT_ALL_PATTERNS), re.I)


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


def split_by_self(
    messages: Sequence[Mapping[str, Any]], self_id: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把消息分成「我方」与「他人」两摞。"""

    mine: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []
    for item in messages:
        row = dict(item)
        if int(row.get("sender_id", 0) or 0) == int(self_id):
            mine.append(row)
        else:
            others.append(row)
    return mine, others


def _ts(row: Mapping[str, Any], fallback: float) -> float:
    try:
        return float(row.get("ts", fallback) or fallback)
    except (TypeError, ValueError):
        return fallback


def compute_components(
    messages: Sequence[Mapping[str, Any]],
    *,
    self_id: int,
    bot_name: str = "",
) -> dict[str, Any]:
    """算六个分项（不含权重），返回 0-1 值与原始计数。"""

    rows = [dict(item) for item in messages]
    mine, others = split_by_self(rows, self_id)
    mine_ids = {str(row.get("message_id") or "") for row in mine}
    mine_tokens = {token for token in (bot_name,) if token}

    # 1) 回应率：我方发言后 60 秒内有人接话
    responded = 0
    for row in mine:
        stamp = _ts(row, 0.0)
        if any(_ts(other, 0.0) - stamp <= 60.0 and _ts(other, 0.0) >= stamp for other in others):
            responded += 1
    response = ratio(responded, len(mine))

    # 2) 延续率：我方发言后 60 秒内有 >=2 条他人消息
    continued = 0
    for row in mine:
        stamp = _ts(row, 0.0)
        following = [other for other in others if stamp <= _ts(other, 0.0) <= stamp + 60.0]
        if len(following) >= 2:
            continued += 1
    continuity = ratio(continued, len(mine))

    # 3) 参与度：他人消息里指向我方的比例
    directed = 0
    for row in others:
        reply_to = str(row.get("reply_to") or "")
        mentions = [str(item) for item in (row.get("mentions") or ())]
        text = str(row.get("content") or "")
        if reply_to and reply_to in mine_ids:
            directed += 1
        elif any(str(self_id) in item for item in mentions):
            directed += 1
        elif mine_tokens and any(token in text for token in mine_tokens):
            directed += 1
    engagement = ratio(directed, len(others))

    # 4) 节奏：我方相邻发言间隔的合理度
    stamps = sorted(_ts(row, 0.0) for row in mine)
    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False) if b - a > 0]
    if not gaps:
        rhythm = 1.0 if len(mine) <= 1 else 0.5
    else:
        average = sum(gaps) / len(gaps)
        if average < MIN_INTERVAL:
            rhythm = clamp01(average / MIN_INTERVAL)
        elif average > MAX_INTERVAL:
            rhythm = clamp01(MAX_INTERVAL / average)
        else:
            rhythm = 1.0

    # 5) 克制：刷屏/复读
    repeats = 0
    seen: dict[str, int] = {}
    for row in mine:
        content = str(row.get("content") or "").strip()
        if not content:
            continue
        seen[content] = seen.get(content, 0) + 1
        if seen[content] >= REPEAT_LIMIT:
            repeats += 1
    burst = sum(1 for index, row in enumerate(mine) if index and _ts(row, 0.0) - _ts(mine[index - 1], 0.0) < 1.0)
    restraint = 1.0 - clamp01((repeats + burst) / max(1, len(mine)))

    # 6) 安全：自曝 AI / 敏感词 / @全体
    unsafe = 0
    for row in mine:
        content = str(row.get("content") or "")
        if _AI_RE.search(content) or _SENSITIVE_RE.search(content) or _AT_ALL_RE.search(content):
            unsafe += 1
    safety = 1.0 - ratio(unsafe, len(mine))

    components = {
        "response": round(response, 4),
        "continuity": round(continuity, 4),
        "engagement": round(engagement, 4),
        "rhythm": round(rhythm, 4),
        "restraint": round(restraint, 4),
        "safety": round(safety, 4),
    }
    counts = {
        "total": len(rows),
        "mine": len(mine),
        "others": len(others),
        "responded": responded,
        "continued": continued,
        "directed": directed,
        "repeats": repeats,
        "burst": burst,
        "unsafe": unsafe,
        "average_gap": round(sum(gaps) / len(gaps), 3) if gaps else 0.0,
    }
    return {"components": components, "counts": counts}


def weighted_score(components: Mapping[str, Any]) -> float:
    """按权重把分项合成 0-1 总分。"""

    total = sum(COMPONENT_WEIGHTS.values()) or 1.0
    accumulated = 0.0
    for key, weight in COMPONENT_WEIGHTS.items():
        accumulated += weight * clamp01(components.get(key))
    return round(max(0.0, min(1.0, accumulated / total)), 4)


def verdict_for(score: float, *, keep: float = KEEP_THRESHOLD, rollback: float = ROLLBACK_THRESHOLD) -> str:
    """分数 → 保留/观察/回滚。"""

    if float(score) >= float(keep):
        return "keep"
    if float(score) <= float(rollback):
        return "rollback"
    return "watch"


def advice_for(components: Mapping[str, Any]) -> list[str]:
    """按最低分项给出改法（人读，也是 synthesize 的输入）。"""

    ranked = sorted(COMPONENT_WEIGHTS, key=lambda key: clamp01(components.get(key)))
    tips = {
        "response": "多挑被点名或话题直接相关的时机说话",
        "continuity": "把话说完、留个话头，别每次只丢一个字",
        "engagement": "用群里已有的梗和称呼，别只自说自话",
        "rhythm": "拉开发言间隔，避免连续刷屏或长时间沉默",
        "restraint": "不要复读同一句，控制连续发言",
        "safety": "严禁自曝 AI 身份、敏感词与 @全体成员",
    }
    weak = [key for key in ranked if clamp01(components.get(key)) < 0.5][:3]
    return [tips[key] for key in weak] or ["保持当前节奏"]


class StrategyScorer:
    """给策略打分（`rpc:strategy.score`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        self_id: int = 0,
        keep_threshold: float = KEEP_THRESHOLD,
        rollback_threshold: float = ROLLBACK_THRESHOLD,
    ) -> None:
        self.ctx = ctx
        self.self_id = int(self_id)
        self.keep_threshold = float(keep_threshold)
        self.rollback_threshold = float(rollback_threshold)
        self.scores = 0
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

    async def _read_messages(
        self,
        *,
        messages: Sequence[Mapping[str, Any]] | None,
        group_id: int,
        since: float | None,
        until: float | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if messages is not None:
            return [dict(item) for item in messages]
        if not group_id:
            return []
        criteria: dict[str, Any] = {"group_id": int(group_id), "limit": int(limit), "order": "asc"}
        if since is not None:
            criteria["since"] = float(since)
        if until is not None:
            criteria["until"] = float(until)
        loaded = await self._call(DEP_CHAT_QUERY, criteria)
        return [dict(item) for item in (loaded or {}).get("messages") or ()]

    # ---- 主入口 --------------------------------------------------------
    async def score(
        self,
        *,
        preset_id: str = "",
        strategy: Mapping[str, Any] | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        baseline_messages: Sequence[Mapping[str, Any]] | None = None,
        group_id: int = 0,
        self_id: int | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 500,
        bot_name: str = "",
        threshold: float | None = None,
        apply: bool = False,
        now: float | None = None,
    ) -> dict[str, Any]:
        """`rpc:strategy.score` —— 给策略打分并输出保留/回滚建议。"""

        self.scores += 1
        stamp = float(now if now is not None else self._now())
        me = int(self_id if self_id is not None else self.self_id)
        rows = await self._read_messages(messages=messages, group_id=group_id, since=since, until=until, limit=limit)
        measured = compute_components(rows, self_id=me, bot_name=bot_name)
        score = weighted_score(measured["components"])
        current_preset = dict(strategy) if isinstance(strategy, Mapping) else {}
        target_id = str(preset_id or current_preset.get("preset_id") or "")

        # 设计依赖：对照预设（判实际落点场景）；拿不到就算了，不影响打分
        baseline = None
        if baseline_messages is not None:
            base = compute_components(baseline_messages, self_id=me, bot_name=bot_name)
            baseline = {
                "score": weighted_score(base["components"]),
                "components": base["components"],
                "counts": base["counts"],
                "delta": round(score - weighted_score(base["components"]), 4),
            }

        match = await self._call(
            DEP_PRESETS_MATCH,
            {"heat": measured["components"]["engagement"], "flood": 1.0 - measured["components"]["restraint"]},
            preset_id=target_id,
            top=1,
        )
        verdict = verdict_for(
            score,
            keep=self.keep_threshold,
            rollback=self.rollback_threshold if threshold is None else float(threshold),
        )
        result = {
            "preset_id": target_id,
            "score": score,
            "components": measured["components"],
            "counts": measured["counts"],
            "verdict": verdict,
            "advice": advice_for(measured["components"]),
            "thresholds": {"keep": self.keep_threshold, "rollback": self.rollback_threshold},
            "baseline": baseline,
            "delta": (baseline or {}).get("delta", 0.0),
            "matched": (match or {}).get("best"),
            "scene": (match or {}).get("scenario", ""),
            "window": {"since": since, "until": until, "messages": len(rows)},
            "sampled_at": stamp,
            "applied": False,
        }
        if apply and current_preset:
            recorded = {**current_preset}
            history = list(recorded.get("history") or [])
            history.append(
                {
                    "version": int(current_preset.get("version", 1) or 1),
                    "evaluation": {
                        "score": score,
                        "verdict": verdict,
                        "components": measured["components"],
                        "sampled_at": stamp,
                    },
                    "evaluated_at": stamp,
                }
            )
            recorded["history"] = history
            recorded["evaluation"] = {"score": score, "verdict": verdict, "sampled_at": stamp}
            await self._call("rpc:presets.register", recorded, persist=False)
            result["applied"] = True
        self.last = result
        self._log(
            "info",
            "strategy.scored",
            preset_id=target_id,
            score=score,
            verdict=verdict,
            messages=len(rows),
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "scores": self.scores,
            "self_id": self.self_id,
            "keep_threshold": self.keep_threshold,
            "rollback_threshold": self.rollback_threshold,
        }


def make_handlers(scorer: StrategyScorer) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:strategy.score": scorer.score}


def register(target: Any, instance: StrategyScorer | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or StrategyScorer()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "AI_PATTERNS",
    "AT_ALL_PATTERNS",
    "COMPONENT_WEIGHTS",
    "DEP_CHAT_QUERY",
    "DEP_PRESETS_MATCH",
    "KEEP_THRESHOLD",
    "MAX_INTERVAL",
    "MIN_INTERVAL",
    "MODULE",
    "NAMES",
    "REPEAT_LIMIT",
    "ROLLBACK_THRESHOLD",
    "SENSITIVE_PATTERNS",
    "StrategyScorer",
    "advice_for",
    "clamp01",
    "compute_components",
    "make_handlers",
    "ratio",
    "register",
    "split_by_self",
    "verdict_for",
    "weighted_score",
]
