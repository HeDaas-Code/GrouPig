"""grouppig.perception.behavior.classifier.rule-engine —— 规则分类引擎（``rpc:behavior.rules.evaluate``）。

用**可解释规则**先判硬模式（设计原文：刷屏 / 冷场 / 复读 / 群体阐述等）。
设计依赖两条边都真调：``rpc:behavior.rules.evaluate`` → ``rpc:flood.detect``（刷屏信号）、
``rpc:behavior.rules.evaluate`` → ``rpc:rhythm.measure``（节奏信号）。

规则表（``scope=hard`` 表示没命中就交给模型判别）：

======  ============================  =============  ======
规则    条件                          结论           范围
======  ============================  =============  ======
``F1``  ``flood.detect`` 判定刷屏        ``flooding``   hard
``S1``  窗口为空                        ``silence``    hard
``S2``  节奏 ``silent``                 ``silence``    hard
``R1``  复读率 ≥ ``repeat_ratio``        ``repeat``     hard
``R2``  复读次数 ≥ ``repeat_max``        ``repeat``     hard
``D1``  主导发言者占比 ≥ ``dominance``    ``exposition`` soft
``E1``  参与人数 ≥ 3 且平均长度 ≥ ``long`` ``discussion`` soft
``K1``  其余（有消息）                    ``smalltalk``  soft
======  ============================  =============  ======

``hard`` 规则的命中即 ``resolved=True``（聚合器会跳过模型判别），``soft`` 只给候选。
``flags`` 里带 ``self_at`` / ``noise`` / ``emoji`` 等辅助标记，聚合器会做「叫自己名」等修正。

设计：``grouppig.perception.behavior.classifier.rule-engine``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.flood.verdict import FloodVerdict, build_verdict
from grouppig.perception.behavior.rhythm.meter import RhythmMeter, build_meter
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.behavior.classifier.rule-engine"
RPC_EVALUATE = "rpc:behavior.rules.evaluate"

#: 设计依赖：刷屏信号 + 节奏信号。
DOWNSTREAM_FLOOD = "rpc:flood.detect"
DOWNSTREAM_RHYTHM = "rpc:rhythm.measure"

#: 行为类别。
BEHAVIORS: tuple[str, ...] = ("flooding", "silence", "repeat", "discussion", "smalltalk", "exposition")

#: 规则 id。
RULE_FLOOD = "F1:flood_verdict"
RULE_EMPTY = "S1:empty_window"
RULE_SILENT = "S2:rhythm_silent"
RULE_REPEAT_RATIO = "R1:repeat_ratio"
RULE_REPEAT_MAX = "R2:repeat_count"
RULE_DOMINANCE = "D1:single_speaker_dominance"
RULE_MULTI_PARTY = "E1:multi_party_exchange"
RULE_FALLBACK = "K1:casual_smalltalk"

SCOPE_HARD = "hard"
SCOPE_SOFT = "soft"

DEFAULT_REPEAT_RATIO = 0.5
DEFAULT_REPEAT_MAX = 3
DEFAULT_DOMINANCE = 0.6
DEFAULT_LONG_LENGTH = 40


class RuleEngine:
    """可解释硬规则引擎（刷屏 / 冷场 / 复读 / 群体阐述）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        flood: FloodVerdict | None = None,
        rhythm: RhythmMeter | None = None,
        window: Any = None,
        repeat_ratio: float | None = None,
        repeat_max: int | None = None,
        dominance: float | None = None,
        long_length: int | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.flood = flood or build_verdict(config=config, logger=logger, registry=self.registry, window=window)
        self.rhythm = rhythm or build_meter(config=config, logger=logger, registry=self.registry, window=window)
        self.repeat_ratio = float(
            repeat_ratio
            if repeat_ratio is not None
            else config_module.number(config, "perception.behavior.repeat_ratio", DEFAULT_REPEAT_RATIO)
        )
        self.repeat_max = int(
            repeat_max
            if repeat_max is not None
            else config_module.integer(config, "perception.behavior.repeat_max", DEFAULT_REPEAT_MAX)
        )
        self.dominance = float(
            dominance
            if dominance is not None
            else config_module.number(config, "perception.behavior.dominance", DEFAULT_DOMINANCE)
        )
        self.long_length = int(
            long_length
            if long_length is not None
            else config_module.integer(config, "perception.behavior.long_length", DEFAULT_LONG_LENGTH)
        )

    # ---- 纯计算 --------------------------------------------------------
    def evaluate_signals(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        flood: Mapping[str, Any] | None = None,
        rhythm: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """据窗口 + 两个信号给出规则结论（纯函数）。"""

        rows = [dict(row) for row in messages]
        count = len(rows)
        contents = [messages_module.text_of(row) for row in rows]
        summary = messages_module.summarize(rows)
        flags: list[str] = []
        if summary["at_self_count"]:
            flags.append("self_at")
        if any(row.get("reply_to") for row in rows):
            flags.append("has_reply")
        from_bot = sum(1 for row in rows if messages_module.is_bot_message(row))
        if from_bot:
            flags.append("bot_present")
        emoji = sum(text_utils.emoji_count(value) for value in contents)
        if emoji and len(contents) and emoji / len(contents) >= 1.0:
            flags.append("emoji_heavy")
        media = sum(1 for value in contents if "<image>" in value or "<face>" in value)
        if media and len(contents) and media / len(contents) >= 0.5:
            flags.append("media_heavy")

        matches: list[dict[str, Any]] = []

        def add(rule: str, behavior: str, scope: str, confidence: float, detail: str) -> None:
            matches.append(
                {
                    "rule": rule,
                    "behavior": behavior,
                    "scope": scope,
                    "confidence": round(confidence, 4),
                    "detail": detail,
                }
            )

        # 硬规则
        if count == 0:
            add(RULE_EMPTY, "silence", SCOPE_HARD, 0.9, "窗口内没有消息")
        if flood and flood.get("flooding"):
            add(
                RULE_FLOOD,
                "flooding",
                SCOPE_HARD,
                float(flood.get("confidence") or 0.7),
                "刷屏判定命中：" + ",".join(flood.get("rules") or []),
            )
        rhythm_label = str((rhythm or {}).get("label") or "")
        if rhythm_label == "silent":
            add(RULE_SILENT, "silence", SCOPE_HARD, 0.85, "节奏标签 silent（静默）")
        repeat_ratio = 1.0 - (summary["unique_contents"] / count) if count else 0.0
        if count and repeat_ratio >= self.repeat_ratio:
            add(
                RULE_REPEAT_RATIO, "repeat", SCOPE_HARD, min(0.95, 0.5 + repeat_ratio / 2), f"复读率 {repeat_ratio:.2f}"
            )
        if summary["repeat_max"] >= self.repeat_max:
            add(RULE_REPEAT_MAX, "repeat", SCOPE_HARD, 0.8, f"最高重复次数 {summary['repeat_max']}")

        # 软规则（供聚合器/模型参考）
        if count and summary["top_sender_share"] >= self.dominance:
            add(
                RULE_DOMINANCE,
                "exposition",
                SCOPE_SOFT,
                round(summary["top_sender_share"], 4),
                f"单人占比 {summary['top_sender_share']:.2f}",
            )
        if count >= 3 and summary["sender_count"] >= 3 and summary["avg_length"] >= self.long_length:
            add(RULE_MULTI_PARTY, "discussion", SCOPE_SOFT, 0.6, "多人长消息交换")
        if count and not any(item["scope"] == SCOPE_HARD for item in matches):
            add(RULE_FALLBACK, "smalltalk", SCOPE_SOFT, 0.45, "有消息但无硬模式")

        hard = [item for item in matches if item["scope"] == SCOPE_HARD]
        resolved = bool(hard)
        behavior = hard[0]["behavior"] if resolved else (matches[0]["behavior"] if matches else "smalltalk")
        confidence = max((item["confidence"] for item in matches), default=0.0)
        return {
            "behavior": behavior,
            "resolved": resolved,
            "confidence": round(confidence, 4),
            "matches": matches,
            "rules": [item["rule"] for item in matches],
            "signals": {
                "repeat_ratio": round(repeat_ratio, 4),
                "repeat_max": summary["repeat_max"],
                "top_sender_share": round(summary["top_sender_share"], 4),
                "avg_length": summary["avg_length"],
                "sender_count": summary["sender_count"],
                "message_count": count,
                "at_self_count": summary["at_self_count"],
                "rhythm_label": rhythm_label,
                "flooding": bool((flood or {}).get("flooding")),
                "flags": flags,
            },
            "thresholds": {
                "repeat_ratio": self.repeat_ratio,
                "repeat_max": self.repeat_max,
                "dominance": self.dominance,
                "long_length": self.long_length,
            },
        }

    # ---- 数据源 --------------------------------------------------------
    async def evaluate(
        self,
        group_id: int,
        *,
        window: Any = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        seconds: float | None = None,
        now: float | None = None,
        flood: Mapping[str, Any] | None = None,
        rhythm: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """评估某群当前行为（设计依赖 ``rpc:flood.detect`` 与 ``rpc:rhythm.measure``）。"""

        stamp = float(now if now is not None else time.time())
        span = float(seconds or config_module.number(self.config, "perception.classify.window_seconds", 60))
        rows = [dict(row) for row in (messages or ())]
        results: list[calls_module.CallOutcome] = []

        flood_payload = dict(flood) if flood is not None else {}
        if flood is None:
            outcome = await calls_module.call_leaf(
                self.registry,
                DOWNSTREAM_FLOOD,
                lambda gid, **kw: self.flood.detect(gid, **kw),
                int(group_id),
                seconds=span,
                now=stamp,
                messages=messages,
                window=window,
            )
            results.append(outcome)
            if outcome.ok and isinstance(outcome.result, Mapping):
                flood_payload = dict(outcome.result)
                # 刷屏判定里带的窗口快照（含消息体）优先作为本次评估的消息源
                rows = _rows_from_flood(flood_payload) or rows

        rhythm_payload = dict(rhythm) if rhythm is not None else {}
        if rhythm is None:
            outcome = await calls_module.call_leaf(
                self.registry,
                DOWNSTREAM_RHYTHM,
                lambda gid, **kw: self.rhythm.rhythm(gid, **kw),
                int(group_id),
                seconds=span,
                now=stamp,
                messages=messages,
                window=window,
            )
            results.append(outcome)
            if outcome.ok and isinstance(outcome.result, Mapping):
                rhythm_payload = dict(outcome.result)
                rows = [dict(row) for row in (rhythm_payload.get("messages") or rows)]

        if not rows:
            rows = _rows_from_flood(flood_payload)
        if not rows:
            self._log("debug", "behavior.rules.no_messages", group_id=int(group_id), window_seconds=span)
        payload = self.evaluate_signals(rows, flood=flood_payload, rhythm=rhythm_payload)
        return {
            "group_id": int(group_id),
            "window_seconds": span,
            "since": stamp - span,
            "until": stamp,
            "flood": flood_payload,
            "rhythm": rhythm_payload,
            "downstream": calls_module.outcomes(results),
            "at": stamp,
            **payload,
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            getattr(self.logger, level, self.logger.info)(event, **fields)
        except Exception:  # pragma: no cover - 日志失败不影响判定
            pass

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:behavior.rules.evaluate``。"""

        self.registry = registry
        self.flood.registry = registry
        self.rhythm.registry = registry

        async def evaluate(
            group_id: int,
            window: Any = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            seconds: float | None = None,
            now: float | None = None,
            flood: Mapping[str, Any] | None = None,
            rhythm: Mapping[str, Any] | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.evaluate(
                group_id,
                window=window,
                messages=messages,
                seconds=seconds,
                now=now,
                flood=flood,
                rhythm=rhythm,
            )

        registry.register(RPC_EVALUATE, evaluate, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "behaviors": list(BEHAVIORS),
            "repeat_ratio": self.repeat_ratio,
            "repeat_max": self.repeat_max,
            "dominance": self.dominance,
            "long_length": self.long_length,
            "flood": self.flood.snapshot(),
            "rhythm": self.rhythm.snapshot(),
        }


def _rows_from_flood(flood: Mapping[str, Any]) -> list[dict[str, Any]]:
    """从刷屏判定结果里取出窗口消息（``velocity`` / ``repetition`` 都带窗口切片）。"""

    if not isinstance(flood, Mapping):
        return []
    for key in ("velocity", "repetition"):
        section = flood.get(key)
        if isinstance(section, Mapping):
            rows = [
                dict(row)
                for row in (section.get("messages") or () if isinstance(section.get("messages"), list) else ())
            ]
            if rows:
                return rows
    return []


def build_engine(*, config: Any = None, logger: Any = None, **kwargs: Any) -> RuleEngine:
    return RuleEngine(config=config, logger=logger, **kwargs)


def register(registry: Registry, engine: RuleEngine | None = None, **kwargs: Any) -> Registry:
    """把规则引擎注册进注册表（未传实例时现建一个）。"""

    return (engine or build_engine(**kwargs)).register(registry)


__all__ = [
    "BEHAVIORS",
    "DEFAULT_DOMINANCE",
    "DEFAULT_LONG_LENGTH",
    "DEFAULT_REPEAT_MAX",
    "DEFAULT_REPEAT_RATIO",
    "DOWNSTREAM_FLOOD",
    "DOWNSTREAM_RHYTHM",
    "MODULE_ID",
    "RULE_DOMINANCE",
    "RULE_EMPTY",
    "RULE_FALLBACK",
    "RULE_FLOOD",
    "RULE_MULTI_PARTY",
    "RULE_REPEAT_MAX",
    "RULE_REPEAT_RATIO",
    "RULE_SILENT",
    "RPC_EVALUATE",
    "SCOPE_HARD",
    "SCOPE_SOFT",
    "RuleEngine",
    "build_engine",
    "register",
]
