"""grouppig.perception.interrupt.scorer —— 插话价值评分器（``rpc:interrupt.score``）。

对当前窗口的插话价值打分，四个分量（设计原文：话题熟悉度、关系亲疏、冷场程度、被点名）：

=========================  ================================================================
分量                        来源
=========================  ================================================================
``topic_familiarity``       ``behavior.classify`` 的话题集中度 + 关键词与「熟悉词」的重合度
``intimacy``                画像 / 社交域给的亲密度（``profile.intimacy``，0..1）
``silence``                 节奏标签与静默压力（``rhythm.measure`` 的 ``coldness``）
``mentioned``               被 @ / 被点名 / 引用回复自己的消息（硬信号，权重最高）
=========================  ================================================================

另外两道乘子与修正：``cooldown`` 罚项（按 ``penalty`` 比例打折，闸门放不放行由决策器把关）、
``flood`` 罚项（刷屏时几乎闭嘴）。
被点名是「不可以沉默」的信号：``mentioned ≥ perception.interrupt.mention_override``（默认 0.9）
时 :meth:`InterruptScorer.score` 会把 ``total`` 抬到 ``mention_floor``（默认 0.75）——这是一块
**地板**，不乘冷却惩罚；旧实现把它同乘 ``(1 - penalty)``，于是冷却一压就把这条兜底抹平了。
与语用直觉一致（人家点名你了，不吭声比说错话更糟）。

设计依赖（两条边都真调）：``rpc:interrupt.score`` → ``rpc:interrupt.cooldown``（冷却检查）、
``rpc:interrupt.score`` → ``rpc:interrupt.decide``（给出决策）。

设计：``grouppig.perception.interrupt.scorer``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.classifier.features import build_encoder
from grouppig.perception.behavior.rhythm.meter import RhythmMeter, build_meter
from grouppig.perception.interrupt.cooldown import Cooldown, build_cooldown
from grouppig.perception.runtime import calls as calls_module
from grouppig.perception.runtime import config as config_module
from grouppig.perception.runtime import messages as messages_module
from grouppig.perception.runtime import text as text_utils

MODULE_ID = "grouppig.perception.interrupt.scorer"
RPC_SCORE = "rpc:interrupt.score"

#: 设计依赖：冷却检查 + 给出决策。
DOWNSTREAM_COOLDOWN = "rpc:interrupt.cooldown"
DOWNSTREAM_DECISION = "rpc:interrupt.decide"
DOWNSTREAM_FEATURES = "rpc:behavior.features.encode"

#: 四个分量的名字（权重键与之一一对应）。
COMPONENTS: tuple[str, ...] = ("topic_familiarity", "intimacy", "silence", "mentioned")

#: 插话档位。
BANDS: tuple[str, ...] = ("hold", "weak", "medium", "strong")

DEFAULT_INTIMACY = 0.5
DEFAULT_MENTION_OVERRIDE = 0.9
DEFAULT_MENTION_FLOOR = 0.75

#: 熟悉词 → 话题熟悉度加分（话题命中这些词说明自己「有话说」）。
#: 自学期：某群这个词在多少个不同窗口出现过。出现越广越可能是「这个群的常聊话题」，
#: 比任何硬编码词表都准。只在进程内累积，不落库。
DEFAULT_LEARN_AFTER = 3

#: 熟悉词 → 话题熟悉度加分（话题命中这些词说明自己「有话说」）。
FAMILIAR_TOPICS: tuple[str, ...] = (
    "游戏",
    "打本",
    "开黑",
    "上分",
    "副本",
    "代码",
    "bug",
    "部署",
    "加班",
    "上班",
    "考试",
    "作业",
    "外卖",
    "咖啡",
    "奶茶",
    "猫",
    "狗",
    "电影",
    "动漫",
    "音乐",
    "旅游",
)


class InterruptScorer:
    """插话价值评分（四分量加权 + 冷却/刷屏罚项）。"""

    def __init__(
        self,
        *,
        registry: Any = None,
        config: Any = None,
        logger: Any = None,
        cooldown: Cooldown | None = None,
        rhythm: RhythmMeter | None = None,
        encoder: Any = None,
        window: Any = None,
        weights: Mapping[str, float] | None = None,
        intimacy: float | None = None,
        mention_override: float | None = None,
        mention_floor: float | None = None,
        cascade_decision: bool = True,
        learn_after: int | None = None,
        renormalize: bool | None = None,
    ) -> None:
        self.registry = calls_module.registry_of(registry)
        self.config = config
        self.logger = logger
        self.window = window
        self.cooldown = cooldown or build_cooldown(config=config, logger=logger)
        self.rhythm = rhythm or build_meter(config=config, logger=logger, registry=self.registry, window=window)
        self.encoder = encoder or build_encoder(config=config, logger=logger)
        self.weights = dict(weights) if weights is not None else config_module.weights(config)
        self.intimacy = float(
            intimacy
            if intimacy is not None
            else config_module.number(config, "perception.interrupt.intimacy", DEFAULT_INTIMACY)
        )
        self.mention_override = float(
            mention_override
            if mention_override is not None
            else config_module.number(config, "perception.interrupt.mention_override", DEFAULT_MENTION_OVERRIDE)
        )
        self.mention_floor = float(
            mention_floor
            if mention_floor is not None
            else config_module.number(config, "perception.interrupt.mention_floor", DEFAULT_MENTION_FLOOR)
        )
        self.cascade_decision = bool(cascade_decision)
        self.renormalize = (
            bool(config_module.flag(config, "perception.interrupt.renormalize", True))
            if renormalize is None
            else bool(renormalize)
        )
        self.learn_after = max(
            1,
            int(
                learn_after
                if learn_after is not None
                else config_module.integer(config, "perception.interrupt.learn_after", DEFAULT_LEARN_AFTER)
            ),
        )
        # 自学习的话题词表：{群: {词: 出现过的窗口数}}。
        self._seen: dict[int, dict[str, int]] = {}
        self._last: dict[int, dict[str, Any]] = {}

    # ---- 权重归一 ------------------------------------------------------
    def _effective_weights(self, mentioned: float) -> dict[str, float]:
        """按「谁在场」分配权重。

        ``mentioned`` 独占 0.30，但真实语料可以完全无 @（实测 @ = 0）。此时那 0.30 是
        **拿不到的**：总分上限被压到 0.70，把分数结构性挤进 0.138-0.194 这种极窄区间，
        阈值怎么调都只能挤出无动作的 ``wait``。故无人被点名时，把 ``mentioned`` 的权重
        按比例分给其余分量（它们才携带真实信息），四项和仍为 1.0。

        被点名（``mentioned >= 0.9``）时**原样返回**，保证「不可以沉默」的打击面不变。
        """

        base = dict(self.weights)
        if not self.renormalize or mentioned > 0.0:
            return base
        spare = float(base.get("mentioned", 0.0))
        rest = [key for key in base if key != "mentioned"]
        pool = sum(max(0.0, base[key]) for key in rest)
        if spare <= 0.0 or pool <= 0.0:
            return base
        share = spare / pool
        for key in rest:
            base[key] = max(0.0, base[key]) * (1.0 + share)
        base["mentioned"] = 0.0
        return base

    # ---- 纯计算 --------------------------------------------------------
    def compute(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        features: Mapping[str, Any] | None = None,
        behavior: str = "",
        rhythm: Mapping[str, Any] | None = None,
        cooldown: Mapping[str, Any] | None = None,
        flood: Mapping[str, Any] | None = None,
        intimacy: float | None = None,
        self_id: int = 0,
        learned: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        """算插话价值。

        ``learned`` 是自学习话题词表 ``{词: 出现过的窗口数}``（由 :meth:`observe` 维护）；
        命中它说明「这个群常聊这个」，比硬编码词表更贴合真实群。
        """

        rows = [dict(row) for row in messages]
        features = dict(features or {})
        rhythm = dict(rhythm or {})
        cooldown = dict(cooldown or {})
        contents = [messages_module.text_of(row) for row in rows]

        at_self = any(row.get("at_self") for row in rows)
        mentions = [int(row.get("sender_id", 0) or 0) for row in rows]
        self_mentioned = bool(self_id) and any(self_id == sender for sender in mentions)
        replies_to_self = bool(self_id) and any(_reply_target(row) == self_id for row in rows)
        mentioned = 1.0 if at_self else (0.9 if (self_mentioned or replies_to_self) else 0.0)

        focus = float(features.get("topic_focus") or features.get("concentration") or 0.0)
        keywords = {str(item[0]) for item in features.get("keywords") or () if isinstance(item, (list, tuple)) and item}
        if not keywords:
            keywords = {item[0] for item in text_utils.keywords(contents, top=8)}
        learned_terms = {str(term) for term, count in dict(learned or {}).items() if int(count) >= self.learn_after}
        familiar_terms = sorted((keywords & set(FAMILIAR_TOPICS)) | (keywords & learned_terms))
        familiar = len(familiar_terms)
        familiarity = min(1.0, 0.6 * focus + 0.4 * min(1.0, familiar / 2))

        relation = float(intimacy if intimacy is not None else self.intimacy)
        relation = min(1.0, max(0.0, relation))

        rhythm_label = str(rhythm.get("label") or "")
        coldness = float(rhythm.get("coldness") or 0.0)
        rate = float(rhythm.get("rate") or 0.0)
        if rhythm_label == "silent":
            silence = 1.0
        elif rhythm_label == "warming":
            silence = max(coldness, 0.7)
        elif rhythm_label == "dense":
            silence = max(0.0, coldness * 0.5)
        else:
            silence = max(0.15, coldness)

        raw = {
            "topic_familiarity": familiarity,
            "intimacy": relation,
            "silence": silence,
            "mentioned": mentioned,
        }
        weights = self._effective_weights(mentioned)
        total = sum(weights.get(key, 0.0) * value for key, value in raw.items())
        total = min(1.0, max(0.0, total))

        # 「放不放行」由 decision.evaluate 把关，这里只按 penalty 比例打折。
        # 旧实现在 allowed=False 时强制 1.0，等于把分数归零、连被点名的兜底一起抹掉。
        cooldown_penalty = float(cooldown.get("penalty") or 0.0)
        flooding = bool((flood or {}).get("flooding")) or behavior == "flooding"
        if not flooding:
            flood_penalty = 0.0
        else:
            # 有 flood.detect 结论时用它的置信度；只有行为标签时给一个保守罚值
            flood_penalty = float((flood or {}).get("confidence") or 0.0) or 0.75
        adjusted = total * (1.0 - cooldown_penalty) * (1.0 - 0.85 * flood_penalty)
        if mentioned >= self.mention_override and not flooding:
            # 地板就是地板：不乘冷却惩罚，否则被点名时照样说不出口。
            adjusted = max(adjusted, self.mention_floor)
        adjusted = min(1.0, max(0.0, adjusted))

        if adjusted >= 0.75:
            band = "strong"
        elif adjusted >= 0.55:
            band = "medium"
        elif adjusted >= 0.35:
            band = "weak"
        else:
            band = "hold"
        weight_view = {key: round(value, 4) for key, value in weights.items()}
        if weight_view:
            first_weight = next(iter(weight_view))
            weight_view[first_weight] = round(weight_view[first_weight] + 1.0 - sum(weight_view.values()), 4)
        return {
            "total": round(adjusted, 4),
            "raw_total": round(total, 4),
            "band": band,
            "components": {key: round(value, 4) for key, value in raw.items()},
            "weights": weight_view,
            "context": {
                "behavior": behavior,
                "rhythm": rhythm_label,
                "rate": round(rate, 4),
                "message_count": len(rows),
                "at_self": at_self,
                "self_mentioned": self_mentioned,
                "replies_to_self": replies_to_self,
                "familiar_topics": familiar_terms,
                "learned_terms": len(learned_terms),
                "flooding": flooding,
            },
            "penalties": {"cooldown": round(cooldown_penalty, 4), "flood": round(flood_penalty, 4)},
            "thresholds": {
                "mention_override": self.mention_override,
                "mention_floor": self.mention_floor,
                "bands": {"weak": 0.35, "medium": 0.55, "strong": 0.75},
            },
        }

    # ---- 主流程 --------------------------------------------------------
    async def score(
        self,
        group_id: int,
        *,
        window: Any = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        features: Mapping[str, Any] | None = None,
        behavior: str = "",
        rhythm: Mapping[str, Any] | None = None,
        flood: Mapping[str, Any] | None = None,
        intimacy: float | None = None,
        self_id: int = 0,
        now: float | None = None,
        decide: bool | None = None,
    ) -> dict[str, Any]:
        """给某群的插话价值打分；调冷却检查，并按设计依赖把分交给决策器。"""

        stamp = float(now if now is not None else time.time())
        span = float(config_module.number(self.config, "perception.classify.window_seconds", 60))
        source_window = window if window is not None else self.window
        rows = [dict(row) for row in (messages or ())]
        results: list[calls_module.CallOutcome] = []

        if not rows and source_window is not None and callable(getattr(source_window, "slice", None)):
            sliced = source_window.slice(int(group_id), seconds=span, now=stamp)
            rows = [dict(row) for row in (sliced.get("messages") or ())]

        # 特征编码（设计依赖）。旧实现把 ``features`` 原样透传、从不计算：调用方不传就恒为
        # None，于是 ``topic_focus``/``keywords`` 全取不到，``topic_familiarity`` 结构性恒零
        # ——实测同一窗口 topic_focus 本可拿 1.0，光这一项就把 total 从 0.2375 砍到 0.1000。
        if features is None:
            encoded_outcome = await calls_module.call_leaf(
                self.registry,
                DOWNSTREAM_FEATURES,
                lambda **kw: self.encoder.encode(**kw),
                window=rows if rows else source_window,
                messages=None,
                seconds=span,
                now=stamp,
            )
            features = (
                dict(encoded_outcome.result)
                if encoded_outcome.ok and isinstance(encoded_outcome.result, Mapping)
                else {}
            )
            results.append(encoded_outcome)
        features = dict(features or {})
        self.observe(int(group_id), features)

        cooldown_outcome = await calls_module.call_leaf(
            self.registry,
            DOWNSTREAM_COOLDOWN,
            lambda gid, **kw: self.cooldown.check(gid, **kw),
            int(group_id),
            now=stamp,
        )
        results.append(cooldown_outcome)
        cooldown = (
            dict(cooldown_outcome.result)
            if cooldown_outcome.ok and isinstance(cooldown_outcome.result, Mapping)
            else {}
        )

        rhythm_payload = dict(rhythm or {})
        if not rhythm_payload:
            rhythm_outcome = await calls_module.call_leaf(
                self.registry,
                "rpc:rhythm.measure",
                lambda gid, **kw: self.rhythm.rhythm(gid, **kw),
                int(group_id),
                messages=rows,
                window=source_window,
                seconds=span,
                now=stamp,
            )
            results.append(rhythm_outcome)
            if rhythm_outcome.ok and isinstance(rhythm_outcome.result, Mapping):
                rhythm_payload = dict(rhythm_outcome.result)

        payload = self.compute(
            rows,
            features=features,
            behavior=behavior,
            rhythm=rhythm_payload,
            cooldown=cooldown,
            flood=flood,
            intimacy=intimacy,
            self_id=self_id,
            learned=self._seen.get(int(group_id)),
        )
        out: dict[str, Any] = {
            "group_id": int(group_id),
            "window_seconds": span,
            "since": stamp - span,
            "until": stamp,
            "cooldown": cooldown,
            "rhythm": rhythm_payload,
            "flood": dict(flood or {}),
            "behavior": behavior,
            "downstream": {},
            "decision": None,
            "at": stamp,
            **payload,
        }
        want_decide = self.cascade_decision if decide is None else bool(decide)
        if want_decide:
            decision_outcome = await calls_module.maybe_call(
                self.registry,
                DOWNSTREAM_DECISION,
                int(group_id),
                score=out["total"],
                score_detail={key: out[key] for key in ("total", "raw_total", "band", "components", "penalties")},
                behavior=behavior,
                cooldown=cooldown,
                features=features,
                now=stamp,
            )
            results.append(decision_outcome)
            if decision_outcome.ok:
                out["decision"] = decision_outcome.result
        out["downstream"] = calls_module.outcomes(results)
        self._last[int(group_id)] = out
        return out

    # ---- 自学习话题词表 ------------------------------------------------
    def observe(self, group_id: int, features: Mapping[str, Any]) -> None:
        """把本窗口的关键词记进该群的话题词表（每窗口每词只算一次）。"""

        keywords = features.get("keywords") or ()
        terms = {
            str(item[0]) for item in keywords if isinstance(item, (list, tuple)) and item and len(str(item[0])) >= 2
        }
        if not terms:
            return
        bucket = self._seen.setdefault(int(group_id), {})
        for term in terms:
            bucket[term] = bucket.get(term, 0) + 1

    def learned(self, group_id: int) -> dict[str, int]:
        """该群已达到 :attr:`learn_after` 的话题词表（供诊断/测试）。"""

        return {term: count for term, count in self._seen.get(int(group_id), {}).items() if count >= self.learn_after}

    def last(self, group_id: int) -> dict[str, Any] | None:
        return self._last.get(int(group_id))

    # ---- 注册 ----------------------------------------------------------
    def register(self, registry: Registry, *, replace: bool = True) -> Registry:
        """注册 ``rpc:interrupt.score``。"""

        self.registry = registry

        async def score(
            group_id: int,
            window: Any = None,
            messages: Sequence[Mapping[str, Any]] | None = None,
            features: Mapping[str, Any] | None = None,
            behavior: str = "",
            rhythm: Mapping[str, Any] | None = None,
            flood: Mapping[str, Any] | None = None,
            intimacy: float | None = None,
            self_id: int = 0,
            now: float | None = None,
            decide: bool | None = None,
            **_: Any,
        ) -> dict[str, Any]:
            return await self.score(
                group_id,
                window=window,
                messages=messages,
                features=features,
                behavior=behavior,
                rhythm=rhythm,
                flood=flood,
                intimacy=intimacy,
                self_id=self_id,
                now=now,
                decide=decide,
            )

        registry.register(RPC_SCORE, score, module=MODULE_ID, replace=replace)
        return registry

    def snapshot(self) -> dict[str, Any]:
        return {
            "components": list(COMPONENTS),
            "weights": dict(self.weights),
            "renormalize": self.renormalize,
            "learn_after": self.learn_after,
            "intimacy": self.intimacy,
            "mention_override": self.mention_override,
            "mention_floor": self.mention_floor,
            "cooldown": self.cooldown.snapshot(),
            "rhythm": self.rhythm.snapshot(),
            "scored": sorted(self._last),
        }


def _reply_target(message: Mapping[str, Any]) -> int:
    value = message.get("reply_to")
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def build_scorer(*, config: Any = None, logger: Any = None, **kwargs: Any) -> InterruptScorer:
    return InterruptScorer(config=config, logger=logger, **kwargs)


def register(registry: Registry, scorer: InterruptScorer | None = None, **kwargs: Any) -> Registry:
    """把插话价值评分器注册进注册表（未传实例时现建一个）。"""

    return (scorer or build_scorer(**kwargs)).register(registry)


__all__ = [
    "BANDS",
    "COMPONENTS",
    "DEFAULT_LEARN_AFTER",
    "DEFAULT_INTIMACY",
    "DEFAULT_MENTION_FLOOR",
    "DEFAULT_MENTION_OVERRIDE",
    "DOWNSTREAM_COOLDOWN",
    "DOWNSTREAM_DECISION",
    "DOWNSTREAM_FEATURES",
    "FAMILIAR_TOPICS",
    "MODULE_ID",
    "RPC_SCORE",
    "InterruptScorer",
    "build_scorer",
    "register",
]
