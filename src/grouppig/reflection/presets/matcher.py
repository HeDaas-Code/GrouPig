"""grouppig.reflection.presets.matcher —— 预设匹配器（`rpc:presets.match`）。

职责（对应设计 `grouppig.reflection.presets.matcher`「按行为特征与场景匹配最优预设」）：

把感知层/会话层的**行为特征**投进预设库，按「触发器命中 + 场景契合 + 历史效果」排序，
返回最优预设与候选清单。设计依赖 `rpc:presets.match` → `rpc:presets.load`（已在实现中生效）。

**行为特征**（`BehaviorFeatures`，全部可缺省；缺省字段的触发器视为「不适用」而不是「不命中」）::

    {
      "heat": 0.62,            # 群热度 0-1（rpc:chat.window.heat 或 rpc:session.heat）
      "flood": 0.15,           # 刷屏分 0-1（rpc:flood.detect）
      "topics": 2,             # 同时活跃话题数
      "interrupt": 0.7,        # 插话时机分 0-1（rpc:interrupt.score，越高越该说）
      "reply_rate": 0.4,       # 我方回应率 0-1
      "focus": 0.8,            # 话题聚焦度 0-1
      "phase": "discussion",   # 会话阶段
      "keyword": "今晚打本吗",  # 触发文本（keyword_any/keyword_all 用）
      "text": "..."            # 同 keyword，优先 keyword
    }

**打分口径**（确定性、可解释）::

    score = 0.55 * 触发器命中率 + 0.25 * 场景吻合 + 0.20 * 历史效果 - 惩罚

* 触发器命中率 = 命中的条件数 / 适用的条件数；**全部条件都必须命中**才进入候选
  （未命中任一条件即淘汰，`matched=False` 并记下 `missed` 原因）；
* 场景吻合：调用方显式给 `scenario` 时命中同场景 +1，否则按 `phase`/`flood`/`heat` 推断场景后比对；
* 历史效果：预设 `history` 里历次评估的 `score` 均值（无历史记中性 0.5）；
* 惩罚：预设曾因效果差被回滚过（`rolled_back=True`）扣 0.15。

设计：`grouppig.reflection.presets.matcher`（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

#: normify 模块 id。
MODULE = "grouppig.reflection.presets.matcher"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:presets.match",)

#: 设计依赖：读预设库（逐字取自设计 frontmatter 的 `from_api` → `to_api`）。
DEP_PRESETS_LOAD = "rpc:presets.load"

#: 打分权重（和 = 1.0）。
WEIGHT_TRIGGERS = 0.55
WEIGHT_SCENARIO = 0.25
WEIGHT_HISTORY = 0.20

#: 历史效果缺省（无评估记录时的中性值）。
DEFAULT_HISTORY_SCORE = 0.5

#: 被回滚过的预设的惩罚。
ROLLBACK_PENALTY = 0.15

#: 推断场景的阈值（`scenario` 未显式给出时用）。
FLOOD_THRESHOLD = 0.5
HEAT_HIGH = 0.65
HEAT_LOW = 0.15
TOPIC_EXPOSITION = 3

#: `features` 里允许出现的数值键（其余按关键字/阶段处理）。
NUMERIC_KEYS: tuple[str, ...] = ("heat", "flood", "topics", "interrupt", "reply_rate", "focus")


def clamp01(value: Any, default: float = 0.0) -> float:
    """把任意值收进 [0, 1]。"""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, number))


def normalize_features(features: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    """规整行为特征（未知键保留，供扩展触发器使用）。"""

    source = {**(dict(features) if isinstance(features, Mapping) else {}), **kwargs}
    payload: dict[str, Any] = {}
    for key in NUMERIC_KEYS:
        if key in source and source[key] is not None:
            if key == "topics":
                try:
                    payload[key] = max(0, int(source[key]))
                except (TypeError, ValueError):
                    continue
            else:
                payload[key] = clamp01(source[key])
    if source.get("phase"):
        payload["phase"] = str(source["phase"])
    text = source.get("keyword") or source.get("text") or ""
    if text:
        payload["keyword"] = str(text)
    return payload


def infer_scenario(features: Mapping[str, Any]) -> str:
    """从特征推断场景（匹配器与反思结论共用同一口径）。"""

    flood = clamp01(features.get("flood"))
    heat = clamp01(features.get("heat"))
    topics = int(features.get("topics") or 0)
    keyword = str(features.get("keyword") or "")
    if any(token in keyword for token in ("吵", "骂", "滚", "生气")):
        return "conflict"
    if flood >= FLOOD_THRESHOLD:
        return "flooding"
    if heat and heat <= HEAT_LOW:
        return "silence"
    if topics >= TOPIC_EXPOSITION or (heat >= HEAT_HIGH and topics >= 2):
        return "exposition"
    if heat >= HEAT_HIGH:
        return "smalltalk"
    return "calm"


def history_score(preset: Mapping[str, Any]) -> float:
    """预设的历次评估均分（无记录给中性值）。"""

    scores: list[float] = []
    for item in preset.get("history") or ():
        if not isinstance(item, Mapping):
            continue
        evaluation = item.get("evaluation")
        source = evaluation if isinstance(evaluation, Mapping) else item
        value = source.get("score")
        if value is None:
            continue
        try:
            scores.append(float(value))
        except (TypeError, ValueError):
            continue
    return sum(scores) / len(scores) if scores else DEFAULT_HISTORY_SCORE


def _condition_result(key: str, expected: Any, features: Mapping[str, Any]) -> tuple[bool, str]:
    """单条触发器的命中判定；返回 `(是否命中, 未命中原因)`。"""

    if key.endswith("_max"):
        field = key[: -len("_max")]
        if field not in features or features[field] is None:
            return True, ""
        actual = float(features[field])
        return (actual <= float(expected), f"{field}={actual:.3f} > {field}_max={float(expected):.3f}")
    if key.endswith("_min"):
        field = key[: -len("_min")]
        if field not in features or features[field] is None:
            return True, ""
        actual = float(features[field])
        return (actual >= float(expected), f"{field}={actual:.3f} < {field}_min={float(expected):.3f}")
    if key == "phase":
        wanted = {str(item) for item in (expected or ())}
        if not wanted or "phase" not in features:
            return True, ""
        actual = str(features["phase"])
        return (actual in wanted, f"phase={actual} 不在 {sorted(wanted)}")
    if key == "keyword_any":
        tokens = [str(item) for item in (expected or ())]
        if not tokens:
            return True, ""
        text = str(features.get("keyword") or "")
        return (any(token in text for token in tokens), f"文本未命中任一词 {tokens}")
    if key == "keyword_all":
        tokens = [str(item) for item in (expected or ())]
        if not tokens:
            return True, ""
        text = str(features.get("keyword") or "")
        return (all(token in text for token in tokens), f"文本未同时包含 {tokens}")
    return True, ""


def check_triggers(triggers: Mapping[str, Any] | None, features: Mapping[str, Any]) -> dict[str, Any]:
    """逐条比对触发器；返回命中率与未命中原因。"""

    items = dict(triggers or {})
    considered = 0
    hit = 0
    missed: list[str] = []
    for key, expected in items.items():
        ok, reason = _condition_result(str(key), expected, features)
        considered += 1
        if ok:
            hit += 1
        else:
            missed.append(f"{key}: {reason}")
    rate = (hit / considered) if considered else 1.0
    return {
        "considered": considered,
        "hit": hit,
        "missed": missed,
        "hit_rate": round(rate, 4),
        "matched": not missed,
    }


def score_preset(
    preset: Mapping[str, Any],
    features: Mapping[str, Any],
    *,
    scenario: str = "",
) -> dict[str, Any]:
    """给单个预设打分（确定性）。"""

    triggers = check_triggers(preset.get("triggers"), features)
    target = scenario or infer_scenario(features)
    scenario_hit = 1.0 if preset.get("scenario") == target else 0.0
    history = history_score(preset)
    penalty = ROLLBACK_PENALTY if preset.get("rolled_back") else 0.0
    score = WEIGHT_TRIGGERS * triggers["hit_rate"] + WEIGHT_SCENARIO * scenario_hit + WEIGHT_HISTORY * history - penalty
    return {
        "preset_id": preset.get("preset_id"),
        "version": int(preset.get("version", 1) or 1),
        "scenario": preset.get("scenario"),
        "score": round(max(0.0, min(1.0, score)), 4),
        "matched": bool(triggers["matched"]),
        "missed": triggers["missed"],
        "components": {
            "triggers": round(triggers["hit_rate"], 4),
            "scenario": scenario_hit,
            "history": round(history, 4),
            "penalty": penalty,
        },
        "actions": dict(preset.get("actions") or {}),
    }


class PresetMatcher:
    """按行为特征匹配最优预设（`rpc:presets.match`）。"""

    def __init__(
        self,
        ctx: Any = None,
        *,
        min_score: float = 0.0,
        top: int = 5,
    ) -> None:
        self.ctx = ctx
        self.min_score = float(min_score)
        self.top = max(1, int(top))
        self.matches = 0
        self.misses = 0
        self.last: dict[str, Any] = {}
        self._fallback: dict[str, Any] | None = None

    # ---- 工具 ----------------------------------------------------------
    def _now(self) -> float:
        clock = getattr(self.ctx, "now", None)
        return float(clock()) if callable(clock) else time.time()

    def _log(self, level: str, event: str, **fields: Any) -> None:
        log = getattr(self.ctx, "log", None)
        if callable(log):
            log(level, event, **fields)

    def set_fallback(self, preset: Mapping[str, Any] | None) -> None:
        """设置兜底预设（匹配不到任何候选时返回它，保证闭环不断）。"""

        self._fallback = dict(preset) if isinstance(preset, Mapping) else None

    async def _read_presets(
        self,
        *,
        presets: Sequence[Mapping[str, Any]] | None,
        scenario: str,
    ) -> list[dict[str, Any]]:
        """读预设库：设计依赖 `rpc:presets.load` 优先，`presets=` 直喂次之。"""

        if presets is not None and not self._has_caller():
            return [dict(item) for item in presets]
        loader = getattr(self.ctx, "call", None)
        if callable(loader) and presets is None:
            loaded = await loader(DEP_PRESETS_LOAD, scenario=scenario)
            return [dict(item) for item in (loaded or {}).get("presets") or ()]
        if presets is not None:
            return [dict(item) for item in presets]
        if callable(loader):
            loaded = await loader(DEP_PRESETS_LOAD, scenario=scenario)
            return [dict(item) for item in (loaded or {}).get("presets") or ()]
        return []

    def _has_caller(self) -> bool:
        return callable(getattr(self.ctx, "call", None))

    async def match(
        self,
        features: Mapping[str, Any] | None = None,
        *,
        scenario: str = "",
        presets: Sequence[Mapping[str, Any]] | None = None,
        top: int | None = None,
        require_match: bool = True,
        **feature_fields: Any,
    ) -> dict[str, Any]:
        """`rpc:presets.match` —— 按行为特征匹配最优预设。

        `require_match=True`（默认）时，只有全部触发器命中的预设才进候选；候选为空时
        退回兜底预设（`fallback=True`），并保留 `missed` 说明为什么没匹配上。
        """

        self.matches += 1
        normalized = normalize_features(features, **feature_fields)
        target_scenario = scenario or infer_scenario(normalized)
        limit = max(1, int(top if top is not None else self.top))
        library = await self._read_presets(presets=presets, scenario=scenario)

        scored = [score_preset(item, normalized, scenario=target_scenario) for item in library]
        scored.sort(key=lambda item: (-item["score"], str(item["preset_id"])))
        eligible = [item for item in scored if item["score"] >= self.min_score]
        candidates = [item for item in eligible if item["matched"]] if require_match else eligible
        by_id = {str(item.get("preset_id")): item for item in library}

        fallback = False
        best = candidates[0] if candidates else None
        if best is None:
            self.misses += 1
            fallback = True
            if self._fallback is not None:
                best = score_preset(self._fallback, normalized, scenario=target_scenario)
            elif eligible:
                best = eligible[0]
        result = {
            "preset": dict(by_id.get(str(best.get("preset_id")), {})) if best else None,
            "best": best,
            "candidates": candidates[:limit],
            "rejected": [item for item in scored if item not in candidates][:limit],
            "count": len(candidates),
            "fallback": fallback,
            "scenario": target_scenario,
            "features": normalized,
            "reason": (best or {}).get("missed") or [] if fallback else [],
            "now": self._now(),
        }
        if best is not None and not fallback:
            result["actions"] = dict(by_id.get(str(best.get("preset_id")), {}).get("actions") or best["actions"])
        self.last = result
        self._log(
            "debug",
            "presets.matched",
            preset_id=(best or {}).get("preset_id"),
            scenario=target_scenario,
            fallback=fallback,
            count=len(candidates),
        )
        return result

    # ---- 诊断 ----------------------------------------------------------
    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE,
            "matches": self.matches,
            "misses": self.misses,
            "min_score": self.min_score,
            "top": self.top,
            "has_fallback": self._fallback is not None,
        }


def make_handlers(matcher: PresetMatcher) -> dict[str, Any]:
    """本叶子的 `{契约名: 处理器}`。"""

    return {"rpc:presets.match": matcher.match}


def register(target: Any, instance: PresetMatcher | None = None) -> Any:
    """把本叶子的处理器注册进 `target`。"""

    instance = instance or PresetMatcher()
    for name, handler in make_handlers(instance).items():
        target.register(name, handler, module=MODULE, replace=True)
    return target


__all__ = [
    "DEFAULT_HISTORY_SCORE",
    "DEP_PRESETS_LOAD",
    "FLOOD_THRESHOLD",
    "MODULE",
    "NAMES",
    "NUMERIC_KEYS",
    "ROLLBACK_PENALTY",
    "PresetMatcher",
    "WEIGHT_HISTORY",
    "WEIGHT_SCENARIO",
    "WEIGHT_TRIGGERS",
    "check_triggers",
    "clamp01",
    "history_score",
    "infer_scenario",
    "make_handlers",
    "normalize_features",
    "register",
    "score_preset",
]
