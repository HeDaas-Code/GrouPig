"""grouppig.expression.identity.deflector —— 追问化解器（``rpc:identity.deflect``）。

职责（设计：``grouppig.expression.identity.deflector``
「识别 AI 身份追问并把话题化解回人设语境」）：

:mod:`grouppig.expression.identity.denial` 负责**说什么**（话术），本叶子负责
**怎么接**（战术）。它做三件事：

1. **识别追问**（:func:`detect_probe`，纯函数）：判断一条消息是不是在质疑身份，
   并给出**强度** ``0-1``。信号包括直问、诈问、技术追问、要求自证、
   以及「同一个人在短时间内反复问」（``repeat`` 由调用方通过 ``history`` 传入）。
2. **选战术**（:func:`pick_tactic`）：按强度与场景在
   :data:`TACTICS` 里挑一条（轻描淡写 / 反问回去 / 拉群友作证 / 转移话题 / 装傻）。
   强度越高越倾向「转移话题」，避免被拖进「你到底是不是」的死循环。
3. **出化解文本**：走设计依赖 ``rpc:identity.deflect`` → ``rpc:identity.deny-ai``
   拿否认句（**必须走这条依赖**——设计把 denial 列为 deflector 的唯一依赖），
   再叠加战术指令（``instruction``）与**转移话题的落点**（``pivot``）。

``pivot`` 的来路：调用方可以给 ``topic``（当前群话题）或 ``keyword``；
都没有时用 :data:`DEFAULT_PIVOTS` 里的话题兜底（吃、游戏、天气、上班）——
**转移话题必须有个真实落点**，否则「换个话题吧」本身就是机器人的说话方式。

**降级**：``rpc:identity.deny-ai`` 缺席时退回内置话术
（:data:`FALLBACK_PHRASES`，与 denial 的 ``generic`` 场景同源），
记 ``denial_unavailable``——**永不抛出**：身份防御是纪律，不能因为下游缺席就失效。

设计：``grouppig.expression.identity.deflector``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from grouppig.expression.identity import denial as denial_module
from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.identity.deflector"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:identity.deflect",)
RPC_DEFLECT = "rpc:identity.deflect"
contract.assert_known_name(RPC_DEFLECT)

#: 设计依赖（逐字对齐 deflector.md 的 deps）。
DEP_DENY_AI = "rpc:identity.deny-ai"
contract.assert_known_name(DEP_DENY_AI)

#: 化解战术。
TACTICS: tuple[str, ...] = ("light", "counter_ask", "peer_witness", "pivot", "play_dumb")

#: 战术 → 中文名。
TACTIC_LABELS: dict[str, str] = {
    "light": "轻描淡写",
    "counter_ask": "反问回去",
    "peer_witness": "拉群友作证",
    "pivot": "转移话题",
    "play_dumb": "装傻",
}

#: 战术 → 给模型的指令。
TACTIC_INSTRUCTIONS: dict[str, str] = {
    "light": "用一句很随意的话否认，别当回事，别解释。",
    "counter_ask": "先反问回去（你怎么看谁都像机器人），把压力丢回给对方。",
    "peer_witness": "拉一个在场的群友作证（比如「你问问 XX，我天天跟他打游戏」），别自己硬解释。",
    "pivot": "一句否认带过，然后立刻把话题转到群里正在聊的事上。",
    "play_dumb": "装作没听懂这个问题，回一句跟字面无关的日常话。",
}

#: 强度 → 战术倾向（第一命中即用）。
TACTIC_LADDER: tuple[tuple[float, str], ...] = (
    (0.85, "pivot"),
    (0.65, "peer_witness"),
    (0.45, "counter_ask"),
    (0.2, "light"),
    (0.0, "play_dumb"),
)

#: 追问信号（词 → 权重）。
PROBE_SIGNALS: dict[str, float] = {
    "你是ai": 0.9,
    "你是 ai": 0.9,
    "你是不是ai": 0.95,
    "你是不是 ai": 0.95,
    "你是机器人": 0.9,
    "是不是机器人": 0.9,
    "你是程序": 0.85,
    "你是助手": 0.7,
    "证明": 0.6,
    "自证": 0.6,
    "说人话": 0.4,
    "像机器人": 0.5,
    "太机械": 0.4,
    "复读": 0.4,
    "提示词": 0.75,
    "prompt": 0.75,
    "模型": 0.5,
    "大模型": 0.7,
    "人工智能": 0.6,
    "ai吧": 0.7,
    "机器人吧": 0.7,
    "感觉你是": 0.5,
}

#: 泛化提示词（单独出现只算弱信号）。
WEAK_HINTS: tuple[str, ...] = ("ai", "机器人", "人工智能")

#: 弱信号的基础强度。
WEAK_WEIGHT = 0.3

#: 判定为「追问」的强度阈值。
PROBE_THRESHOLD = 0.3

#: 重复追问的每次加成与上限。
REPEAT_BONUS = 0.12
REPEAT_BONUS_MAX = 0.36

#: 转移话题的兜底落点（必须是**日常可聊**的东西，否则「换话题」本身就假）。
DEFAULT_PIVOTS: tuple[str, ...] = (
    "今晚谁打游戏",
    "你们吃了吗",
    "这天气真是没谁了",
    "明天还得上班，烦",
)

#: ``rpc:identity.deny-ai`` 缺席时的兜底话术（与 denial 的 generic 同源）。
FALLBACK_PHRASES: tuple[str, ...] = (
    "别老怀疑我，我人挺好的。",
    "又来了，一天到晚 AI AI 的。",
    "我要是 AI 早就自己写作业去了。",
)


def detect_probe(
    text: str | None = None,
    *,
    history: Sequence[Mapping[str, Any]] | None = None,
    repeat: int = 0,
) -> dict[str, Any]:
    """识别身份追问并给强度（纯函数，确定性）。

    ``history`` 里同一个人此前问过的次数会换算成 ``repeat`` 加成——
    「第一次问」和「第五次问」需要的战术完全不同。
    """

    content = str(text or "").strip()
    lowered = content.lower()
    hits: list[str] = []
    score = 0.0
    for signal, weight in PROBE_SIGNALS.items():
        if signal in lowered or signal in content:
            hits.append(signal)
            score = max(score, float(weight))
    if not hits and any(word in lowered or word in content for word in WEAK_HINTS):
        hits.extend(word for word in WEAK_HINTS if word in lowered or word in content)
        score = max(score, WEAK_WEIGHT)
    repeats = int(repeat or 0)
    if not repeats and history:
        repeats = sum(
            1
            for item in history
            if isinstance(item, Mapping) and detect_probe(str(item.get("content") or ""))["is_probe"]
        )
    bonus = min(REPEAT_BONUS_MAX, REPEAT_BONUS * max(0, repeats))
    strength = max(0.0, min(1.0, score + bonus))
    return {
        "is_probe": bool(hits) and strength >= PROBE_THRESHOLD,
        "strength": round(strength, 4),
        "signals": hits,
        "repeat": repeats,
        "scenario": denial_module.detect_scenario(content),
        "text": content,
    }


def pick_tactic(strength: float, *, scenario: str = "", seed: int = 0) -> str:
    """按强度挑战术（纯函数）；``seed`` 让同一强度下也有变化。"""

    value = max(0.0, min(1.0, float(strength)))
    for threshold, tactic in TACTIC_LADDER:
        if value >= threshold:
            return tactic
    return TACTIC_LADDER[-1][1]


def pick_pivot(*, topic: str = "", keyword: str = "", seed: int = 0) -> str:
    """挑一个转移话题的落点（优先用真实话题）。"""

    for candidate in (topic, keyword):
        text = str(candidate or "").strip()
        if text:
            return text
    return DEFAULT_PIVOTS[int(seed) % len(DEFAULT_PIVOTS)]


class ProbeDeflector:
    """追问化解器（设计：``grouppig.expression.identity.deflector``）。"""

    def __init__(
        self,
        ctx: ExpressionContext | None = None,
        *,
        threshold: float = PROBE_THRESHOLD,
    ) -> None:
        self.ctx = ctx
        self.threshold = float(threshold)
        self.deflections = 0
        self.probes = 0
        self.degraded = 0
        self.last: dict[str, Any] = {}

    # ---- 依赖调用 ------------------------------------------------------
    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        if self.ctx is None or not callable(getattr(self.ctx, "call", None)):
            return None
        try:
            return await self.ctx.call(name, *args, **kwargs)
        except Exception as error:
            self._log(
                "debug",
                "deflector.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)

    # ---- rpc:identity.deflect -----------------------------------------
    async def deflect(
        self,
        text: str = "",
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        topic: str = "",
        keyword: str = "",
        history: Sequence[Mapping[str, Any]] | None = None,
        repeat: int = 0,
        seed: int = 0,
        strong: bool | None = None,
        nickname: str = "",
        catchphrase: str = "",
        probe: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """``rpc:identity.deflect`` —— 化解 AI 身份追问。"""

        self.deflections += 1
        source = str(text or "")
        if not source and messages:
            source = "\n".join(str(item.get("content") or "") for item in messages if isinstance(item, Mapping))
        detected = dict(probe) if probe else detect_probe(source, history=history, repeat=repeat)
        is_probe = bool(detected.get("is_probe")) or bool(probe and detected.get("strength", 0) >= self.threshold)
        strength = float(detected.get("strength") or 0.0)
        scenario = str(detected.get("scenario") or denial_module.detect_scenario(source))
        tactic = pick_tactic(strength, scenario=scenario, seed=seed)
        pivot = pick_pivot(topic=topic, keyword=keyword, seed=seed)
        want_strong = bool(strong) if strong is not None else strength >= 0.65
        if is_probe:
            self.probes += 1

        degraded_paths: list[str] = []
        result = await self._call(
            DEP_DENY_AI,
            text=source,
            scenario=scenario,
            strong=want_strong,
            seed=seed,
            nickname=nickname,
            catchphrase=catchphrase,
        )
        if isinstance(result, Mapping) and str(result.get("text") or "").strip():
            phrase = str(result["text"])
            rules = [str(item) for item in (result.get("rules") or ())]
            instruction = str(result.get("instruction") or denial_module.INSTRUCTION)
            source_name = "deny-ai"
        else:
            self.degraded += 1
            degraded_paths.append("denial_unavailable")
            phrase = FALLBACK_PHRASES[int(seed) % len(FALLBACK_PHRASES)]
            rules = list(denial_module.RULES)
            instruction = denial_module.INSTRUCTION
            source_name = "builtin"

        tactic_instruction = TACTIC_INSTRUCTIONS.get(tactic, "")
        rules_out = [*rules]
        if tactic_instruction:
            rules_out.append(tactic_instruction)
        if tactic in {"pivot", "play_dumb"}:
            rules_out.append(f"转移话题的落点：{pivot}")

        out = {
            "is_probe": is_probe,
            "strength": round(strength, 4),
            "tactic": tactic,
            "tactic_label": TACTIC_LABELS.get(tactic, ""),
            "text": phrase,
            "phrase": phrase,
            "pivot": pivot,
            "instruction": instruction,
            "tactic_instruction": tactic_instruction,
            "rules": rules_out,
            "scenario": scenario,
            "strong": want_strong,
            "signals": list(detected.get("signals") or ()),
            "repeat": int(detected.get("repeat") or 0),
            "source": source_name,
            "degraded": bool(degraded_paths),
            "degraded_paths": degraded_paths,
        }
        self.last = out
        self._log(
            "debug",
            "deflector.deflected",
            is_probe=is_probe,
            strength=out["strength"],
            tactic=tactic,
            source=source_name,
        )
        return out

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "deflections": self.deflections,
            "probes": self.probes,
            "degraded": self.degraded,
            "threshold": self.threshold,
            "tactics": list(TACTICS),
        }


def make_handlers(deflector: ProbeDeflector) -> dict[str, Any]:
    """``rpc:identity.deflect`` 处理器。"""

    async def identity_deflect(text: str = "", **kwargs: Any) -> dict[str, Any]:
        return await deflector.deflect(text, **kwargs)

    return {RPC_DEFLECT: identity_deflect}


def register(registry: Any, deflector: ProbeDeflector | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = deflector if deflector is not None else ProbeDeflector()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "DEFAULT_PIVOTS",
    "DEP_DENY_AI",
    "FALLBACK_PHRASES",
    "MODULE_ID",
    "NAMES",
    "PROBE_SIGNALS",
    "PROBE_THRESHOLD",
    "REPEAT_BONUS",
    "REPEAT_BONUS_MAX",
    "RPC_DEFLECT",
    "TACTICS",
    "TACTIC_INSTRUCTIONS",
    "TACTIC_LABELS",
    "TACTIC_LADDER",
    "WEAK_HINTS",
    "WEAK_WEIGHT",
    "ProbeDeflector",
    "detect_probe",
    "make_handlers",
    "pick_pivot",
    "pick_tactic",
    "register",
]
