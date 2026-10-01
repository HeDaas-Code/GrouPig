"""grouppig.social.speech.responder.adapter —— 风格适配器（``rpc:speech.advise`` / ``rpc:speech.tailor``）。

职责（设计：``grouppig.social.speech.responder.adapter``「按画像把草稿改写为目标群友习惯的说话方式」）：

* ``rpc:speech.advise`` —— 读画像给出**风格建议**：目标指标、该做/别做、可用的口头禅与表情；
  设计依赖：``rpc:speech.advise`` → ``rpc:speech.style``（逐字对齐）。
* ``rpc:speech.tailor`` —— 按画像**改写草稿**，再交给校验器把关；
  设计依赖：``rpc:speech.tailor`` → ``rpc:speech.validate``（逐字对齐）。

改写动作（确定性、可回放）：

``strip_boundary`` 清掉越界片段（AI 自曝 / 设定泄露 / 敏感词 / @全体）→
``trim`` 压到画像长度区间 → ``soften`` 收敛过激词 → ``add_filler`` / ``add_emoji`` 补齐画像密度 →
``pad`` 过短时补一句口语 → 再校验；不通过且还有轮次就再来一轮（``max_rounds``）。

设计：``grouppig.social.speech.responder.adapter``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.social.speech.profiler.lexicon import EMOJI_RE, FILLERS
from grouppig.social.speech.responder.validator import (
    AI_DISCLOSURE_PATTERNS,
    AT_ALL_PATTERNS,
    PROMPT_LEAK_PATTERNS,
    SENSITIVE_RE,
    StyleValidator,
)

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.speech.responder.adapter"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:speech.advise", "rpc:speech.tailor")
RPC_ADVISE = "rpc:speech.advise"
RPC_TAILOR = "rpc:speech.tailor"
for _name in RPC_NAMES:
    contract.assert_known_name(_name)

#: 设计依赖（逐字对齐 adapter.md 的 deps）。
DEP_SPEECH_STYLE = "rpc:speech.style"
DEP_SPEECH_VALIDATE = "rpc:speech.validate"
for _name in (DEP_SPEECH_STYLE, DEP_SPEECH_VALIDATE):
    contract.assert_known_name(_name)

#: 默认补的表情（画像里没有表情偏好时使用）。
DEFAULT_EMOJI: tuple[str, ...] = ("😄", "😆", "🐷", "👍", "🤣")

#: 默认补的语气词。
DEFAULT_FILLER = "啦"

#: 连续重复字符的压缩上限（保持「哈哈哈哈」这种人味，砍掉刷屏式重复）。
MAX_REPEAT = 4

#: 过激词 → 温和说法。
SOFTEN_MAP: dict[str, str] = {
    "闭嘴": "先别急",
    "滚": "走吧",
    "傻": "憨",
    "废物": "笨蛋",
    "垃圾": "不太行",
    "蠢": "憨",
    "弱智": "不太聪明",
    "脑残": "离谱",
    "恶心": "受不了",
    "去死": "别闹",
    "有病": "离谱",
    "智障": "离谱",
}

#: 句末标点（trim 时优先在这些位置断开）。
BREAK_CHARS = "。！？!?；;，,、"

#: 画像长度偏差容忍比（与 validator 默认保持一致）。
LENGTH_RATIO = 0.8


#: 语气词密度的合法键名（避免把统计里的 `count` / `density` 当成语气词）。
FILLER_KEYS = frozenset(FILLERS)


def _pick_emoji(portrait: Mapping[str, Any] | None) -> str:
    """从画像里挑一个该群友常用的表情（只认真正的表情字形，没有就用默认）。"""

    lexicon = (portrait or {}).get("lexicon") or {}
    emoji = lexicon.get("emoji")
    if isinstance(emoji, Mapping) and emoji:
        ranked = [
            (key, value)
            for key, value in emoji.items()
            if isinstance(value, (int, float)) and EMOJI_RE.fullmatch(str(key))
        ]
        if ranked:
            ranked.sort(key=lambda item: item[1], reverse=True)
            return str(ranked[0][0])
        for key in emoji:
            match = EMOJI_RE.search(str(key))
            if match is not None:
                return match.group(0)
    if isinstance(emoji, Sequence) and not isinstance(emoji, (str, bytes)):
        for item in emoji:
            match = EMOJI_RE.search(str(item))
            if match is not None:
                return match.group(0)
    return DEFAULT_EMOJI[0]


def _pick_filler(portrait: Mapping[str, Any] | None) -> str:
    """从画像里挑一个该群友常用的语气词（只认真正的语气词，缺省「啦」）。"""

    lexicon = (portrait or {}).get("lexicon") or {}
    fillers = lexicon.get("fillers")
    if isinstance(fillers, Mapping) and fillers:
        ranked = [
            (key, value)
            for key, value in fillers.items()
            if isinstance(value, (int, float)) and str(key) in FILLER_KEYS
        ]
        if ranked:
            ranked.sort(key=lambda item: item[1], reverse=True)
            return str(ranked[0][0])
    if isinstance(fillers, Sequence) and not isinstance(fillers, (str, bytes)):
        for item in fillers:
            if str(item) in FILLER_KEYS:
                return str(item)
    return DEFAULT_FILLER


def _by_length(patterns: Sequence[str]) -> tuple[re.Pattern[str], ...]:
    """把模式字符串编成正则，并按「长的先匹配」（完整模式优先于前缀模式）。"""

    return tuple(re.compile(item) for item in sorted(patterns, key=len, reverse=True))


#: 越界剥离规则：(模式, 标签, 命中后要一起吞掉的尾巴)，按「长的先匹配」排序。
STRIP_RULES: tuple[tuple[re.Pattern[str], str, str], ...] = (
    *(
        (pattern, "ai_self_disclosure", r"(?:助手|机器人|AI|ai|人工智能|语言模型|大模型)")
        for pattern in _by_length(AI_DISCLOSURE_PATTERNS)
    ),
    *((pattern, "prompt_leak", "") for pattern in _by_length(PROMPT_LEAK_PATTERNS)),
    *((pattern, "sensitive", "") for pattern in SENSITIVE_RE),
    *((pattern, "at_all", "") for pattern in _by_length(AT_ALL_PATTERNS)),
)

#: 句末标点（trim 时优先在这些位置断开）。


def strip_boundary(text: str) -> tuple[str, list[str]]:
    """清掉越界**片段**（只删命中的那一小段，不整句抹掉），返回（新文本, 命中的动作）。

    在**删除后的子串**上重跑一遍全部模式，直到不再命中——避免「删掉前半句后残留的
    尾巴仍然越界」（例如「我是 AI 助手」先删「我是 AI」，再让完整模式把残留的
    「助手」也带走）。
    """

    result = str(text or "")
    applied: list[str] = []
    for _ in range(4):
        removed = False
        for pattern, tag, tail in STRIP_RULES:
            region = pattern.search(result)
            if region is None:
                continue
            end = region.end()
            if tail:
                tail_match = re.compile(tail).match(result, end)
                if tail_match is not None:
                    end = tail_match.end()
            start = region.start()
            if start > 0 and result[start - 1] in "，,、":
                start -= 1
            result = f"{result[:start]}{result[end:]}"
            if tag not in applied:
                applied.append(tag)
            removed = True
            break
        if not removed:
            break
    result = re.sub(r"\s{2,}", " ", result)
    result = re.sub(r"^[，,、；;：:！!？?\s]+", "", result)
    result = re.sub(r"[，,、；;：:]+$", "", result)
    return result.strip(), applied


def trim(text: str, limit: int) -> tuple[str, bool]:
    """按句读边界压到 ``limit`` 字以内（返回是否真的裁过）。"""

    draft = str(text or "")
    if len(draft) <= limit:
        return draft, False
    window = draft[:limit]
    cut = max((window.rfind(char) for char in BREAK_CHARS), default=-1)
    trimmed = window[: cut + 1] if cut >= max(4, limit // 3) else window
    return trimmed.rstrip(" ，,、；;"), True


def compact_repeats(text: str, *, limit: int = MAX_REPEAT) -> str:
    """把连续重复 5 次以上的字符压到 ``limit`` 次。"""

    return re.sub(rf"(.)\1{{{limit},}}", lambda match: match.group(1) * limit, str(text or ""))


def soften(text: str) -> tuple[str, bool]:
    """收敛过激词。"""

    result = str(text or "")
    changed = False
    for harsh, mild in SOFTEN_MAP.items():
        if harsh in result:
            result = result.replace(harsh, mild)
            changed = True
    return result, changed


@dataclass
class StyleAdapter:
    """风格适配器（设计：``grouppig.social.speech.responder.adapter``）。"""

    ctx: SocialContext
    max_rounds: int = 2
    max_length: int = 220
    length_ratio: float = LENGTH_RATIO
    advices: int = field(default=0, init=False)
    tailored: int = field(default=0, init=False)

    # ---- 建议 ----------------------------------------------------------
    async def advise(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        draft: str | None = None,
        portrait: Mapping[str, Any] | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """给出回复风格建议（设计依赖：``rpc:speech.advise`` → ``rpc:speech.style``）。"""

        if user_id is None:
            raise ValueError("rpc:speech.advise 需要 user_id")
        user_id = int(user_id)
        style = None
        if portrait is None:
            style = await self.ctx.call(DEP_SPEECH_STYLE, user_id, group_id=int(group_id or 0), refresh=refresh)
            portrait = (style or {}).get("portrait")
        metrics = dict((portrait or {}).get("metrics") or {})
        temper = dict((portrait or {}).get("temper") or {})
        lexicon = dict((portrait or {}).get("lexicon") or {})
        tags = list((portrait or {}).get("tags") or [])
        target = {
            "avg_length": float(metrics.get("avg_length") or 0.0),
            "emoji_density": float(metrics.get("emoji_density") or 0.0),
            "filler_density": float(metrics.get("filler_density") or 0.0),
            "warmth": float(temper.get("warmth") or 0.0),
            "aggressiveness": float(temper.get("aggressiveness") or 0.0),
            "intimacy": float(temper.get("intimacy") or 0.0),
            "label": str(temper.get("label") or ""),
        }
        do, dont = self._guidelines(target, tags)
        result: dict[str, Any] = {
            "user_id": user_id,
            "group_id": int(group_id or 0),
            "portrait_found": bool(portrait) and bool((style or {}).get("found", True)),
            "source": (style or {}).get("source", "provided"),
            "target": target,
            "tags": tags,
            "do": do,
            "dont": dont,
            "suggested_openers": list(lexicon.get("catchphrases") or [])[:3],
            "suggested_emoji": _pick_emoji(portrait),
            "suggested_filler": _pick_filler(portrait),
            "summary": str((portrait or {}).get("summary") or ""),
            "draft_metrics": None,
        }
        if draft is not None:
            result["draft_metrics"] = StyleValidator.text_metrics(draft)
            result["draft_hint"] = self._draft_hint(result["draft_metrics"], target)
        self.advices += 1
        return result

    def _guidelines(self, target: Mapping[str, Any], tags: Sequence[str]) -> tuple[list[str], list[str]]:
        do: list[str] = []
        dont: list[str] = []
        avg_length = float(target.get("avg_length") or 0.0)
        if avg_length:
            do.append(f"长度贴近画像均长 {avg_length:.0f} 字左右")
        if float(target.get("emoji_density") or 0.0) >= 0.3:
            do.append("每条至少带 1 个表情")
        else:
            dont.append("别堆表情")
        if float(target.get("filler_density") or 0.0) >= 0.03:
            do.append("用语气词收尾（啊/啦/嘛）")
        if float(target.get("aggressiveness") or 0.0) >= 0.25:
            do.append("可以互怼，但别上升到人身攻击")
        else:
            dont.append("别用攻击性词")
        if float(target.get("intimacy") or 0.0) >= 0.3:
            do.append("可以用亲昵称呼（宝/兄弟）")
        else:
            dont.append("别过度亲昵")
        if float(target.get("warmth") or 0.0) <= -0.25:
            dont.append("别太热情，简短回一句就够")
        if "爱提问" in tags:
            do.append("可以反问一句拉回对话")
        dont.append("不要自曝 AI 身份")
        dont.append("不要 @全体成员")
        return do, dont

    @staticmethod
    def _draft_hint(metrics: Mapping[str, Any], target: Mapping[str, Any]) -> dict[str, Any]:
        avg_length = float(target.get("avg_length") or 0.0)
        length = float(metrics.get("length") or 0.0)
        hints: list[str] = []
        if avg_length:
            ratio = length / avg_length
            if ratio > 1 + LENGTH_RATIO:
                hints.append("偏长，建议缩短")
            elif ratio < 1 / (1 + LENGTH_RATIO):
                hints.append("偏短，可以补一句")
        if float(target.get("emoji_density") or 0.0) - float(metrics.get("emoji_count") or 0.0) > 1:
            hints.append("补一个表情")
        if float(target.get("filler_density") or 0.0) - float(metrics.get("filler_density") or 0.0) > 0.08:
            hints.append("补一个语气词")
        return {"hints": hints, "length_ratio": round(length / avg_length, 3) if avg_length else None}

    # ---- 改写 ----------------------------------------------------------
    async def tailor(
        self,
        text: str,
        *,
        user_id: int | None = None,
        group_id: int = 0,
        portrait: Mapping[str, Any] | None = None,
        max_rounds: int | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """按画像改写草稿（设计依赖：``rpc:speech.tailor`` → ``rpc:speech.validate``）。"""

        original = str(text or "")
        rounds = max(1, int(self.max_rounds if max_rounds is None else max_rounds))
        if portrait is None and user_id is not None:
            style = await self.ctx.call(DEP_SPEECH_STYLE, int(user_id), group_id=int(group_id or 0))
            portrait = (style or {}).get("portrait")
        metrics = dict((portrait or {}).get("metrics") or {})
        target_length = float(metrics.get("avg_length") or 0.0)
        applied: list[str] = []
        draft = original
        validation: dict[str, Any] = {}
        rounds_used = 0
        for round_index in range(rounds):
            rounds_used = round_index + 1
            draft, step_applied = self._apply_transforms(
                draft, portrait=portrait, target_length=target_length, last_round=round_index == rounds - 1
            )
            for item in step_applied:
                if item not in applied:
                    applied.append(item)
            validation = await self.ctx.call(
                DEP_SPEECH_VALIDATE,
                draft,
                user_id=user_id,
                group_id=int(group_id or 0),
                portrait=portrait,
                strict=strict,
            )
            if validation.get("ok"):
                break
            if not step_applied:
                break
        self.tailored += 1
        return {
            "user_id": int(user_id or 0),
            "group_id": int(group_id or 0),
            "text": draft,
            "original": original,
            "changed": draft != original,
            "applied": applied,
            "rounds": rounds_used,
            "validation": validation,
            "ok": bool(validation.get("ok")),
            "portrait_found": bool(portrait),
        }

    def _apply_transforms(
        self,
        text: str,
        *,
        portrait: Mapping[str, Any] | None,
        target_length: float,
        last_round: bool,
    ) -> tuple[str, list[str]]:
        """一轮改写：剥离越界 → 收敛过激 → 定长 → 补语气词/表情。"""

        applied: list[str] = []
        draft, steps = strip_boundary(text)
        applied.extend(steps)
        draft, changed = soften(draft)
        if changed:
            applied.append("soften")
        draft = compact_repeats(draft)
        if re.search(r"(.)\1{3,}", draft):
            applied.append("compact_repeats")
        # 先定长（画像均长优先，其次硬上限），再补语气词/表情，避免越改越长
        limit = self.max_length
        if target_length:
            limit = min(limit, max(4, int(target_length * (1 + self.length_ratio))))
        draft, trimmed = trim(draft, limit)
        if trimmed:
            applied.append("trim")
        if not draft.strip():
            draft = "嗯嗯"
            applied.append("fallback")
        if target_length and len(draft) < max(2.0, target_length / (1 + self.length_ratio)):
            draft = self._pad(draft, portrait)
            applied.append("pad")
        target_emoji = float(((portrait or {}).get("metrics") or {}).get("emoji_density") or 0.0)
        if target_emoji >= 0.3 and not EMOJI_RE.search(draft):
            draft = f"{draft}{_pick_emoji(portrait)}"
            applied.append("add_emoji")
        target_filler = float(((portrait or {}).get("metrics") or {}).get("filler_density") or 0.0)
        if target_filler >= 0.03 and not any(char in draft for char in FILLERS):
            draft = f"{draft}{_pick_filler(portrait)}"
            applied.append("add_filler")
        return draft.strip(), applied

    @staticmethod
    def _pad(text: str, portrait: Mapping[str, Any] | None) -> str:
        filler = _pick_filler(portrait)
        suffix = "，你说呢" if not text.endswith(("？", "?")) else ""
        return f"{text}{suffix}{filler}"

    def status(self) -> dict[str, Any]:
        return {"advices": self.advices, "tailored": self.tailored, "max_rounds": self.max_rounds}


def make_handlers(ctx: SocialContext, adapter: StyleAdapter) -> dict[str, Any]:
    """``rpc:speech.advise`` / ``rpc:speech.tailor`` 处理器。"""

    async def speech_advise(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        draft: str | None = None,
        portrait: Mapping[str, Any] | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        return await adapter.advise(user_id, group_id=group_id, draft=draft, portrait=portrait, refresh=refresh)

    async def speech_tailor(
        text: str = "",
        *,
        user_id: int | None = None,
        group_id: int = 0,
        portrait: Mapping[str, Any] | None = None,
        max_rounds: int | None = None,
        strict: bool = False,
        draft: str | None = None,
    ) -> dict[str, Any]:
        return await adapter.tailor(
            draft if draft is not None else text,
            user_id=user_id,
            group_id=group_id,
            portrait=portrait,
            max_rounds=max_rounds,
            strict=strict,
        )

    return {RPC_ADVISE: speech_advise, RPC_TAILOR: speech_tailor}


def register(registry: Any, adapter: StyleAdapter, *, replace: bool = True) -> None:
    for name, handler in make_handlers(adapter.ctx, adapter).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEFAULT_EMOJI",
    "DEP_SPEECH_STYLE",
    "DEP_SPEECH_VALIDATE",
    "MODULE_ID",
    "RPC_ADVISE",
    "RPC_NAMES",
    "RPC_TAILOR",
    "SOFTEN_MAP",
    "StyleAdapter",
    "make_handlers",
    "register",
    "soften",
    "strip_boundary",
    "trim",
]
