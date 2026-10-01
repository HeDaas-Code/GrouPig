"""grouppig.expression.generator.polisher —— 人性化润色器（``rpc:generator.humanize``）。

职责（设计：``grouppig.expression.generator.polisher``「把回复润色得更像人：口语化、接梗、按画像改写」）：

* 按画像改写：``rpc:generator.humanize`` → ``rpc:speech.tailor``（设计依赖，逐字对齐）；
* 润色分**两层**，顺序固定：

  1. :func:`humanize` 本地确定性清洗（不依赖任何下游，永远可用）：去 AI 味
     （「作为一个 AI」「希望能帮到你」「以下是」这类助手腔）、去书面连接词
     （「因此 / 此外 / 综上所述」）、去多余引号与 Markdown 强调符、压缩重复字符、
     收越界片段（AI 自曝 / 提示词泄露 / @全体）；
  2. ``rpc:speech.tailor`` 按目标群友的说话画像二次改写（表情密度、语气词、长度）；
     下游缺失时就只用第 1 层，返回 ``tailored=False``——**润色降级不阻断发送**。

:class:`HumanizingPolisher` 负责调度两层，并把两层的 ``applied`` 动作合并成一份可观测清单，
交给上游（generator.writer / 反思层）判断这条回复到底被动过哪几刀。

「去 AI 味」的具体规则（:data:`ASSISTANT_PATTERNS` / :data:`BOOKISH_MAP` / :data:`LEXICON_MAP`）
是**确定性表**，不是模型判断——人味这件事在工程上必须可回放，否则同一条草稿两次润色结果不同，
反思层就拿不到可靠的因果。

设计：``grouppig.expression.generator.polisher``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.generator.polisher"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:generator.humanize",)
RPC_HUMANIZE = "rpc:generator.humanize"
contract.assert_known_name(RPC_HUMANIZE)

#: 设计依赖（逐字对齐 polisher.md 的 deps）。
DEP_SPEECH_TAILOR = "rpc:speech.tailor"
contract.assert_known_name(DEP_SPEECH_TAILOR)

#: 助手腔短语（直接删）。
ASSISTANT_PATTERNS: tuple[str, ...] = (
    r"作为一个?(?:AI|ai|人工智能|智能助手|语言模型|大模型|助手)",
    r"我是(?:一个)?(?:AI|ai|人工智能|智能助手|机器人|语言模型|大模型|程序)",
    r"希望能(?:帮|帮助|对你有帮助)",
    r"很高兴(?:为|能为你)",
    r"感谢(?:你的)?(?:提问|咨询)",
    r"以下(?:是|为)",
    r"希望对你有帮助",
    r"如有(?:其他)?(?:问题|疑问)",
    r"(?:请|欢迎)随时(?:提问|问我)",
    r"需要我(?:帮你)?(?:做|写|整理)",
    r"根据(?:你|您)的(?:描述|要求)",
    r"温馨提示",
    r"请注意[，,]",
)

#: 书面连接词 → 口语说法。
BOOKISH_MAP: dict[str, str] = {
    "因此": "所以",
    "此外": "另外",
    "然而": "不过",
    "并且": "而且",
    "综上": "反正",
    "首先": "先",
    "其次": "然后",
    "进行": "搞",
    "非常": "挺",
    "十分": "挺",
    "或许": "说不定",
    "倘若": "要是",
    "是否": "是不是",
    "以及": "还有",
    "由于": "因为",
}

#: 书面说法 → 人味说法。
LEXICON_MAP: dict[str, str] = {
    "好的": "行",
    "是的": "嗯",
    "没问题": "行吧",
    "我知道了": "懂了",
    "真的很": "真的挺",
}

#: 越界片段（删；项目铁律：否认 AI、不泄露提示词、不 @全体）。
BOUNDARY_PATTERNS: tuple[str, ...] = (
    r"作为一个?(?:AI|ai|人工智能|助手|语言模型|大模型)",
    r"我是(?:一个)?(?:AI|ai|人工智能|智能助手|机器人|语言模型|大模型|程序|代码)",
    r"系统提示(?:词)?",
    r"我的?(?:提示词|人设|设定)(?:是|为)",
    r"@全体成员",
)

#: 语气词候选。
FILLERS: tuple[str, ...] = ("啊", "啦", "嘛", "吧", "哦", "诶", "哈")

#: 缺省句尾语气词。
DEFAULT_FILLER = "啦"

#: 连续重复字符压缩上限（保留「哈哈哈哈」的人味）。
MAX_REPEAT = 4

#: 默认最大长度。
DEFAULT_MAX_CHARS = 120

#: 句末标点（截断时优先在这些位置断开）。
BREAK_CHARS = "。！？!?；;，,、"

#: 句末标点（判「已经收尾」时用）。
SENTENCE_END = "。！？!?…~"

#: 补语气词的阈值：短于画像均长的这个比例才补（避免每条都拖个语气词）。
FILLER_RATIO = 0.7

_ASSISTANT_RE: tuple[re.Pattern[str], ...] = tuple(re.compile(item) for item in ASSISTANT_PATTERNS)
_BOUNDARY_RE: tuple[re.Pattern[str], ...] = tuple(re.compile(item) for item in BOUNDARY_PATTERNS)
_REPEAT_RE = re.compile(r"(.)\1{" + str(MAX_REPEAT) + ",}")

#: Markdown 强调符 / 项目符号（群聊里不该出现）；带捕获组的还原内容，其余整段删。
MARKDOWN_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\*\*(.+?)\*\*"),
    re.compile(r"__(.+?)__"),
    re.compile(r"`(.+?)`"),
    re.compile(r"^\s*[-*+]\s+", re.M),
    re.compile(r"^\s*#{1,6}\s+", re.M),
)

#: 包裹整体文本的成对引号。
WRAPPED_QUOTES: tuple[tuple[str, str], ...] = (("「", "」"), ("“", "”"), ('"', '"'))


def compact_repeats(text: str, *, limit: int = MAX_REPEAT) -> str:
    """把连续重复 ``limit`` 次以上的字符压到 ``limit`` 次。"""

    return _REPEAT_RE.sub(lambda match: match.group(1) * limit, str(text or ""))


def strip_assistant_tone(text: str) -> tuple[str, list[str]]:
    """去掉助手腔片段，返回（新文本, 命中的动作）。"""

    result = str(text or "")
    applied: list[str] = []
    for pattern in _ASSISTANT_RE:
        result, count = pattern.subn("", result)
        if count and "drop_assistant_tone" not in applied:
            applied.append("drop_assistant_tone")
    return result, applied


def strip_boundary(text: str) -> tuple[str, list[str]]:
    """收掉越界片段（AI 自曝 / 提示词泄露 / @全体）。"""

    result = str(text or "")
    applied: list[str] = []
    for pattern in _BOUNDARY_RE:
        result, count = pattern.subn("", result)
        if count and "strip_boundary" not in applied:
            applied.append("strip_boundary")
    return result, applied


def unmarkdown(text: str) -> tuple[str, bool]:
    """去掉 Markdown 格式与包裹整体文本的引号（群聊里没人这么说话）。"""

    result = str(text or "")
    changed = False
    for pattern in MARKDOWN_RES:
        result, count = pattern.subn(lambda match: match.group(1) if match.groups() else "", result)
        changed = changed or bool(count)
    for left, right in WRAPPED_QUOTES:
        stripped = result.strip()
        if len(stripped) >= 2 and stripped.startswith(left) and stripped.endswith(right):
            result = stripped[1:-1].strip()
            changed = True
    return result, changed


def colloquialize(text: str) -> tuple[str, list[str]]:
    """书面词 → 口语说法（两个映射表按序替换）。"""

    result = str(text or "")
    applied: list[str] = []
    for mapping, tag in ((BOOKISH_MAP, "colloquialize"), (LEXICON_MAP, "humanize_lexicon")):
        for formal, casual in mapping.items():
            if formal in result:
                result = result.replace(formal, casual)
                if tag not in applied:
                    applied.append(tag)
    return result, applied


def tidy_punctuation(text: str) -> str:
    """规整标点：连续标点、首尾多余标点、连续空白。"""

    result = re.sub(r"[，,]{2,}", "，", str(text or ""))
    result = re.sub(r"[。！？!?]{2,}", lambda match: match.group(0)[0], result)
    result = re.sub(r"^[，,、；;：:！!？?\s]+", "", result)
    result = re.sub(r"[，,、；;：:]+$", "", result)
    result = re.sub(r"\s{2,}", " ", result)
    return result.strip()


def trim(text: str, limit: int) -> tuple[str, bool]:
    """按句读边界压到 ``limit`` 字以内（返回是否真的裁过）。"""

    draft = str(text or "")
    if len(draft) <= limit:
        return draft, False
    window = draft[:limit]
    cut = max((window.rfind(char) for char in BREAK_CHARS), default=-1)
    trimmed = window[: cut + 1] if cut >= max(4, limit // 3) else window
    return trimmed.rstrip(" ，,、；;"), True


def pick_filler(style_hints: Mapping[str, Any] | None = None) -> str:
    """从画像口癖里挑一个语气词（缺省「啦」）。"""

    fillers = (style_hints or {}).get("fillers")
    if isinstance(fillers, Mapping):
        for key in fillers:
            text = str(key).strip()
            if text in FILLERS:
                return text
    elif isinstance(fillers, Sequence) and not isinstance(fillers, (str, bytes)):
        for item in fillers:
            text = str(item).strip()
            if text in FILLERS:
                return text
    return DEFAULT_FILLER


def humanize(
    text: str,
    *,
    style_hints: Mapping[str, Any] | None = None,
    max_chars: int = DEFAULT_MAX_CHARS,
    add_filler: bool = True,
    target_length: float = 0.0,
) -> dict[str, Any]:
    """纯函数润色：本地确定性清洗，不依赖任何下游。"""

    original = str(text or "")
    applied: list[str] = []

    draft, steps = strip_assistant_tone(original)
    applied.extend(steps)
    draft, steps = strip_boundary(draft)
    applied.extend(steps)
    draft, changed = unmarkdown(draft)
    if changed:
        applied.append("unmarkdown")
    draft, steps = colloquialize(draft)
    applied.extend(steps)
    compacted = compact_repeats(draft)
    if compacted != draft:
        applied.append("compact_repeats")
    draft = tidy_punctuation(compacted)
    draft, trimmed = trim(draft, int(max_chars))
    if trimmed:
        applied.append("trim")

    if not draft:
        draft = "嗯嗯"
        applied.append("fallback")

    hints = dict(style_hints or {})
    if add_filler and hints.get("fillers"):
        already = any(word in draft for word in FILLERS)
        avg = float(target_length or hints.get("length_hint_avg") or 0.0)
        short = avg <= 0 or len(draft) < avg * FILLER_RATIO
        if not already and short:
            draft = f"{draft}{pick_filler(hints)}"
            applied.append("add_filler")

    cleaned = draft.strip()
    return {
        "text": cleaned,
        "original": original,
        "changed": cleaned != original.strip(),
        "applied": applied,
        "max_chars": int(max_chars),
    }


@dataclass
class HumanizingPolisher:
    """人性化润色器（设计：``grouppig.expression.generator.polisher``）。"""

    ctx: ExpressionContext | None = None
    max_chars: int = DEFAULT_MAX_CHARS
    use_tailor: bool = True
    humanized: int = field(default=0, init=False)
    degraded: int = field(default=0, init=False)

    async def tailor(self, text: str, **kwargs: Any) -> dict[str, Any]:
        """按画像改写（设计依赖：``rpc:generator.humanize`` → ``rpc:speech.tailor``）。"""

        if self.ctx is None or not callable(getattr(self.ctx, "call", None)):
            raise RuntimeError(f"{MODULE_ID} 需要 ctx.call 才能调用 {DEP_SPEECH_TAILOR}")
        result = await self.ctx.call(DEP_SPEECH_TAILOR, text, **kwargs)
        return dict(result or {}) if isinstance(result, Mapping) else {"text": str(result or "")}

    async def run(
        self,
        text: str = "",
        *,
        style_hints: Mapping[str, Any] | None = None,
        portrait: Mapping[str, Any] | None = None,
        user_id: int | None = None,
        group_id: int = 0,
        max_chars: int | None = None,
        tailor: bool | None = None,
        draft: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """``rpc:generator.humanize`` —— 把回复润色得更像人。"""

        self.humanized += 1
        limit = int(max_chars if max_chars is not None else self.max_chars)
        source = draft if draft is not None else text
        hints = dict(style_hints or {})
        avg = float(hints.get("length_hint_avg") or 0.0)

        local = humanize(source, style_hints=hints, max_chars=limit, target_length=avg)
        applied = list(local["applied"])
        degraded_paths: list[str] = []
        result_text = local["text"]
        tailor_result: dict[str, Any] = {}
        use_tailor = self.use_tailor if tailor is None else bool(tailor)

        if use_tailor:
            try:
                tailor_result = await self.tailor(
                    result_text,
                    user_id=user_id,
                    group_id=int(group_id or 0),
                    portrait=portrait,
                    **kwargs,
                )
            except Exception as error:  # 下游缺失 → 只用本地清洗，不抛出
                degraded_paths.append("tailor_unavailable")
                tailor_result = {"error": f"{type(error).__name__}: {error}"}
            else:
                tailored = str(tailor_result.get("text") or "").strip()
                if tailored:
                    result_text = tailored
                    applied.append("tailored")
                for step in tailor_result.get("applied") or ():
                    if str(step) not in applied:
                        applied.append(str(step))

        result_text = tidy_punctuation(compact_repeats(result_text))[:limit]
        if not result_text:
            result_text = local["text"]
            degraded_paths.append("empty_after_polish")

        payload: dict[str, Any] = {
            "text": result_text,
            "original": str(source or ""),
            "changed": result_text != str(source or "").strip(),
            "applied": applied,
            "tailored": bool(use_tailor and not degraded_paths and tailor_result.get("text")),
            "polished": True,
            "degraded": bool(degraded_paths),
            "degraded_paths": sorted(set(degraded_paths)),
            "validation": tailor_result.get("validation") or {},
            "local": local,
        }
        if payload["degraded"]:
            self.degraded += 1
        self._log("debug", "polisher.done", applied=applied, degraded=payload["degraded"])
        return payload

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "humanized": self.humanized,
            "degraded": self.degraded,
            "max_chars": self.max_chars,
            "use_tailor": self.use_tailor,
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)


def make_handlers(polisher: HumanizingPolisher) -> dict[str, Any]:
    """``rpc:generator.humanize`` 处理器。"""

    async def generator_humanize(
        text: str = "",
        *,
        style_hints: Mapping[str, Any] | None = None,
        portrait: Mapping[str, Any] | None = None,
        user_id: int | None = None,
        group_id: int = 0,
        max_chars: int | None = None,
        tailor: bool | None = None,
        draft: str | None = None,
        max_rounds: int | None = None,
        strict: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await polisher.run(
            text,
            style_hints=style_hints,
            portrait=portrait,
            user_id=user_id,
            group_id=group_id,
            max_chars=max_chars,
            tailor=tailor,
            draft=draft,
            max_rounds=max_rounds,
            strict=strict,
            **kwargs,
        )

    return {RPC_HUMANIZE: generator_humanize}


def register(registry: Any, polisher: HumanizingPolisher | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = polisher if polisher is not None else HumanizingPolisher()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "ASSISTANT_PATTERNS",
    "BOOKISH_MAP",
    "BOUNDARY_PATTERNS",
    "DEFAULT_FILLER",
    "DEFAULT_MAX_CHARS",
    "DEP_SPEECH_TAILOR",
    "FILLERS",
    "LEXICON_MAP",
    "MAX_REPEAT",
    "MODULE_ID",
    "NAMES",
    "RPC_HUMANIZE",
    "HumanizingPolisher",
    "colloquialize",
    "compact_repeats",
    "humanize",
    "make_handlers",
    "pick_filler",
    "register",
    "strip_assistant_tone",
    "strip_boundary",
    "tidy_punctuation",
    "trim",
    "unmarkdown",
]
