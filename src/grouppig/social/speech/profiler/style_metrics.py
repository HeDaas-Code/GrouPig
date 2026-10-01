"""grouppig.social.speech.profiler.style-metrics —— 风格指标计算器（``rpc:speech.profile`` / ``rpc:speech.style``）。

职责（设计：``grouppig.social.speech.profiler.style-metrics``「计算说话风格指标：句长、语气词密度、表情密度、回复长度」）：

* ``rpc:speech.profile`` —— 为群友**建立**说话画像，设计依赖逐字对齐：

  - → ``rpc:chat.query``：取历史消息；
  - → ``rpc:profile.get``：取昵称与既有 ``speaking_style``（作为基线/回退来源）；
  - → ``rpc:speech.lexicon``：口头禅与表情偏好；
  - → ``rpc:speech.temper``：语气温度。

* ``rpc:speech.style`` —— **查询**说话风格：先读进程内画像缓存，未命中再读档案里的
  ``speaking_style``，仍为空且 ``build=True`` 时才现算一份（设计里 ``speech.style`` 是
  responder 的读取入口，因此必须能自愈）。

画像只落在**进程内缓存**（``ttl`` 秒）：设计给 ``style-metrics`` 的依赖里没有
``rpc:profile.update``，所以本模块不写档案；需要持久化时由调用方把
``portrait["metrics"] / ["lexicon"] / ["temper"]`` 通过 ``rpc:profile.update`` 落到
``member_profiles.speaking_style``。

设计：``grouppig.social.speech.profiler.style-metrics``（叶子模块）。
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.social.speech.profiler.lexicon import (
    EMOJI_RE,
    FACE_CODE_RE,
    FILLERS,
    KAOMOJI_RE,
    PUNCT_RE,
    SENTENCE_SPLIT_RE,
    tokenize,
)

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.speech.profiler.style-metrics"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:speech.profile", "rpc:speech.style")
RPC_PROFILE = "rpc:speech.profile"
RPC_STYLE = "rpc:speech.style"
for _name in RPC_NAMES:
    contract.assert_known_name(_name)

#: 设计依赖（逐字对齐 style-metrics.md 的 deps）。
DEP_CHAT_QUERY = "rpc:chat.query"
DEP_PROFILE_GET = "rpc:profile.get"
DEP_SPEECH_LEXICON = "rpc:speech.lexicon"
DEP_SPEECH_TEMPER = "rpc:speech.temper"
for _name in (DEP_CHAT_QUERY, DEP_PROFILE_GET, DEP_SPEECH_LEXICON, DEP_SPEECH_TEMPER):
    contract.assert_known_name(_name)

#: 画像缓存默认存活时间（秒）。
DEFAULT_TTL = 600.0

#: 指标键（顺序即画像里 ``metrics`` 的键序）。
METRIC_KEYS: tuple[str, ...] = (
    "avg_length",
    "median_length",
    "max_length",
    "avg_sentence_length",
    "sentences_per_message",
    "filler_density",
    "emoji_density",
    "face_density",
    "punct_density",
    "exclaim_rate",
    "question_rate",
    "reply_ratio",
    "avg_reply_length",
    "messages_per_minute",
    "vocabulary_richness",
    "char_count",
)


def compute_metrics(messages: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """按消息列表算风格指标（纯函数）。"""

    contents: list[str] = []
    reply_lengths: list[int] = []
    stamps: list[float] = []
    tokens: list[str] = []
    char_count = 0
    filler_count = 0
    emoji_count = 0
    face_count = 0
    punct_count = 0
    exclaim = 0
    question = 0
    sentence_count = 0
    for row in messages:
        content = str(row.get("content") or "")
        if not content:
            continue
        contents.append(content)
        char_count += len(content)
        stamps.append(float(row.get("ts") or 0.0))
        filler_count += sum(1 for char in content if char in FILLERS)
        emoji_count += len(EMOJI_RE.findall(content))
        face_count += len(FACE_CODE_RE.findall(content)) + len(KAOMOJI_RE.findall(content))
        punct_count += len(PUNCT_RE.findall(content))
        exclaim += content.count("！") + content.count("!")
        question += content.count("？") + content.count("?")
        sentences = [part for part in SENTENCE_SPLIT_RE.split(content) if part.strip()]
        sentence_count += max(1, len(sentences))
        tokens.extend(tokenize(content))
        if str(row.get("reply_to") or ""):
            reply_lengths.append(len(content))
    count = len(contents)
    if count == 0:
        return {key: 0.0 for key in METRIC_KEYS}
    lengths = [len(item) for item in contents]
    span = (max(stamps) - min(stamps)) / 60.0 if len(stamps) > 1 else 0.0
    return {
        "avg_length": round(statistics.fmean(lengths), 4),
        "median_length": float(statistics.median(lengths)),
        "max_length": float(max(lengths)),
        "avg_sentence_length": round(char_count / max(1, sentence_count), 4),
        "sentences_per_message": round(sentence_count / count, 4),
        "filler_density": round(filler_count / max(1, char_count), 4),
        "emoji_density": round(emoji_count / count, 4),
        "face_density": round(face_count / count, 4),
        "punct_density": round(punct_count / max(1, char_count), 4),
        "exclaim_rate": round(exclaim / count, 4),
        "question_rate": round(question / count, 4),
        "reply_ratio": round(len(reply_lengths) / count, 4),
        "avg_reply_length": round(statistics.fmean(reply_lengths), 4) if reply_lengths else 0.0,
        "messages_per_minute": round(count / span, 4) if span > 0 else float(count),
        "vocabulary_richness": round(len(set(tokens)) / max(1, len(tokens)), 4),
        "char_count": float(char_count),
    }


def style_tags(metrics: Mapping[str, Any], temper: Mapping[str, Any] | None = None) -> list[str]:
    """把指标翻译成人话标签。"""

    tags: list[str] = []
    avg_length = float(metrics.get("avg_length") or 0.0)
    if 0 < avg_length <= 8:
        tags.append("简短")
    elif avg_length >= 30:
        tags.append("话多")
    if float(metrics.get("emoji_density") or 0.0) >= 0.3:
        tags.append("爱用表情")
    if float(metrics.get("face_density") or 0.0) >= 0.3:
        tags.append("爱用表情包")
    if float(metrics.get("filler_density") or 0.0) >= 0.04:
        tags.append("语气词多")
    if float(metrics.get("exclaim_rate") or 0.0) >= 0.2:
        tags.append("爱感叹")
    if float(metrics.get("question_rate") or 0.0) >= 0.2:
        tags.append("爱提问")
    if float(metrics.get("vocabulary_richness") or 0.0) >= 0.7:
        tags.append("用词丰富")
    label = str((temper or {}).get("label") or "")
    if label:
        tags.append(label)
    return tags


def summarize(portrait: Mapping[str, Any]) -> str:
    """画像 → 一段中文描述（给模型/人看）。"""

    metrics = portrait.get("metrics") or {}
    temper = portrait.get("temper") or {}
    lexicon = portrait.get("lexicon") or {}
    catchphrases = list(lexicon.get("catchphrases") or [])[:2]
    nickname = str(portrait.get("nickname") or portrait.get("user_id") or "")
    parts = [
        f"{nickname}：均长 {float(metrics.get('avg_length') or 0):.1f} 字",
        f"表情 {float(metrics.get('emoji_density') or 0):.2f}/条",
        f"语气词 {float(metrics.get('filler_density') or 0) * 100:.1f}%",
    ]
    if temper.get("label"):
        parts.append(f"语气{temper['label']}")
    if catchphrases:
        parts.append("口头禅「" + "」「".join(str(item) for item in catchphrases) + "」")
    return "，".join(parts) + "。"


def _as_messages(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, Mapping):
        rows = response.get("messages") or []
    elif isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        rows = list(response)
    else:  # pragma: no cover - 防御式
        rows = []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


@dataclass
class StyleMetrics:
    """风格指标计算器 + 画像缓存（设计：``grouppig.social.speech.profiler.style-metrics``）。"""

    ctx: SocialContext
    ttl: float = DEFAULT_TTL
    default_limit: int = 300
    _cache: dict[tuple[int, int], dict[str, Any]] = field(default_factory=dict, init=False, repr=False)
    builds: int = field(default=0, init=False)
    hits: int = field(default=0, init=False)

    # ---- 建画像 --------------------------------------------------------
    async def profile(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        since: float | None = None,
        limit: int | None = None,
        use_lexicon: bool = True,
        use_temper: bool = True,
    ) -> dict[str, Any]:
        """建立（并缓存）说话画像。"""

        if user_id is None:
            raise ValueError("rpc:speech.profile 需要 user_id")
        user_id = int(user_id)
        rows = await self._collect(
            user_id=user_id,
            group_id=group_id,
            messages=messages,
            since=since,
            limit=self.default_limit if limit is None else int(limit),
        )
        stored = await self._stored_style(user_id)
        lexicon = (
            await self.ctx.call(DEP_SPEECH_LEXICON, user_id, group_id=int(group_id or 0), messages=rows)
            if use_lexicon
            else {}
        )
        temper = (
            await self.ctx.call(DEP_SPEECH_TEMPER, user_id, group_id=int(group_id or 0), messages=rows)
            if use_temper
            else {}
        )
        metrics = compute_metrics(rows)
        portrait: dict[str, Any] = {
            "user_id": user_id,
            "group_id": int(group_id or 0),
            "nickname": str((stored or {}).get("nickname") or ""),
            "version": int((stored or {}).get("version") or 0) + 1,
            "sample_size": len(rows),
            "built_at": self.ctx.now(),
            "metrics": metrics,
            "lexicon": {
                "catchphrases": list((lexicon or {}).get("catchphrases") or []),
                "top_words": list((lexicon or {}).get("top_words") or []),
                "emoji": (lexicon or {}).get("emoji") or {},
                "fillers": (lexicon or {}).get("fillers") or {},
            },
            "temper": {
                "warmth": float((temper or {}).get("warmth") or 0.0),
                "aggressiveness": float((temper or {}).get("aggressiveness") or 0.0),
                "intimacy": float((temper or {}).get("intimacy") or 0.0),
                "label": str((temper or {}).get("label") or ""),
            },
            "tags": style_tags(metrics, temper),
        }
        portrait["summary"] = summarize(portrait)
        self._cache[(user_id, int(group_id or 0))] = portrait
        self.builds += 1
        self.ctx.log("debug", "speech.profile.built", user_id=user_id, sample=len(rows))
        return {
            "user_id": user_id,
            "group_id": int(group_id or 0),
            "portrait": portrait,
            "metrics": metrics,
            "tags": portrait["tags"],
            "summary": portrait["summary"],
            "sample_size": len(rows),
            "source": "built",
            "stored_style": stored,
        }

    # ---- 查风格 --------------------------------------------------------
    async def style(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        build: bool = True,
        refresh: bool = False,
        max_age: float | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """查询说话画像（缓存 → 档案 ``speaking_style`` → 现算）。"""

        if user_id is None:
            raise ValueError("rpc:speech.style 需要 user_id")
        user_id = int(user_id)
        key = (user_id, int(group_id or 0))
        age_limit = self.ttl if max_age is None else float(max_age)
        if not refresh:
            cached = self._cache.get(key)
            if cached is not None and (self.ctx.now() - float(cached.get("built_at") or 0.0)) <= age_limit:
                self.hits += 1
                return self._view(cached, source="cache", found=True)
        stored = await self._stored_style(user_id)
        if stored:
            portrait = self._portrait_from_stored(user_id, int(group_id or 0), stored)
            self._cache[key] = portrait
            return self._view(portrait, source="profile", found=True)
        if not build:
            return {
                "user_id": user_id,
                "group_id": int(group_id or 0),
                "found": False,
                "source": "none",
                "portrait": None,
                "metrics": {},
                "tags": [],
                "summary": "",
            }
        built = await self.profile(user_id, group_id=group_id, limit=limit)
        return self._view(built["portrait"], source="built", found=True)

    # ---- 内部 ----------------------------------------------------------
    @staticmethod
    def _view(portrait: Mapping[str, Any], *, source: str, found: bool) -> dict[str, Any]:
        return {
            "user_id": int(portrait.get("user_id") or 0),
            "group_id": int(portrait.get("group_id") or 0),
            "found": found,
            "source": source,
            "portrait": dict(portrait),
            "metrics": dict(portrait.get("metrics") or {}),
            "tags": list(portrait.get("tags") or []),
            "summary": str(portrait.get("summary") or ""),
        }

    def _portrait_from_stored(self, user_id: int, group_id: int, stored: Mapping[str, Any]) -> dict[str, Any]:
        style = stored.get("speaking_style")
        style = dict(style) if isinstance(style, Mapping) else {}
        metrics = {key: float((style.get("metrics") or {}).get(key) or 0.0) for key in METRIC_KEYS}
        temper = dict(style.get("temper") or {})
        lexicon = dict(style.get("lexicon") or {})
        portrait: dict[str, Any] = {
            "user_id": user_id,
            "group_id": group_id,
            "nickname": str(stored.get("nickname") or ""),
            "version": int(stored.get("version") or 0),
            "sample_size": int(style.get("sample_size") or 0),
            "built_at": float(style.get("built_at") or self.ctx.now()),
            "metrics": metrics,
            "lexicon": lexicon,
            "temper": temper,
            "tags": list(style.get("tags") or style_tags(metrics, temper)),
        }
        portrait["summary"] = str(style.get("summary") or summarize(portrait))
        return portrait

    async def _stored_style(self, user_id: int) -> dict[str, Any] | None:
        try:
            response = await self.ctx.call(DEP_PROFILE_GET, user_id, with_facts=False)
        except Exception as error:  # pragma: no cover - 无档案时退化为空
            self.ctx.log("warning", "style_metrics.read_profile_failed", user_id=user_id, error=repr(error))
            return None
        profile = (response or {}).get("profile") if isinstance(response, Mapping) else None
        return dict(profile) if isinstance(profile, Mapping) else None

    async def _collect(
        self,
        *,
        user_id: int,
        group_id: int,
        messages: Sequence[Mapping[str, Any]] | None,
        since: float | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if messages is not None:
            return [dict(row) for row in messages if isinstance(row, Mapping)]
        criteria: dict[str, Any] = {"sender_id": user_id, "limit": max(1, int(limit)), "order": "asc"}
        if group_id:
            criteria["group_id"] = int(group_id)
        if since is not None:
            criteria["since"] = float(since)
        return _as_messages(await self.ctx.call(DEP_CHAT_QUERY, criteria))

    def invalidate(self, user_id: int | None = None, *, group_id: int | None = None) -> int:
        """清缓存（``user_id=None`` 清全部）。"""

        if user_id is None:
            removed = len(self._cache)
            self._cache.clear()
            return removed
        keys = [key for key in self._cache if key[0] == int(user_id) and (group_id is None or key[1] == int(group_id))]
        for key in keys:
            self._cache.pop(key, None)
        return len(keys)

    def status(self) -> dict[str, Any]:
        return {"cached": len(self._cache), "builds": self.builds, "hits": self.hits, "ttl": self.ttl}


def make_handlers(ctx: SocialContext, metrics: StyleMetrics) -> dict[str, Any]:
    """``rpc:speech.profile`` / ``rpc:speech.style`` 处理器。"""

    async def speech_profile(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        since: float | None = None,
        limit: int | None = None,
        use_lexicon: bool = True,
        use_temper: bool = True,
    ) -> dict[str, Any]:
        return await metrics.profile(
            user_id,
            group_id=group_id,
            messages=messages,
            since=since,
            limit=limit,
            use_lexicon=use_lexicon,
            use_temper=use_temper,
        )

    async def speech_style(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        build: bool = True,
        refresh: bool = False,
        max_age: float | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        return await metrics.style(
            user_id, group_id=group_id, build=build, refresh=refresh, max_age=max_age, limit=limit
        )

    return {RPC_PROFILE: speech_profile, RPC_STYLE: speech_style}


def register(registry: Any, metrics: StyleMetrics, *, replace: bool = True) -> None:
    for name, handler in make_handlers(metrics.ctx, metrics).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "DEFAULT_TTL",
    "DEP_CHAT_QUERY",
    "DEP_PROFILE_GET",
    "DEP_SPEECH_LEXICON",
    "DEP_SPEECH_TEMPER",
    "METRIC_KEYS",
    "MODULE_ID",
    "RPC_NAMES",
    "RPC_PROFILE",
    "RPC_STYLE",
    "StyleMetrics",
    "compute_metrics",
    "make_handlers",
    "register",
    "style_tags",
    "summarize",
]
