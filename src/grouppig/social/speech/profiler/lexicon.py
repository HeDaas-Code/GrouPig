"""grouppig.social.speech.profiler.lexicon —— 口头禅统计器（``rpc:speech.lexicon``）。

职责（设计：``grouppig.social.speech.profiler.lexicon``「统计群友高频词、口头禅与表情偏好」）：

* 读历史消息：``rpc:speech.lexicon`` → ``rpc:chat.query``（设计依赖，逐字对齐）；
* 中文不引 jieba：用「2/3/4 元组 + 极大频繁串选择」抽高频词与口头禅（离线、确定性）；
* 统计表情偏好：Unicode emoji、颜文字、QQ 表情码 ``[微笑]``；
* 统计语气词密度（:data:`FILLERS`）。

本模块同时是 speech 子域**文本词表的唯一出处**：:data:`FILLERS` / :data:`EMOJI_RE` /
:data:`FACE_CODE_RE` / :func:`tokenize` / :func:`ngram_counts` 被 ``temper`` 与 ``style-metrics``
复用（纯函数与常量，不构成契约依赖）。

设计：``grouppig.social.speech.profiler.lexicon``（叶子模块）。
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.social import SocialContext

MODULE_ID = "grouppig.social.speech.profiler.lexicon"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
RPC_NAMES: tuple[str, ...] = ("rpc:speech.lexicon",)
RPC_LEXICON = "rpc:speech.lexicon"
contract.assert_known_name(RPC_LEXICON)

#: 设计依赖（逐字对齐 lexicon.md 的 deps）。
DEP_CHAT_QUERY = "rpc:chat.query"
contract.assert_known_name(DEP_CHAT_QUERY)

#: 语气词 / 口头禅候选（也用于 filler 密度统计）。
FILLERS: tuple[str, ...] = (
    "啊",
    "呀",
    "吧",
    "嘛",
    "啦",
    "哦",
    "噢",
    "嗯",
    "诶",
    "哎",
    "哈",
    "嘿",
    "喂",
    "嘞",
    "咯",
    "哟",
    "唉",
    "呜",
    "哇",
    "嘻",
    "呗",
    "咧",
    "捏",
    "滴",
    "哒",
    "呐",
)

#: 高频词黑名单（对话里几乎人人都在用，没有区分度）。
STOP_GRAMS: frozenset[str] = frozenset(
    {
        "我们",
        "你们",
        "他们",
        "她们",
        "什么",
        "怎么",
        "这个",
        "那个",
        "就是",
        "可以",
        "不是",
        "没有",
        "一个",
        "现在",
        "时候",
        "自己",
        "知道",
        "觉得",
        "应该",
        "如果",
        "因为",
        "所以",
        "但是",
        "然后",
        "还是",
        "已经",
        "一下",
        "这样",
        "那样",
        "起来",
        "出来",
        "真的",
        "好像",
        "可能",
        "感觉",
        "有点",
        "要不",
        "还有",
        "而且",
        "不过",
        "虽然",
        "只是",
        "其实",
        "今天",
        "明天",
        "昨天",
        "晚上",
        "早上",
        "中午",
    }
)

#: CJK 连续串。
CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]+")
#: 英文 / 数字词。
ASCII_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_'-]*|\d{2,}")
#: emoji 区段（符号 + 表情）。
EMOJI_RE = re.compile(
    "[\U0001f300-\U0001faff\U0001f000-\U0001f2ff\u2600-\u27bf\u2b00-\u2bff\u2190-\u21ff\u2900-\u297f]+"
)
#: QQ 表情码 ``[微笑]`` / ``[doge]``。
FACE_CODE_RE = re.compile(r"\[[^\[\]\s]{1,10}\]")
#: 颜文字 ``(╯°□°）╯`` 之类（保守：括号内 1~8 个非汉字字符）。
KAOMOJI_RE = re.compile(r"[（(][^\u4e00-\u9fff\s]{1,8}[）)]")
#: 标点（用于切句与密度统计）。
PUNCT_RE = re.compile(r"[，。！？、；：\u201c\u201d\u2018\u2019（）【】《》…—～·,.!?;:()\[\]<>\"']")
#: 句末标点（切句用）。
SENTENCE_SPLIT_RE = re.compile(r"[。！？!?；;\n]+")


def tokenize(text: str) -> list[str]:
    """粗粒度分词：CJK 连续串按字、英文/数字按词（供 n-gram 与词表统计）。"""

    tokens: list[str] = []
    for run in CJK_RUN_RE.findall(str(text or "")):
        tokens.extend(run)
    tokens.extend(ASCII_WORD_RE.findall(str(text or "")))
    return tokens


def cjk_runs(text: str) -> list[str]:
    return CJK_RUN_RE.findall(str(text or ""))


def ngram_counts(text: str, sizes: Sequence[int] = (2, 3, 4)) -> Counter[str]:
    """CJK 串的 n-gram 计数（英文/数字词原样计入）。"""

    counts: Counter[str] = Counter()
    for run in cjk_runs(text):
        for size in sizes:
            if len(run) < size:
                continue
            for start in range(len(run) - size + 1):
                counts[run[start : start + size]] += 1
    for word in ASCII_WORD_RE.findall(str(text or "")):
        if len(word) >= 2:
            counts[word.lower()] += 1
    return counts


def select_phrases(counts: Counter[str], *, min_count: int = 3, top: int = 10) -> list[dict[str, Any]]:
    """从 n-gram 计数里选「极大频繁串」：先长后短，被更长串包含的短串丢弃。"""

    candidates = [(gram, count) for gram, count in counts.items() if count >= min_count and gram not in STOP_GRAMS]
    candidates.sort(key=lambda item: (-len(item[0]), -item[1], item[0]))
    selected: list[tuple[str, int]] = []
    for gram, count in candidates:
        if any(gram in chosen and count <= chosen_count for chosen, chosen_count in selected):
            continue
        selected.append((gram, count))
    selected.sort(key=lambda item: (-item[1], -len(item[0]), item[0]))
    return [{"phrase": gram, "count": count} for gram, count in selected[: max(1, int(top))]]


def count_fillers(text: str) -> Counter[str]:
    """语气词计数。"""

    counts: Counter[str] = Counter()
    for char in str(text or ""):
        if char in FILLERS:
            counts[char] += 1
    return counts


def count_emoji(text: str) -> Counter[str]:
    """emoji 计数（逐字形统计，连续表情算多个）。"""

    counts: Counter[str] = Counter()
    for match in EMOJI_RE.finditer(str(text or "")):
        counts.update(match.group(0))
    return counts


def count_faces(text: str) -> Counter[str]:
    """QQ 表情码 + 颜文字计数。"""

    counts: Counter[str] = Counter()
    for regex in (FACE_CODE_RE, KAOMOJI_RE):
        for match in regex.finditer(str(text or "")):
            counts[match.group(0)] += 1
    return counts


def _as_messages(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, Mapping):
        rows = response.get("messages") or []
    elif isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        rows = list(response)
    else:  # pragma: no cover - 防御式
        rows = []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


@dataclass
class LexiconCounter:
    """口头禅统计器（设计：``grouppig.social.speech.profiler.lexicon``）。"""

    ctx: SocialContext
    min_count: int = 3
    top: int = 10
    calls: int = field(default=0, init=False)

    async def lexicon(
        self,
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 300,
        min_count: int | None = None,
        top: int | None = None,
    ) -> dict[str, Any]:
        """统计高频词 / 口头禅 / 表情偏好。"""

        rows = await self._collect(
            user_id=user_id, group_id=group_id, messages=messages, since=since, until=until, limit=limit
        )
        return self.analyze(
            rows,
            user_id=user_id,
            group_id=group_id,
            min_count=self.min_count if min_count is None else int(min_count),
            top=self.top if top is None else int(top),
        )

    def analyze(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        user_id: int | None = None,
        group_id: int = 0,
        min_count: int | None = None,
        top: int | None = None,
    ) -> dict[str, Any]:
        """纯统计（不读库，便于单测）。"""

        threshold = self.min_count if min_count is None else int(min_count)
        limit = self.top if top is None else int(top)
        grams: Counter[str] = Counter()
        fillers: Counter[str] = Counter()
        emoji: Counter[str] = Counter()
        faces: Counter[str] = Counter()
        char_count = 0
        message_count = 0
        for row in messages:
            sender = int(row.get("sender_id") or 0)
            if user_id is not None and sender != int(user_id):
                continue
            content = str(row.get("content") or "")
            if not content:
                continue
            message_count += 1
            char_count += len(content)
            grams.update(ngram_counts(content))
            fillers.update(count_fillers(content))
            emoji.update(count_emoji(content))
            faces.update(count_faces(content))
        phrases = select_phrases(grams, min_count=threshold, top=limit)
        filler_total = sum(fillers.values())
        emoji_total = sum(emoji.values())
        face_total = sum(faces.values())
        return {
            "user_id": int(user_id or 0),
            "group_id": int(group_id or 0),
            "message_count": message_count,
            "char_count": char_count,
            "top_words": phrases,
            "catchphrases": [item["phrase"] for item in phrases if len(item["phrase"]) >= 2][:limit],
            "emoji": {
                "count": emoji_total,
                "density": round(emoji_total / message_count, 4) if message_count else 0.0,
                "top": [{"emoji": key, "count": value} for key, value in emoji.most_common(limit)],
            },
            "faces": {
                "count": face_total,
                "density": round(face_total / message_count, 4) if message_count else 0.0,
                "top": [{"face": key, "count": value} for key, value in faces.most_common(limit)],
            },
            "fillers": {
                "count": filler_total,
                "density": round(filler_total / char_count, 4) if char_count else 0.0,
                "top": [{"filler": key, "count": value} for key, value in fillers.most_common(limit)],
            },
            "lexicon": {
                "words": {item["phrase"]: item["count"] for item in phrases},
                "catchphrases": [item["phrase"] for item in phrases[: min(5, limit)]],
                "emoji": [key for key, _ in emoji.most_common(5)],
                "fillers": [key for key, _ in fillers.most_common(5)],
            },
        }

    async def _collect(
        self,
        *,
        user_id: int | None,
        group_id: int,
        messages: Sequence[Mapping[str, Any]] | None,
        since: float | None,
        until: float | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if messages is not None:
            return [dict(row) for row in messages if isinstance(row, Mapping)]
        criteria: dict[str, Any] = {"limit": max(1, int(limit)), "order": "asc"}
        if group_id:
            criteria["group_id"] = int(group_id)
        if user_id:
            criteria["sender_id"] = int(user_id)
        if since is not None:
            criteria["since"] = float(since)
        if until is not None:
            criteria["until"] = float(until)
        self.calls += 1
        return _as_messages(await self.ctx.call(DEP_CHAT_QUERY, criteria))

    def status(self) -> dict[str, Any]:
        return {"calls": self.calls, "min_count": self.min_count, "top": self.top}


def make_handlers(ctx: SocialContext, counter: LexiconCounter) -> dict[str, Any]:
    """``rpc:speech.lexicon`` 处理器。"""

    async def speech_lexicon(
        user_id: int | None = None,
        *,
        group_id: int = 0,
        messages: Sequence[Mapping[str, Any]] | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 300,
        min_count: int | None = None,
        top: int | None = None,
    ) -> dict[str, Any]:
        return await counter.lexicon(
            user_id,
            group_id=group_id,
            messages=messages,
            since=since,
            until=until,
            limit=limit,
            min_count=min_count,
            top=top,
        )

    return {RPC_LEXICON: speech_lexicon}


def register(registry: Any, counter: LexiconCounter, *, replace: bool = True) -> None:
    for name, handler in make_handlers(counter.ctx, counter).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)


__all__ = [
    "ASCII_WORD_RE",
    "CJK_RUN_RE",
    "DEP_CHAT_QUERY",
    "EMOJI_RE",
    "FACE_CODE_RE",
    "FILLERS",
    "KAOMOJI_RE",
    "LexiconCounter",
    "MODULE_ID",
    "PUNCT_RE",
    "RPC_LEXICON",
    "RPC_NAMES",
    "SENTENCE_SPLIT_RE",
    "STOP_GRAMS",
    "cjk_runs",
    "count_emoji",
    "count_faces",
    "count_fillers",
    "make_handlers",
    "ngram_counts",
    "register",
    "select_phrases",
    "tokenize",
]
