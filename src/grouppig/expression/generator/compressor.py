"""grouppig.expression.generator.compressor —— 提示词压缩器（``rpc:generator.compress``）。

职责（设计：``grouppig.expression.generator.compressor``「按预算压缩提示词：裁剪旧消息、摘要化聊天线、去冗余」）：

* 读聊天线：``rpc:generator.compress`` → ``rpc:thread.load``（设计依赖，逐字对齐）；
* 三段式压缩（**确定性、可回放**，同输入必同输出）:

  1. :func:`dedup_messages` —— 去冗余：同人多条同义消息合并、重复串压缩
     （「哈哈哈哈」保留人味但砍掉刷屏）、去掉纯表情刷屏里的冗余条目；
  2. :func:`summarize_threads` —— 摘要化聊天线：把每条线压成一行
     「标题｜关键词｜参与者数｜消息数｜最后活跃」，而不是把线上消息全塞进上下文；
  3. :func:`trim_messages` —— 裁剪旧消息：保留最近 N 条 + 被我方提及/回复的关键条目，
     其余按 :data:`DROP_DECAYS` 逐级丢弃（先丢最旧的无人理会的短消息）。

``rpc:generator.compress`` 按 ``budget_tokens`` 循环「去冗余 → 摘要化 → 裁剪」，
直到估算 token 落在预算内；实在压不下去就返回 ``truncated=True`` 并把最旧的整段丢掉，
让调用方（:mod:`grouppig.expression.generator.context`）能决定是否降级话题。

token 估算复用 :func:`grouppig.infra.token_budget.policy.estimate_tokens`（与预算器同一口径），
避免「压缩器以为够了、预算器却夹取」的两套尺子。

设计：``grouppig.expression.generator.compressor``（叶子模块）。
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract
from grouppig.infra.token_budget.policy import estimate_messages_tokens, estimate_tokens

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.generator.compressor"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:generator.compress",)
RPC_COMPRESS = "rpc:generator.compress"
contract.assert_known_name(RPC_COMPRESS)

#: 设计依赖（逐字对齐 compressor.md 的 deps）。
DEP_THREAD_LOAD = "rpc:thread.load"
contract.assert_known_name(DEP_THREAD_LOAD)

#: 默认输入预算（与 ``[token.policies.chat]`` 的 max_input_tokens 同量级）。
DEFAULT_BUDGET_TOKENS = 1600

#: 一条消息至少保留多少字（裁剪时不再切到比这更短）。
MIN_MESSAGE_CHARS = 2

#: 连续重复字符的压缩上限（保留「哈哈哈哈」的人味）。
MAX_REPEAT = 4

#: 单条消息裁断上限（超长消息先自身截断，避免一条吃掉整个预算）。
MAX_MESSAGE_CHARS = 200

#: 摘要化的聊天线最多保留几条。
DEFAULT_MAX_THREADS = 8

#: 裁剪时至少保留的最近消息条数（低于这个数就不再往下砍）。
MIN_KEEP = 4

#: 重要消息的标记（被 @ / 回复 / 我方消息 / 含问号），裁剪时优先保留。
IMPORTANT_WEIGHT = 3.0

#: 每轮压缩至少丢掉的条数（保证循环收敛）。
MIN_DROP_PER_ROUND = 1

#: 重复字符正则。
_REPEAT_RE = re.compile(r"(.)\1{" + str(MAX_REPEAT) + ",}")

#: 纯表情 / 纯符号消息正则（用于判定「可丢」）。
_EMPTYISH_RE = re.compile(r"^[\s\W_]+$", re.UNICODE)

#: 句末标点（超长消息按句读裁断）。
_BREAK_CHARS = "。！？!?；;，,、"


def compact_repeats(text: str, *, limit: int = MAX_REPEAT) -> str:
    """把连续重复 ``limit`` 次以上的字符压到 ``limit`` 次。"""

    return _REPEAT_RE.sub(lambda match: match.group(1) * limit, str(text or ""))


def normalize_text(text: Any, *, max_chars: int = MAX_MESSAGE_CHARS) -> str:
    """规整单条消息文本：压空白 → 压重复 → 超长按句读截断。"""

    content = " ".join(str(text or "").split())
    content = compact_repeats(content)
    if len(content) <= max_chars:
        return content
    window = content[:max_chars]
    cut = max((window.rfind(char) for char in _BREAK_CHARS), default=-1)
    return (window[: cut + 1] if cut >= max(4, max_chars // 3) else window).rstrip(" ，,、；;")


def is_important(row: Mapping[str, Any], *, self_id: int = 0) -> bool:
    """这条消息是否值得在压缩时优先保留。"""

    if int(row.get("sender_id", 0) or 0) == int(self_id or 0) and self_id:
        return True
    if row.get("reply_to") or row.get("mentions"):
        return True
    content = str(row.get("content") or "")
    return "?" in content or "？" in content


def message_weight(row: Mapping[str, Any], *, self_id: int = 0, index: int = 0) -> float:
    """消息的保留权重（越大越该留）：重要度 + 位置（越新越大）+ 内容长度。"""

    weight = 1.0 + (IMPORTANT_WEIGHT if is_important(row, self_id=self_id) else 0.0)
    weight += min(1.0, len(str(row.get("content") or "")) / 40.0)
    weight += index * 0.05
    return weight


def dedup_messages(
    rows: Sequence[Mapping[str, Any]],
    *,
    window: int = 3,
) -> list[dict[str, Any]]:
    """去冗余：把「同一个人在 ``window`` 条内连发的近似消息」合并成一条。

    近似判定：规整后的文本相同，或前 12 个字符相同（连续刷同一句的典型形态）。
    """

    out: list[dict[str, Any]] = []
    seen: list[tuple[int, str]] = []
    for row in rows:
        item = dict(row)
        item["content"] = normalize_text(row.get("content"))
        key = (int(item.get("sender_id", 0) or 0), item["content"][:12])
        if item["content"] and key in seen[-window:]:
            # 同一人短窗口内的重复：并进上一条（保留最早的时间，累加计数）
            for existing in reversed(out):
                if int(existing.get("sender_id", 0) or 0) != key[0]:
                    continue
                if existing["content"][:12] != key[1]:
                    continue
                existing["merged"] = int(existing.get("merged", 1)) + 1
                break
            continue
        item["merged"] = 1
        out.append(item)
        seen.append(key)
    return out


def summarize_threads(
    threads: Sequence[Mapping[str, Any]] | None,
    *,
    max_threads: int = DEFAULT_MAX_THREADS,
) -> dict[str, Any]:
    """摘要化聊天线：每条线压成一行，按最后活跃时间倒序。"""

    items: list[dict[str, Any]] = []
    for thread in threads or ():
        row = dict(thread)
        thread_id = str(row.get("thread_id") or "")
        title = str(row.get("title") or "").strip()
        keywords = [str(item) for item in (row.get("keywords") or ()) if str(item).strip()][:6]
        line = "｜".join(
            part
            for part in (
                title or thread_id or "(未命名)",
                "、".join(keywords) if keywords else "",
                f"{len(row.get('participants') or ())}人",
                f"{int(row.get('message_count') or 0)}条",
            )
            if part
        )
        items.append(
            {
                "thread_id": thread_id,
                "title": title,
                "summary": normalize_text(row.get("summary"), max_chars=80),
                "keywords": keywords,
                "participants": [int(item) for item in (row.get("participants") or ())],
                "message_count": int(row.get("message_count") or 0),
                "last_ts": float(row.get("last_ts") or 0.0),
                "status": str(row.get("status") or "open"),
                "line": line,
            }
        )
    items.sort(key=lambda item: (-item["last_ts"], item["thread_id"]))
    kept = items[: max(1, int(max_threads))]
    return {
        "threads": kept,
        "dropped": max(0, len(items) - len(kept)),
        "count": len(kept),
        "total": len(items),
        "text": "\n".join(item["line"] for item in kept),
    }


def trim_messages(
    rows: Sequence[Mapping[str, Any]],
    *,
    budget_tokens: int,
    self_id: int = 0,
    min_keep: int = MIN_KEEP,
    drop: int = MIN_DROP_PER_ROUND,
) -> dict[str, Any]:
    """裁剪旧消息直到估算 token 进预算：先丢最旧的低权重条目。"""

    items = [dict(row) for row in rows]
    rounds = 0
    dropped: list[dict[str, Any]] = []
    while estimate_messages_tokens(_as_model_messages(items)) > budget_tokens and len(items) > min_keep:
        ranked = sorted(
            range(len(items)),
            key=lambda index: (message_weight(items[index], self_id=self_id, index=index), items[index].get("ts") or 0),
        )
        removed = 0
        for index in sorted(ranked[: max(MIN_DROP_PER_ROUND, int(drop))], reverse=True):
            dropped.append(items.pop(index))
            removed += 1
        rounds += 1
        if not removed:  # pragma: no cover - 防御：没有可丢的条目
            break
    return {
        "messages": items,
        "dropped": dropped,
        "dropped_count": len(dropped),
        "rounds": rounds,
        "truncated": bool(dropped) and estimate_messages_tokens(_as_model_messages(items)) > budget_tokens,
    }


def _as_model_messages(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """把聊天流水行投影成「角色 + 内容」形态，供 ``estimate_messages_tokens`` 估算。"""

    return [{"role": "user", "content": str(row.get("content") or "")} for row in rows]


def compress(
    messages: Sequence[Mapping[str, Any]] | None = None,
    *,
    threads: Sequence[Mapping[str, Any]] | None = None,
    budget_tokens: int = DEFAULT_BUDGET_TOKENS,
    self_id: int = 0,
    max_threads: int = DEFAULT_MAX_THREADS,
    min_keep: int = MIN_KEEP,
) -> dict[str, Any]:
    """纯函数压缩：去冗余 → 摘要化聊天线 → 裁剪旧消息（三段全部可回放）。"""

    original = [dict(row) for row in (messages or ())]
    deduped = dedup_messages(original)
    thread_summary = summarize_threads(threads, max_threads=max_threads)
    before = estimate_messages_tokens(_as_model_messages(original))
    trimmed = trim_messages(deduped, budget_tokens=int(budget_tokens), self_id=self_id, min_keep=min_keep)
    after = estimate_messages_tokens(_as_model_messages(trimmed["messages"]))
    after += estimate_tokens(thread_summary["text"])
    return {
        "messages": trimmed["messages"],
        "threads": thread_summary["threads"],
        "thread_summary": thread_summary["text"],
        "budget_tokens": int(budget_tokens),
        "tokens_before": before,
        "tokens_after": after,
        "saved_tokens": max(0, before - after),
        "ratio": round(after / before, 4) if before else 1.0,
        "deduped": len(original) - len(deduped),
        "dropped": trimmed["dropped_count"],
        "rounds": trimmed["rounds"],
        "truncated": bool(trimmed["truncated"]),
        "within_budget": after <= int(budget_tokens),
    }


@dataclass
class PromptCompressor:
    """提示词压缩器（设计：``grouppig.expression.generator.compressor``）。"""

    ctx: ExpressionContext | None = None
    budget_tokens: int = DEFAULT_BUDGET_TOKENS
    max_threads: int = DEFAULT_MAX_THREADS
    self_id: int = 0
    min_keep: int = MIN_KEEP
    compressions: int = field(default=0, init=False)
    saved_tokens: int = field(default=0, init=False)

    async def load_threads(self, **kwargs: Any) -> list[dict[str, Any]]:
        """读聊天线（设计依赖：``rpc:generator.compress`` → ``rpc:thread.load``）。"""

        if self.ctx is None or not callable(getattr(self.ctx, "call", None)):
            raise RuntimeError(f"{MODULE_ID} 需要 ctx.call 才能调用 {DEP_THREAD_LOAD}")
        result = await self.ctx.call(DEP_THREAD_LOAD, **kwargs)
        if isinstance(result, Mapping):
            return [dict(item) for item in (result.get("threads") or ())]
        return [dict(item) for item in (result or ())]

    def compress(
        self,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        threads: Sequence[Mapping[str, Any]] | None = None,
        budget_tokens: int | None = None,
        max_threads: int | None = None,
        min_keep: int | None = None,
    ) -> dict[str, Any]:
        """同步压缩（``threads`` 已给时无需读库）。"""

        result = compress(
            messages,
            threads=threads,
            budget_tokens=int(budget_tokens if budget_tokens is not None else self.budget_tokens),
            self_id=self.self_id,
            max_threads=int(max_threads if max_threads is not None else self.max_threads),
            min_keep=int(min_keep if min_keep is not None else self.min_keep),
        )
        self.compressions += 1
        self.saved_tokens += int(result["saved_tokens"])
        return result

    async def run(
        self,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        threads: Sequence[Mapping[str, Any]] | None = None,
        budget_tokens: int | None = None,
        session_id: str | None = None,
        group_id: int | None = None,
        thread_limit: int = 20,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """``rpc:generator.compress`` —— 压缩提示词以省 token（缺 ``threads`` 时按设计依赖读库）。"""

        if threads is None:
            try:
                threads = await self.load_threads(
                    session_id=session_id, group_id=group_id, limit=thread_limit, **kwargs
                )
            except Exception as error:  # 下游缺失时压缩仍应可用（截断而不是崩）
                threads = []
                self._log("debug", "compressor.threads_unavailable", error=str(error))
        result = self.compress(messages, threads=threads, budget_tokens=budget_tokens)
        result["sources"] = {"threads": len(threads or ()), "session_id": str(session_id or "")}
        self._log(
            "debug",
            "compressor.done",
            tokens_before=result["tokens_before"],
            tokens_after=result["tokens_after"],
            saved=result["saved_tokens"],
            truncated=result["truncated"],
        )
        return result

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "compressions": self.compressions,
            "saved_tokens": self.saved_tokens,
            "budget_tokens": self.budget_tokens,
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)


def make_handlers(compressor: PromptCompressor) -> dict[str, Any]:
    """``rpc:generator.compress`` 处理器。"""

    async def generator_compress(
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        threads: Sequence[Mapping[str, Any]] | None = None,
        budget_tokens: int | None = None,
        session_id: str | None = None,
        group_id: int | None = None,
        thread_limit: int = 20,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await compressor.run(
            messages,
            threads=threads,
            budget_tokens=budget_tokens,
            session_id=session_id,
            group_id=group_id,
            thread_limit=thread_limit,
            **kwargs,
        )

    return {RPC_COMPRESS: generator_compress}


def register(registry: Any, compressor: PromptCompressor | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = compressor if compressor is not None else PromptCompressor()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "DEFAULT_BUDGET_TOKENS",
    "DEFAULT_MAX_THREADS",
    "DEP_THREAD_LOAD",
    "IMPORTANT_WEIGHT",
    "MAX_MESSAGE_CHARS",
    "MAX_REPEAT",
    "MIN_KEEP",
    "MIN_MESSAGE_CHARS",
    "MODULE_ID",
    "NAMES",
    "RPC_COMPRESS",
    "PromptCompressor",
    "compact_repeats",
    "compress",
    "dedup_messages",
    "is_important",
    "make_handlers",
    "message_weight",
    "normalize_text",
    "register",
    "summarize_threads",
    "trim_messages",
]
