"""grouppig.expression.generator.context —— 上下文打包器（``rpc:generator.compose``）。

职责（设计：``grouppig.expression.generator.context``
「打包生成上下文：会话摘要、聊天线、档案、画像、唤醒上下文」+「生成最终回复文本」）：

这是表达层的**总装叶子**，设计 frontmatter 给了它 6 条出向依赖，全部逐字实现在此：

* ``rpc:generator.compose`` → ``rpc:generator.compress``（压缩提示词）；
* ``rpc:generator.compose`` → ``rpc:generator.write``（生成文本）；
* ``rpc:generator.compose`` → ``rpc:persona.style``（人设风格）；
* ``rpc:generator.compose`` → ``rpc:identity.deny-ai``（否认 AI）；
* ``rpc:generator.compose`` → ``rpc:slang.inject``（注入黑话）；
* ``rpc:generator.compose`` → ``rpc:token.reserve``（预留预算）。

**上下文拼装顺序**（:data:`BLOCK_ORDER`，稳定且可断言；见 docs/EXPRESSION.md）::

    人设（persona）→ 会话摘要（session）→ 聊天线（threads）→ 听众画像（profile）
    → 关系分（relationship）→ 当前消息（messages）→ 黑话（slang）→ 身份防御（identity）

每一块都是**可选**的：对应下游没挂时不抛异常，只把该块记进返回体的 ``missing`` 并继续——
表达层是闭环的最后一环，不能因为某个上游缺席就整条链路断掉。

两处「补数」是设计外补充（设计未给表达层读库的契约名），一律用**已有的契约名字**而非自造：

* 会话摘要：``rpc:archive.load``（memory 域，按 ``session_id`` 取档案的标题/摘要/关键词）；
* 听众画像：``rpc:profile.get``（social 域，取目标群友的标签/兴趣/说话风格）；
* 关系分/分层：``rpc:relationship.get``（social 域，取 ``tier_label`` / ``score``）；
* 近期消息：``rpc:chat.window``（memory 域，调用方没直接给 ``messages`` 时按群取窗）。

预算闭环：先 ``rpc:token.reserve`` 拿本场景的输入/输出预算 → 用输入预算做压缩 →
把输出预算作为 ``max_tokens`` 传给 ``rpc:generator.write``；``rpc:token.reserve``
不可用时退回配置里的场景策略（:func:`fallback_budget`），并在 ``degraded_paths`` 里记明。

设计：``grouppig.expression.generator.context``（叶子模块）。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from grouppig.infra.runtime import contract

if TYPE_CHECKING:  # pragma: no cover - 仅供类型标注
    from grouppig.expression import ExpressionContext

MODULE_ID = "grouppig.expression.generator.context"

#: 契约 rpc 名字（逐字对齐 api-index.json）。
NAMES: tuple[str, ...] = ("rpc:generator.compose",)
RPC_COMPOSE = "rpc:generator.compose"
contract.assert_known_name(RPC_COMPOSE)

#: 设计依赖（逐字对齐 context.md 的 6 条 deps）。
DEP_COMPRESS = "rpc:generator.compress"
DEP_WRITE = "rpc:generator.write"
DEP_PERSONA_STYLE = "rpc:persona.style"
DEP_IDENTITY_DENY = "rpc:identity.deny-ai"
DEP_SLANG_INJECT = "rpc:slang.inject"
DEP_TOKEN_RESERVE = "rpc:token.reserve"
for _name in (
    DEP_COMPRESS,
    DEP_WRITE,
    DEP_PERSONA_STYLE,
    DEP_IDENTITY_DENY,
    DEP_SLANG_INJECT,
    DEP_TOKEN_RESERVE,
):
    contract.assert_known_name(_name)

#: 契约内补数名字（设计未给表达层读库的入口，用 memory / social 已有的契约名，不自造）。
DEP_ARCHIVE_LOAD = "rpc:archive.load"
DEP_PROFILE_GET = "rpc:profile.get"
DEP_CHAT_WINDOW = "rpc:chat.window"
#: 关系分/分层（social 域）。设计没给表达层这条边，但**没有它语气就没有亲密度维度**：
#: 在接通之前，陌生人与死党拿到逐字相同的上下文，回复自然一模一样。
DEP_RELATIONSHIP_GET = "rpc:relationship.get"
for _name in (DEP_ARCHIVE_LOAD, DEP_PROFILE_GET, DEP_CHAT_WINDOW, DEP_RELATIONSHIP_GET):
    contract.assert_known_name(_name)

#: 上下文块顺序（稳定输出；``compose`` 返回的 ``block_order`` 就是它）。
BLOCK_ORDER: tuple[str, ...] = (
    "persona",
    "session",
    "threads",
    "profile",
    "relationship",
    "messages",
    "slang",
    "identity",
)

#: 上下文块的标题（拼进 context_block 时用）。
BLOCK_TITLES: dict[str, str] = {
    "persona": "【你是谁】",
    "session": "【这场会话】",
    "threads": "【在聊的线】",
    "profile": "【对方是谁】",
    "relationship": "【和对方的关系】",
    "messages": "【最近的消息】",
    "slang": "【群里的梗】",
    "identity": "【身份纪律】",
}

#: 分层 → 语气提示（让 ``tier`` 真的能改变措辞，而不只是多一行数字）。
#: 键取自 social 的 tier 值（``close`` / ``friend`` / ``acquaintance`` / ``stranger``）；
#: 中文标签以 ``rpc:relationship.get`` 返回的 ``tier_label`` 为准，这里不重复维护译文。
TIER_HINTS: dict[str, str] = {
    "close": "可以随便开玩笑、叫外号、少客套",
    "friend": "熟识，正常插科打诨就行",
    "acquaintance": "普通群友，客气一点，别过度自来熟",
    "stranger": "不熟，先别自来熟，客气克制",
}

#: 没有 affinity 边（``found=False``）= 陌生人：必须**显式说出来**。
#: social 的分层默认档就是 ``stranger``，缺失不等于「没有信息」，而等于「关系还没建立」。
STRANGER_BLOCK = "关系：陌生（还没怎么聊过）；先别自来熟，按普通群友来"


#: 默认场景（token 预算策略键）。
DEFAULT_SCENARIO = "chat"

#: 默认近期消息条数 / 聊天线条数。
DEFAULT_MESSAGE_LIMIT = 12
DEFAULT_THREAD_LIMIT = 6

#: 上下文块的字符上限（防止单块吃掉整段预算）。
MAX_BLOCK_CHARS = 600

#: 场景 → 兜底预算（``rpc:token.reserve`` 不可用时用）。
FALLBACK_BUDGETS: dict[str, tuple[int, int]] = {
    "smalltalk": (800, 120),
    "chat": (1600, 320),
    "discussion": (4000, 800),
    "reflection": (8000, 1500),
}


def fallback_budget(scenario: str = DEFAULT_SCENARIO) -> tuple[int, int]:
    """场景 →（输入预算, 输出预算）；未知场景退回 ``chat``。"""

    return FALLBACK_BUDGETS.get(str(scenario or DEFAULT_SCENARIO), FALLBACK_BUDGETS[DEFAULT_SCENARIO])


def truncate_block(text: str, *, limit: int = MAX_BLOCK_CHARS) -> str:
    """把单块文本压到上限（保留句读边界）。"""

    content = str(text or "").strip()
    if len(content) <= limit:
        return content
    window = content[:limit]
    cut = max((window.rfind(char) for char in "。！？!?；;，,、"), default=-1)
    return (window[: cut + 1] if cut >= max(4, limit // 3) else window).rstrip()


def render_session_block(archive: Mapping[str, Any] | None) -> str:
    """会话档案 → 上下文块（标题 / 摘要 / 关键词 / 消息数）。"""

    if not archive:
        return ""
    parts: list[str] = []
    title = str(archive.get("title") or "")
    summary = str(archive.get("summary") or "")
    keywords = [str(item) for item in (archive.get("keywords") or ()) if str(item).strip()]
    if title:
        parts.append(f"标题：{title}")
    if summary:
        parts.append(f"摘要：{summary}")
    if keywords:
        parts.append("关键词：" + "、".join(keywords[:8]))
    count = int(archive.get("message_count") or 0)
    if count:
        parts.append(f"消息数：{count}")
    return "；".join(parts)


def render_threads_block(threads: Sequence[Mapping[str, Any]] | None) -> str:
    """聊天线 → 上下文块（每条一行，含摘要与关键词）。"""

    lines: list[str] = []
    for thread in threads or ():
        row = dict(thread)
        title = str(row.get("title") or row.get("thread_id") or "")
        summary = str(row.get("summary") or "")
        keywords = [str(item) for item in (row.get("keywords") or ()) if str(item).strip()][:5]
        line = title
        if summary:
            line += f"：{summary}"
        if keywords:
            line += "（" + "、".join(keywords) + "）"
        if line.strip():
            lines.append(f"- {line.strip()}")
    return "\n".join(lines)


def render_profile_block(profile: Mapping[str, Any] | None) -> str:
    """群友档案 → 上下文块（昵称 / 标签 / 兴趣 / 说话风格）。"""

    if not profile:
        return ""
    parts: list[str] = []
    nickname = str(profile.get("nickname") or "")
    if nickname:
        parts.append(f"昵称：{nickname}")
    tags = [str(item) for item in (profile.get("tags") or ()) if str(item).strip()]
    if tags:
        parts.append("标签：" + "、".join(tags[:6]))
    interests = [str(item) for item in (profile.get("interests") or ()) if str(item).strip()]
    if interests:
        parts.append("兴趣：" + "、".join(interests[:6]))
    summary = str(profile.get("persona_summary") or "")
    if summary:
        parts.append(f"画像：{summary}")
    style = profile.get("speaking_style") or {}
    if isinstance(style, Mapping):
        metrics = style.get("metrics") or {}
        avg = float(metrics.get("avg_length") or 0.0) if isinstance(metrics, Mapping) else 0.0
        if avg:
            parts.append(f"平时一句 {avg:.0f} 字左右")
        temper = style.get("temper") or {}
        if isinstance(temper, Mapping) and temper.get("label"):
            parts.append(f"语气偏{temper.get('label')}")
    return "；".join(parts)


def render_relationship_block(relationship: Mapping[str, Any] | None) -> str:
    """关系分/分层 → 上下文块（亲密度是语气的第一调节量）。

    ``rpc:relationship.get`` 的返回形状（见 ``docs/SOCIAL.md`` 与
    ``grouppig.social.graph.relationship.rules.RelationshipRules.get``）：

    * 带 ``user_id``：``{found, score, tier, tier_label, interactions, last_ts, source}``；
    * 不带 ``user_id``：整网统计 ``{count, average, max, min}`` —— 没有点名对象，
      对「该怎么跟这个人说话」没有指导意义，**不出块**（免得模型对着平均分瞎猜）。

    没有 affinity 边（``found=False``）时**不返回空串**：社交分层的默认档就是「陌生」，
    「没有关系」本身就是一条要传达给模型的信息（否则陌生人与死党又回到同一个语气）。
    """

    if not isinstance(relationship, Mapping) or not relationship:
        return ""
    if relationship.get("user_id") is None and "count" in relationship:
        return ""
    score = relationship.get("score")
    found = bool(relationship.get("found")) and score is not None
    if not found:
        return STRANGER_BLOCK
    tier = str(relationship.get("tier") or "")
    label = str(relationship.get("tier_label") or "") or tier
    parts = [f"关系：{label}（{float(score):.0f} 分）"]
    interactions = int(relationship.get("interactions") or 0)
    if interactions:
        parts.append(f"已互动 {interactions} 次")
    hint = TIER_HINTS.get(tier)
    if hint:
        parts.append(hint)
    return "；".join(parts)


def render_messages_block(messages: Sequence[Mapping[str, Any]] | None, *, self_id: int = 0) -> str:
    """近期消息 → 上下文块（``昵称: 内容``，我方标「我」）。"""

    lines: list[str] = []
    for row in messages or ():
        item = dict(row)
        sender = int(item.get("sender_id", 0) or 0)
        who = "我" if self_id and sender == int(self_id) else str(item.get("sender_name") or sender)
        content = str(item.get("content") or "").strip()
        if content:
            lines.append(f"{who}: {content}")
    return "\n".join(lines)


def render_slang_block(slang: Any) -> str:
    """黑话注入结果 → 上下文块（尽可能宽松地兼容下游返回形态）。"""

    if not slang:
        return ""
    if isinstance(slang, Mapping):
        text = str(slang.get("text") or slang.get("block") or "")
        if text:
            return text
        entries = slang.get("entries") or slang.get("terms") or []
    else:
        entries = slang if isinstance(slang, Sequence) and not isinstance(slang, (str, bytes)) else []
    lines: list[str] = []
    for item in entries or ():
        if isinstance(item, Mapping):
            term = str(item.get("term") or "")
            meaning = str(item.get("meaning") or "")
            if term:
                lines.append(f"- {term}：{meaning}" if meaning else f"- {term}")
        elif str(item).strip():
            lines.append(f"- {item}")
    return "\n".join(lines)


def render_identity_block(identity: Any) -> str:
    """``rpc:identity.deny-ai`` 的返回 → 上下文块（兼容多种返回形态）。"""

    if not identity:
        return ""
    if isinstance(identity, Mapping):
        for key in ("text", "reply", "deflection", "block", "instruction"):
            value = identity.get(key)
            if value:
                return str(value)
        rules = identity.get("rules") or ()
        return "\n".join(f"- {item}" for item in rules)
    return str(identity)


def pack_blocks(blocks: Mapping[str, str], *, order: Sequence[str] = BLOCK_ORDER) -> str:
    """按 :data:`BLOCK_ORDER` 把非空块拼成 ``context_block``（稳定输出）。"""

    parts: list[str] = []
    for key in order:
        body = truncate_block(blocks.get(key, ""))
        if not body:
            continue
        parts.append(f"{BLOCK_TITLES.get(key, key)}\n{body}")
    return "\n\n".join(parts)


@dataclass
class ContextPacker:
    """上下文打包器（设计：``grouppig.expression.generator.context``）。"""

    ctx: ExpressionContext | None = None
    scenario: str = DEFAULT_SCENARIO
    self_id: int = 0
    message_limit: int = DEFAULT_MESSAGE_LIMIT
    thread_limit: int = DEFAULT_THREAD_LIMIT
    composes: int = field(default=0, init=False)
    degraded: int = field(default=0, init=False)

    # ---- 依赖调用（全部容错） ------------------------------------------
    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """调用契约名字；缺处理器 / 下游异常时返回 ``None`` 而不是抛出。"""

        if self.ctx is None or not callable(getattr(self.ctx, "call", None)):
            return None
        try:
            return await self.ctx.call(name, *args, **kwargs)
        except Exception as error:
            self._log(
                "debug",
                "compose.dependency_failed",
                name=name,
                error=f"{type(error).__name__}: {error}",
            )
            return None

    async def reserve_budget(self, scenario: str) -> dict[str, Any]:
        """``rpc:token.reserve``（设计依赖）→ 输入/输出预算；不可用则用配置兜底。"""

        reserve = await self._call(DEP_TOKEN_RESERVE, scenario)
        if isinstance(reserve, Mapping) and (reserve.get("input_tokens") or reserve.get("output_tokens")):
            return {
                "input_tokens": int(reserve.get("input_tokens") or 0),
                "output_tokens": int(reserve.get("output_tokens") or 0),
                "reservation_id": str(reserve.get("reservation_id") or ""),
                "source": "token.reserve",
            }
        input_tokens, output_tokens = fallback_budget(scenario)
        return {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "reservation_id": "",
            "source": "fallback",
        }

    # ---- 主流程 --------------------------------------------------------
    async def compose(
        self,
        *,
        group_id: int = 0,
        user_id: int | None = None,
        session_id: str | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        archive: Mapping[str, Any] | None = None,
        profile: Mapping[str, Any] | None = None,
        relationship: Mapping[str, Any] | None = None,
        message: Mapping[str, Any] | None = None,
        scenario: str | None = None,
        keyword: str = "",
        suggestion: str = "",
        seed: int = 0,
        self_id: int | None = None,
        message_limit: int | None = None,
        thread_limit: int | None = None,
        candidates: int | None = None,
        images: Sequence[str] | None = None,
        window_seconds: int = 300,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """``rpc:generator.compose`` —— 打包上下文、压缩提示词、生成最终回复文本。"""

        self.composes += 1
        scene = str(scenario or self.scenario)
        me = int(self_id if self_id is not None else self.self_id)
        missing: list[str] = []
        degraded_paths: list[str] = []

        # 0) 预算：先预留，压缩与生成都按它夹取
        budget = await self.reserve_budget(scene)
        if budget["source"] == "fallback":
            degraded_paths.append("token_reserve_unavailable")

        # 1) 人设（设计依赖 rpc:persona.style）
        style_hints: dict[str, Any] = {}
        persona_block = ""
        persona = await self._call(DEP_PERSONA_STYLE, as_text=True, user_id=user_id, group_id=int(group_id or 0))
        if isinstance(persona, Mapping):
            style_hints = dict(persona.get("style_hints") or {})
            persona_block = str(persona.get("persona_block") or "")
        if not style_hints:
            missing.append("persona")
            degraded_paths.append("persona_unavailable")

        # 2) 会话摘要（补数：rpc:archive.load）
        if archive is None and session_id:
            loaded = await self._call(DEP_ARCHIVE_LOAD, str(session_id))
            archive = loaded.get("archive") if isinstance(loaded, Mapping) else None
        session_block = render_session_block(archive)
        if not session_block:
            missing.append("session")

        # 3) 聊天线（调用方没给就交给 compressor 按设计依赖读）
        if threads is None and session_id:
            loaded_threads = await self._call(
                DEP_COMPRESS,
                messages,
                budget_tokens=int(budget["input_tokens"]),
                session_id=str(session_id),
                group_id=int(group_id or 0),
                thread_limit=int(thread_limit if thread_limit is not None else self.thread_limit),
            )
            if isinstance(loaded_threads, Mapping):
                threads = loaded_threads.get("threads")
        threads_block = render_threads_block(threads)
        if not threads_block:
            missing.append("threads")

        # 4) 听众画像（补数：rpc:profile.get）
        if profile is None and user_id:
            loaded_profile = await self._call(DEP_PROFILE_GET, int(user_id), group_id=int(group_id or 0))
            profile = loaded_profile.get("profile") if isinstance(loaded_profile, Mapping) else None
        profile_block = render_profile_block(profile)
        if not profile_block:
            missing.append("profile")

        # 4.5) 关系分 / 亲密度（补数：rpc:relationship.get）
        #      此前社交图是**只写不读**的（全仓只有 social 域内部读它），于是
        #      「陌生人和死党」拿到逐字相同的上下文，语气自然一模一样。
        #      把 tier_label + score 显式喂进 context_block，模型才有得可调。
        if relationship is None and user_id:
            loaded_relationship = await self._call(
                DEP_RELATIONSHIP_GET,
                int(user_id),
                group_id=int(group_id or 0),
            )
            relationship = loaded_relationship if isinstance(loaded_relationship, Mapping) else None
        relationship_block = render_relationship_block(relationship)
        if not relationship_block:
            missing.append("relationship")

        # 5) 当前消息（调用方没给就按群取窗口：补数 rpc:chat.window）
        rows = list(messages or ())
        if not rows and group_id:
            window = await self._call(
                DEP_CHAT_WINDOW,
                int(group_id),
                seconds=int(window_seconds),
                limit=int(message_limit if message_limit is not None else self.message_limit),
            )
            if isinstance(window, Mapping):
                rows = [dict(item) for item in (window.get("messages") or ())]
        if message:
            rows = [*rows, dict(message)]
        keep = int(message_limit if message_limit is not None else self.message_limit)
        if keep > 0:
            rows = rows[-keep:]
        if not rows:
            missing.append("messages")

        # 6) 压缩（设计依赖 rpc:generator.compress）
        compressed = await self._call(
            DEP_COMPRESS,
            rows,
            threads=threads,
            budget_tokens=int(budget["input_tokens"]),
            self_id=me,
        )
        if isinstance(compressed, Mapping):
            packed_messages = [dict(item) for item in (compressed.get("messages") or ())]
            tokens_before = int(compressed.get("tokens_before") or 0)
            tokens_after = int(compressed.get("tokens_after") or 0)
            truncated = bool(compressed.get("truncated"))
        else:
            packed_messages = rows
            tokens_before = tokens_after = 0
            truncated = False
            missing.append("compress")
            degraded_paths.append("compress_unavailable")
        messages_block = render_messages_block(packed_messages, self_id=me)

        # 7) 黑话（设计依赖 rpc:slang.inject；t9 落地后自动接通）
        slang = await self._call(
            DEP_SLANG_INJECT,
            messages=packed_messages,
            group_id=int(group_id or 0),
            keyword=keyword,
            scene=scene,
        )
        slang_block = render_slang_block(slang)
        if not slang_block:
            missing.append("slang")

        # 8) 身份防御（设计依赖 rpc:identity.deny-ai；t9 落地后自动接通）
        identity = await self._call(
            DEP_IDENTITY_DENY,
            text=keyword or messages_block,
            strong=False,
        )
        identity_block = render_identity_block(identity)
        if not identity_block:
            missing.append("identity")

        blocks = {
            "persona": persona_block,
            "session": session_block,
            "threads": threads_block,
            "profile": profile_block,
            "relationship": relationship_block,
            "messages": messages_block,
            "slang": slang_block,
            "identity": identity_block,
        }
        context_block = pack_blocks(blocks)

        # 9) 生成（设计依赖 rpc:generator.write；writer 内部再交 polisher）
        generated = await self._call(
            DEP_WRITE,
            persona_block=persona_block,
            context_block=context_block,
            style_hints=style_hints,
            suggestion=suggestion,
            keyword=keyword,
            seed=int(seed),
            candidates=candidates,
            images=images,
            polish=True,
            user_id=user_id,
            group_id=int(group_id or 0),
            scenario=scene,
            max_tokens=int(budget["output_tokens"]),
        )
        if not isinstance(generated, Mapping):
            missing.append("write")
            degraded_paths.append("write_unavailable")
            generated = {}
        else:
            for path in generated.get("degraded_paths") or ():
                if str(path) not in degraded_paths:
                    degraded_paths.append(str(path))

        payload: dict[str, Any] = {
            "text": str(generated.get("text") or ""),
            "context_block": context_block,
            "blocks": {key: truncate_block(value) for key, value in blocks.items()},
            "block_order": list(BLOCK_ORDER),
            "persona_block": persona_block,
            "style_hints": style_hints,
            "relationship": dict(relationship) if isinstance(relationship, Mapping) else {},
            "budget": budget,
            "tokens_before": tokens_before,
            "tokens_after": tokens_after,
            "truncated": truncated,
            "generated": dict(generated),
            "candidates": list(generated.get("candidates") or ()),
            "count": int(generated.get("count") or 0),
            "polished": bool(generated.get("polished")),
            "slang": slang if isinstance(slang, Mapping) else {},
            "identity": identity if isinstance(identity, Mapping) else {},
            "missing": sorted(set(missing)),
            "degraded": bool(degraded_paths),
            "degraded_paths": sorted(set(degraded_paths)),
            "scenario": scene,
            "group_id": int(group_id or 0),
            "user_id": int(user_id or 0),
            "session_id": str(session_id or ""),
        }
        if payload["degraded"]:
            self.degraded += 1
        self._log(
            "debug",
            "compose.done",
            scenario=scene,
            missing=payload["missing"],
            degraded=payload["degraded"],
        )
        return payload

    def status(self) -> dict[str, Any]:
        return {
            "module": MODULE_ID,
            "composes": self.composes,
            "degraded": self.degraded,
            "scenario": self.scenario,
            "block_order": list(BLOCK_ORDER),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        logger = getattr(self.ctx, "log", None)
        if callable(logger):
            logger(level, event, **fields)


def make_handlers(packer: ContextPacker) -> dict[str, Any]:
    """``rpc:generator.compose`` 处理器。"""

    async def generator_compose(
        group_id: int = 0,
        *,
        user_id: int | None = None,
        session_id: str | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        threads: Sequence[Mapping[str, Any]] | None = None,
        archive: Mapping[str, Any] | None = None,
        profile: Mapping[str, Any] | None = None,
        relationship: Mapping[str, Any] | None = None,
        message: Mapping[str, Any] | None = None,
        scenario: str | None = None,
        keyword: str = "",
        suggestion: str = "",
        seed: int = 0,
        self_id: int | None = None,
        message_limit: int | None = None,
        thread_limit: int | None = None,
        candidates: int | None = None,
        images: Sequence[str] | None = None,
        window_seconds: int = 300,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await packer.compose(
            group_id=group_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            threads=threads,
            archive=archive,
            profile=profile,
            relationship=relationship,
            message=message,
            scenario=scenario,
            keyword=keyword,
            suggestion=suggestion,
            seed=seed,
            self_id=self_id,
            message_limit=message_limit,
            thread_limit=thread_limit,
            candidates=candidates,
            images=images,
            window_seconds=window_seconds,
            **kwargs,
        )

    return {RPC_COMPOSE: generator_compose}


def register(registry: Any, packer: ContextPacker | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 ``rpc:`` 处理器注册进注册表。"""

    instance = packer if packer is not None else ContextPacker()
    for name, handler in make_handlers(instance).items():
        registry.register(name, handler, module=MODULE_ID, replace=replace)
    return registry


__all__ = [
    "BLOCK_ORDER",
    "BLOCK_TITLES",
    "DEFAULT_MESSAGE_LIMIT",
    "DEFAULT_SCENARIO",
    "DEFAULT_THREAD_LIMIT",
    "DEP_ARCHIVE_LOAD",
    "DEP_CHAT_WINDOW",
    "DEP_COMPRESS",
    "DEP_IDENTITY_DENY",
    "DEP_PERSONA_STYLE",
    "DEP_PROFILE_GET",
    "DEP_RELATIONSHIP_GET",
    "DEP_SLANG_INJECT",
    "DEP_TOKEN_RESERVE",
    "DEP_WRITE",
    "FALLBACK_BUDGETS",
    "MODULE_ID",
    "NAMES",
    "RPC_COMPOSE",
    "STRANGER_BLOCK",
    "TIER_HINTS",
    "ContextPacker",
    "fallback_budget",
    "make_handlers",
    "pack_blocks",
    "register",
    "render_identity_block",
    "render_messages_block",
    "render_profile_block",
    "render_relationship_block",
    "render_session_block",
    "render_slang_block",
    "render_threads_block",
    "truncate_block",
]
