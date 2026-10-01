"""grouppig.session.topic.detector.ranker —— 话题排序器（``rpc:topic.detect`` / ``rpc:topic.resolve`` / ``kafka:grouppig.topic.changed``）。

职责（对应设计 ``grouppig.session.topic.detector.ranker``「对候选话题排序并归一，输出当前话题与置信度」）：

* ``rpc:topic.detect`` —— 取候选话题 → 打分归一 → 输出当前话题；话题变了就
  ``rpc:session.open``（设计依赖）开启新会话，并发布 ``kafka:grouppig.topic.changed``；
* ``rpc:topic.resolve`` —— 把模糊话题（用户随口说法 / 短文本）归一到已有话题：
  走 ``rpc:topic.similarity``（设计依赖）算相似度取最像的候选，再 ``rpc:session.update``（设计依赖）落库；
* ``rpc:model.classify``（设计依赖，可选）—— 候选短语当标签让轻量模型挑一个，
  失败或未注入 caller 时自动退回词面打分（``method: "lexical"``）。

打分（确定性、可单测）：``confidence = 0.45*cohesion + 0.30*support + 0.25*recency``，
其中 cohesion = 候选关键词与消息窗关键词的 Jaccard，support = 消息条数饱和度，
recency = 末条消息的时间衰减；随后按最高分归一（top = 原始分，其余为相对分）。
``rpc:topic.detect`` 对外报的 ``confidence`` 是**原始分**（与 ``DEFAULT_THRESHOLD`` 同一把尺），
归一后的相对分另放在 ``relative_confidence``。

设计：``grouppig.session.topic.detector.ranker``（叶子模块）。
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.messages import (
    extract_keywords,
    jaccard,
    message_ids_of,
    normalize_messages,
    recency_factor,
    snippet,
    texts_of,
    tokenize,
)

#: normify 模块 id。
MODULE = "grouppig.session.topic.detector.ranker"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:topic.detect", "rpc:topic.resolve")
TOPIC_CHANGED = "kafka:grouppig.topic.changed"
TOPICS = (TOPIC_CHANGED,)

RPC_DETECT, RPC_RESOLVE = RPC

#: 跨域 / 跨模块依赖名字。
RPC_MODEL_CLASSIFY = "rpc:model.classify"

#: 归一化阈值：低于它认为「还是同一个话题」。
DEFAULT_THRESHOLD = 0.45

#: 打分权重。
WEIGHT_COHESION = 0.45
WEIGHT_SUPPORT = 0.30
WEIGHT_RECENCY = 0.25

#: 半衰期（秒）与「足够热闹」的消息条数。
HALF_LIFE = 1800.0
SUPPORT_SATURATION = 12.0

#: 「新消息」与「当前会话话题」的归并门槛（共享关键词个数 + 重叠系数 + 复现）。
#:
#: 同一段对话里措辞会一直变（「周末一起去爬山吧」→「爬山好啊我也想去爬山」→
#: 「那就周六早上八点集合去爬山」），候选短语因此每条都不同、``topic_id`` 也跟着变，
#: 于是每条消息都判 changed 并开新会话 —— 会话被切成 1~2 条消息的碎片，归档 / 反思 /
#: 聊天线全部作用在碎片上。
#:
#: 判「还是同一件事」用的信号是**新消息与当前会话话题的共享实词**。
#:
#: **为什么光看「共享个数 + 重叠系数」不够（旧版就是这个洞）**：两侧词表都由
#: ``extract_keywords(top=10)`` 截断，于是 ``min(len) ≤ 10``，``shared ≥ 1`` 必然推出
#: ``overlap = shared / min(len) ≥ 0.1`` —— 系数这一项在 ``MERGE_THRESHOLD = 0.1`` 下
#: 数学上不可能否决任何东西，判据退化成「沾到一个共同词就归并」。中文群聊里
#: 「一起 / 起去」这类词几乎每条消息都有：爬山会话碰上「我们一起去打游戏吧」共享
#: 「一起 / 起去」两个词、系数 0.333；「一起吃饭吗」共享「一起」、系数 0.25 —— 两条都被
#: 并进爬山会话，``changed`` 恒为 ``False``，``kafka:grouppig.topic.changed`` 一次都不发，
#: 一个会话 / 聊天线 / 反思单元横跨两个不相干的话题。
#: 把系数抬到 0.2 只补住「两条 10 词词表只共享 1 个词」这一种情形（1/10 = 0.1 < 0.2），
#: 对上面两例无效（0.333 / 0.25 都过线）；而且会误伤真正的续聊 —— 实测同一话题的续聊有
#: 1/6 落在 0.167（「爬山带什么装备好」→「爬山要不要带头灯」，只共享「爬山」、短边 6 个词），
#: 抬阈值等于拿「短边有几个词」当运气。所以系数保持 0.1 只做**下限**（调用方仍可用
#: ``merge_threshold`` 收紧），真正的判别力来自下面的复现条件。
#:
#: **真正的判别力：共享词必须「复现」。** 说明「还是同一件事」的不是「共享了词」，而是
#: **共享的词在当前会话里反复出现**：爬山那段每一步都共享「爬山」（4 条会话消息里出现
#: 4 次，第 3 步甚至只共享它一个词），而误归并那两例共享的「一起 / 起去」只出现在会话
#: 的第一条消息里 —— 那是擦边词，不是这段话题的核心。
#: 这也正是「按语料稀有度给共享词加权」的离线可测版本：这里的语料就是当前会话自己的消息，
#: 「在几条消息里出现过」= 文档频次，无需外部语料库。
#: 会话只有 0~1 条消息时词频无从谈起（第一条消息的词表里每个词都只出现一次），此时退回
#: 「窗口内复现」，窗口里也只出现一次的孤词同样不算证据。会话消息靠 ``message_ids`` 在窗口里
#: 定位（``on_message`` 每次都把「最近窗口 + 本条」一起递进来），拿不到 id 时退而用
#: 「窗口里除本条以外的消息」当代理。
MERGE_THRESHOLD = 0.1

#: 归并要求的最少共享关键词个数。
MERGE_MIN_SHARED = 1

#: 共享词要算「复现证据」，至少得在当前会话的这么多条消息里出现过。
MERGE_MIN_REPEAT = 2


def keyword_overlap(left: Sequence[str], right: Sequence[str]) -> tuple[int, float]:
    """两组关键词的「共享词数, 重叠系数」。

    重叠系数 = 共享词数 / 较短一侧的词数。用它而不是 Jaccard，是因为两侧长度天然不对等：
    会话话题的关键词会随对话累积到 10 个，而单条群消息只有 4~8 个词，Jaccard 会被长度差
    压到很低（实测同一话题只有 0.07~0.2），反而分不开「换话题」（0.0）。
    """

    left_set = {str(item) for item in left if str(item)}
    right_set = {str(item) for item in right if str(item)}
    if not left_set or not right_set:
        return 0, 0.0
    shared = len(left_set & right_set)
    return shared, round(shared / min(len(left_set), len(right_set)), 6)


def topic_id_for(phrase: str, *, group_id: int = 0) -> str:
    """话题短语 → 稳定话题 id（同短语同 id，跨会话可归并）。"""

    text = " ".join(str(phrase or "").split())
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]  # noqa: S324 - 稳定 id，非安全用途
    return f"topic-{int(group_id or 0)}-{digest}"


def recurring_keywords(
    texts: Sequence[str],
    keywords: Sequence[str],
    *,
    min_messages: int = MERGE_MIN_REPEAT,
) -> list[str]:
    """``keywords`` 里在这批 ``texts`` 中**复现**的那些（出现在 ≥ ``min_messages`` 条消息里）。

    用「文档频次」而不是「词频」：一个词在同一条消息里重复三遍仍只是一条消息里的说法，
    只有跨消息复现才说明它撑得起这一段对话（见 ``MERGE_MIN_REPEAT`` 的说明）。
    词面判定直接走 ``tokenize``，与 ``extract_keywords`` 同一套切词，保证可复现。
    """

    floor = max(1, int(min_messages))
    counts: dict[str, int] = {}
    for text in texts:
        tokens = set(tokenize(text))
        for keyword in keywords:
            if keyword in tokens:
                counts[keyword] = counts.get(keyword, 0) + 1
    return [str(keyword) for keyword in keywords if counts.get(str(keyword), 0) >= floor]


def score_candidate(
    candidate: Mapping[str, Any],
    *,
    window_keywords: Sequence[str] = (),
    now: float = 0.0,
) -> dict[str, Any]:
    """给单个候选话题打分（纯函数）。"""

    keywords = [str(k) for k in (candidate.get("keywords") or ())]
    phrase = str(candidate.get("phrase") or candidate.get("title") or "")
    if not keywords and phrase:
        keywords = extract_keywords(phrase)
    cohesion = jaccard(keywords, window_keywords)
    count = float(candidate.get("message_count") or len(candidate.get("message_ids") or ()) or 1)
    support = min(1.0, count / SUPPORT_SATURATION)
    last_ts = float(candidate.get("last_ts") or 0.0)
    recency = recency_factor(last_ts, now=now, half_life=HALF_LIFE) if last_ts else 0.0
    raw = WEIGHT_COHESION * cohesion + WEIGHT_SUPPORT * support + WEIGHT_RECENCY * recency
    return {
        **dict(candidate),
        "phrase": phrase,
        "keywords": keywords,
        "score": round(max(0.0, min(1.0, raw)), 6),
        "signals": {"cohesion": round(cohesion, 6), "support": round(support, 6), "recency": round(recency, 6)},
    }


class TopicRanker:
    """候选话题排序、当前话题输出与模糊话题归一。"""

    def __init__(
        self,
        *,
        candidate: Any = None,
        embedder: Any = None,
        sessions: Any = None,
        caller: Any = None,
        publisher: Any = None,
        threshold: float = DEFAULT_THRESHOLD,
        top: int = 5,
        clock: Any = time.time,
    ) -> None:
        self.candidate = candidate
        self.embedder = embedder
        self.sessions = sessions
        self.caller = caller
        self.publisher = publisher
        self.threshold = float(threshold)
        self.top = int(top)
        self.clock = clock

    # ---- 排序 ----------------------------------------------------------
    def rank_candidates(
        self,
        candidates: Sequence[Mapping[str, Any]] | None,
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        top: int | None = None,
    ) -> list[dict[str, Any]]:
        """排序并归一候选话题（``confidence`` 为归一后的置信度）。"""

        stamp = float(now if now is not None else self.clock())
        window_keywords = extract_keywords(texts_of(messages), top=15)
        scored = [score_candidate(item, window_keywords=window_keywords, now=stamp) for item in (candidates or ())]
        scored.sort(key=lambda item: (-item["score"], str(item.get("topic_id", ""))))
        best = scored[0]["score"] if scored else 0.0
        for item in scored:
            item["confidence"] = round(item["score"] / best, 6) if best > 0 else 0.0
        return scored[: max(1, int(top or self.top))]

    # ---- rpc:topic.detect ---------------------------------------------
    async def detect(
        self,
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        candidates: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        threshold: float | None = None,
        open_session: bool = True,
        use_model: bool = False,
        title: str = "",
        merge: bool = True,
        merge_threshold: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """识别当前话题：排序候选 → 与当前会话比较 → 必要时开启新会话并发布话题切换事件。

        ``merge=True``（默认）时，候选话题若与当前会话的话题足够相似（共享实词够多、重叠系数
        过线、且共享词在当前会话里复现过，见 ``MERGE_THRESHOLD``），就**沿用当前会话**而不新开
        一个 —— 同一段对话的措辞变化不该被当成换话题。真正换话题时相似度会掉到阈值以下，仍按
        老路子开新会话并发布 ``kafka:grouppig.topic.changed``。

        ``threshold`` 卡的是**原始分**（``score``，与 ``confidence`` 同一把尺）：原始分不够的
        弱窗口不会开出新会话，只在已有会话里待着（``weak=True``）。
        """

        stamp = float(now if now is not None else self.clock())
        items = normalize_messages(messages)
        generated: dict[str, Any] | None = None
        pool = list(candidates or ())
        if not pool and self.candidate is not None:
            generated = await self.candidate.generate(group_id, items or None, now=stamp, rank=False, **kwargs)
            pool = list(generated.get("candidates") or ())
        ranked = self.rank_candidates(pool, messages=items, now=stamp)
        if not ranked:
            return {
                "topic": None,
                "topic_id": "",
                "confidence": 0.0,
                "relative_confidence": 0.0,
                "score": 0.0,
                "changed": False,
                "merged": False,
                "weak": False,
                "candidates": [],
                "session": None,
                "group_id": int(group_id or 0),
                "method": "empty",
            }

        best = ranked[0]
        threshold = self.threshold if threshold is None else float(threshold)
        method = "lexical"
        if use_model and self.caller is not None and len(ranked) > 1:
            judged = await self._classify(items, ranked)
            if judged is not None:
                index, method = judged
                if index != 0:
                    best = ranked[index]
                    ranked.insert(0, ranked.pop(index))

        current = await self._current_session(group_id)
        current_topic = str((current or {}).get("topic_id", "") or "")
        changed = current_topic != str(best.get("topic_id", ""))
        merged: dict[str, Any] | None = None
        if changed and merge:
            merged = self.merge_with_current(current, best, messages=items, threshold=merge_threshold)
            if merged is not None:
                best = merged
                changed = False
        score = float(best.get("score", 0.0) or 0.0)
        # 原始分没过阈值：这一窗还不足以支撑一个话题 —— 不开新会话、也不发话题切换事件。
        #
        # 原来这里比的是**归一化置信度**，而归一化是拿最高分当分母的（``rank_candidates``），
        # 候选只有一个时它恒等于 1.0，阈值因此从来没起过作用：一条「嗯」（实测 score=0.275 <
        # DEFAULT_THRESHOLD=0.45）照样开会话。改成比原始分之后，弱窗口只会在已有会话里待着
        # （没有会话时不在这里开，交给调用方的兜底路径，保证消息不会掉出会话）。
        weak = score < threshold
        if changed and weak:
            changed = False
            held = dict(best)
            if current_topic:
                held["topic_id"] = current_topic
                held["phrase"] = str((current or {}).get("title") or "") or str(best.get("phrase") or "")
                held["keywords"] = list((current or {}).get("keywords") or ())
            held["weak"] = True
            best = held
        session = current
        opened = False
        if changed and open_session:
            switching = bool(current_topic)
            session, opened = await self._open_session(
                group_id,
                best,
                title=title,
                now=stamp,
                keywords=extract_keywords(texts_of(items)[-1:]) if switching and items else None,
                message_ids=message_ids_of(items[-1:]) if switching and items else None,
            )
            await self._publish_changed(group_id, best, previous_topic=current_topic, now=stamp)

        return {
            "topic": best,
            "topic_id": str(best.get("topic_id", "")),
            "phrase": str(best.get("phrase", "")),
            # confidence 取**原始分**（0~1，与 threshold 同一把尺），而不是「最高分归一」出来的
            # 相对值：相对值在候选只有一个时恒为 1.0，把「一条嗯」和「一屋子人聊爬山」说成一样
            # 可信。相对分另有 relative_confidence，排序用得上，但不该冒充绝对置信度。
            "confidence": score,
            "relative_confidence": float(best.get("confidence", score) or 0.0),
            "score": score,
            "changed": bool(changed),
            "merged": merged is not None,
            "weak": bool(weak),
            "merge_score": float(best.get("merge_score") or 0.0) if merged is not None else 0.0,
            "opened": opened,
            "previous_topic_id": current_topic,
            "candidates": ranked,
            "candidate_count": len(ranked),
            "session": session,
            "group_id": int(group_id or 0),
            "method": method,
            "threshold": threshold,
            "generated": generated,
        }

    def merge_with_current(
        self,
        current: Mapping[str, Any] | None,
        best: Mapping[str, Any],
        *,
        messages: Sequence[Mapping[str, Any]] | None = None,
        threshold: float | None = None,
    ) -> dict[str, Any] | None:
        """新消息仍属于当前会话话题时，把它并回当前话题（返回并回后的候选）。

        判据是**新消息的关键词与当前会话话题关键词的共享实词**（纯词面，不调模型）：
        确定性、离线可测，也不额外消耗模型调用 —— ``rpc:model.embed`` 目前不返回向量本体，
        语义相似度在这个位置拿不到。返回 ``None`` 表示「确实换话题了」，调用方按原逻辑开新会话。

        参考关键词取**新消息本身**而不是窗口候选：窗口里还留着上一话题的消息，候选短语会被
        上一话题拖着走（实测换到打游戏后候选仍是「爬山、去爬山」），拿它当判据会把新话题
        并进旧会话。

        三个条件同时成立才归并（见 ``MERGE_THRESHOLD`` 的说明）：共享词数够、重叠系数过下限，
        并且**至少有一个共享词在当前会话里复现过** —— 只沾到一个擦边词（如「一起」）不算
        同一件事，否则真换话题时 ``changed`` 永远是 ``False``、话题切换事件永远不发。
        """

        if not isinstance(current, Mapping):
            return None
        topic_id = str(current.get("topic_id") or "")
        if not topic_id:
            return None
        topic_keywords = [str(item) for item in (current.get("keywords") or ())]
        if not topic_keywords:
            topic_keywords = extract_keywords(str(current.get("title") or ""))
        window = normalize_messages(messages) if messages else []
        fresh_texts = texts_of(window)[-1:] if window else []
        fresh_keywords = extract_keywords(fresh_texts) if fresh_texts else []
        if not fresh_keywords:
            fresh_keywords = [str(item) for item in (best.get("keywords") or ())]
        if not fresh_keywords:
            fresh_keywords = extract_keywords(str(best.get("phrase") or ""))
        if not topic_keywords or not fresh_keywords:
            return None
        shared, overlap = keyword_overlap(fresh_keywords, topic_keywords)
        limit = MERGE_THRESHOLD if threshold is None else float(threshold)
        if shared < MERGE_MIN_SHARED or overlap < limit:
            return None
        shared_tokens = [token for token in fresh_keywords if token in set(topic_keywords)]
        session_items = self._session_items(current, window, topic_keywords)
        if len(session_items) >= MERGE_MIN_REPEAT:
            recurring = recurring_keywords(texts_of(session_items), shared_tokens)
        else:
            # 会话里能谈这个话题的消息不到 2 条：词表里每个词都只出现过一次，复现无从谈起。
            # 此时退回「窗口内复现」，至少要求共享词在窗口里出现过不止一次；窗口里也只冒过
            # 一次的孤词（会话刚开、又只有这一条新消息）不算证据，宁可开新会话也不串话题。
            recurring = recurring_keywords(texts_of(window), shared_tokens)
        if not recurring:
            return None
        return {
            **dict(best),
            "topic_id": topic_id,
            "phrase": str(current.get("title") or "") or str(best.get("phrase") or ""),
            "keywords": topic_keywords,
            "merged_from": str(best.get("topic_id", "")),
            "merge_score": overlap,
            "merge_shared": shared,
            "merge_recurring": recurring,
        }

    # ---- rpc:topic.resolve --------------------------------------------
    async def resolve(
        self,
        topic: Any = None,
        *,
        text: str | None = None,
        candidates: Sequence[Mapping[str, Any]] | None = None,
        session_id: str | None = None,
        group_id: int | None = None,
        threshold: float | None = None,
        update_session: bool = True,
        use_embedding: bool = True,
        now: float | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        """把模糊话题归一到已有候选（相似度优先），并更新会话。"""

        stamp = float(now if now is not None else self.clock())
        query_text = str(text or (topic if isinstance(topic, str) else "") or "").strip()
        pool = list(candidates or ())
        if not pool and isinstance(topic, Mapping):
            pool = [dict(topic)]
        threshold = self.threshold if threshold is None else float(threshold)

        matched: Mapping[str, Any] | None = None
        score = 0.0
        method = "none"
        scores: list[float] = []
        if pool and self.embedder is not None:
            outcome = await self.embedder.best_match(
                query_text or topic, pool, threshold=threshold, use_embedding=use_embedding
            )
            scores = list(outcome.get("scores") or ())
            method = (
                str((outcome.get("details") or [{}])[outcome["index"]]["method"]) if outcome["index"] >= 0 else "none"
            )
            if outcome["matched"] is not None:
                matched = outcome["matched"]
                score = float(outcome["score"])
        elif pool:
            ranked = self.rank_candidates(pool, now=stamp)
            matched, score, method = ranked[0], float(ranked[0]["score"]), "rank"

        if matched is None and query_text:
            # 没有相似候选时，把模糊说法本身作为一个新话题短语
            matched = {
                "topic_id": topic_id_for(query_text, group_id=int(group_id or 0)),
                "phrase": snippet(query_text, limit=20),
                "keywords": extract_keywords(query_text, top=8),
                "score": 0.0,
                "confidence": 0.0,
            }
            method = "new"

        session: dict[str, Any] | None = None
        if matched is not None and update_session and self.sessions is not None:
            session = await self._update_session(
                session_id, group_id=group_id, topic_id=str(matched.get("topic_id", "")), now=stamp
            )
        return {
            "topic": dict(matched) if matched is not None else None,
            "topic_id": str((matched or {}).get("topic_id", "")),
            "phrase": str((matched or {}).get("phrase", "")),
            "score": round(float(score), 6),
            "scores": scores,
            "method": method,
            "threshold": threshold,
            "resolved": matched is not None,
            "session": session,
            "group_id": int(group_id or 0),
        }

    # ---- 内部 ----------------------------------------------------------
    @staticmethod
    def _session_items(
        current: Mapping[str, Any],
        window: Sequence[Mapping[str, Any]],
        topic_keywords: Sequence[str],
    ) -> list[dict[str, Any]]:
        """从窗口里挑出「当前会话已有的、而且确实在谈这个话题」的消息（复现计数的底座）。

        两个筛子：

        * **归属** —— 消息 id 在会话的 ``message_ids`` 里；拿不到 id（调用方只给了话题关键词）
          时退而用「窗口里除本条以外的消息」当代理 —— ``on_message`` 每次都是「最近窗口 +
          本条」一起递进来，前面那几条正是会话已经吃进去的。
        * **切题** —— 消息至少含一个会话关键词。会话的 ``message_ids`` 未必干净：当前会话没有
          话题时（``switching=False``）``_open_session`` 会把整个窗口的消息都挂到新会话上，
          里面可能夹着「嗯」这类与话题无关的插话；它不该被算进复现的基数，否则一个只有一条
          正经消息的会话会被误判成「够成熟、可以按复现判据卡人」。
        """

        items = [dict(item) for item in window]
        session_ids = {str(item) for item in (current.get("message_ids") or ()) if str(item)}
        if session_ids:
            items = [item for item in items if str(item.get("message_id", "")) in session_ids]
        else:
            items = items[:-1]
        keywords = {str(keyword) for keyword in topic_keywords if str(keyword)}
        return [item for item in items if keywords & set(tokenize(str(item.get("content", "") or "")))]

    async def _classify(
        self, messages: Sequence[Mapping[str, Any]], ranked: Sequence[Mapping[str, Any]]
    ) -> tuple[int, str] | None:
        """让轻量模型在候选短语里挑一个；失败返回 ``None``（退回词面打分）。"""

        text = "\n".join(texts_of(messages)[-12:])
        labels = [str(item.get("phrase", "")) for item in ranked if item.get("phrase")]
        if not text or len(labels) < 2:
            return None
        try:
            response = await self.caller(RPC_MODEL_CLASSIFY, text, labels)
        except Exception:  # noqa: BLE001 - 模型不可用时必须退回词面打分
            return None
        label = str((response or {}).get("label", "") or "") if isinstance(response, Mapping) else ""
        for index, item in enumerate(ranked):
            if label and label == str(item.get("phrase", "")):
                return index, "model"
        return None

    async def _current_session(self, group_id: int | None) -> dict[str, Any] | None:
        if self.sessions is None:
            return None
        result = await self.sessions.current(group_id=group_id)
        return _session_of(result)

    async def _open_session(
        self,
        group_id: int | None,
        best: Mapping[str, Any],
        *,
        title: str,
        now: float,
        keywords: Sequence[str] | None = None,
        message_ids: Sequence[str] | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """开新会话；``keywords`` 给出时用它（而不是窗口候选的关键词）作为会话话题词，``message_ids`` 同理。

        换话题时窗口里还压着上一话题的消息，候选短语与关键词都停在旧话题上；照搬的话，
        新会话一出生就顶着旧话题的词，下一条本该属于新话题的消息会因为「跟会话话题没共享词」
        又被判成换话题，新话题照样碎成好几段。以**新话题的第一条消息**作为会话话题词，
        后续消息才认得回来。
        """

        if self.sessions is None:
            return None, False
        result = await self.sessions.open(
            group_id=group_id,
            topic_id=str(best.get("topic_id", "")),
            title=title or str(best.get("phrase", "")),
            keywords=list(keywords) if keywords else list(best.get("keywords") or ()),
            message_ids=list(message_ids) if message_ids else list(best.get("message_ids") or ()),
            now=now,
        )
        return _session_of(result), True

    async def _update_session(
        self, session_id: str | None, *, group_id: int | None, topic_id: str, now: float
    ) -> dict[str, Any] | None:
        result = await self.sessions.update(
            session_id=session_id, group_id=group_id, topic_id=topic_id, now=now, advance=False
        )
        return _session_of(result)

    async def _publish_changed(
        self,
        group_id: int | None,
        best: Mapping[str, Any],
        *,
        previous_topic: str,
        now: float,
    ) -> None:
        if self.publisher is None:
            return
        payload = {
            "group_id": int(group_id or 0),
            "topic_id": str(best.get("topic_id", "")),
            "phrase": str(best.get("phrase", "")),
            "keywords": list(best.get("keywords") or ()),
            "confidence": float(best.get("confidence", best.get("score", 0.0))),
            "previous_topic_id": previous_topic,
            "ts": now,
        }
        await self.publisher(TOPIC_CHANGED, payload)


def _session_of(result: Any) -> dict[str, Any] | None:
    """从会话状态机的返回体里取出会话字典。"""

    if result is None:
        return None
    if isinstance(result, Mapping):
        session = result.get("session")
        if isinstance(session, Mapping):
            return dict(session)
        if "session_id" in result:
            return dict(result)
    return None


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, ranker: TopicRanker | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表（话题事件由事件总线发布，不进注册表）。"""

    instance = ranker if ranker is not None else TopicRanker()

    async def topic_detect(
        group_id: int | None = None,
        messages: Sequence[Mapping[str, Any]] | None = None,
        *,
        candidates: Sequence[Mapping[str, Any]] | None = None,
        now: float | None = None,
        threshold: float | None = None,
        open_session: bool = True,
        use_model: bool = False,
        title: str = "",
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.detect(
            group_id,
            messages,
            candidates=candidates,
            now=now,
            threshold=threshold,
            open_session=open_session,
            use_model=use_model,
            title=title,
            **kwargs,
        )

    async def topic_resolve(
        topic: Any = None,
        *,
        text: str | None = None,
        candidates: Sequence[Mapping[str, Any]] | None = None,
        session_id: str | None = None,
        group_id: int | None = None,
        threshold: float | None = None,
        update_session: bool = True,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.resolve(
            topic,
            text=text,
            candidates=candidates,
            session_id=session_id,
            group_id=group_id,
            threshold=threshold,
            update_session=update_session,
            now=now,
            **kwargs,
        )

    registry.register(RPC_DETECT, topic_detect, module=MODULE, replace=replace)
    registry.register(RPC_RESOLVE, topic_resolve, module=MODULE, replace=replace)
    return instance


__all__ = [
    "DEFAULT_THRESHOLD",
    "HALF_LIFE",
    "MERGE_MIN_REPEAT",
    "MERGE_MIN_SHARED",
    "MERGE_THRESHOLD",
    "MODULE",
    "RPC",
    "RPC_DETECT",
    "RPC_MODEL_CLASSIFY",
    "RPC_RESOLVE",
    "SUPPORT_SATURATION",
    "TOPICS",
    "TOPIC_CHANGED",
    "TopicRanker",
    "keyword_overlap",
    "recurring_keywords",
    "register",
    "score_candidate",
    "topic_id_for",
]
