"""grouppig.session.topic.embedder.similarity —— 相似度计算器（``rpc:topic.embed`` / ``rpc:topic.similarity``）。

职责（对应设计 ``grouppig.session.topic.embedder.similarity``「调用嵌入模型并计算话题相似度」）：

* ``rpc:topic.embed`` —— 把文本向量化：先查向量缓存（``rpc:topic.embed.cache.get``），
  未命中再调 ``rpc:model.embed``，回填缓存（``rpc:topic.embed.cache.set``）；
* ``rpc:topic.similarity`` —— 计算两段文本 / 两个向量的相似度（余弦优先、关键词 Jaccard 兜底，
  融合权重 ``0.7 * cosine + 0.3 * jaccard``）。

跨域调用（设计依赖 ``rpc:topic.embed`` → ``rpc:model.embed``）通过注入的 ``caller``
（``await caller("rpc:model.embed", texts, ...)``）完成；域内依赖
``rpc:topic.embed.cache.get/set`` 直接协作 :class:`~...cache.EmbeddingCache` 实例
（与 memory 域 ``summary-index`` 直接调 DAO 同例）。

**已知 infra 缺口（本叶子做了兼容）**：``grouppig.infra.runtime.di`` 的 ``rpc:model.embed``
当前返回体只有 ``embedding_dim`` 没有向量本体。本模块按 ``embedding`` / ``vectors`` /
``embeddings`` / ``data[0].embedding`` 依次取值；若都没有但注入了 ``router``，
则回退直连 ``router.embed()`` 取向量（并记录 ``via: "router"``）。

设计：``grouppig.session.topic.embedder.similarity``（叶子模块）。
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from grouppig.session.runtime.errors import DependencyMissing
from grouppig.session.runtime.messages import combined_score, cosine, extract_keywords, jaccard
from grouppig.session.topic.embedder.cache import EmbeddingCache, cache_key, normalize_vector

#: normify 模块 id。
MODULE = "grouppig.session.topic.embedder.similarity"

#: 本叶子负责的契约名字（逐字取自 api-index.json）。
RPC = ("rpc:topic.embed", "rpc:topic.similarity")

RPC_EMBED, RPC_SIMILARITY = RPC

#: 跨域依赖名字（设计边：``rpc:topic.embed`` → ``rpc:model.embed``）。
RPC_MODEL_EMBED = "rpc:model.embed"

#: 向量缓存键前缀（话题向量与消息向量分开）。
TOPIC_PREFIX = "topic"

#: 余弦 / Jaccard 融合权重。
COSINE_WEIGHT = 0.7


def vector_from_response(response: Any) -> list[float]:
    """从 ``rpc:model.embed`` 的返回体里取出向量列表（兼容多种形态）。"""

    if response is None:
        return []
    if isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        items = list(response)
        if items and isinstance(items[0], (int, float)):
            return [float(item) for item in items]
        flattened: list[float] = []
        nested = False
        for item in items:
            found = vector_from_response(item)
            if found:
                nested = True
                flattened.extend(found)
        if nested:
            return flattened
        if items and all(isinstance(item, (int, float)) for item in items):
            return [float(item) for item in items]
        return []
    if isinstance(response, Mapping):
        for key in ("vectors", "embeddings", "embedding", "vector", "values"):
            if key in response:
                found = vector_from_response(response[key])
                if found:
                    return found
        if "data" in response:
            return vector_from_response(response["data"])
        return []
    return []


def normalize_vectors(raw: Any, *, expected: int = 0) -> list[list[float]]:
    """把模型 / 缓存的返回统一成「每条文本一个向量」。

    兼容三种形态：嵌套列表（每文本一个向量）、单层列表套一个向量（一条文本）、
    平铺数字列表（单条向量）。条目数不足时用最后一条补齐，超过则截断。
    """

    if raw is None:
        return []
    if isinstance(raw, Mapping):
        raw = [raw]
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return []
    items = list(raw)
    if not items:
        return []
    if all(isinstance(item, (int, float)) for item in items):
        return [[float(item) for item in items]] * max(1, expected)
    vectors: list[list[float]] = []
    for item in items:
        vector = normalize_vector(item)
        if vector:
            vectors.append(vector)
    if not vectors:
        return []
    if expected > len(vectors):
        vectors = [*vectors, *[list(vectors[-1]) for _ in range(expected - len(vectors))]]
    return vectors[:expected] if expected else vectors


class TopicEmbedder:
    """话题向量化与相似度计算。"""

    def __init__(
        self,
        *,
        caller: Any = None,
        cache: EmbeddingCache | None = None,
        router: Any = None,
        scenario: str = "embed",
        model: str = "",
        top_keywords: int = 10,
        clock: Any = time.time,
    ) -> None:
        self.caller = caller
        self.cache = cache if cache is not None else EmbeddingCache()
        self.router = router
        self.scenario = scenario
        self.model = model
        self.top_keywords = int(top_keywords)
        self.clock = clock

    # ---- 向量化 --------------------------------------------------------
    async def embed(
        self,
        texts: str | Sequence[str],
        *,
        use_cache: bool = True,
        model: str | None = None,
        prefix: str = TOPIC_PREFIX,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """把一段或多段文本向量化（命中缓存的文本不再调模型）。"""

        items = [str(texts)] if isinstance(texts, str) else [str(item) for item in texts]
        stamp = float(now if now is not None else self.clock())
        keys = [cache_key(item, prefix=prefix) for item in items]

        cached = [self.cache.get(key, now=stamp) is not None if use_cache else False for key in keys]
        produced, used_model, via = await self._embed_missing(items, model=model, use_cache=use_cache, **kwargs)
        vectors = [list(vector) for vector in produced]

        dims = {len(vector) for vector in vectors if vector}
        return {
            "texts": items,
            "vectors": vectors,
            "keys": keys,
            "cached": cached,
            "model": used_model or self.model,
            "via": via,
            "dim": max(dims) if dims else 0,
            "count": len(items),
        }

    async def _embed_missing(
        self, texts: Sequence[str], *, model: str | None = None, use_cache: bool = True, **kwargs: Any
    ) -> tuple[list[list[float]], str, str]:
        """调模型取向量；返回 ``(向量列表, 模型名, 取数途径)``。"""

        all_texts = [str(item) for item in texts]
        flat: list[str] = []
        flat_keys: list[str] = []
        vectors: list[list[float]] = []
        used_model = ""
        model_pending = False
        for item in all_texts:
            key = cache_key(item, prefix=TOPIC_PREFIX)
            hit = self.cache.get(key) if use_cache else None
            if hit:
                vectors.append(list(hit))
                continue
            flat.append(item)
            flat_keys.append(key)
        if flat:
            response = await self._call_model_embed(flat, model=model, **kwargs)
            used_model = str((response or {}).get("model", "")) if isinstance(response, Mapping) else ""
            produced = normalize_vectors(vector_from_response(response), expected=len(flat))
            if produced:
                vectors.extend(produced)
                if use_cache:
                    for item_index, key in enumerate(flat_keys):
                        if item_index < len(produced):
                            self.cache.set(key, produced[item_index])
            else:
                model_pending = True
        via = "model" if model_pending else "cache"
        if model_pending and self.router is not None:
            # 兼容 infra 的 rpc:model.embed 尚未返回向量本体的缺口
            result = await self.router.embed(list(flat), model=model)
            single = normalize_vector(getattr(result, "embedding", ()))
            if single:
                vectors.extend([single] * len(flat))
                if use_cache:
                    for key in flat_keys:
                        self.cache.set(key, single)
                used_model = used_model or str(getattr(result, "model", "") or "")
                via = "router"
                model_pending = False
        if model_pending and not vectors:
            raise DependencyMissing(
                "rpc:model.embed 未返回向量（返回体缺少 embedding/vectors）；"
                "请修 infra 的 rpc:model.embed 返回体，或给 TopicEmbedder 注入 router 兜底"
            )
        return normalize_vectors(vectors, expected=len(all_texts) or 1), used_model, via

    async def _call_model_embed(self, texts: Sequence[str], *, model: str | None = None, **kwargs: Any) -> Any:
        if self.caller is None:
            if self.router is not None:
                result = await self.router.embed(list(texts), model=model, **kwargs)
                return {
                    "model": str(getattr(result, "model", "") or ""),
                    "embedding": list(getattr(result, "embedding", ()) or ()),
                }
            raise DependencyMissing(f"rpc:topic.embed 需要 caller 才能调用 {RPC_MODEL_EMBED}")
        payload = {"texts": list(texts), "scenario": self.scenario, **kwargs}
        if model or self.model:
            payload["model"] = model or self.model
        return await self.caller(RPC_MODEL_EMBED, list(texts), **{k: v for k, v in payload.items() if k != "texts"})

    # ---- 相似度 --------------------------------------------------------
    async def similarity(
        self,
        left: Any,
        right: Any = None,
        *,
        use_embedding: bool = True,
        top: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """计算相似度：两侧可以是文本、向量或 ``{"vector"/"embedding"/"keywords"/"text"}`` 字典。"""

        left_vec, left_text, left_keywords = self._unpack(left)
        right_vec, right_text, right_keywords = self._unpack(right)

        if not left_keywords and left_text:
            left_keywords = extract_keywords(left_text, top=top or self.top_keywords)
        if not right_keywords and right_text:
            right_keywords = extract_keywords(right_text, top=top or self.top_keywords)

        if not left_vec and not right_vec and use_embedding and (left_text or right_text):
            embedded = await self.embed([left_text, right_text], **kwargs)
            left_vec, right_vec = embedded["vectors"][0], embedded["vectors"][1]

        cosine_score: float | None = None
        if left_vec and right_vec:
            cosine_score = cosine(left_vec, right_vec)
        lexical = jaccard(left_keywords, right_keywords)
        score = combined_score(cosine_score=cosine_score, jaccard_score=lexical, cosine_weight=COSINE_WEIGHT)
        method = "cosine" if cosine_score is not None else ("jaccard" if (left_keywords or right_keywords) else "none")
        return {
            "score": round(max(0.0, min(1.0, score)), 6),
            "cosine": None if cosine_score is None else round(cosine_score, 6),
            "jaccard": round(lexical, 6),
            "method": method,
            "left": {"text": left_text, "keywords": left_keywords, "dim": len(left_vec)},
            "right": {"text": right_text, "keywords": right_keywords, "dim": len(right_vec)},
        }

    async def best_match(
        self,
        text: Any,
        candidates: Sequence[Any],
        *,
        threshold: float = 0.45,
        use_embedding: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """在候选话题里找最像的一个（``rpc:topic.resolve`` 的归一化依据）。"""

        items = list(candidates or ())
        if not items:
            return {"index": -1, "score": 0.0, "matched": None, "scores": [], "threshold": float(threshold)}
        scores: list[float] = []
        details: list[dict[str, Any]] = []
        for candidate in items:
            result = await self.similarity(text, candidate, use_embedding=use_embedding, **kwargs)
            scores.append(result["score"])
            details.append(result)
        best = max(range(len(scores)), key=lambda index: scores[index])
        matched = items[best] if scores[best] >= float(threshold) else None
        return {
            "index": best if matched is not None else -1,
            "score": scores[best],
            "matched": matched,
            "scores": scores,
            "details": details,
            "threshold": float(threshold),
        }

    # ---- 内部 ----------------------------------------------------------
    @staticmethod
    def _unpack(value: Any) -> tuple[list[float], str, list[str]]:
        """把输入统一成 ``(向量, 文本, 关键词)``。"""

        if value is None:
            return [], "", []
        if isinstance(value, str):
            return [], value, []
        if isinstance(value, Mapping):
            vector = normalize_vector(value.get("vector") or value.get("embedding") or value.get("vectors") or [])
            text = str(value.get("text") or value.get("phrase") or value.get("title") or value.get("content") or "")
            keywords = [str(k) for k in (value.get("keywords") or [])]
            if not keywords and text:
                keywords = extract_keywords(text)
            return vector, text, keywords
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            values = list(value)
            if values and isinstance(values[0], (int, float)):
                return [float(item) for item in values], "", []
            return [], " ".join(str(item) for item in values), []
        return [], str(value), []


# --------------------------------------------------------------------------
# 处理器注册
# --------------------------------------------------------------------------
def register(registry: Any, embedder: TopicEmbedder | None = None, *, replace: bool = True) -> Any:
    """把本叶子的 2 个 ``rpc:`` 处理器注册进注册表。"""

    instance = embedder if embedder is not None else TopicEmbedder()

    async def topic_embed(
        texts: Any = None,
        *,
        text: str | None = None,
        use_cache: bool = True,
        model: str | None = None,
        prefix: str = TOPIC_PREFIX,
        now: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        value = texts if texts is not None else (text if text is not None else "")
        return await instance.embed(value, use_cache=use_cache, model=model, prefix=prefix, now=now, **kwargs)

    async def topic_similarity(
        left: Any = None,
        right: Any = None,
        *,
        use_embedding: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await instance.similarity(left, right, use_embedding=use_embedding, **kwargs)

    registry.register(RPC_EMBED, topic_embed, module=MODULE, replace=replace)
    registry.register(RPC_SIMILARITY, topic_similarity, module=MODULE, replace=replace)
    return instance


__all__ = [
    "COSINE_WEIGHT",
    "normalize_vectors",
    "MODULE",
    "RPC",
    "RPC_EMBED",
    "RPC_MODEL_EMBED",
    "RPC_SIMILARITY",
    "TOPIC_PREFIX",
    "TopicEmbedder",
    "register",
    "vector_from_response",
]
