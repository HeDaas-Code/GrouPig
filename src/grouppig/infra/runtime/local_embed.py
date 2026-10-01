"""grouppig.infra.runtime.local_embed —— 本地确定性嵌入传输（无网络、无额外依赖）。

用户令牌下没有任何 embedding 模型（``/embeddings`` 返回 403 model_not_allowed），
故 ``[model.tasks.embed]`` 指向 ``provider = "local"``，由本模块在本地算向量：

* 字符 n-gram（1–3 字）哈希到固定维度桶，带子线性 TF 权重，再做 L2 归一化；
* 与文本顺序无关、跨进程稳定（用 blake2b 而非 Python 的随机化 hash）；
* 同一段文本必然得到同一向量；共享字/词的短文本词表重叠度高、余弦相似度高。

它是**降级实现**：不具备真实语义泛化能力（近义不同字会被判为不相似）。
接入可用的 embedding 服务后，把 ``config/grouppig.toml`` 的
``[model.tasks.embed].provider`` 改回 ``a6api`` 即可。
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from typing import Any

from grouppig.infra.runtime.errors import TransportError

#: 向量维度（与测试用的哈希嵌入同一数量级，便于阈值复用）。
DEFAULT_DIM = 64

#: n-gram 长度范围。
NGRAM_RANGE = (1, 2, 3)


def _bucket(token: str, dim: int) -> int:
    """把 token 稳定映射到 [0, dim) 的桶（blake2b 不随进程随机化）。"""

    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % dim


def _units(text: str, *, dim: int = DEFAULT_DIM) -> list[str]:
    """切出字符 n-gram：ASCII 按小写词，CJK 按字符滑窗。"""

    value = " ".join(str(text or "").split()).lower()
    if not value:
        return []
    units: list[str] = [word for word in value.split(" ") if word.isascii() and word]
    cjk = "".join(ch for ch in value if not ch.isascii() and not ch.isspace())
    for size in range(NGRAM_RANGE[0], NGRAM_RANGE[1] + 1):
        for start in range(max(0, len(cjk) - size + 1)):
            units.append(cjk[start : start + size])
    return units or [value]


def local_embedding(text: str, *, dim: int = DEFAULT_DIM) -> tuple[float, ...]:
    """本地确定性嵌入：字符 n-gram 哈希词袋 + 子线性 TF + L2 归一化。"""

    units = _units(text, dim=dim)
    if not units:
        return tuple(0.0 for _ in range(dim))
    counts: dict[int, float] = {}
    for unit in units:
        index = _bucket(unit, dim)
        counts[index] = counts.get(index, 0.0) + 1.0
    # 子线性缩放：长文本不会因为字数多而压过短文本
    vector = [0.0] * dim
    for index, count in counts.items():
        vector[index] = 1.0 + math.log(count)
    norm = math.sqrt(sum(v * v for v in vector)) or 1.0
    return tuple(round(v / norm, 12) for v in vector)


class LocalEmbedTransport:
    """本地嵌入传输：只提供 ``/embeddings``；chat / classify 明确拒绝。"""

    def __init__(self, *, dim: int = DEFAULT_DIM, provider: str = "local") -> None:
        self.dim = int(dim)
        self.provider = provider
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    async def complete(
        self,
        payload: dict[str, Any],
        *,
        path: str = "/embeddings",
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]:
        """返回 OpenAI 兼容的 embeddings 响应（``data[].embedding``）。"""

        if not path.endswith("/embeddings"):
            raise TransportError(
                f"local 传输只提供 embeddings，不支持 {path}；chat / classify 请用 HTTP provider",
                retryable=False,
            )
        self.calls.append(dict(payload))
        raw = payload.get("input") or payload.get("input_texts") or []
        if isinstance(raw, str):
            items: Sequence[str] = [raw]
        else:
            items = [str(t) for t in raw]
        return {
            "model": str(payload.get("model") or "local-embed"),
            "data": [
                {"index": index, "embedding": list(local_embedding(text, dim=self.dim))}
                for index, text in enumerate(items)
            ],
            "usage": {"prompt_tokens": sum(len(t) for t in items), "completion_tokens": 0},
        }

    async def aclose(self) -> None:
        self.closed = True


__all__ = ["DEFAULT_DIM", "LocalEmbedTransport", "local_embedding"]
