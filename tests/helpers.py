"""测试辅助：可编程的假模型传输层 + 设计树动态事实（tree.json 推导）。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from grouppig.infra.runtime.errors import TransportError


class FakeTransport:
    """可编程的假模型传输层（记录 payload，可注入失败 / 延迟）。"""

    def __init__(
        self,
        *,
        reply: str = "喵",
        failures: int = 0,
        error: BaseException | None = None,
        delay: float = 0.0,
        usage: dict[str, int] | None = None,
        embedding: Sequence[float] = (0.1, 0.2, 0.3),
        classify_text: str | None = None,
    ) -> None:
        self.reply = reply
        self.remaining_failures = failures
        self.error = error or TransportError("upstream 503", status=503)
        self.delay = delay
        self.usage = usage if usage is not None else {"prompt_tokens": 12, "completion_tokens": 7}
        self.embedding = tuple(embedding)
        self.classify_text = classify_text
        self.calls: list[dict[str, Any]] = []
        self.paths: list[str] = []
        self.closed = False

    async def complete(
        self,
        payload: dict[str, Any],
        *,
        path: str = "/chat/completions",
        timeout: float | None = None,
        provider: str = "",
    ) -> dict[str, Any]:
        self.calls.append(dict(payload))
        self.paths.append(path)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise self.error
        model = str(payload.get("model", ""))
        if path.endswith("/embeddings"):
            return {
                "model": model,
                "data": [{"embedding": list(self.embedding)}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 0},
            }
        content = (
            self.classify_text
            if (self.classify_text is not None and "候选标签" in str(payload.get("messages")))
            else self.reply
        )
        return {
            "model": model,
            "choices": [{"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
            "usage": dict(self.usage),
        }

    async def aclose(self) -> None:
        self.closed = True


# ---- 设计树事实（全部从 normify-grouppig/tree.json 动态推导，不写死任何计数） ----
@dataclass(frozen=True)
class DesignFacts:
    """设计树的动态事实视图。"""

    tree_path: Path
    api_index: dict[str, str]
    modules: dict[str, dict[str, Any]]

    @property
    def module_ids(self) -> frozenset[str]:
        return frozenset(self.modules)

    @property
    def container_ids(self) -> frozenset[str]:
        """有子模块的模块（容器）。"""

        return frozenset(
            mid for mid in self.modules if any(spec.get("parent") == mid for spec in self.modules.values())
        )

    @property
    def leaf_ids(self) -> frozenset[str]:
        return self.module_ids - self.container_ids

    @property
    def counts(self) -> dict[str, int]:
        def prefix(proto: str) -> int:
            return sum(1 for name in self.api_index if name.startswith(f"{proto}:"))

        return {
            "names": len(self.api_index),
            "rpc": prefix("rpc"),
            "kafka": prefix("kafka"),
            "mysql": prefix("mysql"),
            "modules": len(self.module_ids),
            "containers": len(self.container_ids),
            "leaves": len(self.leaf_ids),
        }

    @property
    def topics(self) -> set[str]:
        return {name for name in self.api_index if name.startswith("kafka:")}

    @property
    def tables(self) -> set[str]:
        return {name for name in self.api_index if name.startswith("mysql:")}

    def names_of(self, scope: str) -> set[str]:
        """归属 scope 模块子树（含自身）的全部契约名字。"""

        return {name for name, owner in self.api_index.items() if owner == scope or owner.startswith(f"{scope}.")}

    def owner_of(self, name: str) -> str | None:
        return self.api_index.get(name)


def tree_json_path() -> Path:
    """设计树 tree.json 路径（跟随 GROUPPIG_API_INDEX 环境覆盖）。"""

    from grouppig.infra.runtime import contract

    return contract.api_index_path().parent / "tree.json"


@lru_cache(maxsize=1)
def design_tree_facts() -> DesignFacts:
    """加载并缓存 normify-grouppig/tree.json 的动态事实。"""

    path = tree_json_path()
    tree = json.loads(path.read_text(encoding="utf-8"))
    return DesignFacts(
        tree_path=path,
        api_index={str(k): str(v) for k, v in tree["api_index"].items()},
        modules={str(k): dict(v) for k, v in tree["modules"].items()},
    )
