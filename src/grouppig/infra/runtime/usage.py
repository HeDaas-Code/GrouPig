"""grouppig.infra.runtime.usage —— 模型 token 用量（codec 与 token-budget 共享的数据结构）。

normify id: ``grouppig.infra.runtime.usage``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class TokenUsage:
    """一次模型调用的 token 用量。"""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    model: str = ""
    scenario: str = ""
    request_id: str = ""

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def __add__(self, other: TokenUsage) -> TokenUsage:
        if not isinstance(other, TokenUsage):
            return NotImplemented
        return TokenUsage(
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            model=self.model or other.model,
            scenario=self.scenario or other.scenario,
            request_id=self.request_id or other.request_id,
        )

    def as_dict(self) -> dict[str, int | str]:
        data = asdict(self)
        data["total_tokens"] = self.total_tokens
        return data

    @classmethod
    def from_api(cls, usage: dict | None, *, model: str = "", scenario: str = "", request_id: str = "") -> TokenUsage:
        """从 OpenAI/OpenAI 风格响应的 ``usage`` 字段构造。"""

        usage = usage or {}
        return cls(
            prompt_tokens=int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0),
            completion_tokens=int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
            model=str(usage.get("model") or model),
            scenario=scenario,
            request_id=request_id,
        )


__all__ = ["TokenUsage"]
