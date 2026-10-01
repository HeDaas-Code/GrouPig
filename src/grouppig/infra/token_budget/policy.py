"""grouppig.infra.token_budget.policy —— 预算策略（``rpc:token.policy``）。

normify id: ``grouppig.infra.token-budget.policy``。

维护各场景的 token 预算：闲聊低配、讨论高配。配置来自 ``[token.policies.<scenario>]``，
未配置的场景回落到内置默认值。
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

DEFAULT_POLICIES: dict[str, dict[str, Any]] = {
    "smalltalk": {"max_input_tokens": 800, "max_output_tokens": 120, "on_exceed": "clamp"},
    "chat": {"max_input_tokens": 1600, "max_output_tokens": 320, "on_exceed": "clamp"},
    "discussion": {"max_input_tokens": 4000, "max_output_tokens": 800, "on_exceed": "clamp"},
    "reflection": {"max_input_tokens": 8000, "max_output_tokens": 1500, "on_exceed": "clamp"},
    "classify": {"max_input_tokens": 2000, "max_output_tokens": 64, "on_exceed": "clamp"},
    "embed": {"max_input_tokens": 4000, "max_output_tokens": 0, "on_exceed": "clamp"},
}
FALLBACK_SCENARIO = "chat"


@dataclass(frozen=True)
class BudgetPolicy:
    """单个场景的预算策略。"""

    scenario: str
    max_input_tokens: int
    max_output_tokens: int
    on_exceed: str = "clamp"
    daily_limit: int = 0

    @property
    def total(self) -> int:
        return self.max_input_tokens + self.max_output_tokens

    def clamp_input(self, estimate: int) -> int:
        return max(0, min(int(estimate), self.max_input_tokens))

    def clamp_output(self, estimate: int | None) -> int:
        wanted = self.max_output_tokens if estimate is None else int(estimate)
        return max(0, min(wanted, self.max_output_tokens))

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "max_input_tokens": self.max_input_tokens,
            "max_output_tokens": self.max_output_tokens,
            "on_exceed": self.on_exceed,
            "daily_limit": self.daily_limit,
            "total": self.total,
        }


def estimate_tokens(text: str | None) -> int:
    """启发式 token 估算：CJK 字符≈1 token，其余≈4 字符 1 token。

    不追求与具体分词器完全一致，只用于预留预算；真实消耗以模型返回的 usage 为准。
    """

    if not text:
        return 0
    cjk = 0
    for ch in text:
        code = ord(ch)
        if (
            0x3040 <= code <= 0x30FF
            or 0x3400 <= code <= 0x4DBF
            or 0x4E00 <= code <= 0x9FFF
            or 0xF900 <= code <= 0xFAFF
            or 0xFF00 <= code <= 0xFFEF
        ):
            cjk += 1
    other = len(text) - cjk
    return cjk + math.ceil(other / 4)


def estimate_messages_tokens(messages: Sequence[Mapping[str, Any]]) -> int:
    """估算一段消息列表的输入 token（含每条消息的固定开销）。"""

    total = 0
    for message in messages or ():
        content = message.get("content", "")
        if isinstance(content, (list, tuple)):
            content = "".join(str(part.get("text", "")) for part in content if isinstance(part, Mapping))
        total += estimate_tokens(str(content)) + 4
    return total


_policy_config: Any | None = None


def configure_policies(config: Any | None) -> None:
    """绑定配置对象，使 :func:`get_policy` 能读到 ``[token.policies]``。"""

    global _policy_config
    _policy_config = config


def _spec_for(scenario: str, config: Any | None) -> dict[str, Any]:
    spec: dict[str, Any] = dict(DEFAULT_POLICIES.get(scenario, DEFAULT_POLICIES[FALLBACK_SCENARIO]))
    if config is not None:
        merged = {**DEFAULT_POLICIES.get(scenario, {}), **config.section(f"token.policies.{scenario}")}
        if merged:
            spec = merged
        daily = config.get("token.daily_limit")
        if daily is not None:
            spec = {**spec, "daily_limit": int(daily)}
    return spec


def get_policy(scenario: str = FALLBACK_SCENARIO, *, config: Any | None = None) -> BudgetPolicy:
    """``rpc:token.policy`` —— 读取预算策略。"""

    cfg = config if config is not None else _policy_config
    scenario = scenario or FALLBACK_SCENARIO
    spec = _spec_for(scenario, cfg)
    return BudgetPolicy(
        scenario=scenario,
        max_input_tokens=int(spec.get("max_input_tokens", DEFAULT_POLICIES[FALLBACK_SCENARIO]["max_input_tokens"])),
        max_output_tokens=int(spec.get("max_output_tokens", DEFAULT_POLICIES[FALLBACK_SCENARIO]["max_output_tokens"])),
        on_exceed=str(spec.get("on_exceed", "clamp")),
        daily_limit=int(spec.get("daily_limit", 0) or 0),
    )


def all_policies(config: Any | None = None) -> dict[str, BudgetPolicy]:
    """全部场景策略（内置默认 ∪ 配置覆盖）。"""

    cfg = config if config is not None else _policy_config
    scenarios = set(DEFAULT_POLICIES)
    if cfg is not None:
        scenarios |= set(cfg.sections("token.policies"))
    return {name: get_policy(name, config=cfg) for name in sorted(scenarios)}


def with_policy(policy: BudgetPolicy, **overrides: Any) -> BudgetPolicy:
    clean = {k: v for k, v in overrides.items() if v is not None}
    return replace(policy, **clean) if clean else policy


__all__ = [
    "DEFAULT_POLICIES",
    "FALLBACK_SCENARIO",
    "BudgetPolicy",
    "all_policies",
    "configure_policies",
    "estimate_messages_tokens",
    "estimate_tokens",
    "get_policy",
    "with_policy",
]
