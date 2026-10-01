"""grouppig.infra.token_budget.reporter —— 预算报告器（``rpc:token.report``）。

normify id: ``grouppig.infra.token-budget.reporter``。

记录每次核销的用量，按场景 / 模型聚合，供反思层（``grouppig.reflection``）与
预算优化使用。
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from grouppig.infra.runtime.usage import TokenUsage


@dataclass(frozen=True, slots=True)
class UsageEntry:
    """一次核销记录。"""

    ts: float
    scenario: str
    model: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    request_id: str = ""
    reservation_id: str = ""

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["total_tokens"] = self.total_tokens
        return data


class TokenReporter:
    """内存环形缓冲的用量报告器（MVP；后续可落库到 memory 层）。"""

    def __init__(self, *, max_entries: int = 5000, clock: Any = time.time) -> None:
        self.entries: deque[UsageEntry] = deque(maxlen=max_entries)
        self.clock = clock

    def record(
        self,
        usage: TokenUsage | Mapping[str, Any] | None = None,
        *,
        scenario: str = "",
        model: str = "",
        request_id: str = "",
        reservation_id: str = "",
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
    ) -> UsageEntry:
        if isinstance(usage, TokenUsage):
            prompt = usage.prompt_tokens
            completion = usage.completion_tokens
            model = model or usage.model
            scenario = scenario or usage.scenario
            request_id = request_id or usage.request_id
        elif isinstance(usage, Mapping):
            prompt = int(usage.get("prompt_tokens", 0) or 0)
            completion = int(usage.get("completion_tokens", 0) or 0)
            model = model or str(usage.get("model", ""))
            scenario = scenario or str(usage.get("scenario", ""))
            request_id = request_id or str(usage.get("request_id", ""))
        else:
            prompt = int(prompt_tokens or 0)
            completion = int(completion_tokens or 0)

        entry = UsageEntry(
            ts=float(self.clock()),
            scenario=scenario or "unknown",
            model=model,
            prompt_tokens=prompt,
            completion_tokens=completion,
            request_id=request_id,
            reservation_id=reservation_id,
        )
        self.entries.append(entry)
        return entry

    def report(self, *, since: float | None = None, scenario: str | None = None, limit: int = 20) -> dict[str, Any]:
        """``rpc:token.report`` —— 输出预算报告。"""

        rows = [
            e for e in self.entries if (since is None or e.ts >= since) and (scenario is None or e.scenario == scenario)
        ]
        by_scenario: dict[str, dict[str, Any]] = {}
        by_model: dict[str, dict[str, Any]] = {}
        prompt_total = completion_total = 0
        for entry in rows:
            prompt_total += entry.prompt_tokens
            completion_total += entry.completion_tokens
            for bucket, key in ((by_scenario, entry.scenario), (by_model, entry.model or "unknown")):
                slot = bucket.setdefault(
                    key, {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
                )
                slot["calls"] += 1
                slot["prompt_tokens"] += entry.prompt_tokens
                slot["completion_tokens"] += entry.completion_tokens
                slot["total_tokens"] += entry.total_tokens

        recent = [e.as_dict() for e in rows[-limit:]]
        return {
            "calls": len(rows),
            "prompt_tokens": prompt_total,
            "completion_tokens": completion_total,
            "total_tokens": prompt_total + completion_total,
            "since": since,
            "scenario_filter": scenario,
            "by_scenario": by_scenario,
            "by_model": by_model,
            "recent": recent,
            "generated_at": float(self.clock()),
        }

    def totals(self) -> tuple[int, int]:
        prompt = sum(e.prompt_tokens for e in self.entries)
        completion = sum(e.completion_tokens for e in self.entries)
        return prompt, completion

    def reset(self) -> None:
        self.entries.clear()

    def __len__(self) -> int:
        return len(self.entries)


_reporter = TokenReporter()


def get_reporter() -> TokenReporter:
    return _reporter


def set_reporter(reporter: TokenReporter) -> TokenReporter:
    global _reporter
    previous, _reporter = _reporter, reporter
    return previous


def report(*, since: float | None = None, scenario: str | None = None) -> dict[str, Any]:
    """``rpc:token.report`` 处理器。"""

    return _reporter.report(since=since, scenario=scenario)


__all__ = ["TokenReporter", "UsageEntry", "get_reporter", "report", "set_reporter"]
