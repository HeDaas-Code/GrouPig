"""grouppig.infra.token_budget.meter —— Token 计量器（``rpc:token.reserve`` / ``rpc:token.consume``）。

normify id: ``grouppig.infra.token-budget.meter``；按设计依赖
``rpc:token.policy``（读预算策略）与 ``rpc:token.report``（写报告）。

流程：生成前 ``reserve``（按策略夹取输入/输出预算，登记预留）→ 生成后 ``consume``
（核销真实 usage、写报告、更新账本）。超预算时按策略 ``on_exceed`` 决定夹取还是拒绝。
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.runtime.errors import GrouPigError, TokenBudgetExceeded
from grouppig.infra.runtime.usage import TokenUsage
from grouppig.infra.token_budget.policy import BudgetPolicy, get_policy
from grouppig.infra.token_budget.reporter import TokenReporter, get_reporter


@dataclass(frozen=True, slots=True)
class Reservation:
    """一次生成前的预算预留。"""

    reservation_id: str
    scenario: str
    input_tokens: int
    output_tokens: int
    model: str = ""
    request_id: str = ""
    clamped: bool = False
    requested_input_tokens: int = 0
    requested_output_tokens: int = 0
    policy: Mapping[str, Any] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return self.input_tokens + self.output_tokens

    def as_dict(self) -> dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "scenario": self.scenario,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total": self.total,
            "model": self.model,
            "request_id": self.request_id,
            "clamped": self.clamped,
            "requested_input_tokens": self.requested_input_tokens,
            "requested_output_tokens": self.requested_output_tokens,
            "policy": dict(self.policy),
        }


@dataclass(frozen=True, slots=True)
class BudgetSnapshot:
    """账本快照。"""

    reserved_input: int = 0
    reserved_output: int = 0
    consumed_prompt: int = 0
    consumed_completion: int = 0
    calls: int = 0
    outstanding: int = 0
    daily_limit: int = 0
    over_budget_calls: int = 0

    @property
    def consumed_total(self) -> int:
        return self.consumed_prompt + self.consumed_completion

    @property
    def remaining(self) -> int | None:
        if self.daily_limit <= 0:
            return None
        return max(0, self.daily_limit - self.consumed_total - self.reserved_input - self.reserved_output)

    def as_dict(self) -> dict[str, Any]:
        return {
            "reserved_input": self.reserved_input,
            "reserved_output": self.reserved_output,
            "consumed_prompt": self.consumed_prompt,
            "consumed_completion": self.consumed_completion,
            "consumed_total": self.consumed_total,
            "calls": self.calls,
            "outstanding": self.outstanding,
            "daily_limit": self.daily_limit,
            "remaining": self.remaining,
            "over_budget_calls": self.over_budget_calls,
        }


class TokenMeter:
    """预留 / 核销 token 预算，并维护进程内账本。"""

    def __init__(
        self,
        *,
        policy_provider: Any = None,
        reporter: TokenReporter | None = None,
        logger: Any | None = None,
        config: Any | None = None,
        daily_limit: int = 0,
    ) -> None:
        self.config = config
        self.logger = logger
        self.reporter = reporter if reporter is not None else get_reporter()
        self._policy_provider = policy_provider
        self._daily_limit_override = daily_limit
        self._reservations: dict[str, Reservation] = {}
        self._reserved_input = 0
        self._reserved_output = 0
        self._consumed_prompt = 0
        self._consumed_completion = 0
        self._calls = 0
        self._over_budget = 0

    # ---- 策略 ----------------------------------------------------------
    def policy(self, scenario: str) -> BudgetPolicy:
        if self._policy_provider is not None:
            return self._policy_provider(scenario)
        return get_policy(scenario, config=self.config)

    @property
    def daily_limit(self) -> int:
        if self._daily_limit_override:
            return self._daily_limit_override
        if self.config is not None:
            return int(self.config.get("token.daily_limit", 0) or 0)
        return 0

    # ---- 预留 ----------------------------------------------------------
    async def reserve(
        self,
        scenario: str = "chat",
        *,
        estimated_input_tokens: int = 0,
        estimated_output_tokens: int | None = None,
        model: str = "",
        request_id: str = "",
    ) -> Reservation:
        """``rpc:token.reserve`` —— 预留 token 预算。"""

        policy = self.policy(scenario)
        requested_input = max(0, int(estimated_input_tokens))
        requested_output = (
            policy.max_output_tokens if estimated_output_tokens is None else max(0, int(estimated_output_tokens))
        )
        granted_input = policy.clamp_input(requested_input)
        granted_output = policy.clamp_output(requested_output)
        clamped = (granted_input, granted_output) != (requested_input, requested_output)

        remaining = self.snapshot().remaining
        if remaining is not None and remaining <= 0:
            if policy.on_exceed == "raise":
                raise TokenBudgetExceeded(f"场景 {scenario} 的每日 token 预算已耗尽（limit={self.daily_limit}）")
            granted_input = granted_output = 0
            clamped = True
            self._log("warning", "token.budget_exhausted", scenario=scenario, daily_limit=self.daily_limit)

        reservation = Reservation(
            reservation_id=f"rsv-{uuid.uuid4().hex[:12]}",
            scenario=scenario,
            input_tokens=granted_input,
            output_tokens=granted_output,
            model=model,
            request_id=request_id,
            clamped=clamped,
            requested_input_tokens=requested_input,
            requested_output_tokens=requested_output,
            policy=policy.as_dict(),
        )
        self._reservations[reservation.reservation_id] = reservation
        self._reserved_input += granted_input
        self._reserved_output += granted_output
        if clamped:
            self._log(
                "debug",
                "token.reserve_clamped",
                scenario=scenario,
                requested=(requested_input, requested_output),
                granted=(granted_input, granted_output),
            )
        return reservation

    # ---- 核销 ----------------------------------------------------------
    async def consume(
        self,
        reservation_id: str,
        usage: TokenUsage | Mapping[str, Any] | None = None,
        *,
        scenario: str = "",
        model: str = "",
        request_id: str = "",
    ) -> BudgetSnapshot:
        """``rpc:token.consume`` —— 核销实际消耗并写报告。"""

        reservation = self._reservations.pop(reservation_id, None)
        if reservation is None:
            raise GrouPigError(f"未知的预留 id：{reservation_id!r}（可能已核销或已释放）")
        self._reserved_input -= reservation.input_tokens
        self._reserved_output -= reservation.output_tokens

        if isinstance(usage, TokenUsage):
            prompt, completion = usage.prompt_tokens, usage.completion_tokens
            model = model or usage.model
        elif isinstance(usage, Mapping):
            prompt = int(usage.get("prompt_tokens", 0) or 0)
            completion = int(usage.get("completion_tokens", 0) or 0)
            model = model or str(usage.get("model", ""))
        else:
            prompt = completion = 0

        scenario = scenario or reservation.scenario
        model = model or reservation.model
        request_id = request_id or reservation.request_id

        self._consumed_prompt += prompt
        self._consumed_completion += completion
        self._calls += 1

        # 只有真正突破策略上限才算超预算；估算偏小导致略高于预留不算（记 debug 便于调参）
        policy = reservation.policy or {}
        max_input = int(policy.get("max_input_tokens", reservation.input_tokens))
        max_output = int(policy.get("max_output_tokens", reservation.output_tokens))
        if prompt > max_input or completion > max_output:
            self._over_budget += 1
            self._log(
                "warning",
                "token.over_budget",
                scenario=scenario,
                policy_limits=(max_input, max_output),
                actual=(prompt, completion),
            )
        elif prompt > reservation.input_tokens or completion > reservation.output_tokens:
            self._log(
                "debug",
                "token.estimate_low",
                scenario=scenario,
                reserved=(reservation.input_tokens, reservation.output_tokens),
                actual=(prompt, completion),
            )

        self.reporter.record(
            TokenUsage(
                prompt_tokens=prompt,
                completion_tokens=completion,
                model=model,
                scenario=scenario,
                request_id=request_id,
            ),
            reservation_id=reservation_id,
        )
        snapshot = self.snapshot()
        self._log("debug", "token.consumed", scenario=scenario, model=model, prompt=prompt, completion=completion)
        return snapshot

    def release(self, reservation_id: str) -> bool:
        """放弃预留（调用失败、未产生消耗时）。"""

        reservation = self._reservations.pop(reservation_id, None)
        if reservation is None:
            return False
        self._reserved_input -= reservation.input_tokens
        self._reserved_output -= reservation.output_tokens
        return True

    # ---- 账本 ----------------------------------------------------------
    def snapshot(self) -> BudgetSnapshot:
        return BudgetSnapshot(
            reserved_input=max(0, self._reserved_input),
            reserved_output=max(0, self._reserved_output),
            consumed_prompt=self._consumed_prompt,
            consumed_completion=self._consumed_completion,
            calls=self._calls,
            outstanding=len(self._reservations),
            daily_limit=self.daily_limit,
            over_budget_calls=self._over_budget,
        )

    def reset(self) -> None:
        self._reservations.clear()
        self._reserved_input = self._reserved_output = 0
        self._consumed_prompt = self._consumed_completion = 0
        self._calls = self._over_budget = 0

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        try:
            self.logger.log(level, event, **fields)
        except Exception:  # pragma: no cover
            pass


_meter: TokenMeter | None = None


def get_meter() -> TokenMeter:
    global _meter
    if _meter is None:
        _meter = TokenMeter()
    return _meter


def set_meter(meter: TokenMeter | None) -> TokenMeter | None:
    global _meter
    previous, _meter = _meter, meter
    return previous


__all__ = ["BudgetSnapshot", "Reservation", "TokenMeter", "get_meter", "set_meter"]
