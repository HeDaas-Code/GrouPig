"""grouppig.infra.model_gateway.retry —— 重试 / 超时降级 / 模型切换（``rpc:model.retry``）。

normify id: ``grouppig.infra.model-gateway.retry``；按设计依赖 ``rpc:config.get``
读取 ``[model.retry]`` 参数（见 :func:`policy_from_config`）。

退避公式：``min(max_delay, base_delay * multiplier ** (attempt - 1))``，再叠加
``±jitter`` 比例的抖动；每次尝试有 ``timeout`` 秒上限。``fallback_models`` 按顺序
在主模型失败后切换（降级）。
"""

from __future__ import annotations

import asyncio
import inspect
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from grouppig.infra.runtime.errors import ModelCallError, ModelTimeoutError

Operation = Callable[..., Awaitable[Any]]
AttemptHook = Callable[[int, str | None, BaseException | None, float], Any]


@dataclass(frozen=True)
class RetryPolicy:
    """重试与降级策略。"""

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 8.0
    multiplier: float = 2.0
    jitter: float = 0.1
    timeout: float = 30.0
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def delay_for(self, attempt: int, *, rand: Callable[[], float] = random.random) -> float:
        if attempt <= 1:
            return 0.0
        raw = self.base_delay * (self.multiplier ** (attempt - 2))
        delay = min(self.max_delay, raw)
        if self.jitter:
            spread = delay * self.jitter
            delay = max(0.0, delay + (rand() * 2 - 1) * spread)
        return delay

    def with_overrides(self, **kwargs: Any) -> RetryPolicy:
        clean = {k: v for k, v in kwargs.items() if v is not None}
        return replace(self, **clean) if clean else self

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "base_delay": self.base_delay,
            "max_delay": self.max_delay,
            "multiplier": self.multiplier,
            "jitter": self.jitter,
            "timeout": self.timeout,
        }


@dataclass
class RetryResult:
    """重试执行结果。"""

    value: Any = None
    attempts: int = 0
    model: str | None = None
    elapsed_ms: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.attempts > 0 and not self.errors or self.value is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "attempts": self.attempts,
            "model": self.model,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "errors": list(self.errors),
            "has_value": self.value is not None,
        }


def policy_from_config(config: Any, task: str | None = None) -> RetryPolicy:
    """``rpc:config.get`` → :class:`RetryPolicy`（读 ``[model.retry]``）。"""

    section = config.section("model.retry")
    policy = RetryPolicy(
        max_attempts=int(section.get("max_attempts", 3)),
        base_delay=float(section.get("base_delay", 0.5)),
        max_delay=float(section.get("max_delay", 8.0)),
        multiplier=float(section.get("multiplier", 2.0)),
        jitter=float(section.get("jitter", 0.1)),
        timeout=float(section.get("timeout", 30.0)),
    )
    if task:
        task_section = config.section(f"model.tasks.{task}")
        if task_section.get("max_attempts") is not None:
            policy = policy.with_overrides(max_attempts=int(task_section["max_attempts"]))
        if task_section.get("timeout") is not None:
            policy = policy.with_overrides(timeout=float(task_section["timeout"]))
    return policy


def _arity(operation: Operation) -> int:
    try:
        sig = inspect.signature(operation)
    except (TypeError, ValueError):  # pragma: no cover - 内建 / C 扩展
        return 2
    count = 0
    for param in sig.parameters.values():
        if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
            count += 1
        elif param.kind is param.VAR_POSITIONAL:
            return max(count, 2)
    return count


def _is_retryable(exc: BaseException, policy: RetryPolicy) -> bool:
    if getattr(exc, "retryable", True) is False:
        return False
    return isinstance(exc, policy.retry_on)


async def _call_operation(operation: Operation, attempt: int, model: str | None) -> Any:
    arity = _arity(operation)
    if arity == 0:
        return await operation()
    if arity == 1:
        return await operation(model)
    return await operation(attempt, model)


async def execute_with_retry(
    operation: Operation,
    *,
    policy: RetryPolicy | None = None,
    fallback_models: Sequence[str] = (),
    on_attempt: AttemptHook | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    task: str = "",
    primary_model: str | None = None,
) -> RetryResult:
    """执行 ``operation``，失败重试、超时降级、耗尽后切换 ``fallback_models``。"""

    policy = policy or RetryPolicy()
    sleep = sleep or asyncio.sleep
    models: list[str | None] = [primary_model, *fallback_models] if primary_model else list(fallback_models) or [None]
    started = time.perf_counter()
    attempts = 0
    errors: list[str] = []
    last_error: BaseException | None = None

    for model_index, model in enumerate(models):
        for attempt_in_model in range(1, policy.max_attempts + 1):
            attempts += 1
            attempt_started = time.perf_counter()
            try:
                if policy.timeout and policy.timeout > 0:
                    value = await asyncio.wait_for(_call_operation(operation, attempts, model), timeout=policy.timeout)
                else:
                    value = await _call_operation(operation, attempts, model)
            except TimeoutError as exc:
                last_error = ModelTimeoutError(
                    f"模型调用超时（>{policy.timeout}s，第 {attempts} 次尝试）", attempts=attempts, last_error=exc
                )
                errors.append(f"attempt {attempts} ({model or 'default'}): timeout")
                if on_attempt:
                    _fire(on_attempt, attempts, model, last_error, (time.perf_counter() - attempt_started) * 1000)
            except Exception as exc:
                last_error = exc
                errors.append(f"attempt {attempts} ({model or 'default'}): {exc!r}")
                if on_attempt:
                    _fire(on_attempt, attempts, model, exc, (time.perf_counter() - attempt_started) * 1000)
                if not _is_retryable(exc, policy):
                    raise
            else:
                if on_attempt:
                    _fire(on_attempt, attempts, model, None, (time.perf_counter() - attempt_started) * 1000)
                return RetryResult(
                    value=value,
                    attempts=attempts,
                    model=model,
                    elapsed_ms=(time.perf_counter() - started) * 1000,
                    errors=errors,
                )

            if attempt_in_model < policy.max_attempts:
                await sleep(policy.delay_for(attempt_in_model + 1))
        if model_index < len(models) - 1:
            continue

    message = f"模型调用失败，已尝试 {attempts} 次"
    if task:
        message += f"（task={task}）"
    if last_error is not None:
        message += f"：{last_error!r}"
    raise ModelCallError(message, attempts=attempts, last_error=last_error)


def _fire(hook: AttemptHook, attempt: int, model: str | None, error: BaseException | None, elapsed_ms: float) -> None:
    try:
        hook(attempt, model, error, elapsed_ms)
    except Exception:  # pragma: no cover - 观测钩子不得影响主流程
        pass


async def retry(
    operation: Operation,
    *,
    policy: RetryPolicy | None = None,
    config: Any | None = None,
    task: str = "",
    model: str | None = None,
    fallback_models: Sequence[str] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    on_attempt: AttemptHook | None = None,
) -> RetryResult:
    """``rpc:model.retry`` —— 执行重试降级。

    ``operation`` 接收 ``(attempt: int, model: str | None)``（按签名自适应 0/1/2 参数）；
    ``policy`` 缺省时从 ``config`` 的 ``[model.retry]`` 读取。
    """

    if policy is None and config is not None:
        policy = policy_from_config(config, task or None)
    return await execute_with_retry(
        operation,
        policy=policy,
        fallback_models=tuple(fallback_models or ()),
        on_attempt=on_attempt,
        sleep=sleep,
        task=task,
        primary_model=model,
    )


__all__ = [
    "AttemptHook",
    "Operation",
    "RetryPolicy",
    "RetryResult",
    "execute_with_retry",
    "policy_from_config",
    "retry",
]
