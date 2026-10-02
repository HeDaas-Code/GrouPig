"""grouppig.infra.model_gateway.router —— 模型路由器。

对外 rpc：rpc:model.chat / rpc:model.embed / rpc:model.classify / rpc:model.system1
（LAY A System-1 一次前向回答多个类型化问题，置信度不足时升级对话模型）。

normify id: ``grouppig.infra.model-gateway.router``；按设计依赖
``rpc:model.encode``（请求编码）与 ``rpc:model.retry``（失败重试），并在两侧接上
``rpc:token.reserve`` / ``rpc:token.consume`` 做预算控制。

流程：读任务配置 → 预留预算 → codec 编码 → retry(transport) → codec 解码 → 核销预算。
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from grouppig.infra.model_gateway.codec import ModelRequest, ModelResponse, decode_response, encode_request
from grouppig.infra.model_gateway.retry import RetryPolicy, RetryResult, execute_with_retry, policy_from_config
from grouppig.infra.runtime import laya_system1
from grouppig.infra.runtime import transport as transport_module
from grouppig.infra.runtime.errors import ModelCallError, TransportError
from grouppig.infra.runtime.laya_system1 import (
    normalize_answers,
    normalize_classification,
    normalize_usage,
    redact,
)
from grouppig.infra.runtime.usage import TokenUsage
from grouppig.infra.token_budget.policy import estimate_messages_tokens, estimate_tokens

DEFAULT_SCENARIOS = {"chat": "chat", "embed": "embed", "classify": "classify"}

#: system1 的置信度聚合口径：取所有问题 confidence 的最小值（任一问题不确定就升级）。
SYSTEM1_AGGREGATE = "min"
#: system1 默认升级阈值（低于它就把裁决交给对话模型）。
SYSTEM1_ESCALATE_BELOW = 0.4
#: system1 升级时交给对话模型的系统提示词。
#: 从对话模型回复里抓 JSON 对象（升级裁决的尽力解析）。
_JSON_OBJECT = re.compile(r"\{.*\}", re.S)

SYSTEM1_ESCALATE_PROMPT = (
    "你是裁决器。下面给出若干「类型化问题」以及本地决策模型给出的概率分布。"
    '请对每个 qid 给出最终答案，只输出一个 JSON 对象，形如 {"qid": 答案}，不要解释。'
    "type=choice 的答案必须是该问题的候选之一；type=score 的答案取 0~1 之间的数；"
    'type=noul 的答案写 "noul"（表示无合适选项）。'
)


class ModelRouter:
    """按任务路由到对话 / 嵌入 / 分类模型。

    异常契约（选项 ②：保留裸 TransportError，并在此显式声明）：
    - 不可重试的传输错误（LAY A 的 401/422、provider 未配 base_url 等协议/配置级失败）以
      TransportError 原样抛出（带 status / retryable / body），不做二次包装；
    - 重试与降级全部耗尽、以及解码/归一化失败，一律抛 ModelCallError（带 attempts 与 last_error）。
    """

    def __init__(
        self,
        config: Any,
        *,
        transport: Any | None = None,
        logger: Any | None = None,
        meter: Any | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.meter = meter
        self.environ = environ
        self._transports: dict[str, Any] = {}
        if transport is not None:
            self._transports["*"] = transport
        self._stats = {
            "chat": 0,
            "embed": 0,
            "classify": 0,
            "errors": 0,
            "tokens": 0,
            # 可观测性计数：降级 / LAY A 不可用 / 解码失败，health() 与监控据此发现模型层劣化
            "fallback": 0,
            "laya_unavailable": 0,
            "decode_failed": 0,
        }

    # ---- 配置 ----------------------------------------------------------
    def task_spec(self, task: str) -> dict[str, Any]:
        spec = self.config.sections("model.tasks").get(task)
        if spec is None:
            raise ModelCallError(f"配置缺少 [model.tasks.{task}]", attempts=0)
        return spec

    def provider_for(self, task: str, spec: Mapping[str, Any] | None = None) -> str:
        spec = spec if spec is not None else self.task_spec(task)
        return str(spec.get("provider") or self.config.get("model.default_provider", ""))

    def retry_policy(self, task: str) -> RetryPolicy:
        return policy_from_config(self.config, task)

    def _transport(self, provider: str) -> Any:
        normalized = provider.strip().lower()
        if "*" in self._transports and normalized not in (
            transport_module.LOCAL_EMBED_PROVIDER,
            transport_module.LAYA_PROVIDER,
        ):
            # 注入的 transport 常用于测试或整链路替换，但不该顶掉需要专属协议的 provider：
            # 本地嵌入（否则 embed 打到 chat 服务商的 /embeddings，实测 403）与
            # LAY A（/v1/systemone 非 OpenAI 兼容，服务商模型名会被本地白名单拒绝）。
            # 需要覆盖它们时显式 set_transport(t, provider="local"/"laya")。
            return self._transports["*"]
        if provider not in self._transports:
            self._transports[provider] = transport_module.build_transport(
                self.config, provider=provider, environ=dict(self.environ) if self.environ else None
            )
        return self._transports[provider]

    def set_transport(self, transport: Any, *, provider: str = "*") -> None:
        self._transports[provider] = transport

    # ---- 对外接口 ------------------------------------------------------
    async def chat(
        self,
        messages: Sequence[Mapping[str, Any]] | str,
        *,
        model: str | None = None,
        scenario: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        request_id: str = "",
        provider: str | None = None,
        **params: Any,
    ) -> ModelResponse:
        """``rpc:model.chat`` —— 调用对话模型。"""

        normalized = _normalize_messages(messages)
        spec = self.task_spec("chat")
        merged = {
            "temperature": spec.get("temperature") if temperature is None else temperature,
            "max_tokens": spec.get("max_tokens") if max_tokens is None else max_tokens,
            **params,
        }
        request = ModelRequest(
            task="chat",
            model=model or str(spec.get("model", "")),
            provider=provider or self.provider_for("chat", spec),
            messages=normalized,
            params={k: v for k, v in merged.items() if v is not None},
            request_id=request_id,
        )
        return await self._execute(request, scenario=scenario or "chat")

    async def embed(
        self,
        texts: Sequence[str] | str,
        *,
        model: str | None = None,
        scenario: str | None = None,
        request_id: str = "",
        provider: str | None = None,
        **params: Any,
    ) -> ModelResponse:
        """``rpc:model.embed`` —— 调用嵌入模型。"""

        items = (texts,) if isinstance(texts, str) else tuple(str(t) for t in texts)
        if not items:
            raise ModelCallError("嵌入请求的 texts 不能为空", attempts=0)
        spec = self.task_spec("embed")
        request = ModelRequest(
            task="embed",
            model=model or str(spec.get("model", "")),
            provider=provider or self.provider_for("embed", spec),
            input_texts=items,
            params=dict(params),
            request_id=request_id,
        )
        return await self._execute(request, scenario=scenario or "embed")

    async def classify(
        self,
        text: str,
        labels: Sequence[str],
        *,
        model: str | None = None,
        scenario: str | None = None,
        request_id: str = "",
        provider: str | None = None,
        **params: Any,
    ) -> ModelResponse:
        """``rpc:model.classify`` —— 调用轻量分类模型。"""

        spec = self.task_spec("classify")
        request = ModelRequest(
            task="classify",
            model=model or str(spec.get("model", "")),
            provider=provider or self.provider_for("classify", spec),
            messages=({"role": "user", "content": text},),
            labels=tuple(labels),
            params={"temperature": spec.get("temperature", 0.0), "max_tokens": spec.get("max_tokens"), **params},
            request_id=request_id,
        )
        return await self._execute(request, scenario=scenario or "classify")

    async def system1(
        self,
        state: Any,
        questions: Mapping[str, Mapping[str, Any]],
        *,
        model: str = "auto",
        escalate_below: float = SYSTEM1_ESCALATE_BELOW,
        scenario: str | None = None,
        request_id: str = "",
    ) -> ModelResponse:
        """rpc:model.system1 —— 一次前向回答多个类型化问题，置信度不足时升级对话模型。

        - state 接受 dict 或 str（任意可 JSON 序列化的情境描述）；questions 是
          {qid: {type, instructions, criteria}}，type ∈ choice / score / noul。
        - **一次 HTTP**：所有问题放进同一个请求体（实测 1 问约 169ms、12 问约 527ms，逐问
          调用等于自毁）；请求体由 laya_system1.build_request 本地校验后交给 LAY A 传输层，
          不走 codec（codec 只认 chat / embed / classify）。
        - 聚合口径 SYSTEM1_AGGREGATE = "min"：取所有问题 confidence 的**最小值**，只要有一个
          问题低于 escalate_below 就整体升级，不做「部分问题升级」。
        - 完备性校验（允许部分作答，但绝不静默）：对提问集合与作答集合求差集，LAY A 没作答的
          qid 一律按 confidence=0.0 计入（未作答即不确定 → 必然升级），并在 raw["answered"] /
          raw["missing_qids"] 显式标注作答与缺失，调用方不会拿到「看起来正常但缺决策」的响应。
        - 升级时把 state、问题与 LAY A 的概率一起交给 [model.tasks.chat] 的对话模型裁决：
          响应 raw["source"] == "escalated"，raw["answers"] 保留 LAY A 的概率，raw["verdict"]
          是尽力解析出的 {qid: 答案}；未升级时 raw["source"] == "system1"。
        - 降级口径：system1 的重试计划里只有 LAY A 一个目标（classify 的 fallback_models 会把
          LAY A 请求体发给 OpenAI 兼容端点，是错的），所以「降级」= 升级到对话模型；LAY A 不可用
          （超时 / 401 / 422 / 503 / 响应不合规）时不抛未包装异常，升级路径也失败才抛 ModelCallError
          （包装两次失败原因）；入参本地校验失败仍抛 TransportError（调用方的错，不静默吞掉）。
        """

        body = laya_system1.build_request(state, questions, model=model)
        normalized = dict(body["questions"])
        active_scenario = scenario or "classify"
        threshold = float(escalate_below)
        # system1 复用 classify 的 provider 与重试策略（配置里 classify 指向 LAY A）
        provider = self.provider_for("classify")
        policy = self.retry_policy("classify")
        if provider.strip().lower() != transport_module.LAYA_PROVIDER:
            # 没有可用的决策端点：直接升级，绝不把 LAY A 请求体发给 OpenAI 兼容端点
            return await self._escalate(
                state,
                normalized,
                {},
                reason=f"provider={provider or '未配置'} 不是 LAY A",
                threshold=threshold,
                scenario=active_scenario,
                request_id=request_id,
            )

        request = ModelRequest(
            task="system1", model=str(body["model"]), provider=provider, params={}, request_id=request_id
        )
        reservation = None
        if self.meter is not None:
            reservation = await self.meter.reserve(
                active_scenario,
                estimated_input_tokens=estimate_tokens(json.dumps(body, ensure_ascii=False)),
                estimated_output_tokens=0,
                model=request.model,
                request_id=request_id,
            )
        started = time.perf_counter()
        try:
            result, served = await self._run_plan(request, body, [(provider, request.model)], policy, active_scenario)
            latency_ms = (time.perf_counter() - started) * 1000
            response = self._decode_system_one(
                request,
                result.value,
                provider=served,
                scenario=active_scenario,
                latency_ms=latency_ms,
                attempts=result.attempts,
            )
        except (ModelCallError, TransportError) as exc:
            if reservation is not None and self.meter is not None:
                self.meter.release(reservation.reservation_id)
            self._stats["errors"] = self._stats.get("errors", 0) + 1
            self._stats["laya_unavailable"] = self._stats.get("laya_unavailable", 0) + 1
            self._log("warning", "model.system1_unavailable", scenario=active_scenario, error=redact(repr(exc)))
            return await self._escalate(
                state,
                normalized,
                {},
                reason=f"laya_unavailable: {exc!r}",
                threshold=threshold,
                scenario=active_scenario,
                request_id=request_id,
            )

        answers = {str(qid): dict(item) for qid, item in dict(response.raw.get("answers") or {}).items()}
        answered, missing_qids = _system1_coverage(normalized, answers)
        # 完备性校验：未作答的 qid 视为不确定（confidence=0.0）→ 必然升级
        confidences = _system1_confidences(answers, questions=normalized, missing=missing_qids)
        lowest = min(confidences.values()) if confidences else 0.0
        if reservation is not None and self.meter is not None:
            await self.meter.consume(reservation.reservation_id, response.usage, scenario=active_scenario)
        if lowest < threshold:
            self._log(
                "info",
                "model.system1_escalate",
                reason="low_confidence",
                lowest_confidence=lowest,
                threshold=threshold,
                questions=len(normalized),
            )
            return await self._escalate(
                state,
                normalized,
                answers,
                reason=f"low_confidence: min={lowest:.3f} < {threshold}",
                threshold=threshold,
                scenario=active_scenario,
                request_id=request_id,
                answered=answered,
                missing_qids=missing_qids,
            )

        self._stats["system1"] = self._stats.get("system1", 0) + 1
        self._stats["tokens"] = self._stats.get("tokens", 0) + response.usage.total_tokens
        self._log(
            "debug",
            "model.system1",
            scenario=active_scenario,
            model=response.model,
            questions=len(normalized),
            lowest_confidence=lowest,
            latency_ms=round(latency_ms, 3),
        )
        return replace(
            response,
            text=_system1_summary(answers),
            raw={
                **dict(response.raw),
                "source": "system1",
                "confidences": confidences,
                "answered": answered,
                "missing_qids": missing_qids,
                "aggregate": SYSTEM1_AGGREGATE,
                "escalate_below": threshold,
                "lowest_confidence": lowest,
            },
        )

    # ---- 执行链 --------------------------------------------------------
    async def _execute(self, request: ModelRequest, *, scenario: str) -> ModelResponse:
        spec = self.task_spec(request.task)
        policy = self.retry_policy(request.task)
        plan = self._attempt_plan(request, spec)

        reservation = None
        if self.meter is not None:
            reservation = await self.meter.reserve(
                scenario,
                estimated_input_tokens=self._estimate_input(request),
                estimated_output_tokens=self._estimate_output(request, spec),
                model=request.model,
                request_id=request.request_id,
            )
            effective_max = request.params.get("max_tokens")
            if reservation.output_tokens and (effective_max is None or reservation.output_tokens < effective_max):
                request = replace(request, params={**request.params, "max_tokens": reservation.output_tokens})

        started = time.perf_counter()
        wall_started = time.time()
        trace_id = request.request_id or f"trace-model-{uuid.uuid4().hex[:16]}"
        span_id = f"span-model-{uuid.uuid4().hex[:16]}"
        try:
            payload = encode_request(request)
            result, served_provider = await self._run_plan(
                request, payload, plan, policy, scenario, validate=lambda value: self._check_decodable(request, value)
            )
            latency_ms = (time.perf_counter() - started) * 1000
            try:
                response = self._decode(
                    request, result, provider=served_provider, scenario=scenario, latency_ms=latency_ms
                )
            except (TransportError, ValueError, TypeError) as exc:
                # 解码 / 归一化失败也算这次调用失败：包装成 ModelCallError（带 attempts），
                # 不让裸异常穿过 _execute 抛给调用方。
                self._stats["decode_failed"] = self._stats.get("decode_failed", 0) + 1
                raise ModelCallError(
                    f"模型响应解码失败（task={request.task}）：{exc!r}", attempts=result.attempts, last_error=exc
                ) from exc
            if not response.usage.total_tokens:
                response = replace(
                    response,
                    usage=TokenUsage(
                        prompt_tokens=self._estimate_input(request),
                        completion_tokens=estimate_tokens(response.text),
                        model=response.model,
                        scenario=scenario,
                        request_id=request.request_id,
                    ),
                )
        except BaseException as exc:
            if reservation is not None and self.meter is not None:
                self.meter.release(reservation.reservation_id)
            self._stats["errors"] += 1
            self._observe_panel_span(
                trace_id=trace_id,
                span_id=span_id,
                ts=wall_started,
                duration_ms=(time.perf_counter() - started) * 1000,
                stage="model",
                component="model_gateway",
                status="error",
                model=request.model,
                metadata={"task": request.task, "scenario": scenario, "provider": request.provider},
                summary=f"模型调用失败: {type(exc).__name__}",
            )
            raise

        self._observe_panel_span(
            trace_id=trace_id,
            span_id=span_id,
            ts=wall_started,
            duration_ms=(time.perf_counter() - started) * 1000,
            stage="model",
            component="model_gateway",
            status="ok",
            model=response.model or request.model,
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            metadata={
                "task": request.task,
                "scenario": scenario,
                "provider": request.provider,
                "attempts": result.attempts,
            },
            summary="模型响应已归一化（仅记录元数据）",
        )
        self._stats[request.task] = self._stats.get(request.task, 0) + 1
        self._stats["tokens"] = self._stats.get("tokens", 0) + response.usage.total_tokens
        if reservation is not None and self.meter is not None:
            await self.meter.consume(reservation.reservation_id, response.usage, scenario=scenario)
        self._log(
            "debug",
            "model.call",
            **response.as_dict(),
            scenario=scenario,
            reserved=reservation.total if reservation else None,
        )
        return response

    def _observe_panel_span(self, **fields: Any) -> None:
        """可选的面板观测钩子；失败不能影响模型调用。"""
        try:
            from grouppig.panel.telemetry import record_span

            record_span(**fields)
        except Exception:  # noqa: BLE001
            return

    # ---- 尝试计划（provider 分派与跨服务商降级） ------------------------
    def _attempt_plan(self, request: ModelRequest, spec: Mapping[str, Any]) -> list[tuple[str, str]]:
        """按 (provider, model) 给出尝试顺序：主目标 → fallback_models。

        fallback_models 里写的是「服务商模型名」（如 grok-4.6）。当主目标是专属
        provider（如 laya 的协议枚举 auto）时，降级必须回到默认服务商，否则会把服务商
        模型名发给不认识的端点（LAY A 本地白名单会直接拒）。
        """

        plan: list[tuple[str, str]] = [(request.provider, request.model)]
        fallback_provider = self._fallback_provider(request.provider)
        for item in spec.get("fallback_models", []) or []:
            name = str(item or "").strip()
            if not name or any(name == model for _, model in plan):
                continue
            plan.append((fallback_provider, name))
        return plan

    def _fallback_provider(self, provider: str) -> str:
        """降级目标挂在哪个 provider 上。

        专属协议 provider（如 laya）的模型空间是协议枚举（auto/english/...），
        而 fallback_models 里是服务商模型名，只能回到默认服务商；其余 provider
        沿用主目标，保持原有降级语义。
        """

        if provider.strip().lower() == transport_module.LAYA_PROVIDER:
            return str(self.config.get("model.default_provider", "") or provider)
        return provider

    async def _run_plan(
        self,
        request: ModelRequest,
        payload: Mapping[str, Any],
        plan: Sequence[tuple[str, str]],
        policy: RetryPolicy,
        scenario: str,
        *,
        validate: Callable[[Any], None] | None = None,
    ) -> tuple[RetryResult, str]:
        """逐个目标执行重试策略。

        单个目标内沿用 execute_with_retry（重试 → 超时）；不可重试的协议错误
        （LAY A 的 401/422）直接换下一个目标。validate 是解码前自检：形状 / 内容不合规
        （例如 200 但 answers 为空）同样视作该目标失败并推进到下一个目标。
        全部目标失败时：不可重试的传输错误原样冒泡，其余情况抛 ModelCallError（含累计尝试次数）。
        """

        last_error: BaseException | None = None
        attempts = 0
        for index, (provider, model) in enumerate(plan):
            same_target = provider == request.provider and model == request.model
            target = request if same_target else replace(request, provider=provider, model=model)

            async def operation(
                attempt: int,
                switch_model: str | None,
                *,
                _provider: str = provider,
                _model: str = model,
                _path: str = target.path,
            ) -> dict[str, Any]:
                chosen = switch_model or _model
                body = payload if chosen == request.model else {**payload, "model": chosen}
                return await self._transport(_provider).complete(
                    body, path=_path, timeout=policy.timeout, provider=_provider
                )

            try:
                result = await execute_with_retry(
                    operation,
                    policy=policy,
                    fallback_models=(),
                    primary_model=model,
                    task=request.task,
                    on_attempt=self._attempt_hook(target, scenario),
                )
            except ModelCallError as exc:
                last_error, attempts = exc, attempts + exc.attempts
            except TransportError as exc:
                # 例如 LAY A 的 401/422：不可重试，直接换下一个目标
                last_error, attempts = exc, attempts + 1
            else:
                if validate is None:
                    return result, provider
                try:
                    validate(result.value)
                except (TransportError, ValueError, TypeError) as exc:
                    # 解码前的形状 / 内容自检失败：视同该目标失败，推进到下一个目标
                    last_error, attempts = exc, attempts + 1
                    self._stats["decode_failed"] = self._stats.get("decode_failed", 0) + 1
                    self._log(
                        "warning",
                        "model.decode_failed",
                        task=request.task,
                        provider=provider,
                        model=model,
                        error=redact(repr(exc)),
                    )
                else:
                    return result, provider
            if index < len(plan) - 1:
                next_provider, next_model = plan[index + 1]
                self._stats["fallback"] = self._stats.get("fallback", 0) + 1
                self._log(
                    "warning",
                    "model.fallback",
                    task=request.task,
                    failed_provider=provider,
                    failed_model=model,
                    next_provider=next_provider,
                    next_model=next_model,
                    error=redact(repr(last_error)),
                )
        if isinstance(last_error, TransportError) and not last_error.retryable:
            # 不可重试的传输错误（如 provider 没配 base_url）原样冒泡：保留「配置错误
            # 直接报错」的原有契约，只把「重试/降级都耗尽」包装成 ModelCallError。
            raise last_error
        raise ModelCallError(
            f"模型调用失败，已尝试 {attempts} 次（task={request.task}）：{last_error!r}",
            attempts=attempts,
            last_error=last_error,
        )

    @staticmethod
    def _check_decodable(request: ModelRequest, value: Any) -> None:
        """解码前的形状 / 内容自检；不合规抛 TransportError，由 _run_plan 视作该目标失败。"""

        if isinstance(value, ModelResponse):
            return
        if not isinstance(value, Mapping):
            raise TransportError(
                f"模型响应应为 JSON 对象，得到 {type(value).__name__}（task={request.task}）", retryable=False
            )
        if "answers" in value:
            answers = normalize_answers(value)
            if request.task == "classify" and not answers:
                # LAY A 返回 200 但 answers 为空：没有 label 可用，视作该目标失败 → 换 fallback 模型
                raise TransportError("LAY A 响应没有任何答案（answers 为空）", retryable=False)

    async def _escalate(
        self,
        state: Any,
        questions: Mapping[str, Any],
        answers: Mapping[str, Any],
        *,
        reason: str,
        threshold: float,
        scenario: str,
        request_id: str,
        answered: Sequence[str] | None = None,
        missing_qids: Sequence[str] | None = None,
    ) -> ModelResponse:
        """把问题与 LAY A 的概率交给对话模型裁决；返回 source="escalated" 的响应。"""

        if answered is None or missing_qids is None:
            answered, missing_qids = _system1_coverage(questions, answers)
        confidences = _system1_confidences(answers, questions=questions, missing=missing_qids)
        messages = _escalation_messages(state, questions, answers, reason=reason)
        try:
            chat = await self.chat(messages, scenario="chat", request_id=request_id)
        except Exception as exc:  # noqa: BLE001 - 升级路径也不许把底层异常直接透给调用方
            self._stats["errors"] = self._stats.get("errors", 0) + 1
            raise ModelCallError(
                f"system1 升级对话模型失败（触发原因：{reason}）：{exc!r}", attempts=1, last_error=exc
            ) from exc
        verdict = _parse_verdict(chat.text, questions)
        self._log(
            "info",
            "model.system1_escalated",
            scenario=scenario,
            reason=reason,
            questions=len(questions),
            verdict=len(verdict),
        )
        return replace(
            chat,
            task="system1",
            raw={
                "source": "escalated",
                "answers": {str(qid): dict(item) for qid, item in answers.items()},
                "confidences": confidences,
                "answered": list(answered),
                "missing_qids": list(missing_qids),
                "lowest_confidence": min(confidences.values()) if confidences else 0.0,
                "aggregate": SYSTEM1_AGGREGATE,
                "escalate_below": threshold,
                "escalation_reason": reason,
                "verdict": verdict,
                "escalated_to": {
                    "model": chat.model,
                    "provider": chat.provider,
                    "latency_ms": round(chat.latency_ms, 3),
                },
            },
        )

    def _decode(
        self,
        request: ModelRequest,
        result: RetryResult,
        *,
        provider: str,
        scenario: str,
        latency_ms: float,
    ) -> ModelResponse:
        """LAY A 的 answers 响应与 OpenAI 兼容响应分别解码。"""

        value = result.value
        if isinstance(value, ModelResponse):
            return replace(value, provider=provider or value.provider, latency_ms=latency_ms, attempts=result.attempts)
        if not isinstance(value, Mapping):
            # 形状不对（例如 LAY A 返回 JSON 数组）→ 统一走 TransportError 降级路径
            raise TransportError(
                f"模型响应应为 JSON 对象，得到 {type(value).__name__}（task={request.task}）", retryable=False
            )
        if "answers" in value:
            return self._decode_system_one(
                request, value, provider=provider, scenario=scenario, latency_ms=latency_ms, attempts=result.attempts
            )
        return decode_response(
            value,
            task=request.task,
            model=result.model or request.model,
            provider=provider,
            request_id=request.request_id,
            latency_ms=latency_ms,
            attempts=result.attempts,
            labels=request.labels,
        )

    def _decode_system_one(
        self,
        request: ModelRequest,
        data: Mapping[str, Any],
        *,
        provider: str,
        scenario: str,
        latency_ms: float,
        attempts: int,
    ) -> ModelResponse:
        """LAY A 响应（answers: {qid: {type, choice|score|noul, probabilities, confidence}}）→ ModelResponse。"""

        if not isinstance(data, Mapping):
            # 形状不对（例如服务端返回 JSON 数组）→ 统一走 TransportError 降级路径，
            # 不把 AttributeError / TypeError 透给调用方。
            raise TransportError(
                f"LAY A 响应应为 JSON 对象（answers 结构），得到 {type(data).__name__}", retryable=False
            )
        usage = normalize_usage(data)
        model = str(data.get("model") or request.model)
        token_usage = TokenUsage(
            prompt_tokens=usage["prompt_tokens"],
            completion_tokens=usage["completion_tokens"],
            model=model,
            scenario=scenario,
            request_id=request.request_id,
        )
        raw: dict[str, Any] = {"model": model, "routing": data.get("routing"), "usage": data.get("usage")}
        if request.task == "classify":
            parsed = normalize_classification(data, labels=request.labels)
            return ModelResponse(
                task=request.task,
                model=model,
                provider=provider,
                label=parsed["label"],
                scores=parsed["scores"],
                usage=token_usage,
                raw={**raw, "probabilities": parsed["probabilities"], "confidence": parsed["confidence"]},
                latency_ms=latency_ms,
                attempts=attempts,
                request_id=request.request_id,
            )
        return ModelResponse(
            task=request.task,
            model=model,
            provider=provider,
            usage=token_usage,
            raw={**raw, "answers": normalize_answers(data)},
            latency_ms=latency_ms,
            attempts=attempts,
            request_id=request.request_id,
        )

    def _attempt_hook(self, request: ModelRequest, scenario: str):
        def hook(attempt: int, model: str | None, error: BaseException | None, elapsed_ms: float) -> None:
            if error is None:
                return
            self._log(
                "warning",
                "model.attempt_failed",
                task=request.task,
                scenario=scenario,
                attempt=attempt,
                model=model,
                error=repr(error),
                elapsed_ms=round(elapsed_ms, 3),
            )

        return hook

    def _estimate_input(self, request: ModelRequest) -> int:
        if request.task == "embed":
            return sum(estimate_tokens(t) for t in request.input_texts)
        return estimate_messages_tokens(request.messages)

    @staticmethod
    def _estimate_output(request: ModelRequest, spec: Mapping[str, Any]) -> int | None:
        value = request.params.get("max_tokens")
        if isinstance(value, int):
            return value
        configured = spec.get("max_tokens")
        return int(configured) if isinstance(configured, int) else None

    # ---- 生命周期 ------------------------------------------------------
    async def aclose(self) -> None:
        for transport in self._transports.values():
            closer = getattr(transport, "aclose", None)
            if closer is not None:
                result = closer()
                if hasattr(result, "__await__"):
                    await result
        self._transports.clear()

    def health(self) -> dict[str, Any]:
        """健康快照：provider / 传输层 / 预算 + stats 计数（fallback / laya_unavailable / decode_failed）。"""

        return {
            "providers": sorted({self.provider_for(task) for task in ("chat", "embed", "classify")}),
            "transports": sorted(self._transports),
            "meter": self.meter is not None,
            "stats": dict(self._stats),
        }

    def _log(self, level: str, event: str, **fields: Any) -> None:
        if self.logger is None:
            return
        # 日志出口统一脱敏：自由文本（异常 repr、响应片段）里的 Bearer / sk- 密钥不进日志
        safe = {key: (redact(value) if isinstance(value, str) else value) for key, value in fields.items()}
        try:
            self.logger.log(level, event, **safe)
        except Exception:  # pragma: no cover
            pass


def _system1_coverage(questions: Mapping[str, Any], answers: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """(answered, missing_qids)：未作答的 qid 视为不确定，必然触发升级。"""

    answered = [str(qid) for qid in questions if str(qid) in answers]
    missing = [str(qid) for qid in questions if str(qid) not in answers]
    return answered, missing


def _system1_confidences(
    answers: Mapping[str, Any],
    *,
    questions: Mapping[str, Any] | None = None,
    missing: Sequence[str] = (),
) -> dict[str, float]:
    """逐问题 confidence；未作答（missing）的 qid 记 0.0，questions 给定时只统计提问过的 qid。"""

    confidences: dict[str, float] = {}
    for qid, item in answers.items():
        key = str(qid)
        if questions is not None and key not in questions:
            continue
        value = item.get("confidence") if isinstance(item, Mapping) else None
        try:
            confidences[key] = float(value) if value is not None else 0.0
        except (TypeError, ValueError):
            confidences[key] = 0.0
    for qid in missing:
        confidences[str(qid)] = 0.0
    return confidences


def _system1_summary(answers: Mapping[str, Any]) -> str:
    """把答案压成一行 JSON 放进 ModelResponse.text（同时让 ok 为真）。"""

    compact: dict[str, Any] = {}
    for qid, item in answers.items():
        if not isinstance(item, Mapping):
            compact[str(qid)] = item
            continue
        compact[str(qid)] = {
            "type": item.get("type"),
            "answer": item.get("choice", item.get("score", item.get("noul"))),
            "confidence": item.get("confidence"),
        }
    return json.dumps(compact, ensure_ascii=False)


def _escalation_messages(
    state: Any,
    questions: Mapping[str, Any],
    answers: Mapping[str, Any],
    *,
    reason: str,
) -> tuple[dict[str, Any], ...]:
    """升级提示词：情境 + 问题 + LAY A 概率 + 触发原因。"""

    payload = {
        "state": state,
        "questions": questions,
        "laya_answers": dict(answers) or None,
        "escalation_reason": reason,
    }
    return (
        {"role": "system", "content": SYSTEM1_ESCALATE_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    )


def _parse_verdict(text: str, questions: Mapping[str, Any]) -> dict[str, Any]:
    """从对话模型回复里尽力解析 {qid: 答案}；解析不出来返回 {}（不抛错）。"""

    match = _JSON_OBJECT.search(text or "")
    if match is None:
        return {}
    try:
        data = json.loads(match.group(0))
    except ValueError:
        return {}
    if not isinstance(data, Mapping):
        return {}
    verdict: dict[str, Any] = {}
    for qid in questions:
        if qid not in data:
            continue
        value = data[qid]
        if isinstance(value, Mapping):
            value = value.get("answer", value.get("choice", value.get("score")))
        verdict[str(qid)] = value
    return verdict


def _normalize_messages(messages: Sequence[Mapping[str, Any]] | str) -> tuple[dict[str, Any], ...]:
    if isinstance(messages, str):
        return ({"role": "user", "content": messages},)
    out = []
    for message in messages:
        if not isinstance(message, Mapping):
            raise ModelCallError(f"消息必须是映射，得到 {type(message).__name__}", attempts=0)
        out.append(dict(message))
    return tuple(out)


_router: ModelRouter | None = None


def get_router() -> ModelRouter:
    global _router
    if _router is None:
        from grouppig.infra.config.loader import get_config

        _router = ModelRouter(get_config())
    return _router


def set_router(router: ModelRouter | None) -> ModelRouter | None:
    global _router
    previous, _router = _router, router
    return previous


__all__ = ["DEFAULT_SCENARIOS", "ModelRouter", "get_router", "set_router"]
