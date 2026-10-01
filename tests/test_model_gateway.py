"""模型网关测试：编解码、重试降级、路由与预算联动。"""

from __future__ import annotations

import asyncio
import copy

import pytest

from grouppig.infra.config.loader import Config
from grouppig.infra.model_gateway.codec import (
    ModelRequest,
    ModelResponse,
    decode_response,
    encode_request,
    extract_text,
    parse_classification,
)
from grouppig.infra.model_gateway.retry import RetryPolicy, execute_with_retry, policy_from_config
from grouppig.infra.model_gateway.router import SYSTEM1_AGGREGATE, ModelRouter
from grouppig.infra.runtime.errors import ModelCallError, ModelTimeoutError, TransportError
from grouppig.infra.runtime.laya_system1 import REDACTED, _short, redact
from grouppig.infra.runtime.usage import TokenUsage
from grouppig.infra.token_budget.meter import TokenMeter
from grouppig.infra.token_budget.reporter import TokenReporter
from helpers import FakeTransport


# ---- codec ---------------------------------------------------------------
def test_encode_chat_request_is_openai_compatible():
    payload = encode_request(
        ModelRequest(
            task="chat",
            model="m1",
            messages=({"role": "system", "content": "你是群友"}, {"role": "user", "content": "在吗"}),
            params={"temperature": 0.7, "max_tokens": 64, "request_id": "ignored"},
        )
    )
    assert payload["model"] == "m1"
    assert payload["temperature"] == 0.7 and payload["max_tokens"] == 64
    assert [m["role"] for m in payload["messages"]] == ["system", "user"]
    assert "request_id" not in payload


def test_encode_embed_and_classify():
    embed = encode_request(ModelRequest(task="embed", model="e1", input_texts=("a", "b")))
    assert embed == {"model": "e1", "input": ["a", "b"]}

    classify = encode_request(
        ModelRequest(
            task="classify", model="c1", messages=({"role": "user", "content": "在吗"},), labels=("闲聊", "提问")
        )
    )
    assert classify["response_format"] == {"type": "json_object"}
    assert "候选标签：闲聊, 提问" in classify["messages"][1]["content"]
    assert classify["temperature"] == 0.0


def test_encode_accepts_mapping_and_rejects_bad_input():
    payload = encode_request({"task": "chat", "model": "m", "messages": [{"role": "user", "content": "hi"}]})
    assert payload["messages"][0]["content"] == "hi"
    with pytest.raises(ModelCallError):
        encode_request({"task": "chat", "model": "m"})
    with pytest.raises(ModelCallError):
        encode_request(ModelRequest(task="unknown", model="m"))
    with pytest.raises(ModelCallError):
        encode_request("not-a-request")


def test_decode_chat_response_with_usage():
    raw = {
        "model": "m1",
        "choices": [{"message": {"role": "assistant", "content": "在的"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 20, "completion_tokens": 5},
    }
    response = decode_response(raw, task="chat", provider="a6api", request_id="r1")
    assert response.text == "在的"
    assert response.finish_reason == "stop"
    assert response.usage == TokenUsage(
        prompt_tokens=20, completion_tokens=5, model="m1", scenario="chat", request_id="r1"
    )
    assert response.usage.total_tokens == 25
    assert response.ok and response.as_dict()["usage"]["total_tokens"] == 25


def test_decode_embedding_and_multipart_content():
    embedding = decode_response({"data": [{"embedding": [0.5, 1]}], "model": "e1"}, task="embed")
    assert embedding.embedding == (0.5, 1.0)
    assert (
        extract_text(
            {"choices": [{"message": {"content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}}]}
        )
        == "ab"
    )
    assert extract_text({"output_text": "plain"}) == "plain"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('{"label": "提问", "scores": {"提问": 0.9, "闲聊": 0.1}}', "提问"),
        ('```json\n{"label": "闲聊"}\n```', "闲聊"),
        ('{"scores": {"a": 0.2, "b": 0.8}}', "b"),
        ("我觉得这是闲聊", "闲聊"),
        ("完全看不懂", None),
    ],
)
def test_parse_classification_fallbacks(text, expected):
    label, scores = parse_classification(text, ("提问", "闲聊"))
    assert label == expected
    if expected:
        assert scores


def test_decode_rejects_bad_payloads():
    with pytest.raises(ModelCallError):
        decode_response("nope")  # type: ignore[arg-type]


# ---- retry ---------------------------------------------------------------
async def test_retry_succeeds_after_transient_failures():
    calls: list[tuple[int, str | None]] = []

    async def flaky(attempt: int, model: str | None):
        calls.append((attempt, model))
        if attempt < 3:
            raise TransportError("503", status=503)
        return "ok"

    result = await execute_with_retry(
        flaky, policy=RetryPolicy(base_delay=0, jitter=0), primary_model="m1", task="chat"
    )
    assert result.value == "ok" and result.attempts == 3
    assert len(result.errors) == 2
    assert [c[1] for c in calls] == ["m1", "m1", "m1"]


async def test_retry_switches_to_fallback_model_after_exhausting_attempts():
    models: list[str | None] = []

    async def always_fail(attempt: int, model: str | None):
        models.append(model)
        raise TransportError("503", status=503)

    with pytest.raises(ModelCallError) as excinfo:
        await execute_with_retry(
            always_fail,
            policy=RetryPolicy(max_attempts=2, base_delay=0, jitter=0),
            fallback_models=("backup-1",),
            primary_model="primary",
        )
    assert models == ["primary", "primary", "backup-1", "backup-1"]
    assert excinfo.value.attempts == 4


async def test_retry_raises_model_call_error_when_exhausted():
    async def always_fail(attempt: int, model: str | None):
        raise TransportError("500", status=500)

    with pytest.raises(ModelCallError) as excinfo:
        await execute_with_retry(always_fail, policy=RetryPolicy(max_attempts=2, base_delay=0, jitter=0), task="chat")
    assert excinfo.value.attempts == 2
    assert "task=chat" in str(excinfo.value)


async def test_retry_does_not_retry_non_retryable_errors():
    calls = []

    async def bad_request(attempt: int, model: str | None):
        calls.append(attempt)
        raise TransportError("400", status=400, retryable=False)

    with pytest.raises(TransportError):
        await execute_with_retry(bad_request, policy=RetryPolicy(max_attempts=3, base_delay=0, jitter=0))
    assert calls == [1]


async def test_retry_times_out_slow_attempts():
    async def slow(attempt: int, model: str | None):
        await asyncio.sleep(0.2)
        return "late"

    with pytest.raises(ModelCallError) as excinfo:
        await execute_with_retry(slow, policy=RetryPolicy(max_attempts=1, timeout=0.02, base_delay=0, jitter=0))
    assert "超时" in str(excinfo.value)
    assert isinstance(excinfo.value, ModelTimeoutError) or isinstance(excinfo.value.last_error, ModelTimeoutError)


async def test_retry_adapts_to_operation_arity():
    async def zero_args():
        return "z"

    async def one_arg(model):
        return f"1:{model}"

    assert (await execute_with_retry(zero_args, policy=RetryPolicy(max_attempts=1))).value == "z"
    assert (await execute_with_retry(one_arg, policy=RetryPolicy(max_attempts=1), primary_model="m")).value == "1:m"


def test_policy_from_config_reads_model_retry_section(config):
    policy = policy_from_config(config)
    assert policy.max_attempts == 3 and policy.timeout == 30.0
    assert policy.with_overrides(max_attempts=5).max_attempts == 5
    assert policy.delay_for(1) == 0.0
    assert 0 <= policy.delay_for(3) <= policy.max_delay


async def test_rpc_retry_handler_uses_config_policy(container):
    async def flaky(attempt: int, model: str | None):
        if attempt == 1:
            raise TransportError("503", status=503)
        return "recovered"

    result = await container.call("rpc:model.retry", flaky, task="chat", model="m1", sleep=_no_sleep)
    assert result.value == "recovered" and result.attempts == 2


async def _no_sleep(_delay: float) -> None:
    return None


# ---- router --------------------------------------------------------------
def build_router(config, transport, *, meter=None):
    return ModelRouter(config, transport=transport, meter=meter)


async def test_router_chat_encodes_calls_and_decodes(config, fake_transport):
    router = build_router(config, fake_transport)
    response = await router.chat([{"role": "user", "content": "在吗"}], request_id="r1")
    assert isinstance(response, ModelResponse)
    assert response.text == "喵"
    assert response.usage.total_tokens == 19
    assert fake_transport.paths == ["/chat/completions"]
    spec = config.section("model.tasks.chat")
    assert fake_transport.calls[0]["model"] == spec["model"]  # 主模型取自配置，不写死
    assert fake_transport.calls[0]["temperature"] == 0.8


async def test_router_chat_retries_then_succeeds(config):
    transport = FakeTransport(failures=2)
    router = build_router(config, transport)
    response = await router.chat("hi")
    assert response.text == "喵"
    assert response.attempts == 3
    assert len(transport.calls) == 3


async def test_router_chat_raises_after_fallback_exhausted(config):
    transport = FakeTransport(failures=99)
    router = build_router(config, transport)
    with pytest.raises(ModelCallError):
        await router.chat("hi")
    # 主模型 3 次 + fallback 3 次
    assert len(transport.calls) == 6
    spec = config.section("model.tasks.chat")
    expected = {spec["model"], *spec.get("fallback_models", [])}
    assert {call["model"] for call in transport.calls} == expected  # 主模型 + 配置里的 fallback


async def test_router_embed_and_classify(config):
    transport = FakeTransport(classify_text='{"label": "提问", "scores": {"提问": 0.9}}')
    router = build_router(config, transport)
    # embed 的 provider 在仓库配置里是 "local"：注入的 transport 不该顶掉本地嵌入，
    # 否则嵌入请求会打到 chat 服务商的 /embeddings 上（实测 403）。
    assert config.section("model.tasks.embed")["provider"] == "local"
    embedded = await router.embed(["a", "b"])
    assert embedded.provider == "local"
    assert len(embedded.embedding) > 1
    assert transport.calls == []  # 本地嵌入不走注入的（chat）transport

    # classify 的 provider 在仓库配置里是 "laya"（专属协议）：同样不被注入的 "*" 顶掉，
    # 需要显式注册（否则会去连真实 LAY A 端点）。
    assert config.section("model.tasks.classify")["provider"] == "laya"
    router.set_transport(transport, provider="laya")
    classified = await router.classify("在吗", ["提问", "闲聊"])
    assert classified.provider == "laya"
    assert classified.label == "提问"
    assert classified.scores == {"提问": 0.9}


async def test_router_reserves_and_consumes_budget(config):
    reporter = TokenReporter()
    meter = TokenMeter(reporter=reporter, config=config)
    transport = FakeTransport(usage={"prompt_tokens": 100, "completion_tokens": 20})
    router = build_router(config, transport, meter=meter)
    response = await router.chat("hi", scenario="smalltalk")
    assert response.usage.total_tokens == 120
    snapshot = meter.snapshot()
    assert snapshot.calls == 1 and snapshot.outstanding == 0
    assert snapshot.consumed_total == 120
    assert reporter.report()["by_scenario"]["smalltalk"]["calls"] == 1


async def test_router_clamps_max_tokens_to_reserved_budget(config):
    meter = TokenMeter(config=config)
    transport = FakeTransport()
    router = build_router(config, transport, meter=meter)
    await router.chat("hi", scenario="smalltalk", max_tokens=9999)
    assert transport.calls[0]["max_tokens"] == 120  # smalltalk 的 max_output_tokens


async def test_router_releases_reservation_on_failure(config):
    meter = TokenMeter(config=config)
    transport = FakeTransport(failures=99)
    router = build_router(config, transport, meter=meter)
    with pytest.raises(ModelCallError):
        await router.chat("hi")
    snapshot = meter.snapshot()
    assert snapshot.outstanding == 0 and snapshot.calls == 0
    assert snapshot.reserved_input == 0 and snapshot.reserved_output == 0


async def test_router_raises_clear_error_without_base_url(config):
    # 显式构造一个 base_url 为空的服务商：契约是「未配置即明确报错」，不依赖仓库里某个服务商恰好留空
    raw = copy.deepcopy(config.raw)
    raw.setdefault("model", {}).setdefault("providers", {})["unconfigured"] = {}
    raw.setdefault("model", {}).setdefault("tasks", {}).setdefault("chat", {})["provider"] = "unconfigured"
    router = ModelRouter(Config(raw=raw))
    with pytest.raises(TransportError) as excinfo:
        await router.chat("hi")
    assert "base_url" in str(excinfo.value)


async def test_router_health_and_close(config, fake_transport):
    router = build_router(config, fake_transport)
    await router.chat("hi")
    health = router.health()
    assert health["stats"]["chat"] == 1
    await router.aclose()
    assert fake_transport.closed


async def test_rpc_model_handlers_return_dicts(container, fake_transport):
    container.router.set_transport(fake_transport)
    chat = await container.call("rpc:model.chat", [{"role": "user", "content": "hi"}])
    assert chat["text"] == "喵" and chat["task"] == "chat"
    embed = await container.call("rpc:model.embed", ["a"])
    # provider 是 local 时维度由本地嵌入实现决定，不写死 3；rpc 返回里带的是 embedding_dim
    assert embed["embedding_dim"] == 64 > 1
    decoded = await container.call("rpc:model.decode", {"choices": [{"message": {"content": "x"}}]}, "chat")
    assert decoded["text"] == "x"
    assert (await container.call("rpc:token.report"))["calls"] >= 2


# ---- provider 分派（laya）与跨服务商降级 --------------------------------
class RawTransport:
    """直接返回原始响应体：用于模拟 LAY A 的 answers 结构（非 OpenAI 信封）。"""

    def __init__(self, *, data: dict | None = None, error: BaseException | None = None) -> None:
        self.data = data
        self.error = error
        self.calls: list[dict] = []
        self.paths: list[str] = []
        self.closed = False

    async def complete(self, payload, *, path="/chat/completions", timeout=None, provider=""):
        self.calls.append(dict(payload))
        self.paths.append(path)
        if self.error is not None:
            raise self.error
        return self.data

    async def aclose(self) -> None:
        self.closed = True


def laya_answer(label: str, probabilities: dict, *, confidence: float = 0.7) -> dict:
    """LAY A /v1/systemone 的响应形状。"""

    return {
        "model": "typed-decisions",
        "answers": {
            "label": {
                "type": "choice",
                "choice": label,
                "probabilities": dict(probabilities),
                "confidence": confidence,
            }
        },
        "usage": {"input_tokens": 42, "output_tokens": 0},
        "routing": {"model": "typed-decisions"},
    }


async def test_injected_wildcard_transport_does_not_shadow_laya(config):
    """注入的 "*" transport 不得顶掉专属协议 provider（laya）。"""

    star = FakeTransport(reply="通配")
    laya = RawTransport(data=laya_answer("提问", {"提问": 0.72, "闲聊": 0.28}))
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.classify("在吗", ["提问", "闲聊"])
    assert response.provider == "laya"
    assert len(laya.calls) == 1
    assert star.calls == []  # "*" 完全没被用到
    # 请求路径仍由 codec 决定；LAY A 的 /v1/systemone 由 transport 内部替换
    assert laya.paths == ["/chat/completions"]
    await router.aclose()


async def test_router_classify_decodes_laya_answers(config):
    """classify 走 LAY A 时按 answers 结构解码：label 取 choice，scores 取概率。"""

    laya = RawTransport(data=laya_answer("提问", {"提问": 0.72, "闲聊": 0.28}, confidence=0.68))
    router = ModelRouter(config, transport=laya)
    router.set_transport(laya, provider="laya")

    response = await router.classify("在吗", ["提问", "闲聊"])
    assert response.label == "提问"
    assert response.scores == {"提问": 0.72, "闲聊": 0.28}
    assert response.provider == "laya"
    assert response.model == "typed-decisions"
    assert response.usage.prompt_tokens == 42 and response.usage.total_tokens == 42
    assert response.raw["confidence"] == pytest.approx(0.68)
    await router.aclose()


async def test_classify_falls_back_to_default_provider_when_laya_fails(config):
    """LAY A 不可重试的协议错误（401/422）按 fallback_models 回落默认服务商。"""

    spec = config.section("model.tasks.classify")
    assert spec["model"] == "auto" and spec["fallback_models"] == ["grok-4.6"]
    laya = RawTransport(error=TransportError("LAY A 返回 422（请求格式错）", status=422, retryable=False))
    fallback = FakeTransport(classify_text='{"label": "闲聊", "scores": {"闲聊": 0.8}}')
    router = ModelRouter(config, transport=fallback)
    router.set_transport(laya, provider="laya")

    response = await router.classify("在吗", ["提问", "闲聊"])
    assert response.label == "闲聊"
    assert response.provider == "a6api"  # 降级回默认服务商，而不是把 grok-4.6 发给 LAY A
    assert len(laya.calls) == 1  # 422 不可重试：只试一次就换目标
    assert fallback.calls[-1]["model"] == "grok-4.6"
    await router.aclose()


async def test_classify_raises_wrapped_error_when_every_target_fails(config):
    """所有目标都失败时抛包装过的 ModelCallError，而不是未包装的 TransportError。"""

    laya = RawTransport(error=TransportError("LAY A 返回 401（密钥无效）", status=401, retryable=False))
    fallback = FakeTransport(failures=99)
    router = ModelRouter(config, transport=fallback)
    router.set_transport(laya, provider="laya")

    with pytest.raises(ModelCallError) as excinfo:
        await router.classify("在吗", ["提问"])
    assert not isinstance(excinfo.value, TransportError)
    assert excinfo.value.attempts == 4  # laya 1 次 + 降级目标 3 次
    assert "task=classify" in str(excinfo.value)
    await router.aclose()


# ---- rpc:model.system1（LAY A 多问题前向 + 置信度门控） ------------------
def system1_answers(spec: dict) -> dict:
    """按 {qid: (type, answer, confidence)} 组装 LAY A 的 answers 响应。"""

    answers: dict[str, dict] = {}
    for qid, (kind, answer, confidence) in spec.items():
        item: dict = {"type": kind, "probabilities": {}, "confidence": confidence}
        if kind == "choice":
            item["choice"] = answer
            item["probabilities"] = {str(answer): confidence}
        elif kind == "score":
            item["score"] = answer
        else:
            item["noul"] = answer
        answers[qid] = item
    return {
        "model": "typed-decisions",
        "answers": answers,
        "usage": {"input_tokens": 40, "output_tokens": 0},
        "routing": {"model": "typed-decisions"},
    }


MIXED_QUESTIONS = {
    "label": {"type": "choice", "instructions": "属于哪一类？", "criteria": {"提问": "在提问", "闲聊": "在闲聊"}},
    "tone": {"type": "score", "instructions": "语气强度 0~1", "criteria": ["0 平静", "1 激动"]},
    "needs_reply": {"type": "noul", "instructions": "是否需要立刻回复"},
    "urgency": {"type": "score", "instructions": "紧迫度 0~1", "criteria": ["0 不急", "1 很急"]},
    "topic_shift": {"type": "choice", "instructions": "是否换话题", "criteria": {"是": "换了", "否": "没换"}},
    "mood": {"type": "choice", "instructions": "情绪", "criteria": {"正": "正面", "负": "负面"}},
}


def fast_retry_config(config):
    """把重试延迟压到 0，让故障用例不必真等退避。"""

    return config.with_overrides({"model": {"retry": {"base_delay": 0.0, "jitter": 0.0, "max_attempts": 2}}})


async def test_system1_mixed_primitives_use_one_http_call(config):
    """6 个问题（choice/score/noul 混合）只发一次 HTTP，各自结构正确、高置信不升级。"""

    laya = RawTransport(
        data=system1_answers(
            {
                "label": ("choice", "提问", 0.91),
                "tone": ("score", 0.4, 0.88),
                "needs_reply": ("noul", None, 0.85),
                "urgency": ("score", 0.2, 0.83),
                "topic_shift": ("choice", "否", 0.9),
                "mood": ("choice", "正", 0.87),
            }
        )
    )
    star = FakeTransport(reply="通配")
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.system1({"text": "在吗"}, MIXED_QUESTIONS)

    assert len(laya.calls) == 1, "一次调用必须只发一次 HTTP"
    sent = laya.calls[0]
    assert set(sent["questions"]) == set(MIXED_QUESTIONS)
    assert sent["model"] == "auto" and sent["state"] == {"text": "在吗"}
    assert "messages" not in sent  # 不是 OpenAI 请求体
    assert response.task == "system1" and response.provider == "laya"
    assert response.raw["source"] == "system1"
    answers = response.raw["answers"]
    assert answers["label"]["type"] == "choice" and answers["label"]["choice"] == "提问"
    assert answers["tone"]["type"] == "score" and answers["tone"]["score"] == 0.4
    assert answers["needs_reply"]["type"] == "noul" and answers["needs_reply"]["noul"] is None
    assert response.raw["confidences"]["label"] == 0.91
    assert response.raw["lowest_confidence"] == 0.83
    assert response.raw["aggregate"] == SYSTEM1_AGGREGATE == "min"
    assert star.calls == [], "全部问题置信度达标时不该升级"
    assert response.ok is True and "提问" in response.text
    await router.aclose()


async def test_system1_escalates_when_any_question_is_uncertain(config):
    """聚合口径取最小值：只有一个问题低置信也整体升级，且保留 LAY A 概率。"""

    questions = {
        "label": {"type": "choice", "instructions": "分类", "criteria": {"提问": "在提问", "闲聊": "在闲聊"}},
        "tone": {"type": "score", "instructions": "语气强度", "criteria": ["0 平静", "1 激动"]},
        "urgency": {"type": "score", "instructions": "紧迫度", "criteria": ["0 不急", "1 很急"]},
    }
    laya = RawTransport(
        data=system1_answers(
            {"label": ("choice", "提问", 0.95), "tone": ("score", 0.5, 0.9), "urgency": ("score", 0.5, 0.2)}
        )
    )
    star = FakeTransport(reply='{"label": "提问", "urgency": 0.9}')
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.system1({"text": "在吗"}, questions, escalate_below=0.4)

    assert response.raw["source"] == "escalated"
    assert response.raw["answers"]["urgency"]["confidence"] == 0.2  # LAY A 概率保留
    assert response.raw["verdict"] == {"label": "提问", "urgency": 0.9}
    assert "low_confidence" in response.raw["escalation_reason"]
    assert response.raw["escalated_to"]["provider"] == "a6api"
    assert len(star.calls) == 1, "升级必须走一次对话模型"
    prompt = str(star.calls[0]["messages"])
    assert "0.2" in prompt and "urgency" in prompt  # 问题与概率一起交给大模型
    assert response.task == "system1" and response.provider == "a6api"
    await router.aclose()


async def test_system1_aggregate_is_min_not_mean(config):
    """聚合口径写死为 min：均值达标但最小值不达标仍升级；最小值达标则不升级。"""

    questions = {
        "a": {"type": "score", "instructions": "x", "criteria": ["0", "1"]},
        "b": {"type": "score", "instructions": "y", "criteria": ["0", "1"]},
        "c": {"type": "score", "instructions": "z", "criteria": ["0", "1"]},
    }
    data = system1_answers({"a": ("score", 0.5, 0.95), "b": ("score", 0.5, 0.9), "c": ("score", 0.5, 0.55)})
    laya = RawTransport(data=data)
    star = FakeTransport(reply='{"a": 0.5, "b": 0.5, "c": 0.5}')
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    escalated = await router.system1({"t": 1}, questions, escalate_below=0.6)
    assert escalated.raw["source"] == "escalated"  # 均值 0.8 ≥ 0.6，但最小值 0.55 < 0.6
    assert escalated.raw["confidences"]["c"] == 0.55

    kept = await router.system1({"t": 1}, questions, escalate_below=0.5)
    assert kept.raw["source"] == "system1"  # 最小值 0.55 ≥ 0.5
    assert len(star.calls) == 1, "只有第一次（低置信）才升级"
    await router.aclose()


@pytest.mark.parametrize(
    "error",
    [
        TransportError("LAY A 返回 503（/v1/systemone）", status=503, retryable=True),
        TransportError("LAY A 返回 401（密钥无效）", status=401, retryable=False),
        TransportError("LAY A 返回 422（请求格式错）", status=422, retryable=False),
        TimeoutError("LAY A 请求超时"),
    ],
)
async def test_system1_degrades_to_chat_when_laya_fails(config, error):
    """LAY A 503/401/422/超时都不许把未包装异常透给调用方：退回对话模型。"""

    questions = {"label": {"type": "choice", "instructions": "分类", "criteria": {"闲聊": "在闲聊"}}}
    laya = RawTransport(error=error)
    star = FakeTransport(reply='{"label": "闲聊"}')
    router = ModelRouter(fast_retry_config(config), transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.system1({"text": "在吗"}, questions)

    assert response.raw["source"] == "escalated"
    assert response.raw["answers"] == {}
    assert "laya_unavailable" in response.raw["escalation_reason"]
    assert len(star.calls) == 1
    assert response.raw["verdict"] == {"label": "闲聊"}
    await router.aclose()


async def test_system1_wraps_error_when_escalation_also_fails(config):
    """LAY A 与对话模型都失败时抛包装过的 ModelCallError，而不是 TransportError。"""

    fast = config.with_overrides({"model": {"retry": {"base_delay": 0.0, "jitter": 0.0, "max_attempts": 1}}})
    laya = RawTransport(error=TransportError("LAY A 返回 422（请求格式错）", status=422, retryable=False))
    router = ModelRouter(fast, transport=FakeTransport(failures=99))
    router.set_transport(laya, provider="laya")

    with pytest.raises(ModelCallError) as excinfo:
        await router.system1(
            {"text": "在吗"}, {"label": {"type": "choice", "instructions": "分类", "criteria": {"闲聊": "在闲聊"}}}
        )
    assert not isinstance(excinfo.value, TransportError)
    assert "system1 升级对话模型失败" in str(excinfo.value)
    await router.aclose()


async def test_system1_validates_questions_locally(config):
    """问题结构非法时本地报错（不发请求）。"""

    laya = RawTransport(data=system1_answers({"label": ("choice", "提问", 0.9)}))
    router = ModelRouter(config, transport=FakeTransport())
    router.set_transport(laya, provider="laya")

    with pytest.raises(TransportError):
        await router.system1({"t": 1}, {"q": {"type": "chat"}})
    assert laya.calls == []
    await router.aclose()


# ---- t27 修复轮：完备性 / 解码降级 / 异常契约 / 可观测性 / 脱敏 --------
SIX_QUESTIONS = {
    "q1": {"type": "score", "instructions": "x1", "criteria": ["0", "1"]},
    "q2": {"type": "score", "instructions": "x2", "criteria": ["0", "1"]},
    "q3": {"type": "score", "instructions": "x3", "criteria": ["0", "1"]},
    "q4": {"type": "score", "instructions": "x4", "criteria": ["0", "1"]},
    "q5": {"type": "score", "instructions": "x5", "criteria": ["0", "1"]},
    "q6": {"type": "score", "instructions": "x6", "criteria": ["0", "1"]},
}


async def test_system1_counts_unanswered_questions_as_uncertain(config):
    """F1：提 6 问答 1 问 → 缺失 qid 按 confidence=0.0 计入，必然升级并显式标注缺失。"""

    laya = RawTransport(data=system1_answers({"q1": ("score", 0.9, 0.95)}))
    star = FakeTransport(reply='{"q1": 0.9}')
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.system1({"text": "在吗"}, SIX_QUESTIONS)

    assert response.raw["source"] == "escalated", "缺 5 个决策必须升级，不能静默返回 system1"
    assert response.raw["answered"] == ["q1"]
    assert response.raw["missing_qids"] == ["q2", "q3", "q4", "q5", "q6"]
    assert response.raw["confidences"] == {
        "q1": 0.95,
        "q2": 0.0,
        "q3": 0.0,
        "q4": 0.0,
        "q5": 0.0,
        "q6": 0.0,
    }
    assert response.raw["lowest_confidence"] == 0.0
    assert len(laya.calls) == 1 and len(star.calls) == 1
    await router.aclose()


async def test_system1_escalates_when_laya_returns_non_object(config):
    """F2：LAY A 返回 JSON 数组（非对象）→ 走统一降级，得到升级而不是 AttributeError。"""

    laya = RawTransport(data=[1, 2, 3])
    star = FakeTransport(reply='{"label": "闲聊"}')
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.system1(
        {"text": "在吗"}, {"label": {"type": "choice", "instructions": "x", "criteria": {"闲聊": "在闲聊"}}}
    )

    assert response.raw["source"] == "escalated"
    assert "laya_unavailable" in response.raw["escalation_reason"]
    assert response.raw["missing_qids"] == ["label"]
    assert router.health()["stats"]["laya_unavailable"] == 1
    await router.aclose()


async def test_classify_falls_back_when_laya_answers_are_empty(config):
    """F3：LAY A 返回 200 但 answers 为空 → 视作该目标失败，回落 fallback 模型 grok-4.6。"""

    laya = RawTransport(data={"model": "auto", "answers": {}, "usage": {"input_tokens": 10, "output_tokens": 0}})
    star = FakeTransport(reply='{"label": "提问", "scores": {"提问": 1.0}}')
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.classify("在吗", ["提问", "闲聊"])

    assert response.provider == "a6api" and response.model == "grok-4.6"
    assert response.label == "提问"
    assert len(laya.calls) == 1 and len(star.calls) == 1
    stats = router.health()["stats"]
    assert stats["fallback"] == 1 and stats["decode_failed"] == 1
    await router.aclose()


async def test_decode_failure_is_wrapped_when_no_target_decodes(config):
    """F3（兜底）：解码期的类型错误也包成 ModelCallError（带 attempts），不裸抛 ValueError。"""

    embed_fake = RawTransport(data={"data": [{"embedding": ["不是数字"]}]})
    router = ModelRouter(config, transport=FakeTransport())
    router.set_transport(embed_fake, provider="local")

    with pytest.raises(ModelCallError) as excinfo:
        await router.embed(["文本"])

    assert "解码失败" in str(excinfo.value)
    assert excinfo.value.attempts >= 1
    assert router.health()["stats"]["decode_failed"] == 1
    await router.aclose()


async def test_non_retryable_transport_error_surfaces_unwrapped(config):
    """F4（异常契约选项 ②）：不可重试的传输错误以裸 TransportError 抛出（带 status），不二次包装。"""

    embed_fake = RawTransport(error=TransportError("上游 401", status=401, retryable=False))
    router = ModelRouter(config, transport=FakeTransport())
    router.set_transport(embed_fake, provider="local")

    with pytest.raises(TransportError) as excinfo:
        await router.embed(["文本"])

    assert excinfo.value.status == 401
    assert excinfo.value.retryable is False
    assert not isinstance(excinfo.value, ModelCallError)
    await router.aclose()


async def test_system1_laya_unavailable_is_visible_in_health(config):
    """F5：LAY A 整体失效（401）→ 除静默回落外，health() 能看到 laya_unavailable 计数。"""

    laya = RawTransport(error=TransportError("LAY A 返回 401（密钥无效）", status=401, retryable=False))
    star = FakeTransport(reply='{"label": "闲聊"}')
    router = ModelRouter(fast_retry_config(config), transport=star)
    router.set_transport(laya, provider="laya")

    await router.system1(
        {"text": "在吗"}, {"label": {"type": "choice", "instructions": "x", "criteria": {"闲聊": "在闲聊"}}}
    )

    stats = router.health()["stats"]
    assert stats["laya_unavailable"] == 1
    assert stats["errors"] == 1
    await router.aclose()


def test_codec_decode_response_rejects_laya_answers_envelope():
    """F6：decode_response 遇到 LAY A 的 answers 信封必须报错，不能静默返回空结果。"""

    with pytest.raises(ModelCallError) as excinfo:
        decode_response({"answers": {"label": {"type": "choice", "choice": "提问"}}}, task="classify", labels=["提问"])

    assert "system1" in str(excinfo.value)


async def test_system1_does_not_escalate_when_confidence_equals_threshold(config):
    """F7：confidence == escalate_below 时不升级（严格小于才升级），固化该语义。"""

    laya = RawTransport(data=system1_answers({"label": ("choice", "提问", 0.4)}))
    star = FakeTransport(reply='{"label": "提问"}')
    router = ModelRouter(config, transport=star)
    router.set_transport(laya, provider="laya")

    response = await router.system1(
        {"text": "在吗"},
        {"label": {"type": "choice", "instructions": "x", "criteria": {"提问": "在提问"}}},
        escalate_below=0.4,
    )

    assert response.raw["source"] == "system1"
    assert response.raw["lowest_confidence"] == 0.4
    assert response.raw["missing_qids"] == []
    assert star.calls == []
    await router.aclose()


def test_redact_masks_secrets_in_free_text():
    """F8：日志/异常出口的自由文本脱敏（Bearer / sk- / JSON 里的 api_key、token）。"""

    text = 'Authorization: Bearer sk-abcdefgh12345678 {"api_key": "sk-cau9abcdef123", "token": "t0ken-value"}'
    masked = redact(text)

    assert "sk-abcdefgh12345678" not in masked
    assert "sk-cau9abcdef123" not in masked
    assert "t0ken-value" not in masked
    assert "Bearer sk-" not in masked
    assert REDACTED in masked
    assert "sk-" not in _short("Bearer sk-abcdefgh1234 后续自由文本")


async def test_router_log_exit_redacts_secrets(config):
    """F8：日志出口（_log）统一脱敏，异常 repr 里的密钥不进日志。"""

    class RecordingLogger:
        def __init__(self) -> None:
            self.records: list[tuple[str, str, dict]] = []

        def log(self, level: str, event: str, **fields: object) -> None:
            self.records.append((level, event, dict(fields)))

    logger = RecordingLogger()
    laya = RawTransport(error=TransportError("上游 401 Bearer sk-abcdefgh1234", status=401, retryable=False))
    router = ModelRouter(fast_retry_config(config), transport=FakeTransport(reply='{"label": "闲聊"}'), logger=logger)
    router.set_transport(laya, provider="laya")

    await router.system1(
        {"text": "在吗"}, {"label": {"type": "choice", "instructions": "x", "criteria": {"闲聊": "在闲聊"}}}
    )
    router._log("warning", "test.event", error="Bearer sk-abcdefgh1234")

    dumped = str(logger.records)
    assert "sk-abcdefgh1234" not in dumped
    assert any(event == "model.system1_unavailable" for _level, event, _fields in logger.records)
    assert any(event == "test.event" for _level, event, _fields in logger.records)
    await router.aclose()
