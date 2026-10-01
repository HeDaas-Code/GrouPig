"""感知 → 会话 的「特征 → 候选话题」级联回归（F3）。

背景：``Featurizer.features()`` 的级联段曾以错误形态调用 ``rpc:topic.candidate.generate``——
把整条消息 ``dict(message)`` 当**第一位置参数**（那其实是 ``group_id``）、并把 ``features``
传成单个特征 dict。后果有两条，任一条都让这条设计依赖边永久 ``failed``：

1. ``dict(message)`` 被绑到 ``group_id``，``int(group_id or 0)`` 也救不回来；
2. ``features`` 传成 dict 后，处理器的 ``for index, item in enumerate(features)`` 会迭代出**字符串键**，
   紧接着 ``item.get(...)`` 抛 ``AttributeError: str object has no attribute get``。

修法是让级联用契约形态调用（``group_id`` 位置参数 + ``features`` 序列 + ``now``），
并在 ``extract()`` 的返回体里补上 ``ts``（否则候选话题只能拿到 0.0 的时间戳）。
本文件锁住这三点，并额外守住节流路径不被改坏。

注意：本文件**故意不写进** ``tests/test_perception_units.py``——那个文件被并行变更 F1（t36）占用。
"""

from __future__ import annotations

from typing import Any

import pytest

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.normalizer.featurizer import (
    DOWNSTREAM_TOPIC,
    RPC_FEATURES,
    Featurizer,
)
from grouppig.perception.runtime import calls as calls_module
from grouppig.session.topic.detector import candidate as candidate_module
from perception_helpers import BASE_TS, GROUP_ID, SENDER_IDS, message

#: 被级联的下游名字（``rpc:topic.candidate.generate``）。
TOPIC = DOWNSTREAM_TOPIC

#: 内容里必须出现重复的 CJK 二元组（``打本`` 出现两次），否则候选短语生成器的
#: ``min_count`` 门槛过不去、候选数会是 0，用例就变成空转。
CASCADED_TEXT = "打本打本，缺一个奶妈"


def wired_featurizer(*, with_topic: bool = True, throttle: Any = None) -> tuple[Registry, Featurizer]:
    """装配一个同时注册了 featurizer（与可选的候选话题处理器）的真实注册表。

    走**真实 rpc 边**（``registry.acall``），不走进程内直连——测的就是那条设计依赖。
    """

    registry = Registry()
    featurizer = Featurizer(registry=registry, config=None, throttle=throttle)
    featurizer.register(registry)
    if with_topic:
        candidate_module.register(registry)
    return registry, featurizer


def one_message(text: str = CASCADED_TEXT, *, ts: float = BASE_TS, index: int = 0) -> dict[str, Any]:
    return message(index, text, ts=ts, sender_id=SENDER_IDS[0])


# ---- ① 级联不再是 failed ------------------------------------------------
async def test_cascade_topic_call_is_ok_not_failed() -> None:
    """改动前 ``rpc:topic.candidate.generate`` 的 status 是 ``failed``（AttributeError），修后必须是 ``ok``。"""

    registry, _featurizer = wired_featurizer()
    payload = await registry.acall(RPC_FEATURES, one_message(), force=True)

    downstream = payload["downstream"]
    assert payload["cascaded"] is True
    assert TOPIC in downstream["calls"], "级联压根没调用候选话题处理器"
    assert downstream["calls"][TOPIC] == calls_module.STATUS_OK, downstream
    assert downstream["failed"] == [], f"下游仍然失败：{downstream}"
    assert payload["throttled"] == [], "force=True 不该被节流"


# ---- ② 调用形态：group_id 位置参数 + features 序列 + ts 补齐 -------------
async def test_cascade_passes_group_id_positionally_and_features_as_a_sequence() -> None:
    """锁住调用形态，并用真实处理器验证候选真的带上了时间戳。"""

    # 2a. 用一个记录型处理器直接观察实参（这是「形态」的唯一可靠取证方式）
    registry = Registry()
    featurizer = Featurizer(registry=registry, config=None)
    featurizer.register(registry)
    seen: dict[str, Any] = {}

    async def recorder(
        group_id: Any = None,
        messages: Any = None,
        *,
        features: Any = None,
        now: Any = None,
        **_: Any,
    ) -> dict[str, Any]:
        seen.update({"group_id": group_id, "messages": messages, "features": features, "now": now})
        return {"candidates": [], "count": 0, "source": "features", "group_id": int(group_id or 0)}

    registry.register(TOPIC, recorder, module=candidate_module.MODULE, replace=True)
    await registry.acall(RPC_FEATURES, one_message(), force=True)

    assert isinstance(seen["group_id"], int) and not isinstance(seen["group_id"], bool), (
        f"第一位置参数必须是真正的 group_id(int)，实得 {type(seen['group_id']).__name__}: {seen['group_id']!r}"
    )
    assert seen["group_id"] == GROUP_ID, "第一位置参数必须是真实群号，而不是被塞进来的消息 dict"
    assert seen["messages"] is None, "消息 dict 不该被塞进 messages 位置参数"
    assert isinstance(seen["features"], (list, tuple)), f"features 必须是序列，实得 {type(seen['features']).__name__}"
    assert len(seen["features"]) == 1
    assert seen["features"][0]["group_id"] == GROUP_ID
    assert seen["now"] == pytest.approx(BASE_TS), "now 必须是这条消息的时间戳，否则候选时间区间会算成 0"

    # 2b. 真实处理器端到端：候选数 >= 1，且候选带着真实 ts（> 0）
    real_registry, real_featurizer = wired_featurizer()
    extracted = real_featurizer.extract(one_message())
    assert extracted["ts"] == pytest.approx(BASE_TS), "extract() 的返回体里必须有真实 ts"
    assert extracted["ts"] > 0

    payload = await real_registry.acall(RPC_FEATURES, one_message(), force=True)
    assert payload["downstream"]["calls"][TOPIC] == calls_module.STATUS_OK

    # 直连同一条 rpc 边复核候选内容（级联的返回值里只有 status，不带下游 result）
    generated = await real_registry.acall(
        TOPIC,
        extracted["group_id"],
        features=[dict(extracted)],
        now=extracted["ts"],
    )
    assert generated["count"] >= 1, "内容里有两个重复二元组，候选数不该是 0"
    assert generated["source"] == "features"
    candidate = generated["candidates"][0]
    assert candidate["first_ts"] > 0, "候选的第一条时间戳必须是真实 ts（改动前只能拿到 0.0）"
    assert candidate["last_ts"] > 0
    assert candidate["first_ts"] == pytest.approx(BASE_TS)
    assert candidate["group_id"] == GROUP_ID


# ---- ③ 节流路径不被破坏 --------------------------------------------------
async def test_throttled_topic_cascade_is_not_reported_as_failed() -> None:
    """节流拦下时下游不能被记成 ``failed``；处理器缺席时也只能是 ``skipped``。

    说明（与任务描述的措辞差异）：实现里「被节流」并不写进 ``downstream.calls``，而是记在
    ``payload["throttled"]``；``skipped`` 专门留给「处理器未注册」。本用例按实现的真实语义断言，
    两种「没调用」都不会掉进 ``failed``，节流行为本身未被本次修复改变。
    """

    throttle = calls_module.Throttle(min_interval=10_000.0, min_messages=1, clock=lambda: BASE_TS)
    registry, _featurizer = wired_featurizer(throttle=throttle)

    first = await registry.acall(RPC_FEATURES, one_message(), now=BASE_TS)
    assert first["downstream"]["calls"][TOPIC] == calls_module.STATUS_OK

    second = await registry.acall(RPC_FEATURES, one_message(index=1, ts=BASE_TS + 1), now=BASE_TS + 1)
    assert TOPIC in second["throttled"], "第二次必须被节流拦下"
    assert TOPIC not in second["downstream"]["calls"], "被节流的下游不该出现在 calls 里"
    assert second["downstream"]["failed"] == [], "被节流绝不能被记成 failed"
    assert second["cascaded"] is True

    # 处理器缺席（独立注册表）：只能是 skipped
    bare_registry, _bare = wired_featurizer(with_topic=False)
    absent = await bare_registry.acall(RPC_FEATURES, one_message(), force=True)
    assert absent["downstream"]["calls"][TOPIC] == calls_module.STATUS_SKIPPED, absent
    assert absent["downstream"]["failed"] == []
