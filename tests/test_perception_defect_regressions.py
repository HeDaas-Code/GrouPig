"""感知层已确认缺陷的回归测试（判别器空转写 / 去重窗泄漏 / @全体点名）。

每个缺陷的红→绿证据见交付说明。测试命名统一带 ``defect_`` 前缀便于筛选；
个别「护栏」测试（标注了 ``guard``）用于防止过度修复，回退实现时不会变红。
"""

from __future__ import annotations

from typing import Any

from grouppig.infra.runtime.registry import Registry
from grouppig.perception.behavior.classifier.aggregator import build_aggregator
from grouppig.perception.behavior.classifier.llm_judge import LLMJudge
from grouppig.perception.normalizer.dedup import Deduper
from grouppig.perception.observer.window import RollingWindow
from perception_helpers import BASE_TS, GROUP_ID, conversation, message

# ---------------------------------------------------------------------------
# A. 判别器拿到的是空转写
# ---------------------------------------------------------------------------


class ModelStub:
    """假 ``rpc:model.classify``：记录提示词，并按**转写内容**回答。

    转写为空时回 ``silence``（模型对空输入最容易给出的「冷场」），有内容时回 ``smalltalk``；
    于是「判别器看到的是不是真转写」直接体现在群行为上。
    """

    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def classify(self, text: Any, labels: Any = None, **_: Any) -> dict[str, Any]:
        prompt = str(text)
        self.prompts.append(prompt)
        transcript = prompt.split("群聊记录：", 1)[-1].strip()
        label = "smalltalk" if transcript else "silence"
        return {"label": label, "scores": {label: 0.93}, "model": "stub"}


def _window(rows: list[dict[str, Any]], *, seconds: int = 300, now: float | None = None) -> RollingWindow:
    window = RollingWindow(seconds=seconds, clock=lambda: float(now if now is not None else BASE_TS))
    for row in rows:
        window.slide(GROUP_ID, message=row, now=row["ts"])
    return window


def _aggregator(stub: ModelStub, window: RollingWindow) -> Any:
    """按生产接线装配：判别器注册成 ``rpc:behavior.llm.judge``，模型是假的下游。"""

    registry = Registry()
    registry.register("rpc:model.classify", stub.classify, module="tests.stub.model")
    judge = LLMJudge(registry=registry)
    judge.register(registry)
    return build_aggregator(
        registry=registry,
        window=window,
        judge=judge,
        publish=False,
        cascade_interrupt=False,
        cascade_presets=False,
    )


async def test_defect_a_classify_materialises_window_rows_for_judge():
    """生产路径（只给 RollingWindow）下，判别器必须收到真实消息行而不是空转写。"""

    stub = ModelStub()
    window = _window(conversation())
    aggregator = _aggregator(stub, window)

    payload = await aggregator.classify(GROUP_ID, window=window, seconds=300, now=BASE_TS + 10)

    assert stub.prompts, "判别器根本没被调用"
    assert "今晚打本吗" in stub.prompts[-1], "提示词里必须带真实消息（旧实现以「群聊记录：」结尾）"
    assert payload["llm"]["message_count"] == 4
    # 旧实现转写为空 → 模型回 silence（硬标签）→ 群行为被翻成「冷场」
    assert payload["llm"]["label"] == "smalltalk"
    assert payload["behavior"] == "smalltalk"


async def test_defect_a_judge_fallback_path_also_gets_the_rows():
    """判别器 RPC 未注册时走同域直连（``call_leaf`` 的 fallback），同样必须拿到真实消息行。"""

    stub = ModelStub()
    registry = Registry()
    registry.register("rpc:model.classify", stub.classify, module="tests.stub.model")
    window = _window(conversation())
    aggregator = build_aggregator(
        registry=registry,
        window=window,
        judge=LLMJudge(registry=registry),
        publish=False,
        cascade_interrupt=False,
        cascade_presets=False,
    )

    payload = await aggregator.classify(GROUP_ID, window=window, seconds=300, now=BASE_TS + 10)

    assert stub.prompts, "同域直连路径下判别器没被调用"
    assert "今晚打本吗" in stub.prompts[-1]
    assert payload["llm"]["message_count"] == 4


async def test_defect_a_empty_window_returns_empty_window_instead_of_label():
    """空窗口不能产出硬标签：返回 ``ok=False`` / ``source="empty_window"``，且不调模型。"""

    stub = ModelStub()
    registry = Registry()
    registry.register("rpc:model.classify", stub.classify, module="tests.stub.model")
    judge = LLMJudge(registry=registry)

    judged = await judge.judge(window=RollingWindow(seconds=300, clock=lambda: BASE_TS), now=BASE_TS)

    assert judged["ok"] is False
    assert judged["source"] == "empty_window"
    assert judged["label"] == ""
    assert judged["available"] is False
    assert judged["message_count"] == 0
    assert stub.prompts == [], "空窗口不该浪费一次模型调用"
    # 聚合器只认 BEHAVIORS 里的标签：空标签不可能被当成「已定论」
    assert judged["label"] not in ("flooding", "silence", "repeat", "discussion", "smalltalk", "exposition")


async def test_defect_a_judge_still_judges_explicit_messages():
    """guard：给了真实消息行时判别器照旧工作（空窗口护栏不能把正常路径一起挡掉）。"""

    registry = Registry()
    registry.register("rpc:model.classify", ModelStub().classify, module="tests.stub.model")
    judge = LLMJudge(registry=registry)

    judged = await judge.judge(conversation(), now=BASE_TS + 10)

    assert judged["ok"] is True
    assert judged["label"] == "smalltalk"
    assert judged["message_count"] == 4


async def test_defect_a_empty_llm_result_never_resolves_the_group():
    """guard：``ok=False`` / 空标签的模型结论不得被当成「已定论」（不能翻群行为）。"""

    aggregator = build_aggregator(registry=Registry(), publish=False, cascade_interrupt=False, cascade_presets=False)

    decided = aggregator.decide(
        rules={"behavior": "smalltalk", "resolved": False, "confidence": 0.45},
        llm={"ok": False, "source": "empty_window", "label": "", "message_count": 0},
    )

    assert decided["resolved"] is False
    assert decided["behavior"] == "smalltalk"
    assert decided["sources"] == ["rules"]


# ---------------------------------------------------------------------------
# B. 去重窗过期后仍判重复
# ---------------------------------------------------------------------------


def test_defect_b_duplicate_expires_with_the_window():
    """重复计数必须随窗口过期归零，否则 ``duplicate`` 永久为 True。"""

    deduper = Deduper(window_seconds=120, repeat_threshold=3)
    first = deduper.dedup(message(0, "哈哈哈", ts=BASE_TS))
    dup = deduper.dedup(message(1, "哈哈哈", ts=BASE_TS + 1))
    assert first["duplicate"] is False
    assert dup["duplicate"] is True and dup["reason"] == "exact_repeat"

    # 窗口（120s）已过（两条都在窗外）：桶行被清掉后计数必须一起归零
    later = deduper.dedup(message(2, "哈哈哈", ts=BASE_TS + 122))
    assert later["duplicate"] is False, "窗口过期后不能再判重复（否则 threads.segment 被永久跳过）"
    assert later["reason"] == ""
    assert deduper.snapshot()["stats"]["duplicates"] == 1


def test_defect_b_repeat_count_follows_live_window():
    """复读计数与「窗口里还活着的行」一一对应：过期后重新从头数。"""

    deduper = Deduper(window_seconds=60, repeat_threshold=3)
    deduper.dedup(message(0, "6", ts=BASE_TS))
    deduper.dedup(message(1, "6", ts=BASE_TS + 1))
    third = deduper.dedup(message(2, "6", ts=BASE_TS + 2))
    assert third["repeat"] is True

    # 全部过期后重开一轮：第一遍不能再算复读
    fresh = deduper.dedup(message(3, "6", ts=BASE_TS + 1000))
    assert fresh["duplicate"] is False
    assert fresh["repeat"] is False
    assert fresh["repeat_count"] == 1


async def test_defect_b_cleaner_resumes_thread_segment_after_window(perception_container):
    """生产后果：窗口过期后 ``rpc:threads.segment`` 必须重新被尝试（不再被永久跳过）。"""

    container = perception_container
    window = container.perception.window
    rows = (message(0, "哈哈哈", ts=BASE_TS), message(1, "哈哈哈", ts=BASE_TS + 1))

    first = await container.call("rpc:normalizer.clean", rows[0], window=window, now=rows[0]["ts"])
    dup = await container.call("rpc:normalizer.clean", rows[1], window=window, now=rows[1]["ts"])
    assert first["duplicate"] is False
    assert dup["duplicate"] is True
    assert "rpc:threads.segment" not in dup["skipped"], "重复消息本来就不进编织"

    later = await container.call(
        "rpc:normalizer.clean", message(2, "哈哈哈", ts=BASE_TS + 122), window=window, now=BASE_TS + 122
    )
    assert later["duplicate"] is False
    assert "rpc:threads.segment" in later["skipped"], "窗口过期后必须重新尝试编织（会话层缺席即 skipped）"


# ---------------------------------------------------------------------------
# D. @全体成员 被算成「点名自己」（根因在网关事件解码，本域只消费 at_self）
# ---------------------------------------------------------------------------


def _at_segments(qq: str) -> list[dict[str, Any]]:
    return [{"type": "at", "data": {"qq": qq}}, {"type": "text", "data": {"text": " 都来打本"}}]


def _decoded_row(qq: str) -> dict[str, Any]:
    """走完整入站链路：网关解码 → 感知层归一化消息行。"""

    from grouppig.gateway.adapter.event_codec import decode_event
    from grouppig.perception.runtime.messages import from_event

    event = decode_event(
        {
            "post_type": "message",
            "message_type": "group",
            "group_id": GROUP_ID,
            "user_id": 1001,
            "self_id": 999,
            "message_id": 7001,
            "message": _at_segments(qq),
            "time": int(BASE_TS),
        }
    )
    return from_event(event.as_dict(), now=BASE_TS)


def test_defect_d_at_all_does_not_grant_the_mention_floor():
    """``@全体成员`` 不是点名：不得把插话分抬到 mention 地板。

    根因（``QQEvent.mentions`` 把 ``qq == "all"`` 也算命中）在网关事件解码，**不在本域**；
    本域只消费 ``at_self``，这里钉住「@全体 ≠ 点名」这条端到端不变量。
    """

    from grouppig.perception.interrupt.scorer import InterruptScorer

    scorer = InterruptScorer(config=None)
    at_all = scorer.compute([_decoded_row("all")], features={"topic_focus": 0.0}, behavior="smalltalk", self_id=999)
    at_bot = scorer.compute([_decoded_row("999")], features={"topic_focus": 0.0}, behavior="smalltalk", self_id=999)

    assert at_all["components"]["mentioned"] == 0.0, "@全体成员 被算成了点名自己"
    assert at_all["total"] < scorer.mention_floor
    assert at_all["context"]["at_self"] is False
    # 真 @bot 必须仍然拿到地板（护栏：不能把点名信号一起关掉）
    assert at_bot["components"]["mentioned"] == 1.0
    assert at_bot["total"] >= scorer.mention_floor
