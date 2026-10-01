"""perception 域测试辅助：事件/消息构造、窗口回放、容器夹具。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from grouppig.infra.logger import RuntimeLogger  # noqa: E402
from grouppig.infra.model_gateway.router import ModelRouter  # noqa: E402
from grouppig.infra.runtime.bus import EventBus  # noqa: E402
from grouppig.infra.runtime.di import Container  # noqa: E402
from grouppig.infra.runtime.registry import Registry  # noqa: E402
from grouppig.infra.token_budget.meter import TokenMeter  # noqa: E402
from grouppig.perception.runtime.di import Perception, build_perception  # noqa: E402

#: 测试基准时间（固定值，保证断言可复现）。
BASE_TS = 1_700_000_000.0

#: 测试用群号与群友。
GROUP_ID = 100200300
SENDER_IDS = (1001, 1002, 1003)
SELF_ID = 999


def qq_event(
    index: int = 0,
    text: str = "今晚打本吗",
    *,
    group_id: int = GROUP_ID,
    user_id: int = SENDER_IDS[0],
    ts: float = BASE_TS,
    message_id: int | None = None,
    at_self: bool = False,
    segments: list[dict[str, Any]] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """造一条 OneBot v11 群消息事件（``kafka:grouppig.qq.message.received`` 的载荷形态）。"""

    payload: dict[str, Any] = {
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "group_id": group_id,
        "user_id": user_id,
        "self_id": SELF_ID,
        "time": int(ts),
        "message_id": message_id if message_id is not None else 6000 + index,
        "raw_message": text,
        "text": text,
        "segments": segments or [{"type": "text", "data": {"text": text}}],
        "sender": {"nickname": f"群友{user_id % 100}", "role": "member"},
        "at_self": at_self,
    }
    payload.update(overrides)
    return payload


def message(
    index: int = 0,
    text: str = "今晚打本吗",
    *,
    group_id: int = GROUP_ID,
    sender_id: int = SENDER_IDS[0],
    ts: float = BASE_TS,
    **overrides: Any,
) -> dict[str, Any]:
    """造一条已归一化的内部消息（与 ``chat_messages`` 表字段对齐）。"""

    payload: dict[str, Any] = {
        "message_id": f"msg-{index}",
        "group_id": group_id,
        "sender_id": sender_id,
        "sender_name": f"群友{sender_id % 100}",
        "role": "member",
        "msg_type": "text",
        "content": text,
        "raw_content": text,
        "mentions": [],
        "at_self": False,
        "reply_to": "",
        "ts": ts,
        "segments": [{"type": "text", "data": {"text": text}}],
        "event_id": f"evt-{index}",
        "kind": "message.group",
        "self_id": SELF_ID,
        "known": True,
    }
    payload.update(overrides)
    return payload


def burst(
    count: int = 6,
    *,
    text: str = "打本打本",
    step: float = 1.0,
    senders: tuple[int, ...] = SENDER_IDS,
    start: int = 0,
    base_ts: float = BASE_TS,
    **overrides: Any,
) -> list[dict[str, Any]]:
    """造一串（近似）相同内容的消息，用于刷屏/复读场景。"""

    return [
        message(
            start + offset,
            text,
            sender_id=senders[offset % len(senders)],
            ts=base_ts + offset * step,
            **overrides,
        )
        for offset in range(count)
    ]


def conversation(
    *,
    base_ts: float = BASE_TS,
    step: float = 2.0,
    texts: tuple[str, ...] = ("今晚打本吗", "打本打本，缺一个奶妈", "我奶妈，带我", "那今晚八点"),
) -> list[dict[str, Any]]:
    """造一段正常聊天（轮流发言、内容不同）。"""

    return [
        message(index, text, sender_id=SENDER_IDS[index % len(SENDER_IDS)], ts=base_ts + index * step)
        for index, text in enumerate(texts)
    ]


async def warm(
    perception: Perception,
    messages: list[dict[str, Any]],
    *,
    group_id: int = GROUP_ID,
    now: float | None = None,
) -> dict[str, Any]:
    """把消息灌进滚动窗（模拟观察器已收拢过这批消息）。"""

    for row in messages:
        perception.window.slide(group_id, message=row, now=row["ts"])
    return perception.window.slice(group_id, now=now or (messages[-1]["ts"] if messages else BASE_TS))


def make_container(config: Any, *, strict_topics: bool = True) -> Container:
    """infra 容器 + 独立注册表（不污染全局默认注册表）。"""

    registry = Registry()
    logger = RuntimeLogger()
    meter = TokenMeter(logger=logger, config=config)
    return Container(
        config=config,
        registry=registry,
        logger=logger,
        bus=EventBus(logger=logger, strict_topics=strict_topics),
        meter=meter,
        router=ModelRouter(config, logger=logger, meter=meter),
    )


def make_perception(config: Any, container: Container, **options: Any) -> Perception:
    """按容器装配感知层（注册 21 个名字，但不订阅事件）。"""

    perception = build_perception(container=container, **options)
    perception.register()
    perception.wire()
    perception.installed = True
    return perception
