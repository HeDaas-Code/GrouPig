"""gateway 路由层测试：命令识别 / 优先级队列 / 事件分用。"""

from __future__ import annotations

import asyncio

from gateway_helpers import EventCollector, IngestSink, group_message, heartbeat, private_message, recall_notice
from grouppig.gateway.router import command as command_module
from grouppig.gateway.router import demux as demux_module
from grouppig.gateway.router import priority as priority_module
from grouppig.gateway.router.command import CommandRouter
from grouppig.gateway.router.demux import EventRouter
from grouppig.gateway.router.priority import PriorityQueue
from grouppig.infra.runtime.bus import EventBus
from grouppig.infra.runtime.registry import Registry

SELF_ID = 10001


class RecordingSender:
    """假发送器：记录 composer 调用。"""

    def __init__(self, *, ok: bool = True) -> None:
        self.sent: list[dict] = []
        self.ok = ok

    async def send_reply(self, reply=None, **kwargs):
        payload = dict(reply) if isinstance(reply, dict) else {"text": reply}
        payload.update(kwargs)
        self.sent.append(payload)
        return {"ok": self.ok, "sent": 1}


class StubRetractor:
    def __init__(self) -> None:
        self.notes: list[dict] = []

    def notify(self, **kwargs):
        self.notes.append(kwargs)
        return kwargs


# ---- 命令识别 -----------------------------------------------------------
def test_recognize_slash_commands_and_aliases():
    router = CommandRouter()

    help_match = router.recognize("/help")
    assert help_match.kind == "help"
    assert help_match.is_command is True
    assert help_match.route == command_module.ROUTE_LOCAL
    assert router.recognize("/帮助").kind == "help"
    assert router.recognize("！状态").kind == "status"
    assert router.recognize("/ping").kind == "ping"

    toggle = router.recognize("/闭嘴")
    assert toggle.kind == "toggle"
    assert toggle.name == "toggle"

    persona = router.recognize("/人设 你是谁")
    assert persona.kind == "persona_probe"
    assert persona.route == command_module.ROUTE_PERSONA
    assert persona.args == ("你是谁",)

    unknown = router.recognize("/不存在的命令")
    assert unknown.kind == "unknown"
    assert unknown.is_command is True


def test_recognize_fixed_phrases_without_prefix():
    router = CommandRouter()

    assert router.recognize("在吗").kind == "ping"
    assert router.recognize("闭嘴").kind == "toggle"
    assert router.recognize("闭嘴吧").kind == "toggle"
    assert router.recognize("别刷屏").kind == "toggle"
    assert router.recognize("说话").kind == "toggle"


def test_recognize_does_not_hijack_normal_messages():
    router = CommandRouter()

    for text in ("说话的语气很重要", "今天在吗的时候我睡着了", "大家早上好", "帮我想个名字"):
        match = router.recognize(text)
        assert match.is_command is False, text
        assert match.kind in ("none", "mention")

    plain = router.recognize("普通消息")
    assert plain.kind == "none"
    assert plain.route == command_module.ROUTE_PERCEPTION


def test_recognize_mention_only_and_empty():
    router = CommandRouter()

    mention = router.recognize(f"@{SELF_ID}", segments=[{"type": "at", "data": {"qq": str(SELF_ID)}}])
    assert mention.kind == "mention"
    assert mention.is_command is False
    assert router.recognize("").kind == "none"


# ---- 命令执行 -----------------------------------------------------------
def test_execute_local_commands():
    router = CommandRouter()

    help_result = router.execute("/help")
    assert help_result.handled is True
    assert "/help" in help_result.reply
    assert help_result.data["commands"]

    ping = router.execute("/ping")
    assert ping.handled is True
    assert ping.reply == "在呢，怎么啦？"

    whoami = router.execute("/whoami", user_id=200, group_id=100, nickname="小明")
    assert "小明" in whoami.reply
    assert whoami.data["user_id"] == 200

    status = router.execute("/status", status={"connected": True, "queue_depth": 3})
    assert "连接正常" in status.reply
    assert "队列 3 条" in status.reply

    off = router.execute("/闭嘴")
    assert off.handled is True
    assert off.data["action"] == "off"
    assert router.features["speak"] is False

    on = router.execute("/说话")
    assert on.data["action"] == "on"
    assert router.features["speak"] is True

    query = router.execute("/开关")
    assert query.data["action"] == "query"


def test_execute_persona_and_unknown_and_plain():
    router = CommandRouter()

    persona = router.execute("/人设", user_id=200, group_id=100)
    assert persona.handled is False
    assert persona.route == command_module.ROUTE_PERSONA
    assert persona.data["user_id"] == 200

    unknown = router.execute("/没这个")
    assert unknown.handled is True
    assert "没听过" in unknown.reply

    plain = router.execute("随便聊聊")
    assert plain.handled is False
    assert plain.route == command_module.ROUTE_PERCEPTION


async def test_rpc_command_handlers():
    router = CommandRouter()
    registry = Registry()
    command_module.register(registry, router)
    assert set(registry.names()) == {"rpc:command.recognize", "rpc:command.execute"}

    match = await registry.acall("rpc:command.recognize", "/help")
    assert match["kind"] == "help"

    result = await registry.acall("rpc:command.execute", text="/ping")
    assert result["handled"] is True
    assert result["reply"] == "在呢，怎么啦？"


# ---- 优先级队列 ---------------------------------------------------------
def test_priority_mapping():
    assert priority_module.priority_for("command") == 0
    assert priority_module.priority_for("message.group", at_self=True) == 1
    assert priority_module.priority_for("message.private") == 2
    assert priority_module.priority_for("message.group") == 3
    assert priority_module.priority_for("notice.group_recall") == 4
    assert priority_module.priority_for("meta_event.heartbeat") == 5
    assert priority_module.priority_for("something.else") == 6


def test_queue_orders_by_priority_then_fifo():
    queue = PriorityQueue(max_depth=100, watermark=0)
    payloads = [{"kind": "message.group", "text": text, "event_id": text} for text in ("a", "b")]
    queue.enqueue(payloads[0])
    queue.enqueue({"kind": "notice.group_recall", "event_id": "notice"})
    queue.enqueue({"kind": "message.group", "text": "cmd", "event_id": "cmd"}, priority=0)
    queue.enqueue(payloads[1])

    order = []
    while True:
        item = queue.next()
        if item is None:
            break
        order.append(item.event_id)
    assert order == ["cmd", "a", "b", "notice"]
    assert queue.stats.dequeued == 4
    assert queue.depth == 0


def test_queue_sheds_meta_above_watermark():
    queue = PriorityQueue(max_depth=100, watermark=2)
    assert queue.enqueue({"kind": "message.group"})["accepted"] is True
    assert queue.enqueue({"kind": "message.group"})["accepted"] is True
    assert queue.throttled is True

    meta = queue.enqueue({"kind": "meta_event.heartbeat"})
    assert meta["accepted"] is False
    assert meta["reason"] == "watermark_shed"
    assert queue.stats.shed == 1

    assert queue.enqueue({"kind": "message.group"})["accepted"] is True  # 普通消息仍进队


def test_queue_evicts_worst_when_full_and_rejects_worst_incoming():
    queue = PriorityQueue(max_depth=2, watermark=0, shed_above_watermark=False)
    queue.enqueue({"kind": "message.group", "event_id": "m1"})
    queue.enqueue({"kind": "notice.group_recall", "event_id": "n1"})

    accepted = queue.enqueue({"kind": "message.group", "event_id": "m2"}, priority=0)
    assert accepted["accepted"] is True
    assert accepted["evicted"] == 1
    assert queue.stats.evicted == 1
    assert queue.depth == 2

    rejected = queue.enqueue({"kind": "meta_event.heartbeat", "event_id": "meta"})
    assert rejected["accepted"] is False
    assert rejected["reason"] == "queue_full"
    assert queue.stats.rejected == 1

    remaining = [queue.next().event_id for _ in range(queue.depth)]
    assert remaining[0] == "m2"  # 命令/消息优先
    assert "n1" not in remaining  # 通知被淘汰


async def test_queue_wait_next_timeout_and_wakeup():
    queue = PriorityQueue()
    assert await queue.wait_next(timeout=0.01) is None
    assert queue.stats.wait_timeouts == 1

    async def producer():
        await asyncio.sleep(0.02)
        queue.enqueue({"kind": "message.group", "event_id": "late"})

    task = asyncio.create_task(producer())
    item = await queue.wait_next(timeout=1.0)
    await task
    assert item is not None
    assert item.event_id == "late"


def test_queue_snapshot_and_clear():
    queue = PriorityQueue(max_depth=10, watermark=5)
    queue.enqueue({"kind": "message.group"})
    snapshot = queue.snapshot()
    assert snapshot["depth"] == 1
    assert snapshot["by_priority"]["3"] == 1
    assert snapshot["throttled"] is False
    assert queue.clear() == 1
    assert queue.depth == 0


async def test_rpc_priority_handlers():
    queue = PriorityQueue()
    registry = Registry()
    priority_module.register(registry, queue)
    assert set(registry.names()) == {"rpc:priority.enqueue", "rpc:priority.next"}

    result = await registry.acall("rpc:priority.enqueue", {"kind": "message.group", "event_id": "e1"})
    assert result["accepted"] is True
    item = await registry.acall("rpc:priority.next")
    assert item["event_id"] == "e1"
    assert await registry.acall("rpc:priority.next") is None


# ---- 事件分用 -----------------------------------------------------------
def make_router(**kwargs) -> tuple[EventRouter, RecordingSender, StubRetractor]:
    sender = kwargs.pop("sender", RecordingSender())
    retractor = kwargs.pop("retractor", StubRetractor())
    router = EventRouter(
        commands=kwargs.pop("commands", CommandRouter()),
        queue=kwargs.pop("queue", PriorityQueue()),
        sender=sender,
        retractor=retractor,
        **kwargs,
    )
    return router, sender, retractor


async def test_dispatch_group_message_routes_to_perception():
    bus = EventBus(strict_topics=True)
    collector = EventCollector(bus).listen(demux_module.TOPIC_EVENT_ROUTED)
    router, _sender, _retractor = make_router(bus=bus)
    try:
        routed = await router.dispatch(group_message("今天天气不错", self_id=SELF_ID))

        assert routed["route"] == demux_module.ROUTE_PERCEPTION
        assert routed["queued"] is True
        assert routed["priority"] == 3
        assert routed["command"] is None
        assert routed["event"]["kind"] == "message.group"

        assert len(collector.of_topic(demux_module.TOPIC_EVENT_ROUTED)) == 1
        published = collector.of_topic(demux_module.TOPIC_EVENT_ROUTED)[0]
        assert published["route"] == demux_module.ROUTE_PERCEPTION
        assert router.stats.routed == 1
        assert router.queue.depth == 1
    finally:
        collector.close()
        await bus.aclose()


async def test_dispatch_at_self_message_gets_mention_priority():
    router, _sender, _retractor = make_router()
    routed = await router.dispatch(group_message("今天聊点啥", at_self=True, self_id=SELF_ID))
    assert routed["at_self"] is True
    assert routed["priority"] == 1
    item = router.queue.next()
    assert item is not None and item.priority == 1


async def test_command_outranks_mention_priority():
    """命令（含固定短语）优先级高于普通 @：``在吗`` → ping 命令。"""

    router, _sender, _retractor = make_router()
    routed = await router.dispatch(group_message("在吗", at_self=True, self_id=SELF_ID))
    assert routed["route"] == demux_module.ROUTE_COMMAND
    assert routed["command"]["name"] == "ping"
    assert routed["priority"] == 0
    item = router.queue.next()
    assert item is not None and item.priority == 0


async def test_dispatch_local_command_sends_reply():
    router, sender, _retractor = make_router()
    routed = await router.dispatch(group_message("/help", user_id=200, group_id=100, message_id=77, self_id=SELF_ID))

    assert routed["route"] == demux_module.ROUTE_COMMAND
    assert routed["priority"] == 0
    assert routed["command"]["kind"] == "help"
    assert routed["command"]["handled"] is True
    assert len(sender.sent) == 1
    assert "/help" in sender.sent[0]["text"]
    assert sender.sent[0]["reply_to"] == 77
    assert sender.sent[0]["group_id"] == 100
    assert router.stats.local_replies == 1


async def test_dispatch_persona_probe_is_not_answered_locally():
    router, sender, _retractor = make_router()
    routed = await router.dispatch(group_message("/人设", user_id=200, self_id=SELF_ID))

    assert routed["route"] == demux_module.ROUTE_PERSONA
    assert routed["command"]["handled"] is False
    assert sender.sent == []
    assert router.queue.depth == 1


async def test_dispatch_command_reply_failure_is_counted():
    router, sender, _retractor = make_router()

    async def boom(reply=None, **kwargs):
        raise RuntimeError("发送炸了")

    sender.send_reply = boom  # type: ignore[method-assign]
    routed = await router.dispatch(group_message("/ping", self_id=SELF_ID))

    assert routed["route"] == demux_module.ROUTE_COMMAND
    assert router.stats.errors == 1
    assert "发送炸了" in router.stats.last_error


async def test_dispatch_recall_notice_notifies_retractor():
    router, _sender, retractor = make_router()
    routed = await router.dispatch(recall_notice(message_id=55, group_id=100, user_id=200))

    assert routed["route"] == demux_module.ROUTE_NOTICE
    assert routed["priority"] == 4
    assert retractor.notes and retractor.notes[0]["reason"] == "group_recall"
    assert retractor.notes[0]["message_id"] == 55
    assert router.stats.notices == 1


async def test_dispatch_meta_and_unknown_and_private():
    router, _sender, _retractor = make_router()

    meta = await router.dispatch(heartbeat(SELF_ID))
    assert meta["route"] == demux_module.ROUTE_META
    assert meta["priority"] == 5
    assert meta["queued"] is True

    unknown = await router.dispatch({"post_type": "nonsense"})
    assert unknown["route"] == demux_module.ROUTE_DROP
    assert unknown["queued"] is False
    assert router.stats.dropped == 1

    private = await router.dispatch(private_message("在忙吗"))
    assert private["route"] == demux_module.ROUTE_PERCEPTION
    assert private["priority"] == 2


async def test_dispatch_bad_payload_is_dropped():
    router, _sender, _retractor = make_router()
    routed = await router.dispatch("不是事件")  # type: ignore[arg-type]
    assert routed["route"] == demux_module.ROUTE_DROP
    assert routed["reason"] == "bad_payload"


async def test_pump_delivers_to_sink_and_registry():
    sink = IngestSink()
    router, _sender, _retractor = make_router(sink=sink)
    await router.dispatch(group_message("一"))
    await router.dispatch(group_message("二"))

    delivered = await router.pump()
    assert delivered == 2
    assert [item["text"] for item in sink.items] == ["一", "二"]
    assert router.stats.delivered == 2
    assert await router.pump_once() is None


async def test_pump_with_registry_sink_and_missing_observer():
    registry = Registry()
    sink = IngestSink()
    router, _sender, _retractor = make_router(registry=registry, sink=None)
    await router.dispatch(group_message("没有感知层"))

    result = await router.pump_once()
    assert result == {"delivered": False, "reason": "observer_not_registered", "seq": 1}
    assert router.stats.undelivered == 1

    registry.register("rpc:observer.ingest", sink.call)
    await router.dispatch(group_message("有感知层"))
    result = await router.pump_once()
    assert result["delivered"] is True
    assert sink.items[0]["text"] == "没有感知层" or sink.items[0]["text"] == "有感知层"


async def test_attach_subscribes_to_inbound_topic_and_pump_runs():
    bus = EventBus(strict_topics=True)
    sink = IngestSink()
    router, _sender, _retractor = make_router(bus=bus, sink=sink)
    try:
        subscription = router.attach()
        assert subscription.topic == "kafka:grouppig.qq.message.received"
        assert router.status()["attached"] is True

        router.start_pump()
        await bus.publish("kafka:grouppig.qq.message.received", group_message("从总线来"))
        for _ in range(100):
            if sink.items:
                break
            await asyncio.sleep(0.01)
        assert [item["text"] for item in sink.items] == ["从总线来"]

        await router.stop_pump()
        assert router.status()["pumping"] is False
        router.detach()
        assert router.status()["attached"] is False
    finally:
        await bus.aclose()


async def test_rpc_demux_dispatch_handler():
    router, _sender, _retractor = make_router()
    registry = Registry()
    demux_module.register(registry, router)
    assert set(registry.names()) == {"rpc:demux.dispatch"}

    routed = await registry.acall("rpc:demux.dispatch", group_message("走一遍 rpc"))
    assert routed["route"] == demux_module.ROUTE_PERCEPTION
    assert router.queue.depth == 1
