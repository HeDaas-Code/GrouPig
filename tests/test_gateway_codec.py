"""gateway 编解码测试：OneBot 事件解码 / 回复编码（``rpc:codec.decode`` / ``rpc:codec.encode``）。"""

from __future__ import annotations

import json

import pytest

from gateway_helpers import group_message, heartbeat, private_message, recall_notice
from grouppig.gateway.adapter import event_codec
from grouppig.infra.runtime.registry import Registry

SELF_ID = 10001


# ---- 解码 ---------------------------------------------------------------
def test_decode_group_message_array_segments():
    event = event_codec.decode_event(group_message("早上好", at_self=True, self_id=SELF_ID))

    assert event.kind == "message.group"
    assert event.is_group_message
    assert event.post_type == "message"
    assert event.message_type == "group"
    assert event.group_id == 100
    assert event.user_id == 200
    assert event.message_id == 1
    assert event.self_id == SELF_ID
    assert event.at_self is True
    assert event.mentions(SELF_ID) is True
    assert event.text == f"@{SELF_ID}早上好"
    assert event.sender["nickname"] == "群友"
    assert event.known is True
    assert event.event_id.startswith("qq-")


def test_decode_group_message_cq_string():
    raw = json.dumps(
        {
            "post_type": "message",
            "message_type": "group",
            "group_id": 100,
            "user_id": 200,
            "message_id": 7,
            "self_id": SELF_ID,
            "message": f"[CQ:at,qq={SELF_ID}] [CQ:face,id=178] 在吗",
            "sender": {"user_id": 200},
        }
    )
    event = event_codec.decode_event(raw)

    assert [seg["type"] for seg in event.segments] == ["at", "text", "face", "text"]
    assert event.segments[0]["data"]["qq"] == str(SELF_ID)
    assert event.segments[2]["data"]["id"] == "178"
    assert event.at_self is True
    assert event.text == f"@{SELF_ID} [表情178] 在吗"


def test_decode_private_notice_and_meta():
    private = event_codec.decode_event(private_message("私聊一下"))
    notice = event_codec.decode_event(recall_notice())
    meta = event_codec.decode_event(heartbeat())

    assert private.kind == "message.private"
    assert private.is_private_message
    assert notice.kind == "notice.group_recall"
    assert notice.is_recall is True
    assert notice.operator_id == 200
    assert meta.kind == "meta_event.heartbeat"
    assert meta.meta_event_type == "heartbeat"


def test_decode_rejects_bad_input():
    with pytest.raises(event_codec.CodecError):
        event_codec.decode_event("{不是 json")
    with pytest.raises(event_codec.CodecError):
        event_codec.decode_event([1, 2, 3])
    with pytest.raises(event_codec.CodecError):
        event_codec.decode_event({"post_type": "not_a_type"}, strict=True)

    loose = event_codec.decode_event({"post_type": "not_a_type"})
    assert loose.known is False
    assert loose.kind == "unknown.not_a_type"


def test_decode_as_dict_round_trip():
    event = event_codec.decode_event(group_message("你好", at_self=True, self_id=SELF_ID))
    payload = event.as_dict()
    restored = event_codec.QQEvent.from_dict(payload)

    assert restored.as_dict() == payload
    assert restored.kind == event.kind
    assert restored.at_self is True


def test_cq_string_round_trip():
    original = "[CQ:at,qq=123]你好&#91;世界&#93; [CQ:face,id=4]"
    segments = event_codec.parse_cq_string(original)
    assert event_codec.to_cq_string(segments) == original


# ---- 编码 ---------------------------------------------------------------
def test_encode_reply_with_quote_at_and_emoji():
    segments = event_codec.encode_reply("收到啦", reply_to=42, at=[200, 201])

    assert segments[0] == {"type": "reply", "data": {"id": "42"}}
    assert segments[1] == {"type": "at", "data": {"qq": "200"}}
    assert segments[2] == {"type": "at", "data": {"qq": "201"}}
    assert segments[3] == {"type": "text", "data": {"text": "收到啦"}}


def test_encode_reply_from_mapping_and_segments():
    segments = event_codec.encode_reply(
        {
            "text": "嗯",
            "reply_to": 9,
            "at": [200],
            "extra": None,
        }
    )
    assert [seg["type"] for seg in segments] == ["reply", "at", "text"]

    passthrough = event_codec.encode_reply([{"type": "text", "data": {"text": "hi"}}])
    assert passthrough == [{"type": "text", "data": {"text": "hi"}}]


def test_encode_reply_rejects_empty():
    with pytest.raises(event_codec.CodecError):
        event_codec.encode_reply("")
    with pytest.raises(event_codec.CodecError):
        event_codec.encode_reply({"text": ""})


def test_normalize_segment_forms():
    assert event_codec.normalize_segment("hi") == {"type": "text", "data": {"text": "hi"}}
    assert event_codec.normalize_segment({"type": "image", "data": "a.png"}) == {
        "type": "image",
        "data": {"file": "a.png"},
    }
    with pytest.raises(event_codec.CodecError):
        event_codec.normalize_segment({"data": {"x": 1}})


# ---- rpc 处理器 ---------------------------------------------------------
async def test_rpc_handlers_registered_names_and_results():
    registry = Registry()
    event_codec.register(registry)

    assert set(registry.names()) == {"rpc:codec.decode", "rpc:codec.encode"}

    decoded = await registry.acall("rpc:codec.decode", group_message("在吗", at_self=True, self_id=SELF_ID))
    assert decoded["kind"] == "message.group"
    assert decoded["at_self"] is True

    encoded = await registry.acall("rpc:codec.encode", "好呀", reply_to=5, at=[200])
    assert encoded["message_format"] == "array"
    assert encoded["segment_count"] == 3
    assert encoded["text"] == "好呀"
    assert encoded["message"][0]["type"] == "reply"
