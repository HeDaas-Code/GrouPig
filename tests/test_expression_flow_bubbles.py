"""多气泡（human-like pacing）编排侧：多版草稿 → 有序气泡列表 → 逐条发出。

设计缺陷：``_compose`` 每一步都**覆盖** ``record.text``（``state.py:582``），
``flow.end`` 只发 ``record.text``，``emitter`` 只带一个标量 ``text``——
多轮计划辛苦产出的前几版草稿全被丢掉，一条回复永远只有一个气泡。

这里用**真实 composer** 接在 ``rpc:sender.send_reply`` 上，把「3 版草稿 → 3 条群消息」
一路断言到底（不是只断言编排器内部字段）。
"""

from __future__ import annotations

from typing import Any

from expression_flow_helpers import flow_env
from grouppig.gateway.sender.composer import ReplyComposer


class RecordingAdapter:
    """只记录出站消息的假 OneBot 适配器（形状模仿 ``OneBotAdapter.send_group_message``）。"""

    self_id = 1001

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_group_message(self, group_id: Any, message: Any, raise_on_error: bool = False) -> dict[str, Any]:
        segments = [dict(seg) for seg in message]
        self.sent.append({"group_id": group_id, "segments": segments})
        return {
            "ok": True,
            "action": "send_group_msg",
            "status": "ok",
            "retcode": 0,
            "message_id": 9000 + len(self.sent),
            "segments": segments,
        }

    def texts(self) -> list[str]:
        return [
            str(seg.get("data", {}).get("text", ""))
            for row in self.sent
            for seg in row["segments"]
            if seg.get("type") == "text"
        ]


class ScriptedCompose:
    """按脚本逐次返回不同草稿的假 ``rpc:generator.compose``。"""

    def __init__(self, texts: list[str]) -> None:
        self.texts = list(texts)
        self.index = 0
        self.calls = 0

    async def compose(self, **kwargs: Any) -> dict[str, Any]:
        text = self.texts[min(self.index, len(self.texts) - 1)]
        self.index += 1
        self.calls += 1
        return {"text": text, "candidates": [text], "budget": {}, "degraded": False}

    def install(self, registry: Any) -> None:
        registry.register("rpc:generator.compose", self.compose, module="test.fake", replace=True)


async def _prepare(config, texts: list[str]):
    """搭好环境：脚本化 compose + 真实 composer（注入到 ``rpc:sender.send_reply``）。"""

    env = await flow_env(config)
    scripted = ScriptedCompose(texts)
    scripted.install(env.registry)
    adapter = RecordingAdapter()
    composer = ReplyComposer(adapter, emoji_pool=(), quote=False, auto_emoji=False, bubble_delay=0.0)
    env.registry.register("rpc:sender.send_reply", composer.send_reply, module="test.composer", replace=True)
    return env, scripted, adapter, composer


async def _start_and_advance(env, scenario: str = "discussion") -> str:
    """开一条流程并按计划推到最后一步，返回 ``flow_id``。"""

    start = await env.call(
        "rpc:flow.start",
        100200300,
        messages=[{"sender_id": 1002, "content": "打本吗"}],
        scenario=scenario,
    )
    flow_id = str(start["flow_id"])
    for _ in range(int(start.get("step_count") or 0)):
        await env.call("rpc:flow.next", flow_id)
    return flow_id


async def test_multi_step_plan_emits_ordered_bubbles_and_sends_them(config):
    """discussion 场景有 3 个「要出文本」的步骤 → 3 条有序气泡 → 3 次真实发送。"""

    texts = ["先接一句。", "再说第二句，稍微长一点点。", "最后收个尾。"]
    env, scripted, adapter, _composer = await _prepare(config, texts)
    try:
        flow_id = await _start_and_advance(env)
        end = await env.call("rpc:flow.end", flow_id, send=True, publish=True)

        assert scripted.calls == 3, f"计划里有 3 个出文本步骤，实际 compose {scripted.calls} 次"
        assert end["bubbles"] == texts, f"气泡必须按草稿顺序保留：{end['bubbles']}"
        assert end["bubble_count"] == 3
        # 向后兼容：``text`` 仍是「最后那版草稿」
        assert end["text"] == texts[-1]

        assert adapter.texts() == texts, f"实际发出去的气泡顺序不对：{adapter.texts()}"
        assert len(adapter.sent) == 3

        payload = env.events()[0]
        assert payload["bubbles"] == texts
        assert payload["text"] == texts[-1]
    finally:
        await env.aclose()


async def test_send_payload_carries_bubbles(config):
    """直连发送（``send_via_topic=False`` 的路径）也要把气泡一起交给发送方。"""

    env, _scripted, adapter, _composer = await _prepare(config, ["甲", "乙"])
    try:
        flow_id = await _start_and_advance(env)
        await env.call("rpc:flow.end", flow_id, send=True, publish=False)
        assert adapter.texts() == ["甲", "乙"]
    finally:
        await env.aclose()


async def test_consecutive_duplicate_drafts_collapse_to_one_bubble(config):
    """模型对每一步给出同一句话时只发一条：重复同一条不是「像人」，是 bug。"""

    same = ["同一句话。"] * 3
    env, _scripted, adapter, _composer = await _prepare(config, same)
    try:
        flow_id = await _start_and_advance(env)
        end = await env.call("rpc:flow.end", flow_id, send=True, publish=True)
        assert end["bubbles"] == ["同一句话。"]
        assert adapter.texts() == ["同一句话。"]
        assert len(env.events()) == 1
    finally:
        await env.aclose()


async def test_explicit_text_resets_bubbles(config):
    """人工修正（显式 ``text=``）时，气泡列表必须被这一条替换掉，不能把旧草稿一起发。"""

    env, _scripted, adapter, _composer = await _prepare(config, ["旧甲", "旧乙"])
    try:
        flow_id = await _start_and_advance(env)
        end = await env.call("rpc:flow.end", flow_id, text="我自己定的回复", send=True, publish=False)
        assert end["bubbles"] == ["我自己定的回复"]
        assert adapter.texts() == ["我自己定的回复"]
    finally:
        await env.aclose()


async def test_single_draft_still_sends_one_bubble(config):
    """只有一版草稿时行为与改动前一致（一条回复一条消息）。"""

    env, _scripted, adapter, _composer = await _prepare(config, ["就这一句"])
    try:
        start = await env.call(
            "rpc:flow.start",
            100200300,
            messages=[{"sender_id": 1002, "content": "在吗"}],
            scenario="smalltalk",
        )
        end = await env.call("rpc:flow.end", start["flow_id"], send=True, publish=False)
        assert end["bubbles"] == ["就这一句"]
        assert adapter.texts() == ["就这一句"]
    finally:
        await env.aclose()
