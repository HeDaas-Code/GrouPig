"""t1 验收冒烟：infra 底座 + 目录骨架 + 契约，作为下游（t10/t11）的接入模板。

模拟一个下游域（如 memory / gateway）自注册处理器与订阅事件，验证：
装配 → 注册表 → 事件总线 → 模型网关（假传输）→ token 预算 → 健康检查 → 关闭。
"""

from __future__ import annotations

from typing import Any

from grouppig.infra.runtime import contract
from grouppig.infra.runtime.di import build_container
from grouppig.infra.runtime.registry import Registry, rpc, topic
from helpers import FakeTransport

QQ_MESSAGE = "kafka:grouppig.qq.message.received"
TOPIC_CHANGED = "kafka:grouppig.topic.changed"


def pin_laya_transport(router: Any, transport: Any) -> None:
    """把假传输同时钉到 LAY A provider 上（测试隔离）。

    只注册 "*" 不够：``ModelRouter._transport()`` 对 laya 会绕开 "*"
    （``/v1/systemone`` 非 OpenAI 兼容），于是 classify / system1 任务会去连真实
    LAY A 端点 —— 没有密钥时 401 仍能回落通过（假绿），一旦配上有效密钥就会失败。
    显式按 provider 注入，让 LAY A 路径确定性地走假传输。
    """

    router.set_transport(transport, provider="laya")


async def test_infra_backbone_end_to_end(config):
    registry = Registry()  # 隔离的注册表：不污染全局，模拟下游域自注册
    seen: list[dict] = []

    @rpc("rpc:chat.append", registry=registry)
    async def chat_append(message, **_kwargs):
        return {"stored": True, "message": message}

    @topic(QQ_MESSAGE, registry=registry)
    async def on_qq_message(event):
        seen.append(event.payload)

    transport = FakeTransport(reply="在的，咋了", usage={"prompt_tokens": 42, "completion_tokens": 8})
    container = build_container(config=config, transport=transport, registry=registry)
    # classify（provider=laya）会绕开 "*"，必须显式钉住，否则打到真实 LAY A 端点
    pin_laya_transport(container.router, transport)
    await container.start()
    try:
        # 1) 下游域注册的处理器可被容器调用
        stored = await container.call("rpc:chat.append", {"user_id": 1, "text": "在吗"})
        assert stored == {"stored": True, "message": {"user_id": 1, "text": "在吗"}}

        # 2) 事件走设计中的 kafka:grouppig.* 主题名
        container.subscribe(QQ_MESSAGE, on_qq_message)
        await container.publish(QQ_MESSAGE, {"group_id": 123, "text": "在吗"}, source="gateway")
        assert seen == [{"group_id": 123, "text": "在吗"}]

        # 3) 模型调用带重试 + 预算
        reply = await container.call("rpc:model.chat", [{"role": "user", "content": "在吗"}], scenario="smalltalk")
        assert reply["text"] == "在的，咋了"
        assert reply["usage"]["total_tokens"] == 50

        # 4) 预算账本与报告一致
        report = await container.call("rpc:token.report")
        assert report["by_scenario"]["smalltalk"]["total_tokens"] == 50
        assert container.health()["token"]["consumed_total"] == 50

        # 5) 健康检查里 infra 契约无缺号
        health = container.health()
        assert health["registry"]["infra_missing"] == []
        assert contract.check_registry(set(registry.names()), scope="grouppig.infra")["missing"] == []
    finally:
        await container.aclose()
    assert transport.closed is True


async def test_downstream_can_swap_in_its_own_transport(config):
    """下游用假传输即可在没有真实模型/QQ 的环境里验证闭环。"""

    class Scripted:
        def __init__(self) -> None:
            self.payloads: list[dict] = []

        async def complete(self, payload, *, path="/chat/completions", timeout=None, provider=""):
            self.payloads.append(payload)
            is_classify = "候选标签" in str(payload.get("messages"))
            content = '{"label": "提问", "scores": {"提问": 0.9, "闲聊": 0.1}}' if is_classify else "好"
            return {
                "model": payload["model"],
                "choices": [{"message": {"content": content}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        async def aclose(self) -> None:
            pass

    transport = Scripted()
    container = build_container(config=config, transport=transport)
    # 同上：让 classify 的 LAY A 尝试也走这个假传输，而不是真实端点
    pin_laya_transport(container.router, transport)
    await container.start()
    try:
        response = await container.router.classify("在吗", ["提问", "闲聊"])
        assert response.label == "提问"
        assert transport.payloads[-1]["response_format"] == {"type": "json_object"}
    finally:
        await container.aclose()


def test_skeleton_matches_design_tree():
    """目录骨架逐字镜像设计：每个容器模块都有对应包文件。"""

    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    for module_id in contract.container_ids():
        assert contract.module_to_path(module_id, root=root).is_file(), module_id
