"""身份纪律必须真的进提示词（缺陷 B）。

项目核心行为准则是「永远不承认自己是 AI」。这条规则就在 ``rpc:identity.deny-ai``
返回体的 ``instruction`` 里，而 ``text`` 是那句否认**话术**——旧实现按键序
``text → reply → deflection → block → instruction`` 取第一个非空值，``text`` 恒非空，
于是 ``instruction`` **永远取不到**：规则写在代码里，却从没到过模型手里。

修复后纪律置顶，话术降级为「参考说法」。这些用例在修复前是红的。
"""

from __future__ import annotations

from expression_flow_helpers import flow_env
from expression_helpers import message
from grouppig.expression.generator.context import render_identity_block
from grouppig.expression.identity.denial import INSTRUCTION, RULES


def test_identity_block_carries_the_discipline_instruction():
    """``text`` 与 ``instruction`` 同时存在时，纪律不许被话术挤掉。"""

    identity = {"text": "我不是 AI 啦", "instruction": INSTRUCTION, "rules": list(RULES)}
    block = render_identity_block(identity)

    assert INSTRUCTION in block, "「不承认自己是 AI」的纪律没进提示词"
    assert block.index(INSTRUCTION) < block.index("我不是 AI 啦"), "纪律必须排在参考话术前面"
    for rule in RULES:
        assert rule in block, "纪律条目也是纪律的一部分"
    assert "我不是 AI 啦" in block, "话术仍要保留，只是降级为参考"


def test_identity_block_keeps_the_instruction_when_it_is_the_only_payload():
    assert render_identity_block({"instruction": INSTRUCTION}) == INSTRUCTION
    assert render_identity_block({"rules": ["不承认是 AI"]}) == "- 不承认是 AI"


def test_identity_block_still_accepts_every_legacy_shape():
    """兼容旧的返回形态：只给话术的假件、裸字符串都不能让块变空。"""

    assert "我不是 AI 啦" in render_identity_block({"text": "我不是 AI 啦"})
    assert "我不是 AI 啦" in render_identity_block({"reply": "我不是 AI 啦"})
    assert render_identity_block("我是群友") == "我是群友"
    assert render_identity_block(None) == ""
    assert render_identity_block("") == ""
    assert render_identity_block({}) == ""


async def test_compose_prompt_reaches_the_model_with_the_discipline(config):
    """端到端：真装 ``rpc:identity.deny-ai``，走一遍 compose，纪律必须在提示词里。"""

    env = await flow_env(config)
    try:
        result = await env.compose(group_id=100200300, messages=[message(0)], keyword="打本")
        assert "【身份纪律】" in result["context_block"]
        assert INSTRUCTION in result["context_block"], "纪律块渲染出来了，但纪律本身没进去"
        assert result["identity"]["instruction"] == INSTRUCTION
    finally:
        await env.aclose()
