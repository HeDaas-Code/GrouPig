"""行为滞回**保质期**的回归测试：滞回只该防抖动，不该把状态永久冻结。

背景（``src/grouppig/perception/behavior/classifier/aggregator.py`` 的
``DEFAULT_STICKY_TTL`` 说明）：滞回原有两条判据 —— 置信度边际（``switch_margin`` 0.1）
与持续性（``switch_confirmations`` 3）。两条都挡不住一个**高置信度的在任行为**：

* 规则引擎给 ``flooding`` 的置信度是 0.875，任何候选都够不到 ``0.875 + 0.1 = 0.975``；
* ``resolved`` 取代 ``resolved`` 不成立（那条规则只允许「确定性取代不确定性」）；
* 持续性要求**同一个候选**连续出现 3 次，而真实群聊的候选每轮都在变。

于是「刷过屏的群」被判成永久刷屏。而 ``flooding`` 会让插话评分直接 ``hold`` ——
这个群从此**再也不会被主动开口**。

实测（连灌 爬山 / 狂笑 / 打游戏 / 问号 四种风格迥异的群聊）：进入 flooding 后
行为再不变化。本文件里的用例都是**先红后绿**：修复前那几条「应当到期让位」的会失败。
"""

from __future__ import annotations

from typing import Any

import pytest

from grouppig.perception.behavior.classifier.aggregator import (
    DEFAULT_STICKY_TTL,
    BehaviorAggregator,
)

GROUP = 100
T0 = 1_700_000_000.0

#: 一个刷屏窗口（重复内容 → 规则引擎判 flooding，conf 0.875，resolved）。
FLOOD = ("哈哈哈哈哈", "哈哈哈哈", "哈哈哈哈哈", "哈哈哈哈", "哈哈哈哈哈", "哈哈哈哈")

#: 一个正常话题窗口。
NORMAL = ("周末一起去爬山吧", "爬山好啊我也想去爬山", "那就周六早上八点集合去爬山", "爬山要带什么装备")

#: 规则引擎对 flooding 的置信度（测试里当常量用；变了就该有人来解释）。
FLOODING_CONFIDENCE = 0.875


def rows(texts: tuple[str, ...], t0: float, *, step: float = 3.0) -> list[dict[str, Any]]:
    """造一批已归一化的消息行（``classify`` 的判别器只吃行）。"""

    return [
        {
            "group_id": GROUP,
            "sender_id": 2001 + index % 2,
            "content": text,
            "ts": t0 + index * step,
            "message_id": f"msg-{index}",
            "at_self": False,
            "mentions": [],
            "reply_to": "",
        }
        for index, text in enumerate(texts)
    ]


def flooding_incumbent(**kwargs: Any) -> dict[str, Any]:
    """在任 = flooding（高置信度、已判定），候选 = 正常话题（低置信度、未判定）。

    这组参数刻意选成「边际与持续性都够不到」：0.8 不高于 0.875 + 0.1，
    候选已判定而在任者也已判定（「确定性取代不确定性」那条不成立），且没有连续性计数。
    """

    age = float(kwargs.pop("incumbent_age", 0.0))
    aggregator = BehaviorAggregator(config=None, window=None, publish=False, **kwargs)
    return aggregator.decide(
        rules={"behavior": "exposition", "resolved": True, "confidence": 0.8},
        encoded={},
        previous="flooding",
        previous_confidence=FLOODING_CONFIDENCE,
        previous_resolved=True,
        streak=0,
        streak_behavior="",
        incumbent_age=age,
    )


# --------------------------------------------------------------------------
# 红：修复前「在任者永不退位」
# --------------------------------------------------------------------------


def test_incumbent_expires_once_it_outlives_the_ttl():
    """在任行为保持超过 ``sticky_ttl`` 后，滞回必须让位（否则该群被永久静音）。"""

    expired = flooding_incumbent(sticky_ttl=180.0, incumbent_age=180.0)
    assert expired["sticky"] is False, "到期就该让位"
    assert expired["behavior"] == "exposition"
    assert expired["changed"] is True, "让位是一次真实变化，必须重新触发插话评分"
    assert expired["expired"] is True, "要能区分「到期让位」与「边际赢下」"


def test_incumbent_is_still_protected_just_before_the_ttl():
    """保质期只在下沿生效：179s 与 180s 必须给出相反结论。"""

    held = flooding_incumbent(sticky_ttl=180.0, incumbent_age=179.9)
    assert held["sticky"] is True, "差一点点到期，仍要保护"
    assert held["behavior"] == "flooding"
    assert held["expired"] is False


def test_default_ttl_is_positive_and_bounded():
    """默认保质期必须是个正数、且不至于长到「等于没有」。"""

    assert DEFAULT_STICKY_TTL > 0
    assert DEFAULT_STICKY_TTL <= 3600.0, "超过一小时就等于把状态冻结了，失去意义"


def test_zero_ttl_restores_the_old_permanent_hysteresis():
    """``sticky_ttl=0`` 明确表示「不要保质期」，此时旧行为逐位保留。

    这条是给「不想改行为」的部署留的逃生口，也把「旧行为到底长什么样」钉在测试里。
    """

    for age in (0.0, 300.0, 86_400.0):
        held = flooding_incumbent(sticky_ttl=0.0, incumbent_age=age)
        assert held["sticky"] is True, f"关掉保质期后 age={age} 也该保持"
        assert held["expired"] is False


def test_flapping_protection_survives_the_expiry_rule():
    """加保质期不能把原有的防抖动拆掉：低置信度默认值在保质期内照样抢不走。

    这是 ``DEFAULT_STICKY_TTL`` 要小心的那件事 —— 它放宽的是**时间**，不是判据本身。
    """

    aggregator = BehaviorAggregator(config=None, window=None, publish=False, sticky_ttl=180.0)
    for age in (0.0, 60.0, 179.0):
        held = aggregator.decide(
            rules={"behavior": "smalltalk", "resolved": False, "confidence": 0.45},
            encoded={},
            previous="exposition",
            previous_confidence=0.9,
            previous_resolved=True,
            streak=0,
            streak_behavior="",
            incumbent_age=age,
        )
        assert held["sticky"] is True, f"保质期内 age={age} 的低置信度候选不该抢班"
        assert held["behavior"] == "exposition"


# --------------------------------------------------------------------------
# 绿：端到端（走真实 classify + 规则引擎）
# --------------------------------------------------------------------------


async def test_flooding_group_is_no_longer_muted_forever():
    """端到端：刷屏 → 静下来 → 到期后行为必须能离开 flooding。

    这条是缺陷的**用户可见后果**：``flooding`` 会让插话评分 ``hold``，
    而插话评分只在 ``behavior.changed`` 时触发 —— 行为不动，评分就永远不再发生。
    """

    aggregator = BehaviorAggregator(config=None, window=None, publish=False, sticky_ttl=180.0)

    entered = await aggregator.classify(GROUP, messages=rows(FLOOD, T0), seconds=60, now=T0 + 20)
    assert entered["behavior"] == "flooding", "重复内容窗口应当判成刷屏"
    assert aggregator._since[GROUP] == pytest.approx(T0 + 20)

    # 保质期内：换了话题也被拦下（这正是我们要保留的防抖动）。
    within = await aggregator.classify(GROUP, messages=rows(NORMAL, T0 + 30), seconds=60, now=T0 + 50)
    assert within["sticky"] is True
    assert within["behavior"] == "flooding"
    assert within["changed"] is False

    # 保质期外：同样的窗口必须放行，并重新发出行为变化（= 重新触发插话评分）。
    after = await aggregator.classify(GROUP, messages=rows(NORMAL, T0 + 300), seconds=60, now=T0 + 320)
    assert after["sticky"] is False
    assert after["behavior"] != "flooding", "到期后必须离开 flooding，否则该群被永久静音"
    assert after["changed"] is True
    assert after["expired"] is True


async def test_expiry_clock_only_resets_when_the_behavior_actually_changes():
    """计时只在**真正换行为**时重置，否则稳定的群会被每轮分类无限续期。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False, sticky_ttl=180.0)

    first = await aggregator.classify(GROUP, messages=rows(NORMAL, T0), seconds=60, now=T0 + 20)
    assert aggregator._since[GROUP] == pytest.approx(T0 + 20)
    landed = first["behavior"]

    # 同一窗口再来两轮：行为没变，起点不能往后挪。
    for offset in (60.0, 120.0):
        again = await aggregator.classify(GROUP, messages=rows(NORMAL, T0 + offset), seconds=60, now=T0 + offset + 20)
        assert again["behavior"] == landed
        assert aggregator._since[GROUP] == pytest.approx(T0 + 20), "行为没换，计时不该重置"


async def test_incumbent_age_is_reported_for_diagnosis():
    """``incumbent_age`` 要出现在返回体里，否则线上无法判断「为什么还不切」。"""

    aggregator = BehaviorAggregator(config=None, window=None, publish=False, sticky_ttl=180.0)
    await aggregator.classify(GROUP, messages=rows(NORMAL, T0), seconds=60, now=T0 + 20)
    later = await aggregator.classify(GROUP, messages=rows(FLOOD, T0 + 100), seconds=60, now=T0 + 120)
    assert later["incumbent_age"] == pytest.approx(100.0)
