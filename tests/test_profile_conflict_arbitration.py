"""档案事实冲突仲裁测试：写路径必须遵守 ``rpc:profile.conflict`` 的口径。

背景（blocker）：``rpc:profile-store.put`` 过去**无条件**顶替同键异值的旧事实
（``dao.py`` 里 ``supersede=True`` 硬编码），于是 ``source="manual", confidence=0.95``
的人工事实会被后来 ``source="extractor", confidence=0.55`` 的猜测顶掉。

修复后的口径（与 :mod:`grouppig.social.profile.extractor.conflict_resolver` 同源）：

* 综合分 ``score = 来源可信度 × 置信度 × 时间衰减``，新事实领先 ``>= margin`` 才顶替；
* 分差不足 ``margin`` 或新事实落败 → 新事实记 ``status="conflict"``（不生效），旧值继续 ``active``；
* ``supersede=False`` 的既有语义逐字不变；``arbitrate=False`` 才是显式的「盲顶」开关。
"""

from __future__ import annotations

from grouppig.memory.profile_store import dao as profile_dao
from memory_helpers import BASE_TS, GROUP_ID, register_isolated
from social_helpers import profile_row, social_env

# ---------------------------------------------------------------------------
# 红一：真实写路径（rpc:profile.fact.extract → rpc:profile.update → rpc:profile-store.put）
# ---------------------------------------------------------------------------


async def test_low_credibility_extraction_cannot_destroy_manual_fact(config):
    """北京（人工 0.95）不能被后来低可信的「杭州」（抽取器 0.55）顶掉。"""

    env = await social_env(config)
    try:
        await env.stores.profiles.put(
            profile_row(1001),
            facts=[
                {
                    "fact_key": "city",
                    "fact_value": "北京",
                    "category": "identity",
                    "source": "manual",
                    "confidence": 0.95,
                    "observed_at": BASE_TS,
                }
            ],
        )
        extracted = await env.call("rpc:profile.fact.extract", 1001, group_id=GROUP_ID, text="我坐标杭州")
        assert extracted["applied"] is True
        assert extracted["update"]["superseded"] == 0

        facts = await env.stores.profiles.get_facts(1001, status=["active", "superseded", "conflict"])
        active = [fact for fact in facts if fact["status"] == "active"]
        assert [(fact["fact_value"], fact["source"]) for fact in active] == [("北京", "manual")]
        loser = next(fact for fact in facts if fact["fact_value"] == "杭州")
        assert loser["status"] == "conflict"
        assert loser["source"] == "extractor"
        assert all(fact["status"] != "superseded" for fact in facts)
    finally:
        await env.aclose()


async def test_memory_rpc_put_arbitrates_too(stores):
    """memory 自己的 rpc 口子（``rpc:profile-store.put``）同样受保护。"""

    registry = register_isolated(stores)
    await registry.acall(
        "rpc:profile-store.put",
        {"user_id": 1001, "last_seen": BASE_TS},
        facts=[
            {
                "fact_key": "city",
                "fact_value": "北京",
                "source": "manual",
                "confidence": 0.95,
                "observed_at": BASE_TS,
            }
        ],
    )
    result = await registry.acall(
        "rpc:profile-store.put",
        {"user_id": 1001, "last_seen": BASE_TS + 1},
        facts=[
            {
                "fact_key": "city",
                "fact_value": "杭州",
                "source": "extractor",
                "confidence": 0.55,
                "observed_at": BASE_TS + 1,
            }
        ],
    )
    assert result["superseded"] == 0
    assert result["conflicts"] == 1
    assert [fact["fact_value"] for fact in await stores.profiles.get_facts(1001, status="active")] == ["北京"]


# ---------------------------------------------------------------------------
# 红二：势均力敌（分差 < margin）不写坏档案
# ---------------------------------------------------------------------------


async def test_close_call_keeps_old_value(stores):
    """同源同置信度（分差 0 < margin）的新值不顶替旧值，只记 conflict。"""

    await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "北京",
                "source": "extractor",
                "confidence": 0.80,
                "observed_at": BASE_TS,
            }
        ]
    )
    result = await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "杭州",
                "source": "extractor",
                "confidence": 0.79,
                "observed_at": BASE_TS,
            }
        ]
    )
    assert result == {"facts": 1, "superseded": 0, "conflicts": 1}
    assert [fact["fact_value"] for fact in await stores.profiles.get_facts(1001, status="active")] == ["北京"]
    assert [fact["fact_value"] for fact in await stores.profiles.get_facts(1001, status="conflict")] == ["杭州"]


# ---------------------------------------------------------------------------
# 红三：显式「盲顶」开关（旧行为仍可达）
# ---------------------------------------------------------------------------


async def test_blind_supersede_is_still_reachable(stores):
    """``arbitrate=False`` 逐字保留修复前的盲顶行为，供显式选择的调用方使用。"""

    await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "北京",
                "source": "manual",
                "confidence": 0.95,
                "observed_at": BASE_TS,
            }
        ]
    )
    result = await stores.profiles.put_facts(
        [{"user_id": 1001, "fact_key": "city", "fact_value": "杭州", "source": "extractor", "confidence": 0.55}],
        arbitrate=False,
    )
    assert result["superseded"] == 1
    assert result["conflicts"] == 0
    assert [fact["fact_value"] for fact in await stores.profiles.get_facts(1001, status="active")] == ["杭州"]


async def test_profile_update_can_opt_into_blind_supersede(config):
    """``rpc:profile.update`` 也能显式选择盲顶（默认是仲裁）。"""

    env = await social_env(config)
    try:
        await env.stores.profiles.put(
            profile_row(1001),
            facts=[
                {
                    "fact_key": "city",
                    "fact_value": "北京",
                    "source": "manual",
                    "confidence": 0.95,
                    "observed_at": BASE_TS,
                }
            ],
        )
        default = await env.call(
            "rpc:profile.update",
            user_id=1001,
            facts=[
                {
                    "fact_key": "city",
                    "fact_value": "杭州",
                    "source": "extractor",
                    "confidence": 0.55,
                    "observed_at": BASE_TS,
                }
            ],
        )
        assert default["superseded"] == 0
        assert default["conflicts"] == 1

        blinded = await env.call(
            "rpc:profile.update",
            user_id=1001,
            facts=[
                {
                    "fact_key": "city",
                    "fact_value": "广州",
                    "source": "extractor",
                    "confidence": 0.55,
                    "observed_at": BASE_TS,
                }
            ],
            arbitrate=False,
        )
        assert blinded["superseded"] == 1
        assert [fact["fact_value"] for fact in await env.stores.profiles.get_facts(1001, status="active")] == ["广州"]
    finally:
        await env.aclose()


# ---------------------------------------------------------------------------
# 回归护栏：正常学习、同值刷新、首条事实、旧开关语义
# ---------------------------------------------------------------------------


async def test_stronger_fact_still_supersedes(stores):
    """正常学习不能被打断：明显更强的新事实照样顶替。"""

    await stores.profiles.put_facts(
        [{"user_id": 1001, "fact_key": "job", "fact_value": "学生", "confidence": 0.6, "observed_at": BASE_TS}]
    )
    result = await stores.profiles.put_facts(
        [{"user_id": 1001, "fact_key": "job", "fact_value": "程序员", "confidence": 0.9, "observed_at": BASE_TS}]
    )
    assert result == {"facts": 1, "superseded": 1, "conflicts": 0}
    active = await stores.profiles.get_facts(1001, status="active")
    assert [(fact["fact_value"], fact["version"]) for fact in active] == [("程序员", 2)]


async def test_higher_credibility_source_wins_even_with_lower_confidence(stores):
    """来源可信度是乘法项：人工 0.7 也能压过抽取器 0.8（0.70 > 0.64）。"""

    await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "北京",
                "source": "extractor",
                "confidence": 0.8,
                "observed_at": BASE_TS,
            }
        ]
    )
    result = await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "杭州",
                "source": "manual",
                "confidence": 0.7,
                "observed_at": BASE_TS,
            }
        ]
    )
    assert result["superseded"] == 1
    assert [fact["fact_value"] for fact in await stores.profiles.get_facts(1001, status="active")] == ["杭州"]


async def test_first_fact_and_same_value_paths_unchanged(stores):
    """首条事实直接生效；同值重复只刷新置信度，不新增版本。"""

    await stores.profiles.put_facts(
        [{"user_id": 1001, "fact_key": "city", "fact_value": "杭州", "confidence": 0.4, "observed_at": BASE_TS}]
    )
    await stores.profiles.put_facts(
        [{"user_id": 1001, "fact_key": "city", "fact_value": "杭州", "confidence": 0.8, "observed_at": BASE_TS}]
    )
    facts = await stores.profiles.get_facts(1001)
    assert len(facts) == 1
    assert facts[0]["status"] == "active"
    assert facts[0]["version"] == 1
    assert facts[0]["confidence"] == 0.8


async def test_supersede_false_semantics_unchanged(stores):
    """``supersede=False`` 仍是「新事实记 conflict，旧事实一行不动」。"""

    await stores.profiles.put_facts([{"user_id": 1001, "fact_key": "age", "fact_value": "18", "observed_at": BASE_TS}])
    result = await stores.profiles.put_facts(
        [{"user_id": 1001, "fact_key": "age", "fact_value": "30", "observed_at": BASE_TS}], supersede=False
    )
    assert result == {"facts": 1, "superseded": 0, "conflicts": 1}
    active = await stores.profiles.get_facts(1001, status="active")
    assert [(fact["fact_value"], fact["version"], fact["status"]) for fact in active] == [("18", 1, "active")]
    conflict = await stores.profiles.get_facts(1001, status="conflict")
    assert [(fact["fact_value"], fact["version"]) for fact in conflict] == [("30", 2)]


async def test_conflict_never_leaves_two_active_rows(stores):
    """记过 conflict 之后再来一条强事实：仍然只有一行 active（不能顶错行）。"""

    await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "北京",
                "source": "manual",
                "confidence": 0.95,
                "observed_at": BASE_TS,
            }
        ]
    )
    await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "杭州",
                "source": "extractor",
                "confidence": 0.55,
                "observed_at": BASE_TS,
            }
        ]
    )
    result = await stores.profiles.put_facts(
        [
            {
                "user_id": 1001,
                "fact_key": "city",
                "fact_value": "上海",
                "source": "manual",
                "confidence": 0.9,
                "observed_at": BASE_TS + 200 * 86400,
            }
        ]
    )
    assert result["superseded"] == 1
    active = await stores.profiles.get_facts(1001, status="active")
    assert [(fact["fact_value"], fact["version"]) for fact in active] == [("上海", 3)]
    assert await stores.profiles.count_facts(1001, status="active") == 1


# ---------------------------------------------------------------------------
# 纯函数：消解口径
# ---------------------------------------------------------------------------


def test_resolve_collision_scores_credibility_confidence_and_recency():
    """新事实领先 >= margin 才 update；否则 conflict（保持旧值，避免抖动）。"""

    strong = profile_dao.resolve_collision(
        {"fact_value": "北京", "source": "manual", "confidence": 0.95, "observed_at": BASE_TS},
        {"fact_value": "杭州", "source": "extractor", "confidence": 0.55, "observed_at": BASE_TS},
    )
    assert strong.action == profile_dao.ACTION_CONFLICT
    assert strong.gap < 0

    weak = profile_dao.resolve_collision(
        {"fact_value": "北京", "source": "inferred", "confidence": 0.5, "observed_at": BASE_TS},
        {"fact_value": "杭州", "source": "manual", "confidence": 0.9, "observed_at": BASE_TS},
    )
    assert weak.action == profile_dao.ACTION_UPDATE
    assert weak.gap >= profile_dao.DEFAULT_MARGIN

    # 时间衰减：同源同置信度时，更新的观察更可信（旧事实相对衰减）
    fresher = profile_dao.resolve_collision(
        {"fact_value": "北京", "source": "extractor", "confidence": 0.8, "observed_at": BASE_TS - 200 * 86400},
        {"fact_value": "杭州", "source": "extractor", "confidence": 0.8, "observed_at": BASE_TS},
    )
    assert fresher.action == profile_dao.ACTION_UPDATE


def test_memory_policy_matches_social_resolver():
    """memory 侧的仲裁口径必须与 ``rpc:profile.conflict`` 逐字一致（防两份常量漂移）。"""

    from grouppig.social.profile.extractor import conflict_resolver

    assert profile_dao.SOURCE_CREDIBILITY == conflict_resolver.SOURCE_CREDIBILITY
    assert profile_dao.DEFAULT_CREDIBILITY == conflict_resolver.DEFAULT_CREDIBILITY
    assert profile_dao.DEFAULT_HALF_LIFE_DAYS == conflict_resolver.DEFAULT_HALF_LIFE_DAYS
    assert profile_dao.DEFAULT_MARGIN == conflict_resolver.DEFAULT_MARGIN
    assert profile_dao.ACTION_UPDATE == conflict_resolver.ACTION_UPDATE
    assert profile_dao.ACTION_CONFLICT == conflict_resolver.ACTION_CONFLICT
    for source in ("manual", "extractor", "llm", "inferred", "unknown"):
        assert profile_dao.credibility_of(source) == conflict_resolver.credibility_of(source)
