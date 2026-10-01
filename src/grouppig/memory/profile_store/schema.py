"""grouppig.memory.profile-store.schema —— 档案表结构（``mysql:member_profiles`` / ``mysql:profile_facts``）。

* ``member_profiles`` —— 群友档案主表：昵称、别名、标签、说话风格、兴趣、立场、置信度；
* ``profile_facts`` —— 档案事实表：一条事实一行，带版本与状态，支持「新事实顶替旧事实」。

设计：``grouppig.memory.profile-store.schema``（叶子模块）。
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, Float, Index, Integer, String, Table, Text

from grouppig.infra.runtime import contract
from grouppig.memory.runtime.columns import BIGINT_PK, EPOCH, JSON_COL, TABLE_KWARGS, created_column, updated_column
from grouppig.memory.runtime.meta import metadata

#: 契约表名（逐字对齐 api-index.json）。
CONTRACT_TABLES = ("member_profiles", "profile_facts")

#: 事实类别。
FACT_CATEGORIES = ("identity", "preference", "experience", "relation", "opinion", "skill", "other")

#: 事实状态：生效 / 被顶替 / 冲突 / 被否决。
FACT_STATUSES = ("active", "superseded", "conflict", "rejected")

#: 事实来源。
FACT_SOURCES = ("extractor", "llm", "manual", "inferred")

member_profiles = Table(
    "member_profiles",
    metadata,
    Column("user_id", BigInteger, primary_key=True, comment="QQ 号"),
    Column("nickname", String(128), nullable=False, default="", comment="最近一次昵称"),
    Column("aliases", JSON_COL, nullable=False, default=list, comment="见过的所有名字/别名"),
    Column("tags", JSON_COL, nullable=False, default=list, comment="标签（如 技术宅 / 话痨）"),
    Column("persona_summary", Text, nullable=False, default="", comment="画像摘要（给人/模型看的一段话）"),
    Column("speaking_style", JSON_COL, nullable=False, default=dict, comment="说话画像：句长、口癖、表情习惯"),
    Column("interests", JSON_COL, nullable=False, default=list, comment="兴趣话题"),
    Column("stance", JSON_COL, nullable=False, default=dict, comment="立场/观点（话题 → 态度）"),
    Column("group_ids", JSON_COL, nullable=False, default=list, comment="出现过的群号"),
    Column("facts_count", Integer, nullable=False, default=0, comment="生效事实条数"),
    Column("version", Integer, nullable=False, default=1, comment="档案版本号（每次写 +1）"),
    Column("confidence", Float, nullable=False, default=0.5, comment="整体置信度 0~1"),
    Column("first_seen", EPOCH, nullable=False, default=0.0, comment="首次出现时间"),
    Column("last_seen", EPOCH, nullable=False, default=0.0, comment="最近出现时间"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index("ix_member_profiles_last_seen", member_profiles.c.last_seen)
Index("ix_member_profiles_nickname", member_profiles.c.nickname)

profile_facts = Table(
    "profile_facts",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("user_id", BigInteger, nullable=False, comment="QQ 号"),
    Column("fact_key", String(64), nullable=False, comment="事实键（如 hometown / job / favorite_game）"),
    Column("fact_value", Text, nullable=False, default="", comment="事实值"),
    Column("category", String(32), nullable=False, default="other", comment="事实类别"),
    Column("confidence", Float, nullable=False, default=0.5, comment="置信度 0~1"),
    Column("source", String(32), nullable=False, default="extractor", comment="事实来源"),
    Column("evidence", Text, nullable=False, default="", comment="原文证据"),
    Column("message_id", String(64), nullable=False, default="", comment="证据所在消息 id"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="来源群号"),
    Column("status", String(16), nullable=False, default="active", comment="active/superseded/conflict/rejected"),
    Column("version", Integer, nullable=False, default=1, comment="同一 (user_id, fact_key) 的版本号"),
    Column("observed_at", EPOCH, nullable=False, default=0.0, comment="观察到该事实的时间"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index(
    "uq_profile_facts_version", profile_facts.c.user_id, profile_facts.c.fact_key, profile_facts.c.version, unique=True
)
Index("ix_profile_facts_user_status", profile_facts.c.user_id, profile_facts.c.status)
Index("ix_profile_facts_key", profile_facts.c.fact_key)

#: 本模块负责的表。
TABLES = {"member_profiles": member_profiles, "profile_facts": profile_facts}

for _name in CONTRACT_TABLES:
    contract.assert_known_name(f"mysql:{_name}")

__all__ = [
    "CONTRACT_TABLES",
    "FACT_CATEGORIES",
    "FACT_SOURCES",
    "FACT_STATUSES",
    "TABLES",
    "member_profiles",
    "profile_facts",
]
