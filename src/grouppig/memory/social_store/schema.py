"""grouppig.memory.social-store.schema —— 社交网表结构（``mysql:social_edges`` / ``mysql:relationship_scores``）。

* ``social_edges`` —— 以自己为中心的社交网边：谁 @ 谁、谁回谁、共现次数与权重；
* ``relationship_scores`` —— 关系分：分数、分层（tier）、亲密度/信任度/熟悉度与互动计数。

设计：``grouppig.memory.social-store.schema``（叶子模块）。
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, Float, Index, Integer, String, Table

from grouppig.infra.runtime import contract
from grouppig.memory.runtime.columns import BIGINT_PK, EPOCH, JSON_COL, TABLE_KWARGS, created_column, updated_column
from grouppig.memory.runtime.meta import metadata

#: 契约表名（逐字对齐 api-index.json）。
CONTRACT_TABLES = ("social_edges", "relationship_scores")

#: 边类型。
EDGE_TYPES = ("mention", "reply", "co_occur", "affinity", "conflict")

#: 关系分层（分数区间由 social 域规则决定，这里只存枚举）。
RELATIONSHIP_TIERS = ("stranger", "acquaintance", "friend", "close")

#: 自己（机器人）在社交网中的节点 id。
SELF_NODE_ID = 0

social_edges = Table(
    "social_edges",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("src_id", BigInteger, nullable=False, comment="起点 QQ 号（0 表示自己）"),
    Column("dst_id", BigInteger, nullable=False, comment="终点 QQ 号（0 表示自己）"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号；0 表示跨群汇总"),
    Column(
        "edge_type", String(16), nullable=False, default="mention", comment="mention/reply/co_occur/affinity/conflict"
    ),
    Column("weight", Float, nullable=False, default=0.0, comment="累计权重（put_edge 时累加）"),
    Column("count", Integer, nullable=False, default=0, comment="累计发生次数"),
    Column("last_ts", EPOCH, nullable=False, default=0.0, comment="最近一次发生时间"),
    Column("attrs", JSON_COL, nullable=False, default=dict, comment="附加属性（最近证据等）"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index(
    "uq_social_edges_key",
    social_edges.c.src_id,
    social_edges.c.dst_id,
    social_edges.c.group_id,
    social_edges.c.edge_type,
    unique=True,
)
Index("ix_social_edges_src_weight", social_edges.c.src_id, social_edges.c.weight)
Index("ix_social_edges_dst", social_edges.c.dst_id)
Index("ix_social_edges_group_type", social_edges.c.group_id, social_edges.c.edge_type)

relationship_scores = Table(
    "relationship_scores",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("user_id", BigInteger, nullable=False, comment="对方 QQ 号"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号；0 表示跨群汇总"),
    Column("score", Float, nullable=False, default=0.0, comment="关系分（0~100）"),
    Column("tier", String(16), nullable=False, default="stranger", comment="stranger/acquaintance/friend/close"),
    Column("affinity", Float, nullable=False, default=0.0, comment="亲密度"),
    Column("trust", Float, nullable=False, default=0.0, comment="信任度"),
    Column("familiarity", Float, nullable=False, default=0.0, comment="熟悉度"),
    Column("interactions", Integer, nullable=False, default=0, comment="累计互动次数"),
    Column("positive", Integer, nullable=False, default=0, comment="正向互动次数"),
    Column("negative", Integer, nullable=False, default=0, comment="负向互动次数"),
    Column("last_interaction", EPOCH, nullable=False, default=0.0, comment="最近互动时间"),
    Column("decay_at", EPOCH, nullable=False, default=0.0, comment="上次衰减时间"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index("uq_relationship_scores_key", relationship_scores.c.user_id, relationship_scores.c.group_id, unique=True)
Index("ix_relationship_scores_group_score", relationship_scores.c.group_id, relationship_scores.c.score)

#: 本模块负责的表。
TABLES = {"social_edges": social_edges, "relationship_scores": relationship_scores}

for _name in CONTRACT_TABLES:
    contract.assert_known_name(f"mysql:{_name}")

__all__ = [
    "CONTRACT_TABLES",
    "EDGE_TYPES",
    "RELATIONSHIP_TIERS",
    "SELF_NODE_ID",
    "TABLES",
    "relationship_scores",
    "social_edges",
]
