"""grouppig.memory.thread-store.schema —— 聊天线表结构（``mysql:chat_threads`` / ``mysql:chat_thread_edges``）。

* ``chat_threads`` —— 聊天线：一个话题在一段时间里的完整脉络（参与者、关键词、消息区间）；
* ``chat_thread_edges`` —— 聊天线之间的边（分支 / 引用 / 合并），支撑跨会话检索。

设计：``grouppig.memory.thread-store.schema``（叶子模块）。
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, Float, Index, Integer, String, Table, Text

from grouppig.infra.runtime import contract
from grouppig.memory.runtime.columns import BIGINT_PK, EPOCH, JSON_COL, TABLE_KWARGS, created_column, updated_column
from grouppig.memory.runtime.meta import metadata

#: 契约表名（逐字对齐 api-index.json）。
CONTRACT_TABLES = ("chat_threads", "chat_thread_edges")

#: 聊天线状态。
THREAD_STATUSES = ("open", "idle", "closed", "archived")

#: 线边类型。
EDGE_TYPES = ("branch", "reply", "reference", "merge", "split")

chat_threads = Table(
    "chat_threads",
    metadata,
    Column("thread_id", String(64), primary_key=True, comment="聊天线 id"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号"),
    Column("session_id", String(64), nullable=False, default="", comment="所属会话 id"),
    Column("topic_id", String(64), nullable=False, default="", comment="所属话题 id"),
    Column("title", String(255), nullable=False, default="", comment="聊天线标题"),
    Column("summary", Text, nullable=False, default="", comment="聊天线摘要"),
    Column("keywords", JSON_COL, nullable=False, default=list, comment="关键词（跨会话检索用）"),
    Column("participants", JSON_COL, nullable=False, default=list, comment="参与者 QQ 号"),
    Column("message_ids", JSON_COL, nullable=False, default=list, comment="消息 id 序列（有上限）"),
    Column("message_count", Integer, nullable=False, default=0),
    Column("first_ts", EPOCH, nullable=False, default=0.0, comment="首条消息时间"),
    Column("last_ts", EPOCH, nullable=False, default=0.0, comment="末条消息时间"),
    Column("heat", EPOCH, nullable=False, default=0.0, comment="热度：消息数 / 持续时间"),
    Column("status", String(16), nullable=False, default="open", comment="open/idle/closed/archived"),
    Column("embedding", JSON_COL, nullable=True, comment="话题向量（跨会话相似度检索）"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index("ix_chat_threads_session_ts", chat_threads.c.session_id, chat_threads.c.last_ts)
Index("ix_chat_threads_group_status", chat_threads.c.group_id, chat_threads.c.status)
Index("ix_chat_threads_topic", chat_threads.c.topic_id)
Index("ix_chat_threads_last_ts", chat_threads.c.last_ts)

chat_thread_edges = Table(
    "chat_thread_edges",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("thread_id", String(64), nullable=False, comment="所属聊天线 id"),
    Column("edge_type", String(16), nullable=False, default="reply", comment="branch/reply/reference/merge/split"),
    Column("parent_id", String(64), nullable=False, default="", comment="父聊天线 id；空串表示无"),
    Column("child_id", String(64), nullable=False, default="", comment="子聊天线 id；空串表示无"),
    Column("from_message_id", String(64), nullable=False, default="", comment="起点消息 id"),
    Column("to_message_id", String(64), nullable=False, default="", comment="终点消息 id"),
    Column("weight", Float, nullable=False, default=1.0, comment="边权重"),
    Column("attrs", JSON_COL, nullable=False, default=dict, comment="附加属性"),
    created_column(),
    **TABLE_KWARGS,
)

Index(
    "uq_chat_thread_edges_key",
    chat_thread_edges.c.thread_id,
    chat_thread_edges.c.parent_id,
    chat_thread_edges.c.child_id,
    chat_thread_edges.c.edge_type,
    unique=True,
)
Index("ix_chat_thread_edges_thread", chat_thread_edges.c.thread_id)
Index("ix_chat_thread_edges_child", chat_thread_edges.c.child_id)
Index("ix_chat_thread_edges_from_message", chat_thread_edges.c.from_message_id)

#: 本模块负责的表。
TABLES = {"chat_threads": chat_threads, "chat_thread_edges": chat_thread_edges}

for _name in CONTRACT_TABLES:
    contract.assert_known_name(f"mysql:{_name}")

__all__ = ["CONTRACT_TABLES", "EDGE_TYPES", "TABLES", "THREAD_STATUSES", "chat_thread_edges", "chat_threads"]
