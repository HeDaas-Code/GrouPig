"""grouppig.memory.chat-store.schema —— 聊天流水表结构（``mysql:chat_messages`` / ``mysql:chat_window_index``）。

表名逐字对齐 ``normify-grouppig/api-index.json``（导入时即断言，防止表名漂移）：

* ``chat_messages`` —— 原始聊天流水，一切感知/画像/反思的事实来源；
* ``chat_window_index`` —— 滚动时间窗索引，供刷屏（flood）与节奏（rhythm）检测快速切片。

设计：``grouppig.memory.chat-store.schema``（叶子模块）。
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, Index, Integer, String, Table, Text

from grouppig.infra.runtime import contract
from grouppig.memory.runtime.columns import BIGINT_PK, EPOCH, JSON_COL, TABLE_KWARGS, created_column, updated_column
from grouppig.memory.runtime.meta import metadata

#: 契约表名（api-index.json 中 ``mysql:`` 协议的名字，逐字一致）。
CONTRACT_TABLES = ("chat_messages", "chat_window_index")

#: 消息角色：群友 / 自己（机器人）/ 系统。
MESSAGE_ROLES = ("member", "self", "system")

#: 消息类型（OneBot v11 ``message_type`` / 消息段类型的归并结果）。
MESSAGE_TYPES = ("text", "image", "face", "at", "reply", "file", "json", "mixed", "other")

#: 默认时间窗长度（秒）。
DEFAULT_WINDOW_SECONDS = 60

chat_messages = Table(
    "chat_messages",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True, comment="自增主键"),
    Column("message_id", String(64), nullable=False, comment="OneBot 消息 id（去重键）"),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号；0 表示私聊/未知"),
    Column("sender_id", BigInteger, nullable=False, default=0, comment="发送者 QQ 号"),
    Column("sender_name", String(128), nullable=False, default="", comment="发送者群名片/昵称"),
    Column("role", String(16), nullable=False, default="member", comment="member / self / system"),
    Column("msg_type", String(16), nullable=False, default="text", comment="归一化消息类型"),
    Column("content", Text, nullable=False, default="", comment="归一化后的纯文本"),
    Column("mentions", JSON_COL, nullable=False, default=list, comment="被 @ 的 QQ 号列表"),
    Column("reply_to", String(64), nullable=False, default="", comment="回复目标 message_id；空串表示无"),
    Column("raw", JSON_COL, nullable=False, default=dict, comment="原始 OneBot 事件（留档）"),
    Column("ts", EPOCH, nullable=False, default=0.0, comment="事件时间（epoch 秒）"),
    Column("session_id", String(64), nullable=False, default="", comment="所属会话 id；空串表示未归档"),
    Column("topic_id", String(64), nullable=False, default="", comment="所属话题 id"),
    Column("thread_id", String(64), nullable=False, default="", comment="所属聊天线 id"),
    Column("tokens", Integer, nullable=False, default=0, comment="粗估 token 数（供预算/压缩参考）"),
    created_column(),
    **TABLE_KWARGS,
)

Index("uq_chat_messages_message_id", chat_messages.c.message_id, unique=True)
Index("ix_chat_messages_group_ts", chat_messages.c.group_id, chat_messages.c.ts)
Index("ix_chat_messages_sender_ts", chat_messages.c.sender_id, chat_messages.c.ts)
Index("ix_chat_messages_session_ts", chat_messages.c.session_id, chat_messages.c.ts)
Index("ix_chat_messages_thread_ts", chat_messages.c.thread_id, chat_messages.c.ts)

chat_window_index = Table(
    "chat_window_index",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("group_id", BigInteger, nullable=False, default=0, comment="群号"),
    Column("window_seconds", Integer, nullable=False, default=DEFAULT_WINDOW_SECONDS, comment="窗长（秒）"),
    Column("window_start", EPOCH, nullable=False, comment="窗起点（epoch 秒，按窗长对齐）"),
    Column("window_end", EPOCH, nullable=False, comment="窗内最后一条消息时间"),
    Column("message_count", Integer, nullable=False, default=0, comment="窗内消息数"),
    Column("sender_count", Integer, nullable=False, default=0, comment="窗内发言人数"),
    Column("senders", JSON_COL, nullable=False, default=list, comment="窗内发言者 QQ 号（去重、有上限）"),
    Column("content_counts", JSON_COL, nullable=False, default=dict, comment="窗内内容指纹计数（刷屏判定用）"),
    Column(
        "content_samples", JSON_COL, nullable=False, default=dict, comment="指纹 → 内容样本（截断，供重复内容回显）"
    ),
    Column("repeat_max", Integer, nullable=False, default=0, comment="窗内同一内容最大重复次数"),
    Column("top_content", String(128), nullable=False, default="", comment="重复最多的内容原文（截断）"),
    Column("last_message_id", String(64), nullable=False, default="", comment="窗内最后一条消息 id"),
    Column("heat", EPOCH, nullable=False, default=0.0, comment="窗内热度：消息数 / 窗长"),
    created_column(),
    updated_column(),
    **TABLE_KWARGS,
)

Index(
    "uq_chat_window_index_bucket",
    chat_window_index.c.group_id,
    chat_window_index.c.window_seconds,
    chat_window_index.c.window_start,
    unique=True,
)
Index("ix_chat_window_index_group_end", chat_window_index.c.group_id, chat_window_index.c.window_end)
Index("ix_chat_window_index_start", chat_window_index.c.window_start)

#: 本模块负责的表（迁移与契约校验用）。
TABLES = {"chat_messages": chat_messages, "chat_window_index": chat_window_index}

for _name in CONTRACT_TABLES:
    contract.assert_known_name(f"mysql:{_name}")

__all__ = [
    "CONTRACT_TABLES",
    "DEFAULT_WINDOW_SECONDS",
    "MESSAGE_ROLES",
    "MESSAGE_TYPES",
    "TABLES",
    "chat_messages",
    "chat_window_index",
]
