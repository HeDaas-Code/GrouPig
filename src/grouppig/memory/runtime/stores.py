"""grouppig.memory.runtime.stores —— 六类存储的装配。

:class:`MemoryStores` 把六类存储的 DAO 与它们的互相依赖（聊天流水 → 时间窗索引、
黑话词典 → 新鲜度管理器、会话档案 DAO → 摘要索引器）一次性装配好：

* ``MemoryStores.from_dsn(...)`` / ``from_config(...)`` —— 按 DSN 或配置建库；
* ``await stores.migrate()`` —— 建全库（10 张表 + 索引）；
* ``await stores.health()`` —— 表清单、行数概览；
* ``await stores.aclose()`` —— 释放连接池。

normify id: ``grouppig.memory.runtime.stores``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from grouppig.infra.config.loader import Config
from grouppig.memory.chat_store.dao import ChatStoreDAO
from grouppig.memory.chat_store.schema import DEFAULT_WINDOW_SECONDS
from grouppig.memory.chat_store.window_index import WindowIndex
from grouppig.memory.profile_store.dao import ProfileDAO
from grouppig.memory.runtime import schema as schema_module
from grouppig.memory.runtime.db import Database
from grouppig.memory.session_archive.dao import SessionArchiveDAO
from grouppig.memory.session_archive.summary_index import SummaryIndex
from grouppig.memory.slang_kb.dictionary import SlangDictionary
from grouppig.memory.slang_kb.freshness import DEFAULT_HALF_LIFE_DAYS, SlangFreshness
from grouppig.memory.social_store.dao import SocialStoreDAO
from grouppig.memory.thread_store.dao import ThreadDAO


@dataclass
class MemoryStores:
    """六类存储的 DAO 集合。"""

    db: Database
    chat: ChatStoreDAO
    chat_window: WindowIndex
    threads: ThreadDAO
    profiles: ProfileDAO
    social: SocialStoreDAO
    archives: SessionArchiveDAO
    summaries: SummaryIndex
    slang: SlangDictionary
    slang_freshness: SlangFreshness
    window_seconds: int = DEFAULT_WINDOW_SECONDS
    _closed: bool = field(default=False, repr=False)

    # ---- 构造 ----------------------------------------------------------
    @classmethod
    def create(
        cls,
        db: Database,
        *,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        slang_half_life_days: float = DEFAULT_HALF_LIFE_DAYS,
        top_keywords: int = 10,
        advance_on_append: bool = True,
    ) -> MemoryStores:
        """按依赖顺序装配（依赖注入显式传递，便于单测替换）。"""

        window_index = WindowIndex(db, window_seconds=window_seconds)
        chat = ChatStoreDAO(
            db,
            window_index=window_index,
            window_seconds=window_seconds,
            advance_on_append=advance_on_append,
        )
        archives = SessionArchiveDAO(db)
        slang = SlangDictionary(db)
        return cls(
            db=db,
            chat=chat,
            chat_window=window_index,
            threads=ThreadDAO(db),
            profiles=ProfileDAO(db),
            social=SocialStoreDAO(db),
            archives=archives,
            summaries=SummaryIndex(db, dao=archives, top_keywords=top_keywords),
            slang=slang,
            slang_freshness=SlangFreshness(db, dictionary=slang, half_life_days=slang_half_life_days),
            window_seconds=int(window_seconds),
        )

    @classmethod
    def from_dsn(cls, dsn: str, *, echo: bool = False, **kwargs: Any) -> MemoryStores:
        return cls.create(Database.from_dsn(dsn, echo=echo), **kwargs)

    @classmethod
    def from_config(cls, config: Config, *, echo: bool = False, **kwargs: Any) -> MemoryStores:
        return cls.create(Database.from_config(config, echo=echo), **kwargs)

    # ---- 生命周期 ------------------------------------------------------
    async def migrate(self) -> dict[str, Any]:
        """建全库（幂等）。"""

        return await schema_module.create_all(self.db)

    async def drop_all(self) -> dict[str, Any]:
        """删全库（仅开发/测试用）。"""

        return await schema_module.drop_all(self.db)

    async def aclose(self) -> None:
        if not self._closed:
            await self.db.aclose()
            self._closed = True

    # ---- 自检 ----------------------------------------------------------
    async def health(self) -> dict[str, Any]:
        """表清单 + 契约比对 + 行数概览。"""

        existing = await self.db.existing_tables()
        check = schema_module.check_contract(existing)
        return {
            "database": await self.db.health(),
            "schema": schema_module.summary(),
            "contract": {"missing": check["missing"], "unknown": check["unknown"]},
            "rows": {
                "chat_messages": await self.chat.count(),
                "chat_threads": await self.threads.count(),
                "member_profiles": await self.profiles.count(),
                "social_edges": await self.social.count_edges(),
                "slang_entries": await self.slang.count(),
            },
            "window_seconds": self.window_seconds,
        }


__all__ = ["MemoryStores"]
