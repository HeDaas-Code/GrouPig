"""grouppig.memory.runtime.db —— 异步数据库句柄（SQLAlchemy Core + aiosqlite / aiomysql）。

设计要点：

* **只用 SQLAlchemy Core**（不用 ORM），DAO 直接写 ``select() / insert() / update() / delete()``；
* 一次 DAO 调用 = 一个事务（``async with db.begin()``），失败整体回滚；
* SQLite 内存库（``sqlite+aiosqlite:///:memory:``）自动用 ``StaticPool``，多事务共享同一连接；
* SQLite 文件库自动建父目录（默认 ``./var/grouppig.db``）；
* ``upsert()`` 抹平 SQLite ``ON CONFLICT`` 与 MySQL ``ON DUPLICATE KEY`` 的差异，
  并支持用 SQL 表达式累加（``set_overrides``，社交边权重累加需要）。

normify id: ``grouppig.memory.runtime.db``（运行时补充模块，设计树暂无）。
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import DateTime, Table, select, text
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import StaticPool

from grouppig.infra.config.loader import Config
from grouppig.memory.runtime.errors import StoreError
from grouppig.memory.runtime.rows import row_to_dict, rows_to_dicts

SQLITE_PREFIX = "sqlite+aiosqlite:///"
MEMORY_MARKERS = (":memory:", "mode=memory")


def sqlite_file_path(dsn: str) -> Path | None:
    """SQLite 文件库 DSN → 文件路径；内存库或非 SQLite 返回 ``None``。"""

    if not dsn.startswith(SQLITE_PREFIX):
        return None
    raw = dsn[len(SQLITE_PREFIX) :].split("?", 1)[0]
    if not raw or any(marker in dsn for marker in MEMORY_MARKERS):
        return None
    return Path(raw)


def ensure_sqlite_dir(dsn: str) -> Path | None:
    """确保 SQLite 文件库的父目录存在（开发期 ``./var/grouppig.db`` 首次运行需要）。"""

    path = sqlite_file_path(dsn)
    if path is None:
        return None
    parent = path.expanduser().resolve().parent
    parent.mkdir(parents=True, exist_ok=True)
    return parent


def create_engine(dsn: str, *, echo: bool = False) -> AsyncEngine:
    """按 DSN 建异步引擎（内存 SQLite 用 StaticPool，其余用默认连接池 + pre-ping）。"""

    if not dsn:
        raise StoreError("存储 DSN 为空：请配置 storage.dsn")
    kwargs: dict[str, Any] = {"echo": echo, "future": True}
    if dsn.startswith("sqlite") and any(marker in dsn for marker in MEMORY_MARKERS):
        kwargs["poolclass"] = StaticPool
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        kwargs["pool_pre_ping"] = True
        ensure_sqlite_dir(dsn)
    try:
        return create_async_engine(dsn, **kwargs)
    except Exception as exc:  # pragma: no cover - 驱动缺失/DSN 非法
        raise StoreError(f"创建数据库引擎失败（dsn={dsn!r}）：{exc}") from exc


def is_single_connection_engine(engine: AsyncEngine) -> bool:
    """引擎是否全进程只有**一条**连接（``StaticPool`` 或 SQLite 内存库）。

    只有这种引擎才需要串行化：MySQL 与 SQLite 文件库走真实连接池，每个事务各拿一条
    连接，互相看不见对方未提交的写。判断依据是池实现本身（``StaticPool`` 就是
    「一个池一条连接」），再兜一层 SQLite 内存库的 DSN 判断，避免自定义池实现漏网。
    """

    if isinstance(getattr(engine, "pool", None), StaticPool):
        return True
    url = str(getattr(engine, "url", "") or "")
    return url.startswith("sqlite") and any(marker in url for marker in MEMORY_MARKERS)


class _ConnectionGate:
    """串行化「同一个连接」上的并发使用（任务可重入；非单连接引擎上是空操作）。

    ``sqlite+aiosqlite:///:memory:`` 走 ``StaticPool``：全进程**共用一条** AsyncConnection。
    一条连接上同时开两个事务时，后开的 ``BEGIN`` 会落在前一个事务中间，谁的 ``COMMIT`` /
    ``ROLLBACK`` 先到，谁就把对方未提交的写一起提交或回滚掉——表现为「写成功了但查不到」。
    这在**跨任务**（网关投递泵 vs 测试任务）和**同任务嵌套**（在事务里再调一个自己开
    事务的 DAO）两种形态下都会发生，所以两种都要挡住。

    MySQL / 文件库走真实连接池，每个事务各拿一条连接，不存在这个问题；此时
    ``enabled=False``，本对象退化成直通：不改变任何行为、也不引入排队。

    **重入语义**：同一个任务再次取用时不重新取锁，而是**复用当前持有的那条连接**。
    提交边界永远归最外层 —— 内层不再 ``BEGIN`` / ``COMMIT``。这比「内层悄悄开一个
    假事务」安全：后者内层的 ``COMMIT`` 会把外层未提交的写一起提交或丢掉，正是要修的
    那个 bug；而复用连接时内层异常会一路冒泡，由最外层统一回滚。
    """

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task | None = None
        self._connection: AsyncConnection | None = None

    @property
    def held_connection(self) -> AsyncConnection | None:
        """当前任务正持有的连接（没有则 ``None``）。"""

        if self._connection is None:
            return None
        return self._connection if self._owner is asyncio.current_task() else None

    @asynccontextmanager
    async def hold(
        self,
        open_connection: Callable[[], AbstractAsyncContextManager[AsyncConnection]],
    ) -> AsyncIterator[AsyncConnection]:
        """独占连接直到退出；``open_connection`` 只在真正需要新连接时才被调用。"""

        if not self.enabled:
            async with open_connection() as conn:
                yield conn
            return

        held = self.held_connection
        if held is not None:
            yield held
            return

        task = asyncio.current_task()
        async with self._lock:
            self._owner = task
            try:
                async with open_connection() as conn:
                    self._connection = conn
                    try:
                        yield conn
                    finally:
                        self._connection = None
            finally:
                self._owner = None


class Database:
    """异步数据库句柄：事务、查询与 upsert 的统一入口。"""

    def __init__(self, engine: AsyncEngine, *, dsn: str = "") -> None:
        self._engine = engine
        self._dsn = dsn
        self._gate = _ConnectionGate(enabled=is_single_connection_engine(engine))

    # ---- 构造 ----------------------------------------------------------
    @classmethod
    def from_dsn(cls, dsn: str, *, echo: bool = False) -> Database:
        return cls(create_engine(dsn, echo=echo), dsn=dsn)

    @classmethod
    def from_config(cls, config: Config, *, path: str = "storage.dsn", echo: bool = False) -> Database:
        return cls.from_dsn(str(config.get(path, "") or ""), echo=echo)

    # ---- 属性 ----------------------------------------------------------
    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def dsn(self) -> str:
        return self._dsn or str(self._engine.url)

    @property
    def dialect(self) -> str:
        return self._engine.dialect.name

    @property
    def is_sqlite(self) -> bool:
        return self.dialect.startswith("sqlite")

    # ---- 事务 ----------------------------------------------------------
    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        """只读连接（不开显式事务）；单连接引擎上独占持有。"""

        async with self._gate.hold(self._engine.connect) as conn:
            yield conn

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[AsyncConnection]:
        """读写事务：正常退出提交，异常回滚；单连接引擎上独占持有。"""

        async with self._gate.hold(self._engine.begin) as conn:
            yield conn

    # ---- 执行 ----------------------------------------------------------
    async def execute(self, statement: Any, params: Mapping[str, Any] | None = None) -> Result:
        async with self.begin() as conn:
            return await conn.execute(statement, dict(params or {}))

    async def fetch_all(self, statement: Any, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        async with self.connect() as conn:
            result = await conn.execute(statement, dict(params or {}))
            return rows_to_dicts(result.mappings().all())

    async def fetch_one(self, statement: Any, params: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
        async with self.connect() as conn:
            result = await conn.execute(statement, dict(params or {}))
            return row_to_dict(result.mappings().first())

    async def scalar(self, statement: Any, params: Mapping[str, Any] | None = None) -> Any:
        async with self.connect() as conn:
            result = await conn.execute(statement, dict(params or {}))
            return result.scalar()

    async def execute_scalar(self, sql: str, params: Mapping[str, Any] | None = None) -> Any:
        """执行原生 SQL 并取第一列（迁移脚本与健康检查用）。"""

        async with self.connect() as conn:
            return (await conn.execute(text(sql), dict(params or {}))).scalar()

    # ---- 写 ------------------------------------------------------------
    async def insert(
        self, table: Table, values: Mapping[str, Any], *, conn: AsyncConnection | None = None
    ) -> dict[str, Any] | None:
        """插入一行，返回主键对应行（自增主键场景）。"""

        payload = _sanitize(table, values)
        if conn is not None:
            result = await conn.execute(table.insert().values(**payload))
            return await self._row_by_pk(table, result, payload, conn=conn)
        async with self.begin() as own:
            result = await own.execute(table.insert().values(**payload))
            return await self._row_by_pk(table, result, payload, conn=own)

    async def upsert(
        self,
        table: Table,
        values: Mapping[str, Any],
        *,
        index_elements: Sequence[str],
        update_columns: Sequence[str] | None = None,
        set_overrides: Mapping[str, Any] | None = None,
        conn: AsyncConnection | None = None,
    ) -> dict[str, Any] | None:
        """按 ``index_elements`` upsert，返回落库后的行。

        * ``update_columns=None`` 时更新除索引列外的全部列；
        * ``set_overrides`` 里的 SQL 表达式优先（例如 ``{"weight": table.c.weight + 1.0}`` 做累加）。
        """

        payload = _sanitize(table, values)
        for key in index_elements:
            if key not in payload:
                raise StoreError(f"upsert {table.name} 缺少索引列 {key!r}")

        statement = self._upsert_statement(table, payload, index_elements, update_columns, set_overrides)
        if conn is not None:
            await conn.execute(statement)
            return await self._row_by_index(table, payload, index_elements, conn=conn)
        async with self.begin() as own:
            await own.execute(statement)
            return await self._row_by_index(table, payload, index_elements, conn=own)

    def _upsert_statement(
        self,
        table: Table,
        payload: Mapping[str, Any],
        index_elements: Sequence[str],
        update_columns: Sequence[str] | None,
        set_overrides: Mapping[str, Any] | None,
    ) -> Any:
        overrides = dict(set_overrides or {})
        columns = (
            list(update_columns) if update_columns is not None else [c for c in payload if c not in index_elements]
        )
        columns = [c for c in columns if c not in overrides]
        if self.dialect.startswith("mysql"):
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            statement = mysql_insert(table).values(**payload)
            assignments = {c: statement.inserted[c] for c in columns}
            assignments.update(overrides)
            if assignments:
                statement = statement.on_duplicate_key_update(**assignments)
            return statement
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        statement = sqlite_insert(table).values(**payload)
        assignments = {c: statement.excluded[c] for c in columns}
        assignments.update(overrides)
        if assignments:
            return statement.on_conflict_do_update(index_elements=list(index_elements), set_=assignments)
        return statement.on_conflict_do_nothing(index_elements=list(index_elements))

    async def _row_by_pk(
        self,
        table: Table,
        result: Result,
        payload: Mapping[str, Any],
        *,
        conn: AsyncConnection,
    ) -> dict[str, Any] | None:
        pk_names = [c.name for c in table.primary_key.columns]
        criteria = {}
        for name in pk_names:
            if name in payload:
                criteria[name] = payload[name]
        if len(criteria) != len(pk_names):
            inserted = getattr(result, "inserted_primary_key", None)
            if inserted and len(inserted) == len(pk_names):
                criteria = dict(zip(pk_names, inserted, strict=True))
        if len(criteria) != len(pk_names):
            return None
        statement = select(table).where(*[table.c[k] == v for k, v in criteria.items()])
        return row_to_dict((await conn.execute(statement)).mappings().first())

    async def _row_by_index(
        self,
        table: Table,
        payload: Mapping[str, Any],
        index_elements: Sequence[str],
        *,
        conn: AsyncConnection,
    ) -> dict[str, Any] | None:
        statement = select(table).where(*[table.c[k] == payload[k] for k in index_elements])
        return row_to_dict((await conn.execute(statement)).mappings().first())

    # ---- 表结构 --------------------------------------------------------
    async def create_all(self, metadata: Any) -> None:
        async with self.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def drop_all(self, metadata: Any) -> None:
        async with self.begin() as conn:
            await conn.run_sync(metadata.drop_all)

    async def existing_tables(self) -> list[str]:
        """库里现有的表名（SQLite 走 ``sqlite_master``，MySQL 走 ``information_schema``）。"""

        async with self.connect() as conn:
            return sorted(await conn.run_sync(lambda sync_conn: list(_table_names(sync_conn))))

    async def table_columns(self, table: str) -> list[str]:
        async with self.connect() as conn:
            return list(await conn.run_sync(lambda sync_conn: _column_names(sync_conn, table)))

    # ---- 生命周期 ------------------------------------------------------
    async def aclose(self) -> None:
        await self._engine.dispose()

    async def health(self) -> dict[str, Any]:
        """``SELECT 1`` + 现有表清单，供容器健康检查。"""

        tables = await self.existing_tables()
        return {
            "dsn": _masked(self.dsn),
            "dialect": self.dialect,
            "tables": tables,
            "table_count": len(tables),
        }


def _sanitize(table: Table, values: Mapping[str, Any]) -> dict[str, Any]:
    """过滤写入值：丢掉表里没有的键；审计时间列（DateTime）不接受字符串回写。

    DAO 常把「读回来的行」再合并回写，此时 ``created_at`` / ``updated_at`` 已是
    ISO 字符串。这类审计字段一律不回写，交给列默认值（插入）与 ``onupdate``（更新），
    避免 SQLite 的 DateTime 类型报错。
    """

    payload: dict[str, Any] = {}
    for key, value in values.items():
        column = table.c.get(key)
        if column is None:
            continue
        if isinstance(column.type, DateTime) and value is not None and not isinstance(value, date | datetime):
            continue
        payload[key] = value
    return payload


def _table_names(sync_conn: Any) -> list[str]:
    return list(sync_conn.dialect.get_table_names(sync_conn))


def _column_names(sync_conn: Any, table: str) -> list[str]:
    from sqlalchemy import inspect as sa_inspect

    return [c["name"] for c in sa_inspect(sync_conn).get_columns(table)]


def _masked(dsn: str) -> str:
    """脱敏 DSN 里的口令（日志/健康检查用）。"""

    return re.sub(r"://([^:/@]+):([^@/]+)@", r"://\1:***@", dsn)


__all__ = ["Database", "create_engine", "ensure_sqlite_dir", "sqlite_file_path"]
