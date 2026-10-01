"""grouppig.memory.runtime —— memory 域的运行时补充模块（设计树外，与 ``grouppig.infra.runtime`` 同例）。

设计树（``normify-grouppig``）里 memory 只有六类存储的 schema / dao / index 叶子，
没有数据库连接、元数据汇总与装配模块；运行期需要它们，因此在此补充：

* :mod:`~grouppig.memory.runtime.meta` —— 全库共享的 :class:`~sqlalchemy.MetaData`
* :mod:`~grouppig.memory.runtime.db` —— 异步引擎、事务与可移植 upsert（SQLAlchemy Core）
* :mod:`~grouppig.memory.runtime.schema` —— 10 张表的元数据汇总与契约校验
* :mod:`~grouppig.memory.runtime.stores` —— 六类存储的装配（:class:`~...stores.MemoryStores`）
* :mod:`~grouppig.memory.runtime.di` —— memory 的 20 个 ``rpc:`` 处理器注册
* :mod:`~grouppig.memory.runtime.migrate` —— 迁移脚本（``python -m grouppig.memory.runtime.migrate``）

normify id: ``grouppig.memory.runtime``（运行时补充模块，设计树暂无）。
"""
