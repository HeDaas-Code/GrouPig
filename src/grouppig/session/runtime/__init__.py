"""grouppig.session.runtime —— session 域的运行时补充模块（设计树外，与 ``grouppig.infra.runtime`` / ``grouppig.memory.runtime`` 同例）。

设计树（``normify-grouppig``）里 session 只有 16 个功能叶子（topic / lifecycle /
threads / wake），没有「消息视图」与「域装配」这两类运行期模块，因此在此补充：

* :mod:`~grouppig.session.runtime.messages` —— 归一化消息视图（会话层唯一输入契约，纯函数）
* :mod:`~grouppig.session.runtime.di` —— 会话层装配（:class:`~...di.SessionLayer` / ``build_session`` / ``install``）

normify id: ``grouppig.session.runtime``（运行时补充模块，设计树暂无）。
"""

__all__: list[str] = []
