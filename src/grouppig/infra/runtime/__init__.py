"""grouppig.infra.runtime —— infra 运行时底座（**非 normify 设计叶子**）。

设计树中不存在这些模块，它们是落地所需的进程内管道：

* :mod:`grouppig.infra.runtime.contract` —— 读 ``normify-grouppig/api-index.json`` 并做契约校验
* :mod:`grouppig.infra.runtime.registry` —— ``rpc:`` / ``kafka:`` 名字 → 处理器的注册表
* :mod:`grouppig.infra.runtime.bus` —— 进程内事件总线，主题名沿用设计中的 ``kafka:grouppig.*``
* :mod:`grouppig.infra.runtime.di` —— 依赖注入容器与 ``build_container`` 入口
* :mod:`grouppig.infra.runtime.transport` —— 模型 HTTP 传输层（model-gateway 的下游）
* :mod:`grouppig.infra.runtime.usage` —— 共享的 token 用量数据结构
* :mod:`grouppig.infra.runtime.errors` —— 异常类型

队长可决定是否把这些 id 补进 normify 树（建议 id：``grouppig.infra.runtime.*``）。
"""
