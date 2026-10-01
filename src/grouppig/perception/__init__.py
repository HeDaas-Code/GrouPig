"""zh: "感知层", en: "Perception Layer"。

观察群聊流：清洗、去重、特征提取、群体行为分类、刷屏检测、节奏感知与插话决策。

装配入口（集成层 t10 调用）::

    from grouppig.perception import install

    perception = await install(container)                 # 注册 21 个 rpc: 名字 + 订阅行为变更事件
    await container.call("rpc:observer.ingest", event)    # 网关投递泵的投递点
    behavior = await container.call("rpc:behavior.classify", group_id)

模块构成（逐字镜像 normify 设计）：

* :mod:`grouppig.perception.observer.buffer` —— 环形缓冲（``rpc:observer.ingest`` / ``rpc:observer.buffer.drain``）
* :mod:`grouppig.perception.observer.window` —— 滚动时间窗（``rpc:observer.window.slide`` / ``rpc:observer.window.slice``）
* :mod:`grouppig.perception.normalizer.cleaner` —— 清洗（``rpc:normalizer.clean`` / ``rpc:normalizer.strip``）
* :mod:`grouppig.perception.normalizer.dedup` —— 去重（``rpc:normalizer.dedup``）
* :mod:`grouppig.perception.normalizer.featurizer` —— 特征（``rpc:normalizer.features`` / ``rpc:normalizer.batch``）
* :mod:`grouppig.perception.behavior.classifier.*` —— 特征编码 / 规则 / 模型判别 / 聚合
* :mod:`grouppig.perception.behavior.flood.*` —— 流速 / 重复度 / 刷屏判定
* :mod:`grouppig.perception.behavior.rhythm.*` —— 节奏测量 / 趋势
* :mod:`grouppig.perception.interrupt.*` —— 冷却 / 插话评分 / 插话决策
* :mod:`grouppig.perception.runtime.*` —— 运行时补充包（阈值、文本工具、下游调用、装配入口）

契约：本域共 21 个 ``rpc:`` + 2 个 ``kafka:`` 名字，全部逐字取自
``normify-grouppig/api-index.json``（见
:data:`grouppig.perception.runtime.di.PERCEPTION_RPC` /
:data:`~grouppig.perception.runtime.di.PERCEPTION_TOPICS`）。

normify id: ``grouppig.perception``（容器模块）。
"""

from __future__ import annotations

from grouppig.perception.runtime.di import (
    EXTERNAL_RPC,
    PERCEPTION_RPC,
    PERCEPTION_TOPICS,
    SCOPE,
    Perception,
    build_perception,
    install,
    register_perception_handlers,
)

__all__ = [
    "EXTERNAL_RPC",
    "PERCEPTION_RPC",
    "PERCEPTION_TOPICS",
    "SCOPE",
    "Perception",
    "build_perception",
    "install",
    "register_perception_handlers",
]
