"""grouppig.session —— 话题会话层（容器包）。

模块构成（逐字镜像 normify 设计 ``grouppig.session`` 子树）::

    topic/detector/{boundary,candidate,ranker}    话题边界 / 候选 / 排序（+ kafka:grouppig.topic.changed）
    topic/embedder/{cache,similarity}             向量缓存 / 相似度
    lifecycle/{state_machine,heat,archive_trigger,event_emitter}
                                                  会话状态机 / 热度 / 归档触发 / 完成事件
    threads/weaver/{segmenter,linker,outliner}    消息分段 / 引用链接 / 大纲
    threads/cross/{reference_parser,matcher}      跨会话引用解析 / 历史聊天线匹配
    wake/{restorer,buffer}                        归档会话唤醒 / 唤醒上下文缓冲
    runtime/{messages,errors,di}                  运行时补充：消息视图 / 异常 / 域装配

契约：本域共 16 个 ``rpc:`` + 2 个 ``kafka:`` 名字，全部逐字取自
``normify-grouppig/api-index.json``（见 :data:``SESSION_RPC`` / :data:``SESSION_TOPICS``）。

装配（t10 集成入口）::

    from grouppig.session.runtime.di import attach_session

    layer = await attach_session(container)     # 注册 16 个 rpc: + 订阅 kafka:grouppig.qq.message.received
    await container.call("rpc:topic.detect", group_id=100, messages=window)

normify id: ``grouppig.session``（容器模块）。
"""

from grouppig.session.runtime.di import (
    RPC_REVIEW,
    SESSION_RPC,
    SESSION_SCOPE,
    SESSION_TOPICS,
    TOPIC_MESSAGE_RECEIVED,
    SessionLayer,
    attach_session,
    get_session,
    register_session_handlers,
)

__all__ = [
    "RPC_REVIEW",
    "SESSION_RPC",
    "SESSION_SCOPE",
    "SESSION_TOPICS",
    "TOPIC_MESSAGE_RECEIVED",
    "SessionLayer",
    "attach_session",
    "get_session",
    "register_session_handlers",
]
