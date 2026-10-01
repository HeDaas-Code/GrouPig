---
uid: b8c65fda
id: grouppig.memory.chat-store.dao
parent: grouppig.memory.chat-store
name: {zh: "聊天流水 DAO", en: "Chat Stream DAO"}
description:
  zh: >
      提供聊天消息的追加、查询与时间窗读取。
      
  en: >
      Provides append, query and window reads for chat messages.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: 7eaabb93ee1fa6c5cca73449218968886056fbe94d2f910fc6d25ed4a2f4061c
source:
  - path: "src/grouppig/memory/chat_store/dao.py"
apis:
  - protocol: rpc
    path: "chat.append"
    description:
      zh: >
          追加一条聊天消息
          
      en: >
          Append one chat message
          
  - protocol: rpc
    path: "chat.query"
    description:
      zh: >
          按条件检索消息
          
      en: >
          Query messages by criteria
          
  - protocol: rpc
    path: "chat.window"
    description:
      zh: >
          取最近时间窗消息
          
      en: >
          Get messages in a recent time window
          
deps:
  - kind: dataflow
    to: grouppig.memory.chat-store.schema
    from_api: "rpc:chat.append"
    to_api: "mysql:chat_messages"
    label: {zh: "写入流水表", en: "Write messages"}
  - kind: call
    to: grouppig.memory.chat-store.window-index
    from_api: "rpc:chat.window"
    to_api: "rpc:chat.window.advance"
    label: {zh: "推进索引", en: "Advance index"}
---
