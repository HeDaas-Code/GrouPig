---
uid: 0e7e0945
id: grouppig.memory.thread-store.dao
parent: grouppig.memory.thread-store
name: {zh: "聊天线 DAO", en: "Thread DAO"}
description:
  zh: >
      提供聊天线的保存、加载与跨会话检索。
      
  en: >
      Provides thread save, load and cross-session retrieval.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: f86d499995e23033ae90b0255a0390a6667693fa854291cff11edf94aabb132e
source:
  - path: "src/grouppig/memory/thread_store/dao.py"
apis:
  - protocol: rpc
    path: "thread.save"
    description:
      zh: >
          保存聊天线
          
      en: >
          Save a chat thread
          
  - protocol: rpc
    path: "thread.load"
    description:
      zh: >
          按会话加载聊天线
          
      en: >
          Load threads by session
          
  - protocol: rpc
    path: "thread.find-cross"
    description:
      zh: >
          跨会话查找聊天线
          
      en: >
          Find threads across sessions
          
deps:
  - kind: dataflow
    to: grouppig.memory.thread-store.schema
    from_api: "rpc:thread.save"
    to_api: "mysql:chat_threads"
    label: {zh: "写入线程表", en: "Write threads"}
---
