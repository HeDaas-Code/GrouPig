---
uid: 6f2e8c91
id: grouppig.memory.thread-store.schema
parent: grouppig.memory.thread-store
name: {zh: "聊天线表结构", en: "Thread Schema"}
description:
  zh: >
      定义聊天线与线边表结构。
      
  en: >
      Defines the chat thread and thread edge table schemas.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: ceabec26556a5717d1e50c40e92a45b7216352b50365e68e1bdc470eb5651da0
source:
  - path: "src/grouppig/memory/thread_store/schema.py"
apis:
  - protocol: mysql
    path: "chat_threads"
    description:
      zh: >
          聊天线表
          
      en: >
          Chat threads table
          
  - protocol: mysql
    path: "chat_thread_edges"
    description:
      zh: >
          聊天线边表
          
      en: >
          Thread edges table
          
---
