---
uid: 3c21c4ac
id: grouppig.memory.chat-store.schema
parent: grouppig.memory.chat-store
name: {zh: "聊天流水表结构", en: "Chat Stream Schema"}
description:
  zh: >
      定义聊天流水表与时间窗索引表结构。
      
  en: >
      Defines the chat stream table and window index table schemas.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: 6b63deec931a10e5400f89d0d49890eee8ff5ca7a7a9a60b349edba5661e15bf
source:
  - path: "src/grouppig/memory/chat_store/schema.py"
apis:
  - protocol: mysql
    path: "chat_messages"
    description:
      zh: >
          聊天流水表
          
      en: >
          Chat messages table
          
  - protocol: mysql
    path: "chat_window_index"
    description:
      zh: >
          时间窗索引表
          
      en: >
          Window index table
          
---
