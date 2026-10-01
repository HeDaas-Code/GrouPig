---
uid: 8e4a62f8
id: grouppig.session.topic.embedder.cache
parent: grouppig.session.topic.embedder
name: {zh: "向量缓存器", en: "Embedding Cache"}
description:
  zh: >
      缓存消息与话题向量，避免重复嵌入。
      
  en: >
      Caches message and topic vectors to avoid repeated embedding calls.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 20679176ad27cc681dd7e938c56354136dce27bf7711dd5acac5e42d916c35ea
source:
  - path: "src/grouppig/session/topic/embedder/cache.py"
apis:
  - protocol: rpc
    path: "topic.embed.cache.get"
    description:
      zh: >
          读取向量缓存
          
      en: >
          Get cached vector
          
  - protocol: rpc
    path: "topic.embed.cache.set"
    description:
      zh: >
          写入向量缓存
          
      en: >
          Set cached vector
          
---
