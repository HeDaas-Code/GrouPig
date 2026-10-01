---
uid: "55228659"
id: grouppig.memory.session-archive.dao
parent: grouppig.memory.session-archive
name: {zh: "会话档案 DAO", en: "Session Archive DAO"}
description:
  zh: >
      提供会话档案的保存与加载。
      
  en: >
      Provides session archive save and load.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: 12f89ea020b534a91d336861f6530009c5b168a6f20e495d87e4853947cc287f
source:
  - path: "src/grouppig/memory/session_archive/dao.py"
apis:
  - protocol: rpc
    path: "archive.save"
    description:
      zh: >
          保存会话档案
          
      en: >
          Save session archive
          
  - protocol: rpc
    path: "archive.load"
    description:
      zh: >
          读取会话档案
          
      en: >
          Load session archive
          
  - protocol: mysql
    path: "session_archives"
    description:
      zh: >
          会话档案表
          
      en: >
          Session archive table
          
---
