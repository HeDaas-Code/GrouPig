---
uid: c23e90bf
id: grouppig.memory.profile-store.dao
parent: grouppig.memory.profile-store
name: {zh: "档案 DAO", en: "Profile DAO"}
description:
  zh: >
      提供档案的读取与写入。
      
  en: >
      Provides profile reads and writes.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: 42fd45e1579f45ee46d7d7c89ec3733c3064ecc552d02a079a201d8b5cad28f3
source:
  - path: "src/grouppig/memory/profile_store/dao.py"
apis:
  - protocol: rpc
    path: "profile-store.get"
    description:
      zh: >
          读档案存储
          
      en: >
          Read the profile store
          
  - protocol: rpc
    path: "profile-store.put"
    description:
      zh: >
          写档案存储
          
      en: >
          Write the profile store
          
deps:
  - kind: dataflow
    to: grouppig.memory.profile-store.schema
    from_api: "rpc:profile-store.put"
    to_api: "mysql:member_profiles"
    label: {zh: "写入档案表", en: "Write profiles"}
---
