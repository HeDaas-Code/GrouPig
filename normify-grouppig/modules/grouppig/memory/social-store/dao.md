---
uid: fd5557d5
id: grouppig.memory.social-store.dao
parent: grouppig.memory.social-store
name: {zh: "社交网 DAO", en: "Social Graph DAO"}
description:
  zh: >
      提供社交边的读取与写入。
      
  en: >
      Provides social edge reads and writes.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: e66284241639c2a18fc231e84b6e77e225bb1bfb0f48d94dba0f668717677731
source:
  - path: "src/grouppig/memory/social_store/dao.py"
apis:
  - protocol: rpc
    path: "social-store.get-edges"
    description:
      zh: >
          读社交网边
          
      en: >
          Read social graph edges
          
  - protocol: rpc
    path: "social-store.put-edge"
    description:
      zh: >
          写社交网边
          
      en: >
          Write a social graph edge
          
deps:
  - kind: dataflow
    to: grouppig.memory.social-store.schema
    from_api: "rpc:social-store.put-edge"
    to_api: "mysql:social_edges"
    label: {zh: "写入社交边", en: "Write edges"}
---
