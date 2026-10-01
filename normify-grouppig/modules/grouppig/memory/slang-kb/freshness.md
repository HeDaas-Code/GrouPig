---
uid: 2cecc140
id: grouppig.memory.slang-kb.freshness
parent: grouppig.memory.slang-kb
name: {zh: "黑话新鲜度管理器", en: "Slang Freshness Manager"}
description:
  zh: >
      管理黑话新鲜度：使用刷新、长期不用衰减。
      
  en: >
      Manages slang freshness: refresh on use, decay on long disuse.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: 6792d098ed51904d0c8a3f857aa8bc980ffc252e7f2d41a93d3492eaa4c57117
source:
  - path: "src/grouppig/memory/slang_kb/freshness.py"
apis:
  - protocol: rpc
    path: "slang.decay"
    description:
      zh: >
          衰减长期不用的黑话
          
      en: >
          Decay long-unused slang entries
          
  - protocol: rpc
    path: "slang.refresh"
    description:
      zh: >
          刷新黑话新鲜度
          
      en: >
          Refresh slang freshness
          
deps:
  - kind: call
    to: grouppig.memory.slang-kb.dictionary
    from_api: "rpc:slang.decay"
    to_api: "rpc:slang.lookup"
    label: {zh: "读词条", en: "Read entries"}
---
