---
uid: 3c86c256
id: grouppig.memory.slang-kb.dictionary
parent: grouppig.memory.slang-kb
name: {zh: "黑话词典", en: "Slang Dictionary"}
description:
  zh: >
      提供黑话词条的查询与写入。
      
  en: >
      Provides slang entry lookup and upsert.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: ceb766b348d8ef65c78b6f3aff75c7af5425d12be306ca9a384d1cb2dd3f6505
source:
  - path: "src/grouppig/memory/slang_kb/dictionary.py"
apis:
  - protocol: rpc
    path: "slang.lookup"
    description:
      zh: >
          查询黑话
          
      en: >
          Look up slang
          
  - protocol: rpc
    path: "slang.upsert"
    description:
      zh: >
          写入黑话
          
      en: >
          Upsert slang
          
  - protocol: mysql
    path: "slang_entries"
    description:
      zh: >
          黑话词条表
          
      en: >
          Slang entry table
          
---
