---
uid: 23f5207b
id: grouppig.memory.session-archive.summary-index
parent: grouppig.memory.session-archive
name: {zh: "会话摘要索引器", en: "Session Summary Index"}
description:
  zh: >
      生成会话摘要并建立检索索引。
      
  en: >
      Generates session summaries and builds the retrieval index.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: 416bf58f11d3456b58837d018f4b452f4f8e1d208a48dcb84b6b2cf69cd53cf7
source:
  - path: "src/grouppig/memory/session_archive/summary_index.py"
apis:
  - protocol: rpc
    path: "archive.summarize"
    description:
      zh: >
          生成会话摘要
          
      en: >
          Summarize a session
          
  - protocol: rpc
    path: "archive.find"
    description:
      zh: >
          检索会话摘要
          
      en: >
          Find session summaries
          
deps:
  - kind: call
    to: grouppig.memory.session-archive.dao
    from_api: "rpc:archive.summarize"
    to_api: "rpc:archive.save"
    label: {zh: "保存摘要", en: "Save summary"}
---
