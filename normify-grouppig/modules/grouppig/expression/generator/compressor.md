---
uid: 3903e47b
id: grouppig.expression.generator.compressor
parent: grouppig.expression.generator
name: {zh: "提示词压缩器", en: "Prompt Compressor"}
description:
  zh: >
      按预算压缩提示词：裁剪旧消息、摘要化聊天线、去冗余。
      
  en: >
      Compresses prompts under budget: trims old messages, summarizes threads and removes redundancy.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.967Z"
fingerprint: b00409f20662387c0739fe3ef1899e4d490ee8398e1c9662d8c01afe394d5adf
source:
  - path: "src/grouppig/expression/generator/compressor.py"
apis:
  - protocol: rpc
    path: "generator.compress"
    description:
      zh: >
          压缩提示词以省 token
          
      en: >
          Compress prompts to save tokens
          
deps:
  - kind: call
    to: grouppig.memory.thread-store.dao
    from_api: "rpc:generator.compress"
    to_api: "rpc:thread.load"
    label: {zh: "读聊天线", en: "Read threads"}
---
