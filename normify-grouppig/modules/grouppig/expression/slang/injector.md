---
uid: f696d59e
id: grouppig.expression.slang.injector
parent: grouppig.expression.slang
name: {zh: "黑话注入器", en: "Slang Injector"}
description:
  zh: >
      在回复中自然使用已学黑话。
      
  en: >
      Naturally uses learned slang in replies.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: fbdde63c99334d8bdd511c772db7648de77e01e3a2a02161ddc6cd07ebf30ea9
source:
  - path: "src/grouppig/expression/slang/injector.py"
apis:
  - protocol: rpc
    path: "slang.inject"
    description:
      zh: >
          在回复中自然使用黑话
          
      en: >
          Naturally use slang in replies
          
deps:
  - kind: call
    to: grouppig.memory.slang-kb.dictionary
    from_api: "rpc:slang.inject"
    to_api: "rpc:slang.lookup"
    label: {zh: "查黑话", en: "Lookup slang"}
---
