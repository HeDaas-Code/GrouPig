---
uid: 8cf7fde2
id: grouppig.expression.slang.learner
parent: grouppig.expression.slang
name: {zh: "黑话学习器", en: "Slang Learner"}
description:
  zh: >
      从语境中学习新黑话并写入知识库。
      
  en: >
      Learns new slang from context and writes it into the knowledge base.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: 6abb7b6f4bb0be1067e37b2d4e0c6d2a8d26f53fc005c53eff4a703f0f56bebf
source:
  - path: "src/grouppig/expression/slang/learner.py"
apis:
  - protocol: rpc
    path: "slang.learn"
    description:
      zh: >
          学习并沉淀新黑话
          
      en: >
          Learn and persist new slang
          
deps:
  - kind: call
    to: grouppig.memory.slang-kb.dictionary
    from_api: "rpc:slang.learn"
    to_api: "rpc:slang.upsert"
    label: {zh: "写黑话", en: "Upsert slang"}
---
