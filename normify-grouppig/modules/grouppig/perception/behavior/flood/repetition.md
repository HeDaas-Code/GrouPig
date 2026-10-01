---
uid: d23e71e2
id: grouppig.perception.behavior.flood.repetition
parent: grouppig.perception.behavior.flood
name: {zh: "重复度检测器", en: "Repetition Detector"}
description:
  zh: >
      检测复读、相似文本与图片轰炸。
      
  en: >
      Detects repeats, near-duplicate texts and image bombing.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 53a709cb60561288cb6726f91b03557ecc68bb4ed52bd75639a27ab9cc6a5240
source:
  - path: "src/grouppig/perception/behavior/flood/repetition.py"
apis:
  - protocol: rpc
    path: "flood.repetition"
    description:
      zh: >
          计算重复度
          
      en: >
          Compute repetition
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:flood.repetition"
    to_api: "rpc:chat.query"
    label: {zh: "查近期消息", en: "Query recent messages"}
---
