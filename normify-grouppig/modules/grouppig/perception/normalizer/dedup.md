---
uid: 529d350c
id: grouppig.perception.normalizer.dedup
parent: grouppig.perception.normalizer
name: {zh: "去重器", en: "Deduper"}
description:
  zh: >
      识别重复消息、复读与合并转发，输出去重后的消息集。
      
  en: >
      Detects duplicate messages, repeats and merged forwards, outputting deduplicated message sets.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 7f62871d724d22f416003f066e3ed88cf6dc31c0833632a9ed221c1218dad89b
source:
  - path: "src/grouppig/perception/normalizer/dedup.py"
apis:
  - protocol: rpc
    path: "normalizer.dedup"
    description:
      zh: >
          去重消息
          
      en: >
          Deduplicate messages
          
---
