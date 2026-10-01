---
uid: b52a9c78
id: grouppig.perception.behavior.flood.verdict
parent: grouppig.perception.behavior.flood
name: {zh: "刷屏判定器", en: "Flood Verdict"}
description:
  zh: >
      综合流速与重复度给出刷屏结论。
      
  en: >
      Combines velocity and repetition into a flooding verdict.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 23b1a4f85059cd0939e225ace8c5838cd7dadfa6a60f85aee74ee57e71edd0d9
source:
  - path: "src/grouppig/perception/behavior/flood/verdict.py"
apis:
  - protocol: rpc
    path: "flood.detect"
    description:
      zh: >
          检测是否刷屏
          
      en: >
          Detect flooding state
          
deps:
  - kind: call
    to: grouppig.perception.behavior.flood.velocity
    from_api: "rpc:flood.detect"
    to_api: "rpc:flood.velocity"
    label: {zh: "流速计算", en: "Velocity"}
  - kind: call
    to: grouppig.perception.behavior.flood.repetition
    from_api: "rpc:flood.detect"
    to_api: "rpc:flood.repetition"
    label: {zh: "重复度", en: "Repetition"}
---
