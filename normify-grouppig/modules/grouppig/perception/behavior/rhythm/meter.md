---
uid: 085be3f2
id: grouppig.perception.behavior.rhythm.meter
parent: grouppig.perception.behavior.rhythm
name: {zh: "节奏测量器", en: "Rhythm Meter"}
description:
  zh: >
      测量当前节奏并输出节奏标签。
      
  en: >
      Measures the current rhythm and outputs a rhythm label.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 7091960916038972fc4c3b497287b3cd7dff59c83eea6354774eba2abf0a424d
source:
  - path: "src/grouppig/perception/behavior/rhythm/meter.py"
apis:
  - protocol: rpc
    path: "rhythm.measure"
    description:
      zh: >
          测量当前群聊节奏
          
      en: >
          Measure current chat rhythm
          
deps:
  - kind: call
    to: grouppig.perception.behavior.rhythm.trend
    from_api: "rpc:rhythm.measure"
    to_api: "rpc:rhythm.trend"
    label: {zh: "节奏趋势", en: "Rhythm trend"}
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:rhythm.measure"
    to_api: "rpc:chat.window"
    label: {zh: "取时间窗", en: "Get window"}
---
