---
uid: bdd1cb85
id: grouppig.perception.behavior.flood.velocity
parent: grouppig.perception.behavior.flood
name: {zh: "流速计算器", en: "Velocity Calculator"}
description:
  zh: >
      计算窗口内消息速率与加速度。
      
  en: >
      Computes message rate and acceleration within the window.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.753Z"
fingerprint: 658c1331a64472bc34beb48b8c1754164c55771095b9021cfd74b54a00565600
source:
  - path: "src/grouppig/perception/behavior/flood/velocity.py"
apis:
  - protocol: rpc
    path: "flood.velocity"
    description:
      zh: >
          计算消息流速
          
      en: >
          Compute message velocity
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:flood.velocity"
    to_api: "rpc:chat.window"
    label: {zh: "取时间窗", en: "Get window"}
---
