---
uid: 27a97670
id: grouppig.gateway
parent: grouppig
name: {zh: "接入层", en: "Gateway Layer"}
description:
  zh: >
      QQ 群消息的进出通道：OneBot/NapCat 协议适配、事件分用、优先级队列与节流发送。
      
  en: >
      Inbound/outbound channel for QQ group messages: OneBot/NapCat adaptation, event demux, priority queue and throttled sending.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:42:32.939Z"
fingerprint: 1aac170509ab062651e12274e2c82a7f9a66f0386cf1018899680e9dccc09371
source:
  - path: "src/grouppig/gateway/__init__.py"
deps:
  - kind: call
    to: grouppig.perception
    label: {zh: "消息感知", en: "Perception"}
---
