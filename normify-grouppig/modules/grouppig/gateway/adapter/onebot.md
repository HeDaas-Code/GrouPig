---
uid: b7f60ea4
id: grouppig.gateway.adapter.onebot
parent: grouppig.gateway.adapter
name: {zh: "OneBot 协议实现", en: "OneBot Protocol Core"}
description:
  zh: >
      实现 OneBot/NapCat 消息收发与事件订阅，把底层协议差异封装为统一接口。
      
  en: >
      Implements OneBot/NapCat send/receive and event subscription, hiding protocol differences behind a unified interface.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: b9d37c11972ae5c4d9c9d8093e966367b438d93d1dddbf2d1d9fe50108a0a433
source:
  - path: "src/grouppig/gateway/adapter/onebot.py"
apis:
  - protocol: rpc
    path: "onebot.start"
    description:
      zh: >
          启动 OneBot 适配器
          
      en: >
          Start the OneBot adapter
          
  - protocol: rpc
    path: "onebot.send"
    description:
      zh: >
          发送 OneBot 消息
          
      en: >
          Send a OneBot message
          
  - protocol: kafka
    path: "grouppig.qq.message.received"
    description:
      zh: >
          收到群消息事件
          
      en: >
          Group message received event
          
deps:
  - kind: call
    to: grouppig.gateway.adapter.connector
    from_api: "rpc:onebot.start"
    to_api: "rpc:connector.connect"
    label: {zh: "建立连接", en: "Connect"}
  - kind: call
    to: grouppig.gateway.adapter.event-codec
    from_api: "rpc:onebot.send"
    to_api: "rpc:codec.encode"
    label: {zh: "编码消息", en: "Encode message"}
---
