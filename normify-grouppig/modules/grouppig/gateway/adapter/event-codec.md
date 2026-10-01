---
uid: ee6357fd
id: grouppig.gateway.adapter.event-codec
parent: grouppig.gateway.adapter
name: {zh: "事件编解码器", en: "Event Codec"}
description:
  zh: >
      把 OneBot 事件 JSON 解码为内部事件对象，把回复编码为 OneBot 消息段。
      
  en: >
      Decodes OneBot event JSON into internal event objects and encodes replies into OneBot message segments.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: c75e6fbd3b13f024794398a29eb659a7f1e97fcdc3cf27848f7dcbd8a91c89bc
source:
  - path: "src/grouppig/gateway/adapter/event_codec.py"
apis:
  - protocol: rpc
    path: "codec.decode"
    description:
      zh: >
          解码事件 JSON
          
      en: >
          Decode event JSON
          
  - protocol: rpc
    path: "codec.encode"
    description:
      zh: >
          编码回复消息
          
      en: >
          Encode reply message
          
---
