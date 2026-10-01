---
uid: c0a30b02
id: grouppig.gateway.adapter.connector
parent: grouppig.gateway.adapter
name: {zh: "连接管理器", en: "Connection Manager"}
description:
  zh: >
      维护 WebSocket/HTTP 长连接：心跳、重连、断线缓冲。
      
  en: >
      Maintains WebSocket/HTTP long connections: heartbeat, reconnect and offline buffering.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: 016da50e6496495cde5176cd3a57583aa97111526105a2909200df09497c83c5
source:
  - path: "src/grouppig/gateway/adapter/connector.py"
apis:
  - protocol: rpc
    path: "connector.connect"
    description:
      zh: >
          建立并维护连接
          
      en: >
          Establish and maintain connection
          
  - protocol: rpc
    path: "connector.heartbeat"
    description:
      zh: >
          发送心跳保活
          
      en: >
          Send heartbeat keepalive
          
---
