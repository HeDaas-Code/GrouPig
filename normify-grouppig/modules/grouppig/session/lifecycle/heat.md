---
uid: 18431ea1
id: grouppig.session.lifecycle.heat
parent: grouppig.session.lifecycle
name: {zh: "会话热度管理器", en: "Session Heat Manager"}
description:
  zh: >
      按参与人数与消息频率维护会话热度，热度低于阈值进入冷却。
      
  en: >
      Maintains session heat by participant count and message frequency; low heat moves the session to cooling.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: 78e3caaf2361dc99511de7b5c2f455b86043503f0357ae563bf4d5cf30875dd8
source:
  - path: "src/grouppig/session/lifecycle/heat.py"
apis:
  - protocol: rpc
    path: "session.heat"
    description:
      zh: >
          计算会话热度
          
      en: >
          Compute session heat
          
deps:
  - kind: call
    to: grouppig.memory.chat-store.dao
    from_api: "rpc:session.heat"
    to_api: "rpc:chat.window"
    label: {zh: "取消息窗", en: "Get message window"}
---
