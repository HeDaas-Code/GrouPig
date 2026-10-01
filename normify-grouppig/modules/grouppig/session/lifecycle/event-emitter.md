---
uid: 37e0dfcd
id: grouppig.session.lifecycle.event-emitter
parent: grouppig.session.lifecycle
name: {zh: "会话事件发布器", en: "Session Event Emitter"}
description:
  zh: >
      发布会话完成事件，携带会话摘要与聊天线引用。
      
  en: >
      Publishes session completed events carrying session summaries and thread references.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: 91e3a72e70266649186d8560f2de4d3da5e7df0c7988ff84bc866721af1e3013
source:
  - path: "src/grouppig/session/lifecycle/event_emitter.py"
apis:
  - protocol: kafka
    path: "grouppig.session.completed"
    description:
      zh: >
          会话完成事件
          
      en: >
          Session completed event
          
deps:
  - kind: event
    to: grouppig.reflection.session-review.timeline
    from_api: "kafka:grouppig.session.completed"
    to_api: "rpc:review.on-session-completed"
    label: {zh: "触发反思", en: "Trigger review"}
---
