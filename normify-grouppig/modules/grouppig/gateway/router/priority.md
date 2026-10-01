---
uid: 4d8a1c5b
id: grouppig.gateway.router.priority
parent: grouppig.gateway.router
name: {zh: "优先级队列", en: "Priority Queue"}
description:
  zh: >
      按事件优先级与积压水位排序，向感知层投递消息，避免洪水压垮下游。
      
  en: >
      Orders events by priority and backlog level before delivering to perception, preventing downstream flooding.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: abdd6d83f433653c8692b90acad71f95f52d5815f285e3ad8c33c74d03910859
source:
  - path: "src/grouppig/gateway/router/priority.py"
apis:
  - protocol: rpc
    path: "priority.enqueue"
    description:
      zh: >
          事件入队
          
      en: >
          Enqueue an event
          
  - protocol: rpc
    path: "priority.next"
    description:
      zh: >
          取出下一条事件
          
      en: >
          Pop the next event
          
deps:
  - kind: call
    to: grouppig.perception.observer.buffer
    from_api: "rpc:priority.next"
    to_api: "rpc:observer.ingest"
    label: {zh: "投递消息", en: "Deliver message"}
---
