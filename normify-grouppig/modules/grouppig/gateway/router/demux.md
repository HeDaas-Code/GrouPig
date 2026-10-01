---
uid: 725018ef
id: grouppig.gateway.router.demux
parent: grouppig.gateway.router
name: {zh: "事件分用器", en: "Event Demuxer"}
description:
  zh: >
      按事件类型（群消息/通知/撤回/命令）分用，并写入优先级队列。
      
  en: >
      Demuxes events by type (group message, notice, recall, command) and writes them into the priority queue.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.971Z"
fingerprint: 319641505708f1e14cbc0cb55a64e8e3529dc00119f3ee69bc05efb75cbafb3c
source:
  - path: "src/grouppig/gateway/router/demux.py"
apis:
  - protocol: rpc
    path: "demux.dispatch"
    description:
      zh: >
          分用一条群事件
          
      en: >
          Demux one group event
          
  - protocol: kafka
    path: "grouppig.event.routed"
    description:
      zh: >
          事件已路由
          
      en: >
          Event routed
          
deps:
  - kind: event
    to: grouppig.gateway.adapter.onebot
    from_api: "rpc:demux.dispatch"
    to_api: "kafka:grouppig.qq.message.received"
    label: {zh: "订阅群消息", en: "Subscribe messages"}
  - kind: call
    to: grouppig.gateway.router.command
    from_api: "rpc:demux.dispatch"
    to_api: "rpc:command.recognize"
    label: {zh: "识别命令", en: "Recognize command"}
  - kind: call
    to: grouppig.gateway.router.priority
    from_api: "rpc:demux.dispatch"
    to_api: "rpc:priority.enqueue"
    label: {zh: "入优先级队列", en: "Enqueue"}
---
