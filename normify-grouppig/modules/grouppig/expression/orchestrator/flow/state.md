---
uid: f4f8771b
id: grouppig.expression.orchestrator.flow.state
parent: grouppig.expression.orchestrator.flow
name: {zh: "心流状态存储", en: "Flow State Store"}
description:
  zh: >
      保存当前心流状态与上下文，提供启动、推进、结束接口。
      
  en: >
      Stores the current flow state and context, providing start/next/end interfaces.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.969Z"
fingerprint: 8b1a81c702c809b4971570100125c2477f316178f0bdf52f3f60cdf879db8553
source:
  - path: "src/grouppig/expression/orchestrator/flow/state.py"
apis:
  - protocol: rpc
    path: "flow.start"
    description:
      zh: >
          开启一轮心流编排
          
      en: >
          Start a flow orchestration
          
  - protocol: rpc
    path: "flow.next"
    description:
      zh: >
          推进到下一结构步骤
          
      en: >
          Advance to the next structural step
          
  - protocol: rpc
    path: "flow.end"
    description:
      zh: >
          结束编排并产出回复
          
      en: >
          End the flow and produce the reply
          
deps:
  - kind: call
    to: grouppig.expression.orchestrator.flow.transition
    from_api: "rpc:flow.start"
    to_api: "rpc:flow.transition"
    label: {zh: "状态转移", en: "Transition"}
  - kind: call
    to: grouppig.expression.orchestrator.planner.structure
    from_api: "rpc:flow.start"
    to_api: "rpc:planner.plan"
    label: {zh: "多轮规划", en: "Multi-turn plan"}
  - kind: call
    to: grouppig.expression.generator.context
    from_api: "rpc:flow.next"
    to_api: "rpc:generator.compose"
    label: {zh: "生成回复", en: "Compose reply"}
  - kind: call
    to: grouppig.gateway.sender.composer
    from_api: "rpc:flow.end"
    to_api: "rpc:sender.send_reply"
    label: {zh: "发送回复", en: "Send reply"}
  - kind: event
    to: grouppig.expression.orchestrator.flow.emitter
    from_api: "rpc:flow.end"
    to_api: "kafka:grouppig.reply.composed"
    label: {zh: "发布完成", en: "Publish composed"}
---
