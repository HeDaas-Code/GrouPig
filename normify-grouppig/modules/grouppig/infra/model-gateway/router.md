---
uid: 86fd33e6
id: grouppig.infra.model-gateway.router
parent: grouppig.infra.model-gateway
name: {zh: "模型路由器", en: "Model Router"}
description:
  zh: >
      按任务路由到对话、嵌入或分类模型。
      
  en: >
      Routes tasks to chat, embedding or classification models.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-29T19:07:38.995Z"
fingerprint: 74bbf306f082457110474439433ebf6111600b63f35e78340d6ccf0b4d6216e7
source:
  - path: "src/grouppig/infra/model_gateway/router.py"
apis:
  - protocol: rpc
    path: "model.chat"
    description:
      zh: >
          调用对话模型
          
      en: >
          Call the chat model
          
  - protocol: rpc
    path: "model.embed"
    description:
      zh: >
          调用嵌入模型
          
      en: >
          Call the embedding model
          
  - protocol: rpc
    path: "model.classify"
    description:
      zh: >
          调用轻量分类模型
          
      en: >
          Call the lightweight classifier
          
  - protocol: rpc
    path: "model.system1"
    description:
      zh: >
          System-1 结构化决策：一次前向返回多个问题的概率分布与置信度，置信度低于阈值时升级到对话模型
          
      en: >
          System-1 structured decision: one forward pass returns probability distributions and confidences for multiple questions, escalating to the chat model when confidence falls below a threshold
          
deps:
  - kind: call
    to: grouppig.infra.model-gateway.retry
    from_api: "rpc:model.chat"
    to_api: "rpc:model.retry"
    label: {zh: "失败重试", en: "Retry on failure"}
  - kind: call
    to: grouppig.infra.model-gateway.codec
    from_api: "rpc:model.chat"
    to_api: "rpc:model.encode"
    label: {zh: "请求编码", en: "Encode request"}
  - kind: call
    to: grouppig.infra.runtime.laya-system1
    from_api: "rpc:model.system1"
    label: {zh: "System-1 前向", en: "System-1 forward pass"}
---
