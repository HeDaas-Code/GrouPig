---
uid: dda2dca5
id: grouppig.session.topic.embedder.similarity
parent: grouppig.session.topic.embedder
name: {zh: "相似度计算器", en: "Similarity Calculator"}
description:
  zh: >
      调用嵌入模型并计算话题相似度。
      
  en: >
      Calls the embedding model and computes topic similarity.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: a3881be601e8f1ca71e7717123d72ba235ffbaa63218704d313da8164712e305
source:
  - path: "src/grouppig/session/topic/embedder/similarity.py"
apis:
  - protocol: rpc
    path: "topic.embed"
    description:
      zh: >
          把文本向量化
          
      en: >
          Embed text into vectors
          
  - protocol: rpc
    path: "topic.similarity"
    description:
      zh: >
          计算话题相似度
          
      en: >
          Compute topic similarity
          
deps:
  - kind: call
    to: grouppig.session.topic.embedder.cache
    from_api: "rpc:topic.embed"
    to_api: "rpc:topic.embed.cache.get"
    label: {zh: "查缓存", en: "Check cache"}
  - kind: call
    to: grouppig.infra.model-gateway.router
    from_api: "rpc:topic.embed"
    to_api: "rpc:model.embed"
    label: {zh: "调用嵌入模型", en: "Call embedding model"}
---
