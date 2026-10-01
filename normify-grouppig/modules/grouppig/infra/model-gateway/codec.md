---
uid: f7d846c7
id: grouppig.infra.model-gateway.codec
parent: grouppig.infra.model-gateway
name: {zh: "请求响应编解码器", en: "Request/Response Codec"}
description:
  zh: >
      把内部请求编码为模型 API 格式，把响应解码为内部结构。
      
  en: >
      Encodes internal requests to model API formats and decodes responses into internal structures.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.750Z"
fingerprint: 3d13461a4d25e88495e8341dab457e53425d253250b2a17624c5d3157289a5fc
source:
  - path: "src/grouppig/infra/model_gateway/codec.py"
apis:
  - protocol: rpc
    path: "model.encode"
    description:
      zh: >
          编码模型请求
          
      en: >
          Encode a model request
          
  - protocol: rpc
    path: "model.decode"
    description:
      zh: >
          解码模型响应
          
      en: >
          Decode a model response
          
---
