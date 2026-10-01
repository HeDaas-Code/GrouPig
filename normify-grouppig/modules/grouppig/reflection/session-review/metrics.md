---
uid: b5be1f13
id: grouppig.reflection.session-review.metrics
parent: grouppig.reflection.session-review
name: {zh: "行为指标计算器", en: "Behavior Metrics Calculator"}
description:
  zh: >
      计算本段会话行为指标：插话率、回应率、冷场时长、预设命中率。
      
  en: >
      Computes session behavior metrics: interrupt rate, reply rate, silence duration, preset hit rate.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.972Z"
fingerprint: db91dc5a859c0f480f7bad3f90ec88b7e47f570d373341743383a9d1f7d3ca96
source:
  - path: "src/grouppig/reflection/session_review/metrics.py"
apis:
  - protocol: rpc
    path: "review.metrics"
    description:
      zh: >
          计算行为指标
          
      en: >
          Compute behavior metrics
          
deps:
  - kind: call
    to: grouppig.reflection.session-review.timeline
    from_api: "rpc:review.metrics"
    to_api: "rpc:review.timeline"
    label: {zh: "读时间线", en: "Read timeline"}
---
