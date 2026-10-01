---
uid: ae7b9c8a
id: grouppig.session.topic.detector.candidate
parent: grouppig.session.topic.detector
name: {zh: "候选话题生成器", en: "Candidate Topic Generator"}
description:
  zh: >
      从消息特征中生成候选话题短语与标签。
      
  en: >
      Generates candidate topic phrases and labels from message features.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: cfb682473a9abeccedce3d0c2f7420a3f8f6b3331c7850f4bee72981820c1cca
source:
  - path: "src/grouppig/session/topic/detector/candidate.py"
apis:
  - protocol: rpc
    path: "topic.candidate.generate"
    description:
      zh: >
          生成候选话题
          
      en: >
          Generate candidate topics
          
deps:
  - kind: call
    to: grouppig.session.topic.detector.boundary
    from_api: "rpc:topic.candidate.generate"
    to_api: "rpc:topic.boundary.detect"
    label: {zh: "边界检测", en: "Boundary detection"}
  - kind: call
    to: grouppig.session.topic.detector.ranker
    from_api: "rpc:topic.candidate.generate"
    to_api: "rpc:topic.detect"
    label: {zh: "话题排序", en: "Rank topics"}
---
