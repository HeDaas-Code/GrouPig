---
uid: 035fa85b
id: grouppig.perception.normalizer.featurizer
parent: grouppig.perception.normalizer
name: {zh: "特征提取器", en: "Featurizer"}
description:
  zh: >
      提取轻量特征：长度、表情密度、@ 密度、问句、关键词，供话题与行为模块使用。
      
  en: >
      Extracts lightweight features (length, emoji density, mention density, question marks, keywords) for topic and behavior modules.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T16:36:08.650Z"
fingerprint: 08e0bad57c514a45a01fa8aba415418a2246ddd9d0a13446e41007dc5b7016b9
source:
  - path: "src/grouppig/perception/normalizer/featurizer.py"
apis:
  - protocol: rpc
    path: "normalizer.features"
    description:
      zh: >
          提取轻量特征
          
      en: >
          Extract lightweight features
          
  - protocol: rpc
    path: "normalizer.batch"
    description:
      zh: >
          批量提取特征
          
      en: >
          Extract features in batch
          
deps:
  - kind: call
    to: grouppig.perception.behavior.classifier.aggregator
    from_api: "rpc:normalizer.features"
    to_api: "rpc:behavior.classify"
    label: {zh: "行为分类", en: "Classify behavior"}
  - kind: call
    to: grouppig.session.topic.detector.candidate
    from_api: "rpc:normalizer.features"
    to_api: "rpc:topic.candidate.generate"
    label: {zh: "候选话题", en: "Candidate topics"}
---
