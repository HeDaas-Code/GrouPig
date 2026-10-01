---
uid: 9359eb8c
id: grouppig.social.graph.relationship.rules
parent: grouppig.social.graph.relationship
name: {zh: "关系分规则引擎", en: "Relationship Rule Engine"}
description:
  zh: >
      按互动事件调整关系分：被回应、被@、观点一致、冲突。
      
  en: >
      Adjusts relationship scores by interaction events: being replied to, being mentioned, agreement, conflict.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: b2adfd1f3ba84c95ddc63037a3b2b7e3ead5a2650b86e29fc51a4db1c3e408e1
source:
  - path: "src/grouppig/social/graph/relationship/rules.py"
apis:
  - protocol: rpc
    path: "relationship.get"
    description:
      zh: >
          读取关系分
          
      en: >
          Get a relationship score
          
  - protocol: rpc
    path: "relationship.adjust"
    description:
      zh: >
          调整关系分
          
      en: >
          Adjust a relationship score
          
deps:
  - kind: call
    to: grouppig.social.graph.relationship.decay
    from_api: "rpc:relationship.adjust"
    to_api: "rpc:relationship.decay"
    label: {zh: "时间衰减", en: "Time decay"}
  - kind: call
    to: grouppig.social.graph.manager.tiering
    from_api: "rpc:relationship.adjust"
    to_api: "rpc:graph.tiering"
    label: {zh: "更新分层", en: "Update tiering"}
---
