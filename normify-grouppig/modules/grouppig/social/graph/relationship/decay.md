---
uid: cb3cd5fd
id: grouppig.social.graph.relationship.decay
parent: grouppig.social.graph.relationship
name: {zh: "关系衰减器", en: "Relationship Decay"}
description:
  zh: >
      关系分随时间缓慢衰减，长期不互动会下降。
      
  en: >
      Decays relationship scores slowly over time; long silence lowers the score.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: c11f254b3d516fe0a665c42bb2d581b506ea5d610c8f09a1ece2cc1595570109
source:
  - path: "src/grouppig/social/graph/relationship/decay.py"
apis:
  - protocol: rpc
    path: "relationship.decay"
    description:
      zh: >
          计算关系衰减
          
      en: >
          Compute relationship decay
          
deps:
  - kind: call
    to: grouppig.memory.social-store.dao
    from_api: "rpc:relationship.decay"
    to_api: "rpc:social-store.get-edges"
    label: {zh: "读现有关系", en: "Read current scores"}
---
