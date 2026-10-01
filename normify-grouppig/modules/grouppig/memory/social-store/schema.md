---
uid: 4ee52ee1
id: grouppig.memory.social-store.schema
parent: grouppig.memory.social-store
name: {zh: "社交网表结构", en: "Social Graph Schema"}
description:
  zh: >
      定义社交边与关系分表结构。
      
  en: >
      Defines the social edge and relationship score table schemas.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.752Z"
fingerprint: 496e9d44c51e3b862f7bcf473cf7e32d916d94a0a85ef539b0a8ff0a1be9d1d6
source:
  - path: "src/grouppig/memory/social_store/schema.py"
apis:
  - protocol: mysql
    path: "social_edges"
    description:
      zh: >
          社交边表
          
      en: >
          Social edges table
          
  - protocol: mysql
    path: "relationship_scores"
    description:
      zh: >
          关系分表
          
      en: >
          Relationship scores table
          
---
