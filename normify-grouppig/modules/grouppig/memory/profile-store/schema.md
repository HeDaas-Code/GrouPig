---
uid: b32fa184
id: grouppig.memory.profile-store.schema
parent: grouppig.memory.profile-store
name: {zh: "档案表结构", en: "Profile Schema"}
description:
  zh: >
      定义群友档案与事实表结构。
      
  en: >
      Defines the member profile and profile fact table schemas.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:30:44.751Z"
fingerprint: a6e9348635e063f7a063c935531d5ad0436ac90d51b5f9ac9430968046125f5c
source:
  - path: "src/grouppig/memory/profile_store/schema.py"
apis:
  - protocol: mysql
    path: "member_profiles"
    description:
      zh: >
          群友档案表
          
      en: >
          Member profiles table
          
  - protocol: mysql
    path: "profile_facts"
    description:
      zh: >
          档案事实表
          
      en: >
          Profile facts table
          
---
