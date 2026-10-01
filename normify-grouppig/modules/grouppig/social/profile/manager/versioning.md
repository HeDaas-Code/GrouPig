---
uid: 6348e7d1
id: grouppig.social.profile.manager.versioning
parent: grouppig.social.profile.manager
name: {zh: "档案版本控制器", en: "Profile Versioning"}
description:
  zh: >
      更新档案时保留旧版本，支持回滚与审计。
      
  en: >
      Keeps old versions on profile updates, supporting rollback and auditing.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: cddc7aef8987f93146950f55d381b89a538e921f960ef0959fbe5bd9757a2523
source:
  - path: "src/grouppig/social/profile/manager/versioning.py"
apis:
  - protocol: rpc
    path: "profile.update"
    description:
      zh: >
          更新群友档案
          
      en: >
          Update a member profile
          
deps:
  - kind: call
    to: grouppig.social.profile.manager.lookup
    from_api: "rpc:profile.update"
    to_api: "rpc:profile.get"
    label: {zh: "读旧版本", en: "Read old version"}
  - kind: call
    to: grouppig.memory.profile-store.dao
    from_api: "rpc:profile.update"
    to_api: "rpc:profile-store.put"
    label: {zh: "写档案", en: "Write profile store"}
  - kind: event
    to: grouppig.social.profile.manager.events
    from_api: "rpc:profile.update"
    to_api: "kafka:grouppig.profile.updated"
    label: {zh: "发布变更", en: "Publish change"}
---
