---
uid: 52c100e1
id: grouppig.social.profile.manager.events
parent: grouppig.social.profile.manager
name: {zh: "档案事件发布器", en: "Profile Event Emitter"}
description:
  zh: >
      发布档案更新事件，供社交网与画像模块订阅。
      
  en: >
      Publishes profile update events for the social graph and portrait modules to subscribe.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.974Z"
fingerprint: 7b9bf80952f9aa1c1b08a57f48063b76f8967dc30eb6929aacbe0631acc4224a
source:
  - path: "src/grouppig/social/profile/manager/events.py"
apis:
  - protocol: kafka
    path: "grouppig.profile.updated"
    description:
      zh: >
          档案更新事件
          
      en: >
          Profile updated event
          
---
