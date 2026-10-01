---
uid: 835a2fb9
id: grouppig.social.graph.manager.events
parent: grouppig.social.graph.manager
name: {zh: "社交事件发布器", en: "Social Event Emitter"}
description:
  zh: >
      发布社交网变化事件，供反思与表达层订阅。
      
  en: >
      Publishes social graph change events for reflection and expression layers.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.973Z"
fingerprint: 28383b90cb8d46f38fc695280f226c02bf8169347c69086e353e4644e992f3ae
source:
  - path: "src/grouppig/social/graph/manager/events.py"
apis:
  - protocol: kafka
    path: "grouppig.social.changed"
    description:
      zh: >
          社交网变化事件
          
      en: >
          Social graph changed event
          
---
