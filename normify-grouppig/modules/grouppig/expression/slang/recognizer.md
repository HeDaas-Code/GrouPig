---
uid: 3b62003d
id: grouppig.expression.slang.recognizer
parent: grouppig.expression.slang
name: {zh: "黑话识别器", en: "Slang Recognizer"}
description:
  zh: >
      识别消息中的黑话与圈内梗。
      
  en: >
      Recognizes slang and in-group memes in messages.
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-24T10:35:55.970Z"
fingerprint: de2fadf5444f35939f37d1fed2eb7d297926f786d2c257e017358dc29aba07b0
source:
  - path: "src/grouppig/expression/slang/recognizer.py"
apis:
  - protocol: rpc
    path: "slang.recognize"
    description:
      zh: >
          识别群聊黑话
          
      en: >
          Recognize slang in chat
          
deps:
  - kind: call
    to: grouppig.memory.slang-kb.dictionary
    from_api: "rpc:slang.recognize"
    to_api: "rpc:slang.lookup"
    label: {zh: "查黑话", en: "Lookup slang"}
---
