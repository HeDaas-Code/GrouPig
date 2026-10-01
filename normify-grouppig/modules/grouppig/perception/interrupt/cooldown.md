---
uid: 3f6ac86e
id: grouppig.perception.interrupt.cooldown
parent: grouppig.perception.interrupt
name: {zh: "冷却控制器", en: "Cooldown Controller"}
description:
  zh: >
      记录最近发言时间，防止频繁插话。四道闸：冷却、最小间隔、每小时上限、退避。退避只统计最近 decline_window 秒内的「连续克制」，窗口外的拒绝自动过期——所以静默足够久后闸门一定会重开（旧实现无过期，会棘轮式锁死）。
      
  en: >
      Tracks recent speaking times to prevent over-eager interruptions. Four gates: cooldown, minimum gap, hourly cap, and backoff. Backoff counts only restraint within the last decline_window seconds; older declines expire, so the gate always reopens after enough silence (the old implementation never expired and ratcheted shut).
      
revision: "0000000000000000000000000000000000000000"
updated_at: "2026-09-25T16:08:40.611Z"
fingerprint: ee71de34820cae65b1f9f783f19dee0194dcb7b96d24cc271718416dc57118d8
source:
  - path: "src/grouppig/perception/interrupt/cooldown.py"
apis:
  - protocol: rpc
    path: "interrupt.cooldown"
    description:
      zh: >
          查询冷却状态
          
      en: >
          Query cooldown state
          
---
