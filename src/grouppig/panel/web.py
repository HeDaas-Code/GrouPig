"""GrouPig Web command console.

A dependency-free, local-first terminal dashboard.  The API deliberately exposes
one shared dashboard view-model to keep the Web and curses consoles in lockstep.
Mutating endpoints are allow-listed and audited; there is no arbitrary SQL or
Python execution surface.
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import sys
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from grouppig.panel.management import (
    apply_patch,
    audit_tail,
    clear_events,
    config_view,
    control_pump,
    reload_config,
    table_rows,
    validate_patch,
)
from grouppig.panel.plugins import discover_plugins, plugin_payload
from grouppig.panel.snapshot import EVENTS, SnapshotOptions, build_snapshot
from grouppig.panel.telemetry import get_store
from grouppig.panel.viewmodel import build_dashboard_snapshot

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})
TOKEN_HEADER = "X-Panel-Token"


class PanelBindError(RuntimeError):
    """面板拒绝绑定到未显式允许的非本机地址。"""


def is_loopback_host(host: str) -> bool:
    name = str(host or "").strip().lower()
    if name in ("localhost", "::1", "[::1]"):
        return True
    try:
        return ipaddress.ip_address(name.strip("[]")).is_loopback
    except ValueError:
        return False


def host_name_of(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("["):
        end = text.find("]")
        return text[: end + 1] if end != -1 else text
    return text.rsplit(":", 1)[0] if ":" in text else text


@dataclass(frozen=True)
class PanelSettings:
    token: str | None = None
    allow_remote: bool = False
    allowed_hosts: frozenset[str] | None = None

    @classmethod
    def for_bind(cls, host: str, *, token: str | None = None, allow_remote: bool = False) -> PanelSettings:
        if not is_loopback_host(host) and not allow_remote:
            raise PanelBindError(f"拒绝绑定非本机地址 {host!r}：请显式加 --panel-allow-remote，并自行提供反向代理鉴权")
        wildcard = str(host or "").strip() in ("", "0.0.0.0", "::", "[::]")
        allowed = None if wildcard else LOOPBACK_HOSTS | {host_name_of(host).lower()}
        return cls(token=str(token) if token else None, allow_remote=bool(allow_remote), allowed_hosts=allowed)

    @classmethod
    def from_config(
        cls, config: Any = None, *, host: str = "127.0.0.1", token: str | None = None, allow_remote: bool = False
    ) -> PanelSettings:
        if config is not None:
            if not token:
                configured = config.get("panel.token", None)
                token = str(configured) if configured else None
            if not allow_remote:
                allow_remote = bool(config.get("panel.allow_remote", False))
        return cls.for_bind(host, token=token, allow_remote=allow_remote)

    def check_host(self, value: str) -> bool:
        if self.allowed_hosts is None:
            return True
        name = host_name_of(value).lower()
        return not name or name in self.allowed_hosts

    def check_token(self, supplied: str | None) -> bool:
        return self.token is None or hmac.compare_digest(str(supplied or ""), self.token)


PAGE = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GrouPig // 群猪控制台</title>
<style>
:root{--bg:#061018;--panel:#0a1a23;--panel2:#0d222c;--line:#1c4552;--text:#c4d6d5;--dim:#719096;--green:#8ed6aa;--amber:#e0b86a;--red:#e47d7d;--cyan:#7fc6d6;--mono:"IBM Plex Mono","Cascadia Mono","Liberation Mono",monospace}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:12px/1.5 var(--mono);letter-spacing:.01em}body:before{content:"";position:fixed;inset:0;pointer-events:none;background:repeating-linear-gradient(0deg,rgba(255,255,255,.018) 0 1px,transparent 1px 4px);opacity:.35;z-index:20}
header{height:58px;border-bottom:1px solid var(--line);background:#07151e;display:flex;align-items:center;padding:0 18px;gap:16px;position:sticky;top:0;z-index:5}header h1{font-size:14px;color:var(--green);letter-spacing:.14em;margin:0}header .meta{color:var(--dim)}#connection{margin-left:auto;color:var(--green)}
.layout{display:grid;grid-template-columns:190px minmax(0,1fr);min-height:calc(100vh - 58px)}nav{border-right:1px solid var(--line);padding:12px 10px;background:#07151e}nav button{width:100%;text-align:left;background:transparent;border:1px solid transparent;color:var(--dim);font:inherit;padding:8px 9px;cursor:pointer}nav button:hover,nav button.active{color:var(--green);border-color:var(--line);background:#0a2028}nav b{display:block;color:var(--dim);font-size:10px;margin:9px 9px 4px;letter-spacing:.1em}.content{padding:16px;min-width:0}.toolbar{display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--line);padding-bottom:11px;margin-bottom:14px}.toolbar h2{margin:0;color:var(--cyan);font-size:13px}.toolbar .spacer{flex:1}.muted{color:var(--dim)}button,.btn{background:#102a34;border:1px solid var(--line);color:var(--cyan);padding:6px 10px;font:inherit;cursor:pointer}button:hover{border-color:var(--green);color:var(--green)}select,input,textarea{background:#07151e;color:var(--text);border:1px solid var(--line);font:inherit;padding:5px}.view{display:none}.view.active{display:block}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}.card{grid-column:span 3;background:var(--panel);border:1px solid var(--line);padding:12px;min-width:0}.card.wide{grid-column:span 6}.card.full{grid-column:1/-1}.card h3{margin:0 0 9px;color:var(--cyan);font-size:11px;font-weight:normal;letter-spacing:.08em}.metric{font-size:25px;color:var(--green);line-height:1.1}.metric small{font-size:11px;color:var(--dim)}.kv{display:grid;grid-template-columns:minmax(100px,42%) 1fr;gap:4px 10px}.kv dt{color:var(--dim)}.kv dd{margin:0;overflow-wrap:anywhere}.table-scroll{max-width:100%;overflow:auto}.tree-table{font-size:11px;min-width:420px}.tree-table th{position:sticky;top:0;background:var(--panel);z-index:1}.tree-table td{overflow-wrap:anywhere}.tree-table .tree-field{color:var(--text);white-space:nowrap}.tree-table .tree-parent td{color:var(--cyan)}.tree-table .tree-value{color:var(--text)}.tree-table .tree-type{color:var(--dim);white-space:nowrap}.tree-table .tree-empty{color:var(--dim);font-style:italic}.status{display:inline-block;padding:1px 6px;border:1px solid currentColor;font-size:10px}.ok{color:var(--green)}.warn{color:var(--amber)}.critical,.error{color:var(--red)}.info{color:var(--cyan)}table{width:100%;border-collapse:collapse}th,td{text-align:left;border-bottom:1px solid #163642;padding:7px 6px;vertical-align:top}th{color:var(--dim);font-weight:normal}.scroll{max-height:420px;overflow:auto}.timeline{border-left:1px solid var(--line);margin:4px 0 0 8px;padding-left:14px}.event{position:relative;border-bottom:1px solid #163642;padding:7px 0}.event:before{content:"";position:absolute;left:-19px;top:12px;width:7px;height:7px;background:var(--green);border-radius:50%}.event .topic{color:var(--cyan)}.event .time{color:var(--dim);float:right}.mono{white-space:pre-wrap;overflow-wrap:anywhere}.bar{height:8px;background:#102d37;margin:5px 0}.bar i{display:block;height:100%;background:var(--green)}.legend{display:flex;gap:16px;color:var(--dim);font-size:10px;margin-top:7px}.chart{width:100%;height:190px;background:#081820;border:1px solid #153744}.chart text{font:10px var(--mono);fill:var(--dim)}.chart polyline{fill:none;stroke:var(--green);stroke-width:2}.chart .secondary{stroke:var(--cyan)}.empty{padding:28px;text-align:center;color:var(--dim);border:1px dashed var(--line)}.alert{border-left:3px solid var(--amber);padding:8px 10px;background:#1a1d18;margin-bottom:7px}.alert.error{border-color:var(--red);background:#211719}.tabs-note{color:var(--dim);margin-bottom:10px}.plugin-badge{color:var(--dim);font-size:10px;margin-left:8px}@media(max-width:900px){.layout{grid-template-columns:1fr}nav{display:flex;overflow:auto;gap:4px;border-right:0;border-bottom:1px solid var(--line)}nav b{display:none}nav button{min-width:max-content}.card{grid-column:span 6}}@media(max-width:580px){.card,.card.wide{grid-column:1/-1}}
</style></head>
<body><header><h1>GROUPPIG // 群猪控制台</h1><span class="meta">原子化自主对话框架 · 观测控制台</span><span id="connection">● 连接中</span></header>
<div class="layout"><nav id="nav"><b>核心面板</b></nav><main class="content"><div class="toolbar"><h2 id="viewTitle">总览</h2><span class="muted" id="generated">等待数据</span><span class="spacer"></span><label class="muted">历史窗口 <select id="window"><option value="3600">1 小时</option><option value="21600">6 小时</option><option value="86400" selected>24 小时</option><option value="604800">7 天</option></select></label><button id="refresh">刷新</button></div><div id="views"></div></main></div>
<script>
// 兼容旧客户端：完整只读快照仍由 /api/snapshot 提供。
const TOKEN=(new URLSearchParams(location.search)).get('token')||''; const HEAD=TOKEN?{'X-Panel-Token':TOKEN}:{}; const state={dash:null,plugin:'overview',pluginData:null,window:86400};
const LABELS={overview:'总览',runtime:'运行时',domains:'业务域',pumps:'后台泵',activation:'激活网络',energy:'精力系统',memory:'记忆系统',events:'事件时间线',traces:'决策链',tools:'工具链',analysis:'历史分析',config:'配置与审计',infra:'基础设施',perception:'感知',session:'会话',social:'社交画像',reflection:'反思',expression:'表达',gateway:'网关',status:'状态',started:'已启动',uptime:'运行时长',event_count:'实时事件',history_events:'历史事件',history_spans:'历史步骤',model_calls:'模型调用',tool_calls:'工具调用',healthy_domains:'健康业务域',running_pumps:'运行中后台泵',domain_count:'业务域总数',table_rows:'数据行数',registered:'已注册组件',by_prefix:'按前缀统计',names:'名称列表',description:'说明',source:'来源',version:'版本',generated_at:'生成时间',available:'可用',enabled:'已启用',domain:'业务域',pump:'后台泵',interval:'周期',ticks:'运行次数',errors:'错误次数',last_error:'最近错误',drained:'已排空',cleaned:'已清理',empty:'空转次数',triggered:'触发次数',coalesced:'合并次数',driven:'驱动次数',sent:'发送次数',published:'发布次数',no_flow:'无流转次数',steps:'步骤数',groups:'群组数',members:'成员数',facts:'事实数',profiles:'画像数',adjusted:'已调整',tiered:'已分层',pending_groups:'待处理群组',swept:'已扫描',archived:'已归档',seen_groups:'已见群组',runs:'运行次数',pruned:'已清理',slang_retired:'已退役俚语',relationships_decayed:'关系衰减数',keep_seconds:'保留秒数',decay_limit:'衰减上限',considered:'评估数',scored:'评分数',spoke:'发言数',skipped:'跳过数',idle_seconds:'空闲秒数',max_idle_seconds:'最大空闲秒数',min_messages:'最少消息数',limit:'上限',duration_ms:'耗时',latency_ms:'延迟',input_tokens:'输入 Token',output_tokens:'输出 Token',prompt_tokens:'输入 Token',completion_tokens:'输出 Token',total_tokens:'总 Token',confidence:'置信度',decision:'决策摘要',model:'模型',tool_name:'工具名称',stage:'阶段',component:'组件',summary:'摘要',group:'群组',cluster:'聚类',coefficient:'相关系数',left:'指标 A',right:'指标 B',sample_count:'样本数',calls:'调用次数',weights:'激活权重',interest_tags:'兴趣标签',bot_names:'机器人名称',trace_id:'追踪 ID',span_id:'步骤 ID',parent_id:'父步骤 ID',topic:'主题',ts:'时间戳',time:'时间',type:'类型',level:'级别',message:'消息',code:'代码',action:'操作',actor:'操作者',ok:'成功',tables:'数据表',metrics:'指标',events_count:'事件数',spans:'步骤数',tools_count:'工具数',stable:'稳定记忆',fragments:'记忆碎片',promoted:'已晋升',recovery_rate:'恢复速率',reserve:'保留阈值',global_energy:'全局精力',item:'项目',items:'项目',value:'值',kind:'类型',content:'内容',object:'对象',array:'列表',boolean:'布尔值',string:'文本',number:'数字',true:'是',false:'否',rpc:'远程调用',api:'接口',runtime:'运行时',overview:'总览',history:'历史',recent:'最近',current:'当前',count:'数量',total:'总计',error:'错误',warning:'警告',info:'信息',health:'健康状态',config:'配置',settings:'设置',provider:'服务提供方',fallback:'回退',retry:'重试',timeout:'超时',connected:'已连接',disconnected:'未连接',installed:'已安装',missing:'缺失',unknown:'未知','perception.drain':'感知排水泵','expression.flow':'表达流转泵','social.profile':'社交画像泵','session.sweeper':'会话清理泵','maintenance.retention':'维护保留泵','perception.idle_speak':'感知闲时发言泵'};
function label(k){return LABELS[k]||String(k).replaceAll('_',' ')} function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))} function num(v,d=0){return Number.isFinite(Number(v))?Number(v).toLocaleString('zh-CN',{maximumFractionDigits:d}):'—'} function time(v){if(!v)return '—';return new Date(Number(v)*1000).toLocaleString('zh-CN',{hour12:false})} function status(v){let s=String(v||'').toLowerCase();let c=['ok','success','running','online','healthy'].includes(s)?'ok':(['warn','degraded','pending'].includes(s)?'warn':'critical');return `<span class="status ${c}">${esc({ok:'正常',success:'成功',running:'运行中',online:'在线',healthy:'健康',warn:'注意',degraded:'降级',critical:'严重',stopped:'已停止',failed:'失败',error:'错误'}[s]||v||'未知')}</span>`}
async function api(path){let r=await fetch(path,{headers:HEAD,cache:'no-store'}); if(!r.ok)throw new Error(`${r.status} ${await r.text()}`); return r.json()}
function card(title,body,cls=''){return `<section class="card ${cls}"><h3>${esc(title)}</h3>${body}</section>`}
function isObject(v){return v!==null&&typeof v==='object'}
function valueType(v){if(v===null||v===undefined)return '空值';if(Array.isArray(v))return '列表';if(typeof v==='object')return '对象';if(typeof v==='boolean')return '布尔值';if(typeof v==='number')return '数字';return '文本'}
function displayValue(v){if(v===null||v===undefined)return '—';if(typeof v==='boolean')return v?'是':'否';if(typeof v==='number')return num(v);return String(v)}
function childCount(v){return isObject(v)?Object.keys(v).length:0}
function treeRows(value,depth=0){
  if(!isObject(value))return [{depth,key:'值',value:displayValue(value),type:valueType(value),parent:false}];
  const entries=Array.isArray(value)?value.map((v,i)=>[`${label('item')} ${i+1}`,v]):Object.entries(value);
  if(!entries.length)return [{depth,key:'内容',value:'空',type:valueType(value),parent:false,empty:true}];
  const rows=[];
  entries.slice(0,200).forEach(([rawKey,item])=>{
    const nested=isObject(item);
    const key=Array.isArray(value)?rawKey:label(rawKey);
    rows.push({depth,key,value:nested?`${valueType(item)}（${childCount(item)} 项）`:displayValue(item),type:valueType(item),parent:nested});
    if(nested&&depth<24)rows.push(...treeRows(item,depth+1));
  });
  if(entries.length>200)rows.push({depth,key:'其余内容',value:`已折叠 ${entries.length-200} 项`,type:'提示',parent:false});
  return rows;
}
function structuredTable(value){
  const rows=treeRows(value);
  return `<div class="table-scroll"><table class="tree-table"><thead><tr><th>字段</th><th>值</th><th>数据类型</th></tr></thead><tbody>${rows.map(row=>`<tr class="${row.parent?'tree-parent':''}"><td class="tree-field" style="padding-left:${6+row.depth*18}px">${esc(row.key)}</td><td class="tree-value ${row.empty?'tree-empty':''}">${esc(row.value)}</td><td class="tree-type">${esc(row.type)}</td></tr>`).join('')}</tbody></table></div>`;
}
function kv(obj,keys){const selected={};(keys||Object.keys(obj||{})).filter(k=>obj&&obj[k]!==undefined).forEach(k=>selected[k]=obj[k]);return structuredTable(selected)}
function renderValue(v){return structuredTable(v)}
function metric(title,value,sub=''){let n=Number(value);let shown=(typeof value==='number'||(typeof value==='string'&&value.trim()!==''&&Number.isFinite(n)))?num(n):String(value??'—');return card(title,`<div class="metric">${esc(shown)}</div><div class="muted">${esc(sub)}</div>`)}
function lineChart(metrics,keys=['events','spans']){if(!metrics?.length)return '<div class="empty">历史样本还不足，事件产生后这里会出现趋势图。</div>';let w=700,h=180,p=24,max=Math.max(1,...metrics.flatMap(m=>keys.map(k=>Number(m[k]||0))));let points=k=>metrics.map((m,i)=>`${p+i*(w-2*p)/Math.max(1,metrics.length-1)},${h-p-(Number(m[k]||0)/max)*(h-2*p)}`).join(' ');return `<svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" stroke="#1c4552"/>${keys.map((k,i)=>`<polyline class="${i?'secondary':''}" points="${points(k)}"/>`).join('')}<text x="${p}" y="14">峰值 ${num(max)}</text><text x="${w-p-80}" y="${h-5}">${esc(time(metrics[metrics.length-1].ts))}</text></svg><div class="legend">${keys.map((k,i)=>`<span style="color:${i?'var(--cyan)':'var(--green)'}">━ ${label(k)}</span>`).join('')}</div>`}
function bars(obj){let entries=Object.entries(obj||{});if(!entries.length)return '<div class="empty">暂无统计样本</div>';let max=Math.max(1,...entries.map(([,v])=>Number(v)||0));return entries.slice(0,12).map(([k,v])=>`<div><span>${esc(label(k))} <span class="muted">${num(v)}</span></span><div class="bar"><i style="width:${Math.min(100,(Number(v)||0)/max*100)}%"></i></div></div>`).join('')}
function eventList(events){if(!events?.length)return '<div class="empty">暂无事件。真实群聊事件进入总线后会自动记录。</div>';return `<div class="timeline">${events.slice(0,100).map(e=>`<div class="event"><span class="time">${time(e.ts)}</span><div class="topic">${esc(e.topic||'事件')}</div><div class="muted">${esc(e.summary||'')}</div></div>`).join('')}</div>`}
function spanTable(rows){if(!rows?.length)return '<div class="empty">暂无决策/工具步骤。</div>';return `<div class="scroll"><table><thead><tr><th>时间</th><th>阶段</th><th>组件</th><th>状态</th><th>耗时</th><th>摘要</th></tr></thead><tbody>${rows.slice(0,200).map(s=>`<tr><td>${time(s.ts)}</td><td>${esc(label(s.stage))}</td><td>${esc(s.component||s.tool_name||'—')}</td><td>${status(s.status)}</td><td>${num(s.duration_ms,1)} ms</td><td>${esc(s.summary||s.decision||'')}</td></tr>`).join('')}</tbody></table></div>`}
function renderOverview(d){let o=d.overview||{};return `<div class="grid">${metric('系统状态',String(o.status||'unknown')==='ok'?'在线':'注意',`运行时长 ${num(o.uptime,1)} 秒`)}${metric('实时事件',o.event_count,'当前缓冲尾部')}${metric('历史事件',o.history_events,'持久化观测')}${metric('模型调用',o.model_calls,'最近 24 小时')}${metric('工具调用',o.tool_calls,'结构化工具链')}${metric('健康业务域',`${num(o.healthy_domains)}/${num(o.domain_count)}`,'域健康度')}${metric('运行中后台泵',`${num(o.running_pumps)}`,'周期任务')}${metric('历史步骤',o.history_spans,'决策链元数据')}${card('事件与步骤趋势',lineChart((d.history||{}).metrics||[]), 'wide')}${card('告警',((d.alerts||[]).map(a=>`<div class="alert ${a.level==='error'?'error':''}">${esc(a.message||a.code)}</div>`).join('')||'<div class="empty">当前没有告警</div>'),'wide')}${card('业务域状态',`<table><tbody>${Object.entries(d.domains||{}).map(([k,v])=>`<tr><td>${label(k)}</td><td>${status(v.status)}</td><td class="muted">${v.installed?'已安装':'未安装'}</td></tr>`).join('')}</tbody></table>`,'wide')}${card('后台泵状态',`<table><tbody>${(d.pumps||[]).map(v=>`<tr><td>${esc(v.name||'泵')}</td><td>${status(v.status)}</td><td class="muted">${esc(v.last_error||'')}</td></tr>`).join('')}</tbody></table>`,'wide')}</div>`}
function renderActivation(d){let a=d.activation||{},e=a.energy||{};return `<div class="grid">${metric('全局精力',e.global_energy??'—',`保留阈值 ${e.reserve??'—'}`)}${metric('原子记忆碎片',a.memory?.count??0,'持续积攒的事件因子')}${metric('长期记忆',a.memory?.stable??0,'跨事件/跨群晋升')}${metric('激活事件',a.events?.length??0,'多钩子激活回路')}${card('激活网络权重',bars(a.weights),'wide')}${card('精力系统',kv(e,Object.keys(e)),'wide')}${card('兴趣标签',`<div>${(a.interest_tags||[]).map(x=>`<span class="status info">${esc(x)}</span> `).join('')||'—'}</div>`,'wide')}${card('事件线（最近）',eventList(a.events||[]),'full')}</div>`}
function renderMemory(d){let m=d.memory||{};let rows=Object.entries((m.tables||{}).tables||{});return `<div class="grid">${metric('记忆碎片',m.activation?.count??0,'原子化事件因子')}${metric('稳定记忆',m.activation?.stable??0,'长期晋升')}${card('存储表统计',`<table><thead><tr><th>表</th><th>行数</th></tr></thead><tbody>${rows.map(([k,v])=>`<tr><td>${esc(label(k))}</td><td>${typeof v==='object'?esc(v.error):num(v)}</td></tr>`).join('')}</tbody></table>`,'wide')}${card('最近原子碎片',`<div class="scroll">${(m.activation?.fragments||[]).map(x=>`<div class="event">${renderValue(x)}</div>`).join('')||'<div class="empty">暂无碎片</div>'}</div>`,'wide')}</div>`}
function renderTraces(d){return `<div class="grid">${metric('决策步骤',(d.traces?.spans||[]).length,'不显示模型隐性思维链')}${metric('平均耗时',((d.history||{}).summary||{}).avg_latency_ms||0,'毫秒')}${metric('输入 Token',((d.runtime||{}).observability||{}).usage?.prompt_tokens||0,'最近 24 小时')}${metric('输出 Token',((d.runtime||{}).observability||{}).usage?.completion_tokens||0,'最近 24 小时')}${card('结构化决策链',`<div class="tabs-note">仅展示 trace/span、阶段、状态、耗时、模型和决策摘要。</div>${spanTable(d.traces?.spans||[])}`,'full')}</div>`}
function renderTools(d){let t=d.tools||{};return `<div class="grid">${metric('工具调用',t.count||0,'已记录的结构化调用')}${metric('工具失败',t.errors||0,'失败需优先排查')}${card('工具链调用',spanTable(t.spans||[]),'full')}</div>`}
function renderAnalysis(d){let a=d.analysis||{},c=a.clusters||{},r=a.correlations||{};return `<div class="grid">${card('趋势图',lineChart((d.history||{}).metrics||[],['events','spans','tools']),'wide')}${card('分析说明',`<div class="tabs-note">${(a.notes||[]).map(x=>`<p>${esc(x)}</p>`).join('')}</div><div class="kv"><dt>聚类样本</dt><dd>${num(c.sample_count)}</dd><dt>关联样本</dt><dd>${num(r.sample_count)}</dd><dt>关联系数</dt><dd>${r.items?.[0]?num(r.items[0].coefficient,4):'样本不足'}</dd></div>`,'wide')}${card('群组聚类（确定性分层）',c.status==='insufficient_data'?'<div class="empty">样本不足，至少需要多个群组事件。</div>':`<table><thead><tr><th>群组</th><th>事件</th><th>触发</th><th>错误</th><th>类别</th></tr></thead><tbody>${(c.items||[]).map(x=>`<tr><td>${esc(x.group)}</td><td>${num(x.events)}</td><td>${num(x.triggers)}</td><td>${num(x.errors)}</td><td>${esc(x.cluster)}</td></tr>`).join('')}</tbody></table>`,'full')}${card('关联性分析',r.status==='insufficient_data'?'<div class="empty">历史时间桶少于 3 个，暂不输出关联结论。</div>':`<table><thead><tr><th>指标 A</th><th>指标 B</th><th>Pearson 相关系数</th></tr></thead><tbody>${(r.items||[]).map(x=>`<tr><td>${esc(x.left)}</td><td>${esc(x.right)}</td><td>${num(x.coefficient,4)}</td></tr>`).join('')}</tbody></table>`,'full')}</div>`}
function renderEvents(d){return `<div class="grid">${metric('实时尾部',(d.events||[]).length,'最近事件')}${metric('历史总量',(d.history||{}).summary?.events||0,'当前窗口')}${card('事件时间线',eventList(d.events||[]),'full')}</div>`}
function renderConfig(d){return `<div class="grid">${card('配置摘要',`<div class="tabs-note">固定键已映射为中文；敏感值只显示脱敏标记。</div>${kv(d.config||{},Object.keys(d.config||{}).slice(0,40))}`,'wide')}${card('操作审计',`<div class="scroll">${(d.audit||[]).map(x=>`<div class="event"><span class="time">${time(x.ts)}</span><div class="topic">${esc(x.action||'操作')} ${status(x.ok?'ok':'error')}</div><div class="muted">${esc(x.actor||'panel')} · ${esc(x.message||'')}</div></div>`).join('')||'<div class="empty">暂无审计记录</div>'}</div>`,'wide')}</div>`}
function genericRows(value){return structuredTable(value)}
function renderGeneric(plugin){let payload=state.pluginData?.data||{};return `<div class="grid">${card('插件数据',genericRows(payload),'full')}<div class="tabs-note">该标签由原子化模块注册，数据通过统一插件接口提供。</div></div>`}
function renderPlugin(key,d){switch(key){case'overview':return renderOverview(d);case'activation':return renderActivation(d);case'memory':return renderMemory(d);case'events':return renderEvents(d);case'traces':return renderTraces(d);case'tools':return renderTools(d);case'analysis':return renderAnalysis(d);case'config':return renderConfig(d);default:return renderGeneric(key)}}
function mountNav(d){let nav=document.getElementById('nav');nav.innerHTML='<b>核心面板</b>'+(d.plugins||[]).map(p=>`<button data-plugin="${esc(p.key)}" class="${p.key===state.plugin?'active':''}">${esc(p.icon||'▣')} ${esc(p.title)}<span class="plugin-badge">${esc(p.source==='builtin'?'内置':(p.source||'扩展'))}</span></button>`).join('');nav.querySelectorAll('button').forEach(b=>b.onclick=()=>{state.plugin=b.dataset.plugin;state.pluginData=null;render()})}
async function render(){let d=state.dash;if(!d)return;mountNav(d);let p=(d.plugins||[]).find(x=>x.key===state.plugin);if(p&&p.source!=='builtin'&&!state.pluginData){try{state.pluginData=await api('/api/plugins/'+encodeURIComponent(state.plugin))}catch(e){state.pluginData={data:{error:e.message}}}}document.getElementById('viewTitle').textContent=p?.title||label(state.plugin);document.getElementById('generated').textContent=`生成于 ${time(d.generated_at)} · 自动刷新 3 秒`;document.getElementById('views').innerHTML=`<div class="tabs-note">${esc(p?.description||'')}</div>${renderPlugin(state.plugin,d)}`}
async function refresh(){try{state.dash=await api('/api/dashboard?window='+encodeURIComponent(state.window));document.getElementById('connection').textContent='● 在线';document.getElementById('connection').className='ok';render()}catch(e){document.getElementById('connection').textContent='● 连接失败';document.getElementById('connection').className='critical';document.getElementById('views').innerHTML=`<div class="empty">无法读取面板：${esc(e.message)}</div>`}}
document.getElementById('refresh').onclick=refresh;document.getElementById('window').onchange=e=>{state.window=e.target.value;refresh()};refresh();setInterval(refresh,3000);
</script></body></html>"""


class PanelApp:
    def __init__(
        self,
        app: Any = None,
        options: SnapshotOptions | None = None,
        settings: PanelSettings | None = None,
        *,
        loop: Any = None,
    ) -> None:
        self.app = app
        self.options = options or SnapshotOptions()
        self.settings = settings or PanelSettings()
        self.loop = loop
        self.store = get_store()

    def snapshot(self) -> dict[str, Any]:
        return build_snapshot(self.app, self.options)

    def dashboard(self, *, window_seconds: int = 3600) -> dict[str, Any]:
        return build_dashboard_snapshot(self.app, self.options, window_seconds=window_seconds)

    def health(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        return {
            "started": bool(snapshot.get("app", {}).get("started")),
            "contract_missing": len(snapshot.get("contract", {}).get("missing", []) or []),
            "registry_total": snapshot.get("registry", {}).get("total", 0),
        }


def make_handler(panel: PanelApp) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "GrouPigPanel/2"

        def _auth(self, query: dict[str, list[str]]) -> bool:
            return panel.settings.check_host(self.headers.get("Host", "")) and panel.settings.check_token(
                self._supplied_token(query)
            )

        def do_GET(self) -> None:  # noqa: N802
            route = urlparse(self.path)
            query = parse_qs(route.query)
            if not panel.settings.check_host(self.headers.get("Host", "")):
                return self._deny(403, "Host 头不在白名单内")
            if not panel.settings.check_token(self._supplied_token(query)):
                return self._deny(401, "缺少或错误的共享密钥")
            if route.path in ("/", "/index.html"):
                return self._send(200, PAGE, "text/html; charset=utf-8")
            if route.path in ("/api/dashboard", "/api/snapshot"):
                return self._send_json(
                    panel.dashboard(window_seconds=self._window(query)) if route.path.endswith("dashboard") else panel.snapshot()
                )
            if route.path == "/api/health":
                return self._send_json(panel.health())
            if route.path == "/api/events":
                return self._send_json({"events": EVENTS.tail(self._limit(query))})
            if route.path == "/api/history/events":
                return self._send_json(
                    {
                        "events": panel.store.events(
                            since=self._since(query),
                            until=self._until(query),
                            topic=(query.get("topic") or [None])[0],
                            limit=self._limit(query),
                        )
                    }
                )
            if route.path == "/api/history/spans":
                return self._send_json(
                    {
                        "spans": panel.store.spans(
                            since=self._since(query),
                            until=self._until(query),
                            trace_id=(query.get("trace_id") or [None])[0],
                            stage=(query.get("stage") or [None])[0],
                            limit=self._limit(query),
                        )
                    }
                )
            if route.path == "/api/history/metrics":
                return self._send_json(
                    {
                        "metrics": panel.store.metrics(
                            since=self._since(query), until=self._until(query), bucket=self._bucket(query)
                        )
                    }
                )
            if route.path in ("/api/analytics/summary", "/api/analytics/clusters", "/api/analytics/correlations"):
                analysis = panel.store.analytics(since=self._since(query))
                if route.path.endswith("summary"):
                    return self._send_json({"summary": panel.store.summary(since=self._since(query))})
                key = "clusters" if route.path.endswith("clusters") else "correlations"
                return self._send_json({key: analysis[key], "since": analysis["since"], "until": analysis["until"]})
            if route.path.startswith("/api/traces/"):
                trace_id = route.path.removeprefix("/api/traces/").removesuffix("/")
                return self._send_json(
                    {"trace_id": trace_id, "spans": panel.store.spans(trace_id=trace_id, limit=self._limit(query))}
                )
            if route.path == "/api/plugins":
                return self._send_json({"plugins": discover_plugins(panel.app).manifests()})
            if route.path.startswith("/api/plugins/"):
                key = route.path.removeprefix("/api/plugins/").removesuffix("/")
                plugin = discover_plugins(panel.app).get(key)
                if plugin is None:
                    return self._send_json({"error": "plugin not found", "key": key}, status=404)
                return self._send_json(
                    plugin_payload(
                        plugin,
                        panel.app,
                        panel.dashboard(),
                        panel.store,
                        since=self._since(query),
                        until=self._until(query),
                    )
                )
            if route.path == "/api/data/tables":
                return self._send_json({"tables": panel.dashboard().get("tables", {})})
            if route.path.startswith("/api/data/table/"):
                name = route.path.removeprefix("/api/data/table/").removesuffix("/")
                try:
                    limit = int((query.get("limit") or ["50"])[0])
                    offset = int((query.get("offset") or ["0"])[0])
                except ValueError:
                    limit, offset = 50, 0
                return self._send_json(
                    table_rows(panel.app, name, limit=limit, offset=offset, loop=panel.loop), status=200
                )
            if route.path == "/api/config":
                return self._send_json(config_view(panel.app))
            if route.path == "/api/audit":
                return self._send_json({"audit": audit_tail(self._limit(query))})
            if route.path == "/api/runtime":
                return self._send_json(panel.dashboard().get("runtime", {}))
            return self._send_json({"error": "not found", "path": route.path}, status=404)

        def do_POST(self) -> None:  # noqa: N802
            route = urlparse(self.path)
            query = parse_qs(route.query)
            if route.path == "/api/snapshot":
                return self._send_json({"error": "snapshot is read-only; use GET"}, status=405)
            if not panel.settings.check_host(self.headers.get("Host", "")):
                return self._deny(403, "Host 头不在白名单内")
            if not panel.settings.check_token(self._supplied_token(query)):
                return self._deny(401, "缺少或错误的共享密钥")
            try:
                payload = self._json_body()
                if route.path == "/api/events/clear":
                    result = clear_events()
                elif route.path == "/api/config/validate":
                    result = validate_patch(panel.app, payload.get("patch", payload))
                elif route.path == "/api/config/apply":
                    result = apply_patch(
                        panel.app, payload.get("patch", payload), persist=bool(payload.get("persist", False))
                    )
                elif route.path == "/api/config/reload":
                    result = reload_config(panel.app)
                elif route.path == "/api/runtime/reload":
                    result = reload_config(panel.app)
                elif route.path.startswith("/api/pumps/"):
                    name = route.path.removeprefix("/api/pumps/").removesuffix("/")
                    result = control_pump(panel.app, name, str(payload.get("action", "")), loop=panel.loop)
                else:
                    return self._send_json({"error": "not found", "path": route.path}, status=404)
                return self._send_json(result, status=200 if result.get("ok", True) else 400)
            except Exception as exc:  # noqa: BLE001
                return self._send_json({"error": f"{type(exc).__name__}: {exc}"}, status=400)

        def _json_body(self) -> dict[str, Any]:
            length = min(int(self.headers.get("Content-Length", "0") or 0), 1_000_000)
            data = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(data, dict):
                raise ValueError("JSON body must be an object")
            return data

        def _limit(self, query: dict[str, list[str]]) -> int:
            try:
                return max(0, min(2000, int((query.get("limit") or ["50"])[0])))
            except ValueError:
                return 50

        def _since(self, query: dict[str, list[str]]) -> float | None:
            try:
                value = (query.get("since") or [None])[0]
                if value is None:
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        def _until(self, query: dict[str, list[str]]) -> float | None:
            try:
                value = (query.get("until") or [None])[0]
                if value is None:
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        def _window(self, query: dict[str, list[str]]) -> int:
            try:
                return max(300, min(7 * 86400, int((query.get("window") or ["3600"])[0])))
            except (TypeError, ValueError):
                return 3600

        def _bucket(self, query: dict[str, list[str]]) -> int:
            try:
                return max(10, min(86400, int((query.get("bucket") or ["300"])[0])))
            except (TypeError, ValueError):
                return 300

        def _supplied_token(self, query: dict[str, list[str]]) -> str | None:
            return self.headers.get(TOKEN_HEADER) or (query.get("token") or [None])[0]

        def _deny(self, status: int, reason: str) -> None:
            self._send_json({"error": reason, "status": status}, status=status)

        def _send_json(self, payload: Any, *, status: int = 200) -> None:
            self._send(
                status,
                json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
                "application/json; charset=utf-8",
            )

        def _send(self, status: int, body: Any, content_type: str) -> None:
            data = body.encode("utf-8") if isinstance(body, str) else body
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    return Handler


def serve(
    app: Any = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8848,
    options: SnapshotOptions | None = None,
    ready: threading.Event | None = None,
    token: str | None = None,
    allow_remote: bool = False,
    settings: PanelSettings | None = None,
    panel: PanelApp | None = None,
) -> ThreadingHTTPServer:
    if panel is None:
        resolved = settings or PanelSettings.for_bind(host, token=token, allow_remote=allow_remote)
        panel = PanelApp(app, options, resolved)
    else:
        resolved = panel.settings
    if not is_loopback_host(host) and not resolved.allow_remote:
        raise PanelBindError(f"拒绝绑定非本机地址 {host!r}：请显式加 --panel-allow-remote")
    if not is_loopback_host(host):
        print(f"⚠️ GrouPig 面板监听非本机地址 {host}:{port}，请自行提供反向代理/防火墙", file=sys.stderr)
    httpd = ThreadingHTTPServer((host, port), make_handler(panel))
    if ready is not None:
        ready.set()
    return httpd


def serve_forever(
    app: Any = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8848,
    options: SnapshotOptions | None = None,
    token: str | None = None,
    allow_remote: bool = False,
) -> None:
    httpd = serve(app, host=host, port=port, options=options, token=token, allow_remote=allow_remote)
    address, actual_port = httpd.server_address[0], httpd.server_address[1]
    print(f"GrouPig 面板已启动：http://{address}:{actual_port}/（命令控制台，Ctrl-C 退出）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()


__all__ = [
    "LOOPBACK_HOSTS",
    "PAGE",
    "TOKEN_HEADER",
    "PanelApp",
    "PanelBindError",
    "PanelSettings",
    "host_name_of",
    "is_loopback_host",
    "make_handler",
    "serve",
    "serve_forever",
]
