const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const outDir = path.resolve("assets/bigdata-evolution");
fs.mkdirSync(outDir, { recursive: true });

const W = 1800;
const H = 1200;

const pages = [
  {
    file: "01-overview-map.png",
    title: "大数据技术演进总览",
    subtitle: "技术演进不是工具堆叠，而是一条由工程矛盾推动的能力扩展路径",
    accent: "#2563eb",
    body: `
      <div class="overview">
        <div class="orbit">
          ${bubble("数据放不下", "HDFS / 对象存储", "b1")}
          ${bubble("单机算不动", "MapReduce / Spark", "b2")}
          ${bubble("资源难共享", "YARN / K8s", "b3")}
          ${bubble("SQL 门槛", "Hive / Spark SQL", "b4")}
          ${bubble("不能只 T+1", "Kafka / Flink", "b5")}
          ${bubble("查询要更快", "Trino / OLAP", "b6")}
          ${bubble("湖要像表", "Iceberg / Hudi / Delta", "b7")}
          ${bubble("服务 AI", "Feature / Vector / RAG", "b8")}
          <div class="center-disc">
            <div class="mini">Core</div>
            <strong>工程矛盾链</strong>
            <span>问题 → 架构回应 → 新瓶颈 → 下一代</span>
          </div>
        </div>
        <div class="insight-panel">
          <h2>读图方式</h2>
          <div class="rule"><i></i><b>存储扩展</b><span>先解决“放得下、坏不怕”。</span></div>
          <div class="rule"><i></i><b>计算扩展</b><span>再解决“拆得开、算得完”。</span></div>
          <div class="rule"><i></i><b>平台化</b><span>把资源、SQL、治理变成团队能力。</span></div>
          <div class="rule"><i></i><b>服务化</b><span>低延迟查询、实时指标、AI 应用都需要数据服务。</span></div>
          <div class="legend-card">
            <span><em class="c-blue"></em>底座能力</span>
            <span><em class="c-green"></em>计算能力</span>
            <span><em class="c-orange"></em>服务能力</span>
            <span><em class="c-purple"></em>AI 能力</span>
          </div>
        </div>
      </div>
    `,
  },
  {
    file: "02-evolution-timeline.png",
    title: "大数据技术演进时间轴",
    subtitle: "从论文原型到湖仓与 AI 数据工程：每一段演进都在补上一代的短板",
    accent: "#f97316",
    body: `
      <div class="river">
        ${era("Google 论文", "GFS / MapReduce / Bigtable", "分布式存储与并行批计算原型", "e1")}
        ${era("Hadoop 生态", "HDFS / Hive / YARN", "开源底座、SQL 化、资源解耦", "e2")}
        ${era("统一计算", "Spark / Spark SQL", "DAG、内存复用、优化器", "e3")}
        ${era("实时与 OLAP", "Kafka / Flink / ClickHouse", "事件流、状态一致性、低延迟分析", "e4")}
        ${era("湖仓与 AI", "Iceberg / Hudi / Delta / RAG", "表语义、多引擎共享、智能应用", "e5")}
        <svg class="river-line" viewBox="0 0 1500 420" preserveAspectRatio="none">
          <path d="M35,215 C260,65 420,350 650,205 C850,80 990,340 1210,185 C1340,105 1420,150 1480,95" fill="none" stroke="url(#g)" stroke-width="34" stroke-linecap="round"/>
          <defs><linearGradient id="g" x1="0" x2="1"><stop stop-color="#2563eb"/><stop offset=".35" stop-color="#10b981"/><stop offset=".68" stop-color="#f97316"/><stop offset="1" stop-color="#8b5cf6"/></linearGradient></defs>
        </svg>
      </div>
      <div class="takeaway">
        <b>代际逻辑</b>
        <span>MapReduce 解决能不能分布式算，Spark 解决多阶段为什么慢，Flink 解决无界流如何一致，湖仓解决对象存储如何重新获得表语义。</span>
      </div>
    `,
  },
  {
    file: "03-storage-compute.png",
    title: "存储与计算第一性原理",
    subtitle: "分布式系统的基本功：数据切块、副本容错、元数据分离、任务拆分、全局重排",
    accent: "#10b981",
    body: `
      <div class="dual-arch">
        <div class="arch-card storage-card">
          <h2>HDFS：存储侧</h2>
          <div class="nn">NameNode<span>namespace · block map · FsImage · EditLog</span></div>
          <div class="storage-lines"></div>
          <div class="dn-grid">
            ${dn("DN-A", ["B1", "B2", "B5"])}
            ${dn("DN-B", ["B1", "B3", "B4"])}
            ${dn("DN-C", ["B2", "B3", "B5"])}
          </div>
          <div class="mini-bars">
            <label>元数据压力</label><span style="--v:72%"></span>
            <label>副本容错</label><span style="--v:88%"></span>
            <label>小文件风险</label><span style="--v:64%"></span>
          </div>
        </div>
        <div class="arch-card compute-card">
          <h2>Spark / MapReduce：计算侧</h2>
          <div class="compute-flow">
            ${node("Input", "split")}
            ${arrow()}
            ${node("Map", "parallel")}
            ${arrow()}
            ${node("Shuffle", "network + spill", "hot")}
            ${arrow()}
            ${node("Reduce", "aggregate")}
            ${arrow()}
            ${node("Output", "files")}
          </div>
          <div class="dag">
            <div>Job</div><i></i><div>Stage 1</div><i></i><div>Stage 2</div><i></i><div>Task × N</div>
          </div>
          <div class="cost-chart">
            <div><span style="height:34%"></span><b>CPU</b></div>
            <div><span style="height:76%"></span><b>Network</b></div>
            <div><span style="height:62%"></span><b>Spill</b></div>
            <div><span style="height:84%"></span><b>Skew</b></div>
          </div>
        </div>
      </div>
      <div class="legend-wide"><b>关键判断</b><span>小文件问题压在 NameNode 元数据层；Shuffle 慢在全局重分布；Spark 调优本质是减少不必要的数据移动。</span></div>
    `,
  },
  {
    file: "04-platform-stack.png",
    title: "核心平台全景图",
    subtitle: "从存储底座到应用服务：看清每层负责什么，也看清哪些能力贯穿全链路",
    accent: "#0ea5e9",
    body: `
      <div class="platform">
        <div class="stack">
          ${layer("应用服务", "BI 看板 · 实时指标 · 标签服务 · 特征服务 · RAG 应用", "#8b5cf6")}
          ${layer("查询服务", "Trino · ClickHouse · Doris · StarRocks · Presto", "#f97316")}
          ${layer("批流计算", "Hive · Spark · Flink · Kafka · CDC", "#10b981")}
          ${layer("表语义", "Hive Metastore · Iceberg · Hudi · Delta · Catalog", "#2563eb")}
          ${layer("存储底座", "HDFS · S3/OSS/ABS · Parquet · ORC · Avro", "#0f172a")}
        </div>
        <div class="side-rails">
          <h2>贯穿能力</h2>
          ${rail("调度", "依赖、回填、SLA")}
          ${rail("元数据", "表、字段、指标、Owner")}
          ${rail("血缘", "影响分析、故障定位")}
          ${rail("质量", "规则、校验、告警")}
          ${rail("安全成本", "权限、脱敏、配额、冷热分层")}
        </div>
      </div>
      <div class="service-map">
        <span>加工层：Spark / Hive / Flink</span>
        <span>服务层：Trino / ClickHouse / Doris</span>
        <span>共享层：Iceberg / Hudi / Delta</span>
      </div>
    `,
  },
  {
    file: "05-ai-data-engineering.png",
    title: "AI 数据工程链路",
    subtitle: "AI 数据工程不是替代大数据平台，而是把表、特征、向量、检索、评估统一到一个闭环",
    accent: "#8b5cf6",
    body: `
      <div class="pipeline-rich">
        <div class="source-cluster">
          <h2>多源数据</h2>
          ${source("业务库", "DB")}
          ${source("日志事件", "LOG")}
          ${source("文档图片", "DOC")}
          ${source("用户反馈", "FB")}
        </div>
        <div class="pipe-lanes">
          ${lane("清洗切分", "去重 · 脱敏 · Chunk · 元信息", "#2563eb")}
          ${lane("湖仓沉淀", "Snapshot · Schema · Lineage · Quality", "#10b981")}
          ${lane("特征工程", "Offline Feature · Online Feature · Serving", "#f97316")}
          ${lane("向量检索", "Embedding · Vector Index · Recall · Rerank", "#8b5cf6")}
        </div>
        <div class="app-cluster">
          <h2>智能应用</h2>
          ${app("BI", "统计分析")}
          ${app("FS", "特征服务")}
          ${app("RAG", "检索增强")}
          ${app("Agent", "任务执行")}
        </div>
      </div>
      <div class="feedback-loop"><b>反馈闭环</b><span>在线效果、用户反馈、召回质量、延迟成本回流到数据质量、特征与索引更新。</span></div>
    `,
  },
  {
    file: "06-selection-matrix.png",
    title: "技术选型决策矩阵",
    subtitle: "先判断延迟目标和服务模式，再决定组件组合；不是用一个系统解决所有问题",
    accent: "#ef4444",
    body: `
      <div class="matrix-rich">
        <div class="axis x">延迟目标：T+1 → 分钟级 → 秒级 → 交互式</div>
        <div class="axis y">数据形态 / 服务模式</div>
        ${cell("Hive / Spark", "离线数仓 · 宽表加工 · 批量回刷", "c1")}
        ${cell("Kafka + Flink", "CDC · 状态流 · 窗口聚合 · exactly-once", "c2")}
        ${cell("ClickHouse / Doris", "低延迟 OLAP · 高并发看板 · 明细聚合", "c3")}
        ${cell("Trino", "联邦查询 · 即席分析 · 统一查询入口", "c4")}
        ${cell("Iceberg / Hudi / Delta", "表语义 · 快照 · 事务 · 多引擎共享", "c5")}
        ${cell("Feature / Vector / RAG", "特征服务 · 向量检索 · 智能应用", "c6")}
        <div class="quadrant q1">批处理加工</div><div class="quadrant q2">联邦查询</div><div class="quadrant q3">湖仓与 AI</div><div class="quadrant q4">低延迟服务</div>
      </div>
      <div class="decision-strip">
        ${pill("1 延迟", "T+1 / 分钟 / 秒级")}
        ${pill("2 形态", "批量 / CDC / 无界流")}
        ${pill("3 一致性", "最终一致 / exactly-once")}
        ${pill("4 服务", "任务 / SQL / 在线服务")}
      </div>
    `,
  },
  {
    file: "07-interview-map.png",
    title: "大数据面试知识地图",
    subtitle: "用地铁线路组织知识点：底层原理、离线数仓、实时链路、OLAP、湖仓治理、项目表达",
    accent: "#7c3aed",
    body: `
      <div class="metro">
        ${metroLine("底层原理线", ["HDFS", "Block", "副本", "Shuffle", "DAG", "YARN"], "m1")}
        ${metroLine("离线数仓线", ["Hive", "分层", "建模", "SQL 优化", "Spark 调优"], "m2")}
        ${metroLine("实时链路线", ["Kafka", "CDC", "Flink", "Watermark", "Checkpoint"], "m3")}
        ${metroLine("分析服务线", ["Trino", "ClickHouse", "Doris", "MPP", "向量化"], "m4")}
        ${metroLine("湖仓治理线", ["Iceberg", "Snapshot", "Schema", "Lineage", "Quality"], "m5")}
        ${metroLine("项目表达线", ["背景", "难点", "选型", "优化", "指标", "复盘"], "m6")}
        <div class="hub">面试<br/>能力<br/>中心</div>
      </div>
      <div class="answer-strip">
        <span>高分回答公式</span><b>问题背景 → 技术选择 → 架构链路 → 瓶颈定位 → 指标收益 → 复盘边界</b>
      </div>
    `,
  },
];

function bubble(title, text, cls) {
  return `<div class="bubble ${cls}"><strong>${title}</strong><span>${text}</span></div>`;
}
function era(title, tech, desc, cls) {
  return `<div class="era ${cls}"><strong>${title}</strong><b>${tech}</b><span>${desc}</span></div>`;
}
function dn(name, blocks) {
  return `<div class="dn"><strong>${name}</strong><span>${blocks.map((b) => `<i>${b}</i>`).join("")}</span></div>`;
}
function node(title, sub, cls = "") {
  return `<div class="node ${cls}"><strong>${title}</strong><span>${sub}</span></div>`;
}
function arrow() {
  return `<b class="arr">→</b>`;
}
function layer(title, text, color) {
  return `<div class="p-layer" style="--c:${color}"><strong>${title}</strong><span>${text}</span></div>`;
}
function rail(title, text) {
  return `<div class="rail"><strong>${title}</strong><span>${text}</span></div>`;
}
function source(title, code) {
  return `<div class="source"><i>${code}</i><span>${title}</span></div>`;
}
function lane(title, text, color) {
  return `<div class="lane" style="--c:${color}"><strong>${title}</strong><span>${text}</span></div>`;
}
function app(title, text) {
  return `<div class="app"><strong>${title}</strong><span>${text}</span></div>`;
}
function cell(title, text, cls) {
  return `<div class="mx-cell ${cls}"><strong>${title}</strong><span>${text}</span></div>`;
}
function pill(title, text) {
  return `<div class="pill"><strong>${title}</strong><span>${text}</span></div>`;
}
function metroLine(title, stops, cls) {
  return `<div class="metro-line ${cls}"><strong>${title}</strong>${stops.map((s) => `<span>${s}</span>`).join("")}</div>`;
}

function html(page) {
  return `<!doctype html><html><head><meta charset="utf-8"><style>
*{box-sizing:border-box} body{margin:0;background:#e8eef6;color:#101827;font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",Arial,sans-serif}
.canvas{width:${W}px;height:${H}px;padding:52px 64px;background:radial-gradient(circle at 8% 8%,rgba(37,99,235,.15),transparent 25%),radial-gradient(circle at 90% 8%,rgba(249,115,22,.15),transparent 25%),linear-gradient(135deg,#fbfdff,#f4f8fc 55%,#ffffff);position:relative;overflow:hidden}
.canvas:before{content:"";position:absolute;inset:24px;border:1px solid rgba(100,116,139,.2);border-radius:32px;pointer-events:none}
header{text-align:center;margin-bottom:32px;position:relative;z-index:1}.kicker{font-size:18px;letter-spacing:.16em;color:${page.accent};font-weight:900}.title{font-size:54px;font-weight:950;letter-spacing:0;margin:8px 0 12px}.subtitle{display:inline-block;max-width:1120px;padding:12px 24px;background:rgba(255,255,255,.88);border:1px solid #cbd5e1;border-radius:14px;font-size:23px;line-height:1.42;color:#334155;box-shadow:0 10px 28px rgba(15,23,42,.06)}
.overview{display:grid;grid-template-columns:1.15fr .85fr;gap:34px}.orbit{position:relative;height:830px;border-radius:34px;background:linear-gradient(180deg,rgba(255,255,255,.85),rgba(248,251,255,.88));border:1px solid #dbe4f0;box-shadow:0 24px 60px rgba(15,23,42,.1)}.orbit:before,.orbit:after{content:"";position:absolute;border:2px dashed rgba(100,116,139,.25);border-radius:50%;left:120px;right:120px;top:95px;bottom:95px}.orbit:after{left:215px;right:215px;top:190px;bottom:190px}.center-disc{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:300px;height:300px;border-radius:50%;background:linear-gradient(135deg,#111827,#263a5c);color:white;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;box-shadow:0 30px 70px rgba(15,23,42,.32)}.center-disc .mini{color:#93c5fd;font-size:18px;font-weight:900}.center-disc strong{font-size:42px}.center-disc span{width:230px;color:#dbeafe;font-size:20px;line-height:1.35}.bubble{position:absolute;width:220px;min-height:110px;border-radius:24px;padding:22px;background:white;border:1px solid #dbe4f0;box-shadow:0 18px 45px rgba(15,23,42,.11)}.bubble strong{display:block;font-size:25px;margin-bottom:8px}.bubble span{font-size:18px;color:#526071}.b1{left:70px;top:110px}.b2{left:380px;top:40px}.b3{right:70px;top:110px}.b4{right:42px;top:360px}.b5{right:120px;bottom:100px}.b6{left:380px;bottom:40px}.b7{left:70px;bottom:100px}.b8{left:42px;top:360px}.insight-panel{border-radius:34px;background:#101827;color:white;padding:38px;box-shadow:0 28px 70px rgba(15,23,42,.22)}.insight-panel h2{font-size:36px;margin:0 0 28px}.rule{display:grid;grid-template-columns:12px 120px 1fr;gap:14px;align-items:start;margin:22px 0}.rule i{height:52px;border-radius:9px;background:${page.accent}}.rule b{font-size:24px}.rule span{font-size:21px;line-height:1.4;color:#dbeafe}.legend-card{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:38px;padding:22px;border-radius:22px;background:rgba(255,255,255,.08)}.legend-card span{font-size:18px}.legend-card em{display:inline-block;width:18px;height:18px;border-radius:5px;margin-right:8px;vertical-align:-3px}.c-blue{background:#2563eb}.c-green{background:#10b981}.c-orange{background:#f97316}.c-purple{background:#8b5cf6}
.river{position:relative;height:760px;border-radius:34px;background:rgba(255,255,255,.86);border:1px solid #dbe4f0;box-shadow:0 24px 60px rgba(15,23,42,.1);overflow:hidden}.river-line{position:absolute;left:110px;right:110px;top:205px;width:1500px;height:420px;opacity:.9}.era{position:absolute;width:285px;border-radius:24px;background:white;border:1px solid #dbe4f0;padding:24px;box-shadow:0 18px 45px rgba(15,23,42,.12);z-index:2}.era strong{display:block;font-size:29px}.era b{display:block;color:#2563eb;font-size:22px;line-height:1.3;margin:12px 0}.era span{font-size:19px;color:#526071;line-height:1.42}.e1{left:70px;top:80px}.e2{left:395px;bottom:70px}.e3{left:720px;top:90px}.e4{right:395px;bottom:70px}.e5{right:70px;top:80px}.takeaway,.legend-wide,.feedback-loop,.answer-strip{margin-top:26px;border-radius:24px;background:#101827;color:#e2e8f0;padding:26px 34px;font-size:23px;line-height:1.45;box-shadow:0 20px 50px rgba(15,23,42,.18)}.takeaway b,.legend-wide b,.feedback-loop b{font-size:28px;margin-right:18px;color:white}
.dual-arch{display:grid;grid-template-columns:1fr 1fr;gap:32px}.arch-card{height:760px;border-radius:34px;background:white;border:1px solid #dbe4f0;padding:34px;box-shadow:0 24px 60px rgba(15,23,42,.1);position:relative}.arch-card h2{font-size:34px;margin:0 0 26px}.nn{margin:0 auto;width:380px;border-radius:26px;background:linear-gradient(135deg,#1e40af,#0ea5e9);color:white;text-align:center;padding:28px;font-size:30px;font-weight:900;box-shadow:0 20px 44px rgba(37,99,235,.25)}.nn span{display:block;font-size:17px;color:#dbeafe;margin-top:8px}.dn-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;margin-top:130px}.dn{border-radius:22px;background:#f0fdf4;border:1px solid #86efac;padding:22px;text-align:center}.dn strong{font-size:24px}.dn i{display:inline-block;margin:14px 4px 0;padding:8px 10px;border-radius:8px;background:white;border:1px solid #bbf7d0;font-style:normal;font-weight:800}.storage-lines{position:absolute;left:150px;right:150px;top:205px;height:140px;border-left:4px solid #94a3b8;border-right:4px solid #94a3b8;border-bottom:4px solid #94a3b8;border-radius:0 0 28px 28px;opacity:.45}.mini-bars{margin-top:42px;display:grid;grid-template-columns:140px 1fr;gap:14px;align-items:center}.mini-bars label{font-size:19px;font-weight:800}.mini-bars span{height:16px;border-radius:99px;background:linear-gradient(90deg,#2563eb var(--v),#e2e8f0 0)}.compute-flow{display:flex;align-items:center;gap:8px;flex-wrap:nowrap;margin-top:30px}.node{width:108px;min-height:88px;border-radius:20px;background:#eff6ff;border:1px solid #bfdbfe;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center}.node strong{font-size:22px}.node span{font-size:14px;color:#64748b}.node.hot{background:#fff7ed;border-color:#fdba74}.arr{font-size:27px;color:#94a3b8}.dag{display:flex;align-items:center;gap:13px;margin:70px 0 50px}.dag div{padding:22px 28px;border-radius:18px;background:#f5f3ff;border:1px solid #ddd6fe;font-size:22px;font-weight:900}.dag i{width:34px;height:4px;background:#94a3b8}.cost-chart{height:230px;border-radius:24px;background:#f8fafc;border:1px solid #e2e8f0;display:flex;align-items:end;justify-content:space-around;padding:30px}.cost-chart div{height:170px;display:flex;flex-direction:column;align-items:center;justify-content:end}.cost-chart span{width:48px;border-radius:12px 12px 0 0;background:linear-gradient(180deg,#f97316,#ef4444)}.cost-chart b{font-size:16px;margin-top:10px}
.platform{display:grid;grid-template-columns:1fr 430px;gap:34px}.stack{height:760px;display:flex;flex-direction:column-reverse;justify-content:space-between}.p-layer{height:132px;border-radius:28px;background:linear-gradient(90deg,var(--c),color-mix(in srgb,var(--c) 72%,white));color:white;padding:28px 34px;box-shadow:0 20px 45px rgba(15,23,42,.15);position:relative;overflow:hidden}.p-layer:after{content:"";position:absolute;right:-40px;top:-40px;width:180px;height:180px;border-radius:50%;background:rgba(255,255,255,.16)}.p-layer strong{font-size:33px}.p-layer span{display:block;font-size:22px;margin-top:8px;color:rgba(255,255,255,.9)}.side-rails{height:760px;border-radius:34px;background:white;border:1px solid #dbe4f0;padding:32px;box-shadow:0 24px 60px rgba(15,23,42,.1)}.side-rails h2{font-size:34px;margin:0 0 24px}.rail{border-left:8px solid #2563eb;border-radius:18px;background:#f8fafc;padding:18px 20px;margin:16px 0}.rail strong{font-size:24px}.rail span{display:block;font-size:18px;color:#526071;margin-top:6px}.service-map{display:flex;gap:20px;margin-top:26px}.service-map span{flex:1;border-radius:18px;background:white;border:1px solid #dbe4f0;padding:22px;text-align:center;font-size:22px;font-weight:900;box-shadow:0 12px 28px rgba(15,23,42,.08)}
.pipeline-rich{display:grid;grid-template-columns:260px 1fr 260px;gap:32px;align-items:stretch}.source-cluster,.app-cluster{height:760px;border-radius:34px;background:#101827;color:white;padding:30px;box-shadow:0 24px 60px rgba(15,23,42,.2)}.source-cluster h2,.app-cluster h2{font-size:32px;margin:0 0 26px;color:#bfdbfe}.source,.app{height:120px;border-radius:22px;background:rgba(255,255,255,.09);border:1px solid rgba(255,255,255,.16);display:flex;align-items:center;gap:18px;padding:20px;margin:18px 0}.source i,.app strong{width:58px;height:58px;border-radius:18px;background:#2563eb;color:white;display:flex;align-items:center;justify-content:center;font-style:normal;font-size:18px;font-weight:900}.source span,.app span{font-size:22px}.pipe-lanes{height:760px;display:grid;grid-template-rows:repeat(4,1fr);gap:24px;position:relative}.pipe-lanes:before{content:"";position:absolute;left:-28px;right:-28px;top:50%;height:10px;background:linear-gradient(90deg,#2563eb,#10b981,#f97316,#8b5cf6);border-radius:99px;z-index:0}.lane{position:relative;z-index:1;border-radius:30px;background:white;border:1px solid #dbe4f0;padding:32px;box-shadow:0 20px 48px rgba(15,23,42,.1);border-left:14px solid var(--c)}.lane strong{font-size:32px}.lane span{display:block;font-size:22px;color:#526071;margin-top:12px}.feedback-loop{background:linear-gradient(135deg,#1e1b4b,#312e81)}
.matrix-rich{height:760px;position:relative;border-radius:34px;background:white;border:1px solid #dbe4f0;box-shadow:0 24px 60px rgba(15,23,42,.1);padding:80px 90px}.matrix-rich:before{content:"";position:absolute;left:90px;right:90px;top:100px;bottom:90px;background-image:linear-gradient(#dbe4f0 2px,transparent 2px),linear-gradient(90deg,#dbe4f0 2px,transparent 2px);background-size:50% 50%;opacity:.9}.axis{position:absolute;font-size:24px;font-weight:900;color:#334155}.axis.x{bottom:35px;left:520px}.axis.y{left:-10px;top:355px;transform:rotate(-90deg);transform-origin:left top}.quadrant{position:absolute;color:#94a3b8;font-size:28px;font-weight:950}.q1{left:140px;top:125px}.q2{right:145px;top:125px}.q3{left:140px;bottom:115px}.q4{right:145px;bottom:115px}.mx-cell{position:absolute;width:360px;border-radius:24px;background:#f8fafc;border:1px solid #cbd5e1;padding:26px;box-shadow:0 18px 42px rgba(15,23,42,.12)}.mx-cell strong{font-size:30px}.mx-cell span{display:block;font-size:20px;line-height:1.4;color:#526071;margin-top:10px}.c1{left:150px;top:160px}.c2{left:610px;top:235px}.c3{right:155px;top:410px}.c4{right:155px;top:160px}.c5{left:520px;bottom:135px}.c6{left:150px;bottom:150px}.decision-strip{display:grid;grid-template-columns:repeat(4,1fr);gap:18px;margin-top:24px}.pill{border-radius:18px;background:white;border:1px solid #dbe4f0;padding:22px;text-align:center;box-shadow:0 10px 26px rgba(15,23,42,.08)}.pill strong{display:block;font-size:22px}.pill span{font-size:18px;color:#526071}
.metro{height:760px;border-radius:34px;background:white;border:1px solid #dbe4f0;box-shadow:0 24px 60px rgba(15,23,42,.1);position:relative;padding:52px}.hub{position:absolute;left:735px;top:285px;width:210px;height:210px;border-radius:50%;background:#101827;color:white;display:flex;align-items:center;justify-content:center;text-align:center;font-size:34px;font-weight:950;line-height:1.12;box-shadow:0 28px 70px rgba(15,23,42,.28)}.metro-line{position:absolute;display:flex;align-items:center;gap:16px}.metro-line:before{content:"";position:absolute;left:120px;right:0;height:10px;border-radius:99px;z-index:0}.metro-line strong{width:150px;font-size:20px}.metro-line span{position:relative;z-index:1;border-radius:999px;background:white;border:4px solid var(--c);padding:11px 16px;font-size:18px;font-weight:900;box-shadow:0 8px 18px rgba(15,23,42,.08)}.m1{--c:#2563eb;left:70px;top:80px}.m2{--c:#10b981;left:180px;top:205px}.m3{--c:#f97316;left:90px;top:330px}.m4{--c:#8b5cf6;left:180px;top:455px}.m5{--c:#ef4444;left:90px;top:580px}.m6{--c:#0891b2;left:310px;top:690px}.m1:before{background:#2563eb}.m2:before{background:#10b981}.m3:before{background:#f97316}.m4:before{background:#8b5cf6}.m5:before{background:#ef4444}.m6:before{background:#0891b2}.answer-strip{background:#101827}
</style></head><body><div class="canvas"><header><div class="kicker">BIG DATA EVOLUTION</div><div class="title">${page.title}</div><div class="subtitle">${page.subtitle}</div></header>${page.body}</div></body></html>`;
}

(async () => {
  const chromePaths = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
  ];
  const executablePath = chromePaths.find((p) => fs.existsSync(p));
  const browser = await chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}) });
  const page = await browser.newPage({ viewport: { width: W, height: H }, deviceScaleFactor: 2 });
  for (const spec of pages) {
    await page.setContent(html(spec), { waitUntil: "networkidle" });
    await page.screenshot({ path: path.join(outDir, spec.file), fullPage: false });
    console.log(`wrote ${spec.file}`);
  }
  await browser.close();
})();
