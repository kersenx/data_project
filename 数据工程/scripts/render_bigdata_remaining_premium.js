const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const outDir = path.resolve("assets/bigdata-evolution");
fs.mkdirSync(outDir, { recursive: true });

const pages = [
  {
    file: "02-evolution-timeline.png",
    title: "大数据技术演进时间轴",
    subtitle: "从分布式存储论文原型，到湖仓与 AI 数据工程：技术代际由核心瓶颈驱动",
    kind: "timeline",
  },
  {
    file: "03-storage-compute.png",
    title: "存储与计算第一性原理",
    subtitle: "分布式系统的基本逻辑：数据切块、副本容错、元数据分离、任务拆分、全局重排",
    kind: "storage",
  },
  {
    file: "04-platform-stack.png",
    title: "核心平台全景图",
    subtitle: "从存储底座到业务服务：看清每层组件边界，以及治理能力如何贯穿全链路",
    kind: "platform",
  },
  {
    file: "05-ai-data-engineering.png",
    title: "AI 数据工程链路",
    subtitle: "AI 数据工程把湖仓、特征、向量、检索、评估与反馈统一到一个数据闭环",
    kind: "ai",
  },
  {
    file: "06-selection-matrix.png",
    title: "技术选型决策矩阵",
    subtitle: "先判断延迟、数据形态、一致性和服务模式，再组合合适的计算、查询和湖仓组件",
    kind: "matrix",
  },
  {
    file: "07-interview-map.png",
    title: "大数据面试知识地图",
    subtitle: "用知识线路组织底层原理、离线数仓、实时链路、OLAP、湖仓治理和项目表达",
    kind: "interview",
  },
];

function base(title, subtitle, body) {
  return `<!doctype html><html><head><meta charset="utf-8"/><style>
*{box-sizing:border-box}
body{margin:0;background:#eef3f8;font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",Arial,sans-serif;color:#152033}
.canvas{width:1800px;height:1200px;position:relative;overflow:hidden;padding:54px 64px;background:radial-gradient(circle at 10% 8%,rgba(37,99,235,.12),transparent 26%),radial-gradient(circle at 90% 12%,rgba(16,185,129,.11),transparent 24%),linear-gradient(180deg,#fbfdff 0%,#f6f9fd 64%,#ffffff 100%)}
.canvas:before{content:"";position:absolute;inset:26px;border:1px solid #d9e3ef;border-radius:34px;pointer-events:none}
.grid-bg{position:absolute;inset:0;background-image:linear-gradient(#e8eef6 1px,transparent 1px),linear-gradient(90deg,#e8eef6 1px,transparent 1px);background-size:36px 36px;opacity:.42;mask-image:linear-gradient(180deg,black 0%,transparent 82%)}
header{position:relative;z-index:2;text-align:center;margin-bottom:24px}.kicker{font-size:18px;font-weight:900;letter-spacing:.18em;color:#2563eb;text-transform:uppercase}.title{font-size:58px;font-weight:950;letter-spacing:0;margin:8px 0 12px}.subtitle{display:inline-block;max-width:1220px;padding:12px 24px;border-radius:999px;background:white;border:1px solid #ccd8e6;box-shadow:0 10px 28px rgba(15,23,42,.06);font-size:23px;line-height:1.4;color:#405066}
.figure{position:relative;z-index:1;height:890px;margin-top:18px}.card{background:rgba(255,255,255,.9);border:1px solid #dbe4f0;border-radius:28px;box-shadow:0 20px 56px rgba(15,23,42,.08)}
.logic{position:absolute;left:0;right:0;bottom:0;height:118px;background:#101827;color:white;border-radius:28px;display:grid;grid-template-columns:190px 1fr;align-items:center;padding:24px 30px;box-shadow:0 24px 60px rgba(15,23,42,.18)}.logic h2{margin:0;font-size:28px}.logic-flow{display:flex;gap:12px;align-items:center}.logic-flow div{height:64px;min-width:125px;border-radius:17px;background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.15);display:flex;align-items:center;justify-content:center;text-align:center;font-size:17px;font-weight:900}.logic-flow b{color:#93c5fd;font-size:24px}
.legend{position:absolute;right:0;bottom:142px;width:360px;padding:20px;border-radius:24px;background:white;border:1px solid #dbe4f0;box-shadow:0 18px 45px rgba(15,23,42,.08)}.legend h3{margin:0 0 12px;font-size:21px}.legend p{margin:9px 0;font-size:16px;color:#475569}.legend i{display:inline-block;width:16px;height:16px;border-radius:5px;margin-right:8px;vertical-align:-2px;background:var(--c)}

/* timeline */
.tl-main{position:absolute;left:48px;right:48px;top:42px;height:540px;border-radius:34px;background:rgba(255,255,255,.78);border:1px solid #dbe4f0;box-shadow:0 28px 80px rgba(15,23,42,.08);padding:34px}
.river{position:absolute;left:75px;right:75px;top:215px;height:120px}.river svg{width:100%;height:160px;overflow:visible}.era{position:absolute;width:245px;height:145px;border-radius:22px;background:white;border:1px solid #dbe4f0;box-shadow:0 14px 34px rgba(15,23,42,.08);padding:20px}.era strong{display:block;font-size:23px}.era span{display:block;font-size:16px;color:#64748b;margin-top:7px;line-height:1.3}.era b{display:block;margin-top:9px;color:#2563eb;font-size:16px}.e1{left:35px;top:55px}.e2{left:330px;top:320px}.e3{left:625px;top:55px}.e4{right:330px;top:320px}.e5{right:35px;top:55px}.pressure-row{position:absolute;left:48px;right:430px;bottom:142px;height:210px;display:grid;grid-template-columns:repeat(5,1fr);gap:14px}.pressure-mini{border-radius:22px;background:white;border:1px solid #dbe4f0;padding:18px;box-shadow:0 14px 34px rgba(15,23,42,.06)}.pressure-mini i{display:block;width:34px;height:34px;border-radius:12px;background:var(--c);margin-bottom:12px}.pressure-mini strong{font-size:19px}.pressure-mini span{display:block;font-size:14px;color:#64748b;margin-top:6px;line-height:1.25}

/* storage */
.dual{position:absolute;left:28px;right:28px;top:34px;height:700px;display:grid;grid-template-columns:1fr 1fr;gap:28px}.panel{padding:30px}.panel h2{font-size:34px;margin:0 0 22px}.nn{margin:0 auto;width:380px;border-radius:26px;background:linear-gradient(135deg,#2563eb,#38bdf8);color:white;text-align:center;padding:24px;font-size:30px;font-weight:950}.nn span{display:block;font-size:16px;color:#e0f2fe}.dn-row{display:grid;grid-template-columns:repeat(3,1fr);gap:15px;margin-top:108px}.dn{height:120px;border-radius:20px;background:#ecfdf5;border:1px solid #86efac;text-align:center;padding:20px}.dn strong{font-size:23px}.dn span{display:block;margin-top:14px}.dn i{font-style:normal;padding:7px 10px;border-radius:8px;background:white;border:1px solid #bbf7d0;font-weight:900;margin:0 3px}.bars{margin-top:32px;display:grid;grid-template-columns:140px 1fr;gap:13px;align-items:center}.bars label{font-size:18px;font-weight:900}.bars span{height:15px;border-radius:99px;background:linear-gradient(90deg,#2563eb var(--v),#e2e8f0 0)}.compute-flow{display:flex;align-items:center;gap:8px;margin-top:38px}.node{width:108px;height:88px;border-radius:20px;background:#eff6ff;border:1px solid #bfdbfe;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center}.node strong{font-size:22px}.node span{font-size:14px;color:#64748b}.hot{background:#fff7ed;border-color:#fdba74}.arr{font-size:27px;color:#94a3b8}.dag{display:flex;align-items:center;gap:12px;margin:64px 0 42px}.dag div{padding:20px 26px;border-radius:18px;background:#f5f3ff;border:1px solid #ddd6fe;font-size:21px;font-weight:900}.dag i{width:34px;height:4px;background:#94a3b8}.chart{height:210px;border-radius:22px;background:#f8fafc;border:1px solid #e2e8f0;display:flex;justify-content:space-around;align-items:end;padding:28px}.chart div{height:160px;display:flex;flex-direction:column;align-items:center;justify-content:end}.chart span{width:48px;border-radius:12px 12px 0 0;background:linear-gradient(180deg,#f97316,#ef4444)}.chart b{font-size:15px;margin-top:8px}

/* platform */
.platform{position:absolute;left:55px;right:55px;top:35px;height:560px;display:grid;grid-template-columns:1fr 380px;gap:30px}.stack{position:relative;padding:34px}.layer{height:88px;border-radius:24px;margin:9px 0;background:linear-gradient(90deg,var(--c),color-mix(in srgb,var(--c) 72%,white));color:white;padding:17px 26px;box-shadow:0 14px 32px rgba(15,23,42,.12)}.layer strong{font-size:25px}.layer span{display:block;font-size:17px;margin-top:6px;opacity:.92}.railbox{padding:24px}.railbox h2{font-size:27px;margin:0 0 14px}.rail{border-left:7px solid var(--c);border-radius:17px;background:#f8fafc;padding:13px 16px;margin:11px 0}.rail strong{font-size:22px}.rail span{display:block;color:#64748b;font-size:16px;margin-top:4px}.service-strip{position:absolute;left:55px;right:430px;bottom:142px;height:118px;display:grid;grid-template-columns:repeat(3,1fr);gap:18px}.svc{padding:22px;text-align:center}.svc strong{font-size:23px}.svc span{display:block;font-size:16px;color:#64748b;margin-top:7px}

/* ai */
.ai-pipe{position:absolute;left:45px;right:45px;top:35px;height:560px;display:grid;grid-template-columns:270px 1fr 280px;gap:26px}.cluster{padding:24px;background:white;color:#152033}.cluster h2{font-size:28px;margin:0 0 16px}.source,.app{height:88px;border-radius:18px;background:#f8fafc;border:1px solid #e2e8f0;padding:15px;margin:10px 0;display:grid;grid-template-columns:54px 1fr;gap:14px;align-items:center}.source i,.app i{width:52px;height:52px;border-radius:16px;background:#2563eb;color:white;font-style:normal;display:flex;align-items:center;justify-content:center;font-weight:950}.source strong,.app strong{font-size:21px}.source span,.app span{display:block;font-size:14px;color:#64748b}.lanes{display:grid;grid-template-rows:repeat(4,1fr);gap:18px;position:relative}.lanes:before{content:"";position:absolute;left:-20px;right:-20px;top:50%;height:10px;border-radius:99px;background:linear-gradient(90deg,#2563eb,#10b981,#f97316,#8b5cf6)}.lane{position:relative;border-left:12px solid var(--c);padding:20px 24px;background:white}.lane strong{font-size:25px}.lane span{display:block;font-size:17px;color:#64748b;margin-top:8px}.feedback{position:absolute;left:45px;right:430px;bottom:142px;height:112px;border-radius:26px;background:linear-gradient(90deg,#eef6ff,#f5f3ff);border:1px solid #dbe4f0;padding:24px;box-shadow:0 18px 45px rgba(15,23,42,.07)}.feedback strong{font-size:24px}.feedback span{display:block;font-size:17px;color:#475569;margin-top:8px}

/* matrix */
.matrix-main{position:absolute;left:65px;right:65px;top:42px;height:650px;padding:62px 82px}.matrix-main:before{content:"";position:absolute;left:82px;right:82px;top:82px;bottom:72px;background-image:linear-gradient(#dbe4f0 2px,transparent 2px),linear-gradient(90deg,#dbe4f0 2px,transparent 2px);background-size:50% 50%;opacity:.85}.axis{position:absolute;font-size:22px;font-weight:950;color:#334155}.axis.x{bottom:20px;left:510px}.axis.y{left:-6px;top:320px;transform:rotate(-90deg);transform-origin:left top}.mx{position:absolute;width:350px;border-radius:24px;background:#f8fafc;border:1px solid #cbd5e1;padding:24px;box-shadow:0 18px 42px rgba(15,23,42,.1)}.mx strong{font-size:28px}.mx span{display:block;font-size:18px;line-height:1.35;color:#526071;margin-top:8px}.c1{left:150px;top:145px}.c2{left:635px;top:230px}.c3{right:150px;top:360px}.c4{right:150px;top:145px}.c5{left:550px;bottom:115px}.c6{left:150px;bottom:120px}.decision{position:absolute;left:65px;right:430px;bottom:142px;height:130px;display:grid;grid-template-columns:repeat(4,1fr);gap:16px}.pill{text-align:center;padding:22px}.pill strong{font-size:22px}.pill span{display:block;font-size:16px;color:#64748b;margin-top:6px}

/* interview */
.metro{position:absolute;left:45px;right:45px;top:35px;height:700px;padding:42px}.hub{position:absolute;left:1120px;top:250px;width:210px;height:210px;border-radius:50%;background:#101827;color:white;display:flex;align-items:center;justify-content:center;text-align:center;font-size:34px;font-weight:950;line-height:1.12;box-shadow:0 24px 60px rgba(15,23,42,.22)}.line{position:absolute;display:flex;align-items:center;gap:14px}.line:before{content:"";position:absolute;left:126px;right:0;height:9px;border-radius:99px;background:var(--c)}.line strong{width:150px;font-size:20px}.line span{z-index:1;border:4px solid var(--c);border-radius:999px;background:white;padding:10px 15px;font-size:17px;font-weight:900}.m1{--c:#2563eb;left:80px;top:78px}.m2{--c:#10b981;left:180px;top:190px}.m3{--c:#f97316;left:80px;top:302px}.m4{--c:#8b5cf6;left:180px;top:414px}.m5{--c:#ef4444;left:80px;top:526px}.m6{--c:#0891b2;left:300px;top:555px}.answer{position:absolute;left:45px;right:430px;bottom:142px;height:128px;border-radius:26px;background:#101827;color:white;padding:26px 32px;box-shadow:0 24px 60px rgba(15,23,42,.18)}.answer strong{font-size:26px}.answer span{display:block;font-size:20px;margin-top:10px;color:#dbeafe}
</style></head><body><div class="canvas"><div class="grid-bg"></div><header><div class="kicker">Research Synthesis Figure</div><div class="title">${title}</div><div class="subtitle">${subtitle}</div></header>${body}</div></body></html>`;
}

function logic(items) {
  return `<div class="logic"><h2>核心逻辑</h2><div class="logic-flow">${items.map((x, i) => `<div>${x}</div>${i < items.length - 1 ? "<b>→</b>" : ""}`).join("")}</div></div>`;
}

function legend(items) {
  return `<div class="legend"><h3>图例</h3>${items.map(([c, t]) => `<p><i style="--c:${c}"></i>${t}</p>`).join("")}</div>`;
}

function bodyFor(kind) {
  if (kind === "timeline") return `
  <section class="figure">
    <main class="tl-main">
      <div class="river"><svg viewBox="0 0 1450 160" preserveAspectRatio="none"><defs><linearGradient id="r" x1="0" x2="1"><stop stop-color="#2563eb"/><stop offset=".32" stop-color="#10b981"/><stop offset=".66" stop-color="#f97316"/><stop offset="1" stop-color="#8b5cf6"/></linearGradient></defs><path d="M20 92 C220 8 345 148 540 82 C740 18 860 145 1040 76 C1210 18 1315 88 1430 44" fill="none" stroke="url(#r)" stroke-width="28" stroke-linecap="round"/></svg></div>
      <div class="era e1"><strong>Google 论文</strong><span>GFS / MapReduce / Bigtable</span><b>分布式原型</b></div>
      <div class="era e2"><strong>Hadoop 生态</strong><span>HDFS / Hive / YARN</span><b>开源与 SQL 化</b></div>
      <div class="era e3"><strong>统一计算</strong><span>Spark / Spark SQL / AQE</span><b>复用与优化</b></div>
      <div class="era e4"><strong>实时与 OLAP</strong><span>Kafka / Flink / ClickHouse</span><b>秒级与交互</b></div>
      <div class="era e5"><strong>湖仓与 AI</strong><span>Iceberg / Vector / RAG</span><b>治理与智能</b></div>
    </main>
    <div class="pressure-row">
      <div class="pressure-mini"><i style="--c:#2563eb"></i><strong>容量压力</strong><span>单机存储边界</span></div>
      <div class="pressure-mini"><i style="--c:#0ea5e9"></i><strong>计算压力</strong><span>多阶段反复落盘</span></div>
      <div class="pressure-mini"><i style="--c:#10b981"></i><strong>实时压力</strong><span>业务不等 T+1</span></div>
      <div class="pressure-mini"><i style="--c:#f97316"></i><strong>查询压力</strong><span>分析需要低延迟</span></div>
      <div class="pressure-mini"><i style="--c:#8b5cf6"></i><strong>AI 压力</strong><span>特征与检索闭环</span></div>
    </div>
    ${logic(["工程瓶颈", "代表技术", "能力沉淀", "下一代瓶颈"])}
    ${legend([["#2563eb","早期分布式底座"],["#10b981","统一计算与实时"],["#f97316","交互分析"],["#8b5cf6","湖仓与 AI"]])}
  </section>`;

  if (kind === "storage") return `
  <section class="figure">
    <div class="dual">
      <div class="card panel"><h2>HDFS：存储侧</h2><div class="nn">NameNode<span>namespace · block map · FsImage · EditLog</span></div><div class="dn-row"><div class="dn"><strong>DN-A</strong><span><i>B1</i><i>B2</i><i>B5</i></span></div><div class="dn"><strong>DN-B</strong><span><i>B1</i><i>B3</i><i>B4</i></span></div><div class="dn"><strong>DN-C</strong><span><i>B2</i><i>B3</i><i>B5</i></span></div></div><div class="bars"><label>元数据压力</label><span style="--v:72%"></span><label>副本容错</label><span style="--v:88%"></span><label>小文件风险</label><span style="--v:64%"></span></div></div>
      <div class="card panel"><h2>Spark / MapReduce：计算侧</h2><div class="compute-flow"><div class="node"><strong>Input</strong><span>split</span></div><b class="arr">→</b><div class="node"><strong>Map</strong><span>parallel</span></div><b class="arr">→</b><div class="node hot"><strong>Shuffle</strong><span>network + spill</span></div><b class="arr">→</b><div class="node"><strong>Reduce</strong><span>aggregate</span></div><b class="arr">→</b><div class="node"><strong>Output</strong><span>files</span></div></div><div class="dag"><div>Job</div><i></i><div>Stage 1</div><i></i><div>Stage 2</div><i></i><div>Task × N</div></div><div class="chart"><div><span style="height:34%"></span><b>CPU</b></div><div><span style="height:76%"></span><b>Network</b></div><div><span style="height:62%"></span><b>Spill</b></div><div><span style="height:84%"></span><b>Skew</b></div></div></div>
    </div>
    ${logic(["文件切块", "副本容错", "任务拆分", "Shuffle 重排", "血缘恢复"])}
    ${legend([["#2563eb","元数据与块映射"],["#10b981","副本与容错"],["#f97316","Shuffle 成本"],["#8b5cf6","DAG / Stage / Task"]])}
  </section>`;

  if (kind === "platform") return `
  <section class="figure">
    <div class="platform"><div class="card stack">
      <div class="layer" style="--c:#8b5cf6"><strong>应用服务层</strong><span>BI 看板 · 实时指标 · 标签服务 · 特征服务 · RAG 应用</span></div>
      <div class="layer" style="--c:#f97316"><strong>查询服务层</strong><span>Trino · ClickHouse · Doris · StarRocks · Presto</span></div>
      <div class="layer" style="--c:#10b981"><strong>批流计算层</strong><span>Hive · Spark · Flink · Kafka · CDC</span></div>
      <div class="layer" style="--c:#2563eb"><strong>表语义层</strong><span>Hive Metastore · Iceberg · Hudi · Delta · Catalog</span></div>
      <div class="layer" style="--c:#0f172a"><strong>存储底座层</strong><span>HDFS · S3 / OSS / ABS · Parquet · ORC · Avro</span></div>
    </div><div class="card railbox"><h2>贯穿治理能力</h2><div class="rail" style="--c:#2563eb"><strong>调度</strong><span>依赖、回填、SLA</span></div><div class="rail" style="--c:#0ea5e9"><strong>元数据</strong><span>表、字段、指标、Owner</span></div><div class="rail" style="--c:#10b981"><strong>血缘</strong><span>影响分析、故障定位</span></div><div class="rail" style="--c:#f97316"><strong>质量</strong><span>规则、校验、告警</span></div><div class="rail" style="--c:#8b5cf6"><strong>安全成本</strong><span>权限、脱敏、配额、冷热分层</span></div></div></div>
    <div class="service-strip"><div class="card svc"><strong>加工层</strong><span>Spark / Hive / Flink</span></div><div class="card svc"><strong>服务层</strong><span>Trino / ClickHouse / Doris</span></div><div class="card svc"><strong>共享层</strong><span>Iceberg / Hudi / Delta</span></div></div>
    ${logic(["采集存储", "表语义", "批流计算", "查询服务", "治理闭环"])}
  </section>`;

  if (kind === "ai") return `
  <section class="figure">
    <div class="ai-pipe"><div class="card cluster"><h2>多源数据</h2><div class="source"><i>DB</i><div><strong>业务库</strong><span>结构化事实与维度</span></div></div><div class="source"><i>LOG</i><div><strong>日志事件</strong><span>行为、埋点、CDC</span></div></div><div class="source"><i>DOC</i><div><strong>文档图片</strong><span>非结构化知识</span></div></div><div class="source"><i>FB</i><div><strong>用户反馈</strong><span>评估与闭环信号</span></div></div></div>
    <div class="lanes"><div class="card lane" style="--c:#2563eb"><strong>清洗切分</strong><span>去重 · 脱敏 · Chunk · 元信息抽取</span></div><div class="card lane" style="--c:#10b981"><strong>湖仓沉淀</strong><span>Snapshot · Schema · Lineage · Quality</span></div><div class="card lane" style="--c:#f97316"><strong>特征工程</strong><span>Offline Feature · Online Feature · Serving</span></div><div class="card lane" style="--c:#8b5cf6"><strong>向量检索</strong><span>Embedding · Vector Index · Recall · Rerank</span></div></div>
    <div class="card cluster"><h2>智能应用</h2><div class="app"><i>BI</i><div><strong>统计分析</strong><span>指标与看板</span></div></div><div class="app"><i>FS</i><div><strong>特征服务</strong><span>训练/推理一致</span></div></div><div class="app"><i>RAG</i><div><strong>检索增强</strong><span>上下文召回</span></div></div><div class="app"><i>AG</i><div><strong>Agent</strong><span>任务执行与反馈</span></div></div></div></div>
    <div class="feedback"><strong>反馈闭环</strong><span>在线效果、用户反馈、召回质量、延迟成本回流到数据质量、特征与索引更新。</span></div>
    ${logic(["多源采集", "湖仓沉淀", "特征/向量", "RAG 应用", "评估反馈"])}
  </section>`;

  if (kind === "matrix") return `
  <section class="figure"><div class="card matrix-main"><div class="axis x">延迟目标：T+1 → 分钟级 → 秒级 → 交互式</div><div class="axis y">数据形态 / 服务模式</div><div class="mx c1"><strong>Hive / Spark</strong><span>离线数仓 · 宽表加工 · 批量回刷</span></div><div class="mx c2"><strong>Kafka + Flink</strong><span>CDC · 状态流 · 窗口聚合 · exactly-once</span></div><div class="mx c3"><strong>ClickHouse / Doris</strong><span>低延迟 OLAP · 高并发看板 · 明细聚合</span></div><div class="mx c4"><strong>Trino</strong><span>联邦查询 · 即席分析 · 统一查询入口</span></div><div class="mx c5"><strong>Iceberg / Hudi / Delta</strong><span>表语义 · 快照 · 事务 · 多引擎共享</span></div><div class="mx c6"><strong>Feature / Vector / RAG</strong><span>特征服务 · 向量检索 · 智能应用</span></div></div><div class="decision"><div class="card pill"><strong>1 延迟</strong><span>T+1 / 分钟 / 秒级</span></div><div class="card pill"><strong>2 形态</strong><span>批量 / CDC / 无界流</span></div><div class="card pill"><strong>3 一致性</strong><span>最终一致 / exactly-once</span></div><div class="card pill"><strong>4 服务</strong><span>任务 / SQL / 在线服务</span></div></div>${logic(["延迟目标", "数据形态", "一致性", "服务模式", "组件组合"])}${legend([["#2563eb","批处理加工"],["#10b981","实时链路"],["#f97316","低延迟服务"],["#8b5cf6","湖仓与 AI"]])}</section>`;

  return `
  <section class="figure"><div class="card metro"><div class="hub">面试<br/>能力<br/>中心</div><div class="line m1"><strong>底层原理线</strong>${["HDFS","Block","副本","Shuffle","DAG","YARN"].map(s=>`<span>${s}</span>`).join("")}</div><div class="line m2"><strong>离线数仓线</strong>${["Hive","分层","建模","SQL 优化","Spark 调优"].map(s=>`<span>${s}</span>`).join("")}</div><div class="line m3"><strong>实时链路线</strong>${["Kafka","CDC","Flink","Watermark","Checkpoint"].map(s=>`<span>${s}</span>`).join("")}</div><div class="line m4"><strong>分析服务线</strong>${["Trino","ClickHouse","Doris","MPP","向量化"].map(s=>`<span>${s}</span>`).join("")}</div><div class="line m5"><strong>湖仓治理线</strong>${["Iceberg","Snapshot","Schema","Lineage","Quality"].map(s=>`<span>${s}</span>`).join("")}</div><div class="line m6"><strong>项目表达线</strong>${["背景","难点","选型","优化","指标","复盘"].map(s=>`<span>${s}</span>`).join("")}</div></div><div class="answer"><strong>高分回答公式</strong><span>问题背景 → 技术选择 → 架构链路 → 瓶颈定位 → 指标收益 → 复盘边界</span></div>${logic(["知识点", "工程问题", "项目证据", "指标收益", "复盘边界"])}${legend([["#2563eb","底层原理"],["#10b981","离线数仓"],["#f97316","实时链路"],["#8b5cf6","分析与治理"]])}</section>`;
}

(async () => {
  const chromePaths = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
  ];
  const executablePath = chromePaths.find((p) => fs.existsSync(p));
  const browser = await chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}) });
  const page = await browser.newPage({ viewport: { width: 1800, height: 1200 }, deviceScaleFactor: 2 });
  for (const p of pages) {
    await page.setContent(base(p.title, p.subtitle, bodyFor(p.kind)), { waitUntil: "networkidle" });
    await page.screenshot({ path: path.join(outDir, p.file), fullPage: false });
    console.log(`wrote ${p.file}`);
  }
  await browser.close();
})();
