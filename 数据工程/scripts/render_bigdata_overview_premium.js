const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const outDir = path.resolve("assets/bigdata-evolution");
fs.mkdirSync(outDir, { recursive: true });

const html = `<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<style>
*{box-sizing:border-box}
body{margin:0;background:#eef3f8;font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",Arial,sans-serif;color:#152033}
.canvas{width:1800px;height:1200px;position:relative;overflow:hidden;padding:54px 64px;background:
  radial-gradient(circle at 10% 8%,rgba(37,99,235,.12),transparent 26%),
  radial-gradient(circle at 90% 12%,rgba(16,185,129,.11),transparent 24%),
  linear-gradient(180deg,#fbfdff 0%,#f6f9fd 64%,#ffffff 100%)}
.canvas:before{content:"";position:absolute;inset:26px;border:1px solid #d9e3ef;border-radius:34px;pointer-events:none}
.grid-bg{position:absolute;inset:0;background-image:linear-gradient(#e8eef6 1px,transparent 1px),linear-gradient(90deg,#e8eef6 1px,transparent 1px);background-size:36px 36px;opacity:.42;mask-image:linear-gradient(180deg,black 0%,transparent 82%)}
header{position:relative;z-index:2;text-align:center;margin-bottom:24px}.kicker{font-size:18px;font-weight:900;letter-spacing:.18em;color:#2563eb;text-transform:uppercase}.title{font-size:58px;font-weight:950;letter-spacing:0;margin:8px 0 12px}.subtitle{display:inline-block;max-width:1180px;padding:12px 24px;border-radius:999px;background:white;border:1px solid #ccd8e6;box-shadow:0 10px 28px rgba(15,23,42,.06);font-size:23px;line-height:1.4;color:#405066}
.figure{position:relative;z-index:1;height:890px;margin-top:18px}
.left,.right{position:absolute;top:54px;width:310px}.left{left:18px}.right{right:18px}
.side-title{font-size:24px;font-weight:950;margin:0 0 16px;color:#0f172a}.pressure,.outcome{height:92px;margin:13px 0;border-radius:18px;background:rgba(255,255,255,.9);border:1px solid #dbe4f0;box-shadow:0 12px 30px rgba(15,23,42,.06);display:grid;grid-template-columns:48px 1fr;gap:12px;align-items:center;padding:15px}
.pressure i,.outcome i{width:42px;height:42px;border-radius:14px;background:var(--c);display:flex;align-items:center;justify-content:center;color:white;font-style:normal;font-weight:950;font-size:18px;box-shadow:0 8px 18px color-mix(in srgb,var(--c) 28%,transparent)}
.pressure strong,.outcome strong{display:block;font-size:20px}.pressure span,.outcome span{display:block;font-size:15px;color:#64748b;line-height:1.25;margin-top:4px}
.main{position:absolute;left:356px;right:356px;top:38px;height:690px;border-radius:34px;background:rgba(255,255,255,.78);border:1px solid #dbe4f0;box-shadow:0 28px 80px rgba(15,23,42,.08);padding:34px 38px;overflow:hidden}
.main:before{content:"";position:absolute;inset:0;background:linear-gradient(90deg,rgba(239,246,255,.68),transparent 42%,rgba(236,253,245,.62));pointer-events:none}
.axis-title{position:absolute;left:38px;top:24px;font-size:20px;font-weight:900;color:#334155}.era-row{position:absolute;left:60px;right:60px;top:84px;height:88px;display:grid;grid-template-columns:repeat(6,1fr);gap:10px}
.era{position:relative;border-radius:18px;background:white;border:1px solid #cbd5e1;text-align:center;padding:13px 8px;box-shadow:0 10px 24px rgba(15,23,42,.06)}
.era strong{display:block;font-size:18px;color:#0f172a}.era span{font-size:13px;color:#64748b;line-height:1.2}.era:after{content:"";position:absolute;right:-14px;top:36px;width:18px;height:3px;background:#94a3b8}.era:last-child:after{display:none}
.bands{position:absolute;left:65px;right:65px;top:235px;height:330px}
.band{position:absolute;left:0;right:0;height:62px;border-radius:999px;background:linear-gradient(90deg,var(--c1),var(--c2));box-shadow:0 18px 40px color-mix(in srgb,var(--c1) 16%,transparent);display:flex;align-items:center;padding:0 26px;color:white;font-weight:950;font-size:22px}
.band span{margin-left:auto;font-size:17px;font-weight:700;opacity:.92}
.band.b1{top:0;--c1:#2563eb;--c2:#38bdf8;width:58%}
.band.b2{top:76px;left:10%;--c1:#0ea5e9;--c2:#10b981;width:65%}
.band.b3{top:152px;left:23%;--c1:#16a34a;--c2:#84cc16;width:58%}
.band.b4{top:228px;left:40%;--c1:#f97316;--c2:#ef4444;width:56%}
.connector{position:absolute;left:50%;top:184px;width:2px;height:420px;background:linear-gradient(#cbd5e1,transparent);opacity:.8}
.capability-map{position:absolute;left:62px;right:62px;bottom:36px;height:72px;display:grid;grid-template-columns:repeat(5,1fr);gap:14px}
.cap{border-radius:16px;background:#f8fafc;border:1px solid #dbe4f0;display:flex;align-items:center;justify-content:center;text-align:center;font-size:17px;font-weight:900;color:#334155}
.logic-strip{position:absolute;left:356px;right:356px;bottom:40px;height:132px;border-radius:26px;background:#101827;color:white;padding:22px 28px;display:grid;grid-template-columns:210px 1fr;gap:24px;box-shadow:0 24px 60px rgba(15,23,42,.18)}
.logic-strip h2{margin:0;font-size:28px}.logic-flow{display:flex;align-items:center;gap:12px}.logic-flow div{height:72px;min-width:128px;border-radius:18px;background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.15);display:flex;align-items:center;justify-content:center;text-align:center;font-size:17px;font-weight:900;line-height:1.22}.logic-flow b{color:#93c5fd;font-size:24px}
.mini-matrix{position:absolute;left:18px;bottom:40px;width:310px;border-radius:24px;background:white;border:1px solid #dbe4f0;box-shadow:0 18px 45px rgba(15,23,42,.08);padding:20px}.mini-matrix h3{font-size:21px;margin:0 0 14px}.matrix-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.matrix-grid div{border-radius:14px;padding:12px;background:#f8fafc;border:1px solid #e2e8f0;text-align:center;font-size:15px;font-weight:900;color:#334155}.matrix-grid span{display:block;color:#2563eb;margin-top:3px}
.legend{position:absolute;right:18px;bottom:40px;width:310px;border-radius:24px;background:white;border:1px solid #dbe4f0;box-shadow:0 18px 45px rgba(15,23,42,.08);padding:20px}.legend h3{font-size:21px;margin:0 0 14px}.legend p{margin:10px 0;font-size:16px;color:#475569;line-height:1.35}.legend i{display:inline-block;width:16px;height:16px;border-radius:5px;margin-right:8px;vertical-align:-2px;background:var(--c)}
</style>
</head>
<body>
<div class="canvas">
  <div class="grid-bg"></div>
  <header>
    <div class="kicker">Research Synthesis Figure</div>
    <div class="title">大数据技术演进总览</div>
    <div class="subtitle">以“工程压力 → 技术代际 → 平台能力”为主线，理解大数据从分布式底座走向湖仓与 AI 数据工程</div>
  </header>
  <section class="figure">
    <aside class="left">
      <h2 class="side-title">工程压力</h2>
      <div class="pressure" style="--c:#2563eb"><i>1</i><div><strong>数据放不下</strong><span>容量、容错、恢复窗口到达边界</span></div></div>
      <div class="pressure" style="--c:#0ea5e9"><i>2</i><div><strong>单机算不动</strong><span>计算任务需要自动并行化</span></div></div>
      <div class="pressure" style="--c:#10b981"><i>3</i><div><strong>团队要 SQL</strong><span>从工程接口走向分析接口</span></div></div>
      <div class="pressure" style="--c:#f97316"><i>4</i><div><strong>不能只 T+1</strong><span>业务要求分钟级与秒级反馈</span></div></div>
      <div class="pressure" style="--c:#ef4444"><i>5</i><div><strong>查询要更快</strong><span>交互分析与指标服务低延迟</span></div></div>
      <div class="pressure" style="--c:#8b5cf6"><i>6</i><div><strong>AI 需要底座</strong><span>特征、向量、检索、反馈闭环</span></div></div>
    </aside>

    <main class="main">
      <div class="axis-title">技术代际与能力沉淀</div>
      <div class="era-row">
        <div class="era"><strong>Google 论文</strong><span>GFS · MR · Bigtable</span></div>
        <div class="era"><strong>Hadoop</strong><span>HDFS · Hive · YARN</span></div>
        <div class="era"><strong>Spark</strong><span>DAG · SQL · AQE</span></div>
        <div class="era"><strong>实时化</strong><span>Kafka · Flink · CDC</span></div>
        <div class="era"><strong>OLAP</strong><span>Trino · ClickHouse</span></div>
        <div class="era"><strong>湖仓 / AI</strong><span>Iceberg · Vector · RAG</span></div>
      </div>
      <div class="connector"></div>
      <div class="bands">
        <div class="band b1">分布式存储底座 <span>HDFS / Object Storage / Replica</span></div>
        <div class="band b2">统一批流计算 <span>MapReduce / Spark / Flink</span></div>
        <div class="band b3">SQL 与交互分析 <span>Hive / Trino / OLAP</span></div>
        <div class="band b4">湖仓治理与 AI 数据服务 <span>Iceberg / Feature / Vector / RAG</span></div>
      </div>
      <div class="capability-map">
        <div class="cap">可扩展存储</div>
        <div class="cap">可恢复计算</div>
        <div class="cap">统一 SQL</div>
        <div class="cap">低延迟服务</div>
        <div class="cap">AI 就绪数据</div>
      </div>
    </main>

    <aside class="right">
      <h2 class="side-title">平台能力</h2>
      <div class="outcome" style="--c:#2563eb"><i>A</i><div><strong>容量弹性</strong><span>从单机边界变为集群扩展</span></div></div>
      <div class="outcome" style="--c:#0ea5e9"><i>B</i><div><strong>计算复用</strong><span>多阶段、迭代、SQL 共用引擎</span></div></div>
      <div class="outcome" style="--c:#10b981"><i>C</i><div><strong>实时服务</strong><span>从离线报表到事件驱动</span></div></div>
      <div class="outcome" style="--c:#f97316"><i>D</i><div><strong>湖上表语义</strong><span>快照、事务、Schema 演进</span></div></div>
      <div class="outcome" style="--c:#8b5cf6"><i>E</i><div><strong>智能应用接口</strong><span>特征、向量、RAG 与反馈</span></div></div>
    </aside>

    <div class="mini-matrix">
      <h3>问题 × 代表技术</h3>
      <div class="matrix-grid">
        <div>放不下<span>HDFS</span></div>
        <div>算不动<span>Spark</span></div>
        <div>等太久<span>Flink</span></div>
        <div>查不快<span>OLAP</span></div>
        <div>湖难管<span>Iceberg</span></div>
        <div>服务 AI<span>RAG</span></div>
      </div>
    </div>

    <div class="logic-strip">
      <h2>核心逻辑</h2>
      <div class="logic-flow">
        <div>工程压力</div><b>→</b><div>架构回应</div><b>→</b><div>能力沉淀</div><b>→</b><div>新业务形态</div>
      </div>
    </div>

    <div class="legend">
      <h3>图例</h3>
      <p><i style="--c:#2563eb"></i>蓝色：存储与批计算底座</p>
      <p><i style="--c:#10b981"></i>绿色：实时与 SQL 服务</p>
      <p><i style="--c:#f97316"></i>橙色：湖仓治理能力</p>
      <p><i style="--c:#8b5cf6"></i>紫色：AI 数据工程能力</p>
    </div>
  </section>
</div>
</body>
</html>`;

(async () => {
  const chromePaths = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
  ];
  const executablePath = chromePaths.find((p) => fs.existsSync(p));
  const browser = await chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}) });
  const page = await browser.newPage({ viewport: { width: 1800, height: 1200 }, deviceScaleFactor: 2 });
  await page.setContent(html, { waitUntil: "networkidle" });
  await page.screenshot({ path: path.join(outDir, "01-overview-map.png"), fullPage: false });
  await browser.close();
  console.log("wrote assets/bigdata-evolution/01-overview-map.png");
})();
