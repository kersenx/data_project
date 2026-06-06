# Data Project

## 每日股市变化梳理

本仓库包含一个 GitHub Actions 定时任务，用于在每日 16:00（Asia/Shanghai）生成股市变化 Markdown 报告。

- 定时任务文件：`.github/workflows/daily-market-summary.yml`
- 生成脚本：`scripts/daily_market_summary.py`
- 默认输出目录：`reports/`

工作流会在 08:00 UTC 触发，对应 Asia/Shanghai 的 16:00。也可以在 GitHub Actions 页面通过 `workflow_dispatch` 手动触发。

### 自定义关注标的

默认脚本会梳理上证指数、深证成指、创业板指、恒生指数、日经 225、标普 500、纳斯达克和道琼斯。若需调整范围，可以在工作流中设置 `MARKET_TICKERS`：

```bash
MARKET_TICKERS='上证指数=000001.SS,恒生指数=^HSI,标普500=^GSPC'
```

格式为以英文逗号分隔的 `名称=Yahoo Finance代码`。

### 本地运行

```bash
pip install -r requirements.txt
python scripts/daily_market_summary.py
```
