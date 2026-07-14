#!/usr/bin/env python3
"""Generate a daily stock market summary report.

The script is designed for scheduled automation. By default it summarizes a
set of broad market indices, but the ticker list can be overridden with the
MARKET_TICKERS environment variable.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_OUTPUT_DIR = Path("reports")
DEFAULT_TICKERS = {
    "上证指数": "000001.SS",
    "深证成指": "399001.SZ",
    "创业板指": "399006.SZ",
    "恒生指数": "^HSI",
    "日经225": "^N225",
    "标普500": "^GSPC",
    "纳斯达克": "^IXIC",
    "道琼斯": "^DJI",
}
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=7d&interval=1d"


@dataclass(frozen=True)
class MarketMove:
    name: str
    ticker: str
    close: float
    previous_close: float
    change: float
    change_pct: float
    volume: int | None


def parse_tickers(raw_tickers: str | None) -> dict[str, str]:
    """Parse `Name=SYMBOL,Name2=SYMBOL2` overrides from an environment value."""
    if not raw_tickers:
        return DEFAULT_TICKERS

    tickers: dict[str, str] = {}
    for item in raw_tickers.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                "MARKET_TICKERS must use 'Name=SYMBOL' entries separated by commas."
            )
        name, symbol = item.split("=", 1)
        tickers[name.strip()] = symbol.strip()

    if not tickers:
        raise ValueError("MARKET_TICKERS did not contain any valid ticker entries.")
    return tickers


def fetch_json(url: str) -> dict:
    request = Request(url, headers={"User-Agent": "daily-market-summary/1.0"})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_market_move(name: str, ticker: str) -> MarketMove | None:
    """Fetch the latest close and compare it with the previous close."""
    url = YAHOO_CHART_URL.format(ticker=quote(ticker, safe=""))
    try:
        payload = fetch_json(url)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Failed to fetch {name}({ticker}): {exc}")
        return None

    results = payload.get("chart", {}).get("result") or []
    if not results:
        return None

    quote_data = (results[0].get("indicators", {}).get("quote") or [{}])[0]
    closes = quote_data.get("close") or []
    volumes = quote_data.get("volume") or []
    valid_points = [
        (index, close)
        for index, close in enumerate(closes)
        if isinstance(close, int | float)
    ]
    if len(valid_points) < 2:
        return None

    latest_index, close = valid_points[-1]
    _, previous_close = valid_points[-2]
    change = float(close) - float(previous_close)
    change_pct = (change / float(previous_close) * 100) if previous_close else 0.0
    volume_value = volumes[latest_index] if latest_index < len(volumes) else None
    volume = int(volume_value) if isinstance(volume_value, int | float) else None

    return MarketMove(
        name=name,
        ticker=ticker,
        close=float(close),
        previous_close=float(previous_close),
        change=change,
        change_pct=change_pct,
        volume=volume,
    )


def format_signed(value: float, digits: int = 2) -> str:
    return f"{value:+.{digits}f}"


def render_report(moves: Iterable[MarketMove], generated_at: datetime) -> str:
    rows = list(moves)
    lines = [
        f"# 每日股市变化梳理（{generated_at:%Y-%m-%d}）",
        "",
        f"生成时间：{generated_at:%Y-%m-%d %H:%M:%S %Z}",
        "",
        "## 指数表现",
        "",
        "| 市场/指数 | 代码 | 最新收盘 | 涨跌 | 涨跌幅 | 成交量 |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]

    for move in sorted(rows, key=lambda item: item.change_pct, reverse=True):
        volume = "-" if move.volume is None else f"{move.volume:,}"
        lines.append(
            "| "
            f"{move.name} | {move.ticker} | {move.close:.2f} | "
            f"{format_signed(move.change)} | {format_signed(move.change_pct)}% | {volume} |"
        )

    if rows:
        strongest = max(rows, key=lambda item: item.change_pct)
        weakest = min(rows, key=lambda item: item.change_pct)
        up_count = sum(1 for row in rows if row.change > 0)
        down_count = sum(1 for row in rows if row.change < 0)
        flat_count = len(rows) - up_count - down_count
        lines.extend(
            [
                "",
                "## 摘要",
                "",
                f"- 覆盖 {len(rows)} 个指数：上涨 {up_count} 个，下跌 {down_count} 个，持平 {flat_count} 个。",
                f"- 表现最强：{strongest.name}（{format_signed(strongest.change_pct)}%）。",
                f"- 表现最弱：{weakest.name}（{format_signed(weakest.change_pct)}%）。",
            ]
        )
    else:
        lines.extend(["", "## 摘要", "", "- 未获取到足够的行情数据，请检查数据源或代码配置。"])

    lines.extend(
        [
            "",
            "## 说明",
            "",
            "- 数据来源：Yahoo Finance chart 接口。",
            "- 如需调整覆盖范围，请设置 `MARKET_TICKERS` 环境变量，例如：`上证指数=000001.SS,恒生指数=^HSI`。",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a daily market summary report.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the Markdown report will be written.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("MARKET_TIMEZONE", DEFAULT_TIMEZONE),
        help="Timezone used for report timestamps.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    timezone = ZoneInfo(args.timezone)
    generated_at = datetime.now(timezone)
    tickers = parse_tickers(os.getenv("MARKET_TICKERS"))

    moves: list[MarketMove] = []
    missing: list[str] = []
    for name, ticker in tickers.items():
        move = fetch_market_move(name, ticker)
        if move is None:
            missing.append(f"{name}({ticker})")
            continue
        moves.append(move)

    report = render_report(moves, generated_at)
    if missing:
        report += "\n## 未获取到数据\n\n" + "\n".join(f"- {item}" for item in missing) + "\n"

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / f"daily-market-summary-{generated_at:%Y-%m-%d}.md"
    output_path.write_text(report, encoding="utf-8")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
