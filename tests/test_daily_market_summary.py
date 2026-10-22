import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import daily_market_summary as market


class SummaryTests(unittest.TestCase):
    def run_summary(self, results):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        output = Path(temporary.name)
        with patch.dict(os.environ, {"MARKET_TICKERS": "A=AAA,B=BBB"}), patch(
            "sys.argv", ["summary", "--output-dir", str(output)]
        ), patch.object(market, "fetch_market_move", side_effect=results):
            status = market.main()
        reports = list(output.glob("daily-market-summary-*.md"))
        self.assertEqual(len(reports), 1)
        return status, reports[0].read_text(encoding="utf-8")

    def test_all_failed_returns_failure_with_diagnostic_report(self):
        status, report = self.run_summary([None, None])
        self.assertEqual(status, 1)
        self.assertIn("未获取到足够的行情数据", report)
        self.assertIn("A(AAA)", report)
        self.assertIn("B(BBB)", report)

    def test_partial_success_lists_missing_index(self):
        move = market.MarketMove("A", "AAA", 110, 100, 10, 10, 123)
        status, report = self.run_summary([move, None])
        self.assertEqual(status, 0)
        self.assertIn("覆盖 1 个指数", report)
        self.assertIn("B(BBB)", report)
        self.assertIn("+10.00%", report)

    def test_complete_success_has_all_rows(self):
        moves = [market.MarketMove(name, ticker, 110, 100, 10, 10, None)
                 for name, ticker in [("A", "AAA"), ("B", "BBB")]]
        status, report = self.run_summary(moves)
        self.assertEqual(status, 0)
        self.assertIn("覆盖 2 个指数", report)
        self.assertNotIn("## 未获取到数据", report)

    def test_insufficient_closes_are_missing(self):
        with patch.object(market, "fetch_json", return_value={"chart": {"result": [
            {"indicators": {"quote": [{"close": [None, 100]}]}}]}}):
            self.assertIsNone(market.fetch_market_move("A", "AAA"))


if __name__ == "__main__":
    unittest.main()
