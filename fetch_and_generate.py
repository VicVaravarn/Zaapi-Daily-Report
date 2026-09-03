#!/usr/bin/env python3
"""
Zaapi Daily Slack Report
Fetches sales & marketing data from Google Sheets and posts a summary to Slack.
"""

import csv
import sys
import io
import argpars
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any


class GoogleSheetsFetcher:
    """Handles fetching and parsing Google Sheets CSV data."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def fetch_sheet(self, sheet_id: str, sheet_name: str = None,
                    gid: str = None) -> Optional[List[List[str]]]:
        """Fetch and parse a Google Sheet as CSV data."""
        try:
            if gid is not None:
                url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                       f"/export?format=csv&gid={gid}")
            else:
                url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                       f"/gviz/tq?tqx=out:csv&sheet={sheet_name}")
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            response.raise_for_status()
            reader = csv.reader(io.StringIO(response.text))
            return list(reader)
        except Exception as e:
            print(f"Error fetching sheet: {e}", file=sys.stderr)
            return None


class WeekViewParser:
    """Parses the 'Week view' tab of the new Sales Huddle sheet.

    Sheet: 1i2sf3IOvzEvjR6vLpJx4kxMb-EPeqFO3xrkXQqABYZg
    Tab: Week view

    The tab auto-repaints for the current week — no need to compute CW names.

    Layout per section:
      - Section header row: col 1 contains "TH-Outbound" / "TH-Inbound" / "INTL-Inbound"
      - Agent header row: agent names at cols 6, 9, 12, 15 (first agent unnamed)
      - Metric rows: col 1 = metric name ("Won", "Won (New)", "Renewal")
        col 3 = WTD total, cols 6/9/12/15 = per-agent WTD
    """

    TOTAL_WTD_COL = 3
    AGENT_WTD_COLS = [6, 9, 12, 15]

    SECTIONS = {
        "th_outbound": {
            "header": "TH-Outbound",
            "agents": ["Yayee", "Toey"],
            "metrics": ["Won"],
        },
        "th_inbound": {
            "header": "TH-Inbound",
            "agents": ["James", "Run", "Ta", "Dee"],
            "metrics": ["Won (New)", "Renewal"],
        },
        "intl_inbound": {
            "header": "INTL-Inbound",
            "agents": ["KoKo", "Thanom", "Patty"],
            "metrics": ["Won", "Renewal"],
        },
    }

    def __init__(self, sheet_data: List[List[str]]):
        self.data = sheet_data

    def _get_cell(self, row_idx: int, col_idx: int) -> str:
        try:
            if row_idx < len(self.data) and col_idx < len(self.data[row_idx]):
                return self.data[row_idx][col_idx].strip()
        except (IndexError, AttributeError):
            pass
        return ""

    def _find_section_start(self, header_text: str) -> Optional[int]:
        """Find the row index where a section header appears in col 1."""
        for i, row in enumerate(self.data):
            if len(row) > 1 and row[1].strip() == header_text:
                return i
        return None

    def _find_metric_row(self, start_row: int, metric_name: str,
                         max_rows: int = 30) -> Optional[int]:
        """Find a metric row by name within a section (searches col 1)."""
        target = metric_name.lower().strip()
        for i in range(start_row, min(start_row + max_rows, len(self.data))):
            cell = self._get_cell(i, 1).lower().strip()
            if cell == target:
                return i
        return None

    @staticmethod
    def _to_int(val: str) -> int:
        try:
            return int(float(val.replace(",", "").strip() or "0"))
        except (ValueError, TypeError):
            return 0

    def parse(self) -> Dict[str, Any]:
        """Parse all sections and return structured sales data."""
        result = {}

        for key, config in self.SECTIONS.items():
            section_data = {"agents": config["agents"], "metrics": {}}
            start = self._find_section_start(config["header"])

            if start is None:
                print(f"Warning: Section '{config['header']}' not found",
                      file=sys.stderr)
                for metric in config["metrics"]:
                    section_data["metrics"][metric] = {
                        "total": 0,
                        "by_agent": {a: 0 for a in config["agents"]}
                    }
                result[key] = section_data
                continue

            print(f"  Found '{config['header']}' at row {start}")

            for metric in config["metrics"]:
                row_idx = self._find_metric_row(start, metric)
                metric_data = {"total": 0, "by_agent": {}}

                if row_idx is not None:
                    metric_data["total"] = self._to_int(
                        self._get_cell(row_idx, self.TOTAL_WTD_COL))
                    for i, agent in enumerate(config["agents"]):
                        col = self.AGENT_WTD_COLS[i]
                        metric_data["by_agent"][agent] = self._to_int(
                            self._get_cell(row_idx, col))
                    print(f"    {metric}: total={metric_data['total']}, "
                          f"agents={metric_data['by_agent']}")
                else:
                    print(f"  Warning: Metric '{metric}' not found in "
                          f"'{config['header']}'", file=sys.stderr)
                    for agent in config["agents"]:
                        metric_data["by_agent"][agent] = 0

                section_data["metrics"][metric] = metric_data

            result[key] = section_data

        return result


class RegistrationWeeklyParser:
    """Parses the registration_weekly tab of the Ads Data sheet.

    Source sheet: 1s5AC58mAylpSDknU7L7HRJUPrVf36b0TvzD35tW-Wdw
    Tab: registration_weekly

    Columns (1-indexed):
      A cw, B week_start_mon, C week_end_sun, D region, E market,
      F ad_source, G ad_campaign_id,
      H verified, I integrated, J qualified, K highly_qualified,
      L premium, M best, N hqplus, O total

    Aggregates the current ISO-week rows into:
      - GLOBAL (grand total)
      - TH, SEA, ROW (region rollups)
    """

    TH_MARKETS = {"TH", "THAILAND"}
    SEA_SUBMARKET_MAP = {
        "MY": "MY", "MALAYSIA": "MY",
        "SG": "SG", "SINGAPORE": "SG",
        "PH": "PH", "PHILIPPINES": "PH",
    }
    SEA_OTHER_MARKETS = {
        "ID", "INDONESIA", "VN", "VIETNAM", "MM", "MYANMAR",
        "KH", "CAMBODIA", "LA", "LAOS", "BN", "BRUNEI",
        "TL", "TP", "EAST TIMOR",
    }
    UNATTRIBUTED_AD_SOURCES = {"", "UNKNOWN", "NONE", "N/A"}

    def __init__(self, sheet_data: List[List[str]], week_start_mon: str):
        self.data = sheet_data
        self.week_start_mon = week_start_mon

    @staticmethod
    def _to_int(value: str) -> int:
        try:
            return int(float(str(value).replace(",", "").strip() or "0"))
        except (ValueError, TypeError):
            return 0

    def _classify_market(self, market: str):
        if not market:
            return "ROW"
        key = market.strip().upper()
        if key in self.TH_MARKETS:
            return "TH"
        if key in self.SEA_SUBMARKET_MAP or key in self.SEA_OTHER_MARKETS:
            return "SEA"
        return "ROW"

    @staticmethod
    def _empty_bucket():
        return {"qualified_wtd": 0, "hqplus_wtd": 0, "total_wtd": 0}

    @classmethod
    def _stringify(cls, bucket):
        return {k: str(v) for k, v in bucket.items()}

    def parse_data(self) -> Dict[str, Any]:
        regions = {r: self._empty_bucket() for r in ["GLOBAL", "TH", "SEA", "ROW"]}
        result = {"regions": regions, "total": self._empty_bucket()}

        if not self.data or len(self.data) < 2:
            print("Warning: registration_weekly is empty", file=sys.stderr)
            return self._finalize(result)

        header = [c.strip().lower() for c in self.data[0]]
        try:
            idx_week = header.index("week_start_mon")
            idx_market = header.index("market")
            idx_qualified = header.index("qualified")
            idx_hqplus = header.index("hqplus")
        except ValueError as e:
            print(f"Error: registration_weekly missing column: {e}",
                  file=sys.stderr)
            return self._finalize(result)

        max_idx = max(idx_week, idx_market, idx_qualified, idx_hqplus)
        matched = 0
        for row in self.data[1:]:
            if len(row) <= max_idx:
                continue
            if row[idx_week].strip() != self.week_start_mon:
                continue
            matched += 1
            market = row[idx_market]
            q = self._to_int(row[idx_qualified])
            h = self._to_int(row[idx_hqplus])
            t = q + h

            region = self._classify_market(market)

            def _add(bucket):
                bucket["qualified_wtd"] += q
                bucket["hqplus_wtd"] += h
                bucket["total_wtd"] += t

            _add(regions["GLOBAL"])
            _add(regions[region])
            _add(result["total"])

        print(f"  Matched {matched} registration_weekly rows for "
              f"week_start_mon={self.week_start_mon}")
        return self._finalize(result)

    def _finalize(self, result):
        for r, bucket in result["regions"].items():
            result["regions"][r] = self._stringify(bucket)
        result["total"] = self._stringify(result["total"])
        return result


class SlackNotifier:
    """Posts daily summary to Slack via Incoming Webhook."""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @staticmethod
    def _agent_line(by_agent: Dict[str, int]) -> str:
        """Format: Agent1: *X*  |  Agent2: *Y*  |  ..."""
        return "  |  ".join(f"{name}: *{val}*" for name, val in by_agent.items())

    def post_summary(self, sales_data: Dict[str, Any],
                     marketing_data: Dict[str, Any]) -> bool:
        try:
            now = datetime.now()
            date_str = now.strftime("%a %-d %b")
            cw = now.isocalendar()[1]

            # --- Marketing (unchanged) ---
            mktg_total = marketing_data.get("total", {})
            mktg_regions = marketing_data.get("regions", {})
            total_leads = mktg_total.get("total_wtd", "0")
            qualified = mktg_total.get("qualified_wtd", "0")
            hqplus = mktg_total.get("hqplus_wtd", "0")

            def _mkt_row(label, key):
                d = mktg_regions.get(key, {})
                t = d.get("total_wtd", "0")
                q = d.get("qualified_wtd", "0")
                h = d.get("hqplus_wtd", "0")
                return f"` {label:<6} {q:>3}   {h:>3}    {t:>3} `"

            marketing_table = "\n".join([
                "` Region    Q   HQ+  Total `",
                _mkt_row("TH", "TH"),
                _mkt_row("SEA", "SEA"),
                _mkt_row("ROW", "ROW"),
            ])

            # --- Sales ---
            th_ob = sales_data.get("th_outbound", {})
            th_ib = sales_data.get("th_inbound", {})
            intl = sales_data.get("intl_inbound", {})

            # TH-Outbound: Won only
            ob_won = th_ob.get("metrics", {}).get("Won", {})
            ob_won_total = ob_won.get("total", 0)
            ob_won_agents = self._agent_line(ob_won.get("by_agent", {}))

            # TH-Inbound: Won + Renewal
            ib_won = th_ib.get("metrics", {}).get("Won (New)", {})
            ib_ren = th_ib.get("metrics", {}).get("Renewal", {})
            ib_won_total = ib_won.get("total", 0)
            ib_ren_total = ib_ren.get("total", 0)
            ib_won_agents = self._agent_line(ib_won.get("by_agent", {}))
            ib_ren_agents = self._agent_line(ib_ren.get("by_agent", {}))

            # INTL-Inbound: Won + Renewal
            intl_won = intl.get("metrics", {}).get("Won", {})
            intl_ren = intl.get("metrics", {}).get("Renewal", {})
            intl_won_total = intl_won.get("total", 0)
            intl_ren_total = intl_ren.get("total", 0)
            intl_won_agents = self._agent_line(intl_won.get("by_agent", {}))
            intl_ren_agents = self._agent_line(intl_ren.get("by_agent", {}))

            message = (
                f":chart_with_upwards_trend: *Zaapi daily report — "
                f"{date_str} (CW{cw})*\n"
                f"\n"
                f"*Marketing — lead overview (WTD)*\n"
                f"*GLOBAL:* {total_leads} total  |  Qualified: {qualified}"
                f"  |  HQ+: {hqplus}\n"
                f"{marketing_table}\n"
                f"\n"
                f"*TH-Outbound — Won (WTD)*\n"
                f"Total: *{ob_won_total}*  —  {ob_won_agents}\n"
                f"\n"
                f"*TH-Inbound (WTD)*\n"
                f"Won: *{ib_won_total}*  —  {ib_won_agents}\n"
                f"Renewal: *{ib_ren_total}*  —  {ib_ren_agents}\n"
                f"\n"
                f"*INTL-Inbound (WTD)*\n"
                f"Won: *{intl_won_total}*  —  {intl_won_agents}\n"
                f"Renewal: *{intl_ren_total}*  —  {intl_ren_agents}"
            )

            payload = {"text": message}
            response = requests.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()

            if response.text == "ok":
                print("Slack message posted successfully")
                return True
            else:
                print(f"Slack webhook error: {response.text}", file=sys.stderr)
                return False

        except Exception as e:
            print(f"Error posting to Slack: {e}", file=sys.stderr)
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Zaapi Daily Slack Report"
    )
    parser.add_argument(
        "--slack-webhook-url",
        required=True,
        help="Slack Incoming Webhook URL"
    )
    args = parser.parse_args()

    print("Starting Zaapi Daily Report...")
    fetcher = GoogleSheetsFetcher()

    # 1. Fetch sales data from new Week view tab
    print("Fetching sales data (Week view)...")
    sales_sheet = fetcher.fetch_sheet(
        "1i2sf3IOvzEvjR6vLpJx4kxMb-EPeqFO3xrkXQqABYZg",
        sheet_name="Week view"
    )

    sales_data = {}
    if sales_sheet:
        week_parser = WeekViewParser(sales_sheet)
        sales_data = week_parser.parse()
    else:
        print("Warning: Sales data not available", file=sys.stderr)

    # 2. Fetch marketing data (unchanged source)
    print("Fetching marketing data (registration_weekly)...")
    marketing_sheet = fetcher.fetch_sheet(
        "1s5AC58mAylpSDknU7L7HRJUPrVf36b0TvzD35tW-Wdw",
        gid="859536577"
    )

    _today = datetime.now().date()
    current_week_start_mon = (_today - timedelta(days=_today.weekday())).isoformat()
    print(f"  Current week_start_mon: {current_week_start_mon}")

    marketing_data = {"regions": {}, "total": {}}
    if marketing_sheet:
        mkt_parser = RegistrationWeeklyParser(marketing_sheet, current_week_start_mon)
        marketing_data = mkt_parser.parse_data()
    else:
        print("Warning: Marketing data not available", file=sys.stderr)

    # 3. Post to Slack
    notifier = SlackNotifier(args.slack_webhook_url)
    success = notifier.post_summary(sales_data, marketing_data)

    if success:
        print("Done!")
    else:
        print("Report completed with Slack posting errors", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
