#!/usr/bin/env python3
"""
Zaapi Daily Activity Report Generator
Fetches data from Google Sheets and generates a self-contained HTML dashboard.
"""

import csv
import sys
import io
import json
import argparse
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from typing import Dict, List, Tuple, Optional, Any


class GoogleSheetsFetcher:
    """Handles fetching and parsing Google Sheets CSV data."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_csv_url(self, sheet_id: str, sheet_name: str) -> str:
        """Generate CSV export URL for a Google Sheet."""
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    def fetch_sheet(self, sheet_id: str, sheet_name: str) -> Optional[List[List[str]]]:
        """Fetch and parse a Google Sheet as CSV data."""
        try:
            url = self.get_csv_url(sheet_id, sheet_name)
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()

            reader = csv.reader(io.StringIO(response.text))
            data = list(reader)
            return data
        except Exception as e:
            print(f"Error fetching sheet '{sheet_name}': {e}", file=sys.stderr)
            return None

    def get_current_week_sheet_name(self) -> str:
        """Get the current calendar week sheet name (e.g., 'CW10 2026')."""
        today = datetime.now()
        iso_calendar = today.isocalendar()
        week_number = iso_calendar[1]
        year = iso_calendar[0]
        return f"CW{week_number} {year}"


class SalesHuddleParser:
    """Parses Sales Huddle sheet data."""

    def __init__(self, sheet_data: List[List[str]]):
        self.data = sheet_data

    def get_cell(self, row: int, col: int) -> str:
        """Safely get a cell value, handling indices."""
        try:
            if row < len(self.data) and col < len(self.data[row]):
                return self.data[row][col].strip()
            return ""
        except:
            return ""

    def get_date_info(self) -> Dict[str, str]:
        """Extract date information from header section."""
        try:
            date_str = self.get_cell(1, 2)  # Row 2, Col C
            return {"date": date_str}
        except:
            return {"date": datetime.now().strftime("%d/%m/%Y")}

    def parse_outbound_section(self) -> Dict[str, Any]:
        """Parse the Outbound sales section."""
        result = {
            "agents": {},
            "funnel": [],
            "raw_data": []
        }

        try:
            # Outbound section starts at row 9 (index 9)
            # Agents: Yayee (J-L), Toey (M-O)
            # Row 12: Activities (Calls)
            # Row 13: Contact
            # Row 14: Demo Scheduled
            # Row 15: Demo Attended
            # Row 16: Contact to Demo %
            # Row 17: Won

            funnel_metrics = ["Activities (Calls)", "Contact", "Demo Scheduled", "Demo Attended", "Won"]
            funnel_rows = [12, 13, 14, 15, 17]  # Row indices for each metric

            # Extract target and total WTD
            target_wtd = self.get_cell(10, 2)  # Row 11, Col C (Target WTD Total)
            total_wtd = self.get_cell(11, 6)   # Row 12, Col G (Total WTD)
            total_daily = self.get_cell(11, 7)  # Row 12, Col H (Total Daily)
            total_vs_target = self.get_cell(11, 8)  # Row 12, Col I (WTD vs Target)

            result["summary"] = {
                "target_wtd": target_wtd,
                "total_wtd": total_wtd,
                "total_daily": total_daily,
                "total_vs_target": total_vs_target
            }

            # Parse each metric
            for metric_name, row_idx in zip(funnel_metrics, funnel_rows):
                metric_data = {
                    "name": metric_name,
                    "total_wtd": self.get_cell(row_idx, 6),
                    "total_daily": self.get_cell(row_idx, 7),
                    "total_vs_target": self.get_cell(row_idx, 8),
                    "yayee_wtd": self.get_cell(row_idx, 9),
                    "yayee_daily": self.get_cell(row_idx, 10),
                    "yayee_vs_target": self.get_cell(row_idx, 11),
                    "toey_wtd": self.get_cell(row_idx, 12),
                    "toey_daily": self.get_cell(row_idx, 13),
                    "toey_vs_target": self.get_cell(row_idx, 14)
                }
                result["funnel"].append(metric_data)

            # Parse Hot Deals section (rows 18-28)
            # Structure: Row 19 = "Hot Deal", Row 23 = "CTP", Row 27 = "Won" (0-indexed: 18, 22, 26)
            hot_deals = {
                "hot_deal": {"yayee": [], "toey": []},
                "ctp": {"yayee": [], "toey": []},
                "won": {"yayee": [], "toey": []}
            }

            current_category = None
            for row_idx in range(18, min(len(self.data), 30)):
                yayee_val = self.get_cell(row_idx, 9)
                toey_val = self.get_cell(row_idx, 12)

                # Check if this row is a category header
                check_val = yayee_val.lower().strip() if yayee_val else ""
                if check_val == "hot deal":
                    current_category = "hot_deal"
                    continue
                elif check_val == "ctp":
                    current_category = "ctp"
                    continue
                elif check_val == "won":
                    current_category = "won"
                    continue

                if current_category:
                    if yayee_val and yayee_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["yayee"].append(yayee_val)
                    if toey_val and toey_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["toey"].append(toey_val)

            result["hot_deals"] = hot_deals

        except Exception as e:
            print(f"Error parsing outbound section: {e}", file=sys.stderr)

        return result

    def parse_inbound_section(self) -> Dict[str, Any]:
        """Parse the Inbound sales section."""
        result = {
            "funnel": [],
            "hot_deals": {},
            "summary": {}
        }

        try:
            # Inbound section around row 9, columns R onwards
            # Agents: Pleum (AA-AC), Loogpad (AD-AF)
            # Same funnel rows as outbound

            funnel_metrics = ["Activities (Calls)", "Contact", "Demo Scheduled", "Demo Attended", "Won"]
            funnel_rows = [12, 13, 14, 15, 17]

            # Column offsets for Inbound
            total_wtd_col = 23     # X
            total_daily_col = 24   # Y
            total_vs_target_col = 25  # Z
            pleum_wtd_col = 26     # AA
            pleum_daily_col = 27   # AB
            pleum_vs_target_col = 28  # AC
            loogpad_wtd_col = 29   # AD
            loogpad_daily_col = 30 # AE
            loogpad_vs_target_col = 31  # AF

            # Extract summary
            target_wtd = self.get_cell(10, 19)  # T
            result["summary"] = {
                "target_wtd": target_wtd,
                "total_wtd": self.get_cell(11, total_wtd_col),
                "total_daily": self.get_cell(11, total_daily_col),
                "total_vs_target": self.get_cell(11, total_vs_target_col)
            }

            # Parse funnel metrics
            for metric_name, row_idx in zip(funnel_metrics, funnel_rows):
                metric_data = {
                    "name": metric_name,
                    "total_wtd": self.get_cell(row_idx, total_wtd_col),
                    "total_daily": self.get_cell(row_idx, total_daily_col),
                    "total_vs_target": self.get_cell(row_idx, total_vs_target_col),
                    "pleum_wtd": self.get_cell(row_idx, pleum_wtd_col),
                    "pleum_daily": self.get_cell(row_idx, pleum_daily_col),
                    "pleum_vs_target": self.get_cell(row_idx, pleum_vs_target_col),
                    "loogpad_wtd": self.get_cell(row_idx, loogpad_wtd_col),
                    "loogpad_daily": self.get_cell(row_idx, loogpad_daily_col),
                    "loogpad_vs_target": self.get_cell(row_idx, loogpad_vs_target_col)
                }
                result["funnel"].append(metric_data)

            # Parse Hot Deals section (rows 18-28)
            # Hot deal category headers are in the agent columns (Pleum col 26, Loogpad col 29)
            hot_deals = {
                "hot_deal": {"pleum": [], "loogpad": []},
                "ctp": {"pleum": [], "loogpad": []},
                "won": {"pleum": [], "loogpad": []}
            }

            current_category = None
            for row_idx in range(18, min(len(self.data), 30)):
                pleum_val = self.get_cell(row_idx, pleum_wtd_col)
                loogpad_val = self.get_cell(row_idx, loogpad_wtd_col)

                # Check if this row is a category header
                check_val = pleum_val.lower().strip() if pleum_val else ""
                if check_val == "hot deal":
                    current_category = "hot_deal"
                    continue
                elif check_val == "ctp":
                    current_category = "ctp"
                    continue
                elif check_val == "won":
                    current_category = "won"
                    continue

                if current_category:
                    if pleum_val and pleum_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["pleum"].append(pleum_val)
                    if loogpad_val and loogpad_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["loogpad"].append(loogpad_val)

            result["hot_deals"] = hot_deals

        except Exception as e:
            print(f"Error parsing inbound section: {e}", file=sys.stderr)

        return result

    def parse_renewal_section(self) -> Dict[str, Any]:
        """Parse the Account Management - Renewal section."""
        result = {"won": {}, "due_to_renew": []}

        try:
            # Renewal section starts around row 57
            renewal_start = 57

            # Find the renewal section
            for row_idx in range(renewal_start, min(len(self.data), 70)):
                label = self.get_cell(row_idx, 1)
                if "Renewal" in label or "Account Management" in label:
                    # Found the section, parse won data from next rows
                    won_row = row_idx + 2  # Usually 2 rows after header
                    result["won"] = {
                        "total_wtd": self.get_cell(won_row, 2),
                        "total_daily": self.get_cell(won_row, 3),
                        "pleum_wtd": self.get_cell(won_row, 4),
                        "pleum_daily": self.get_cell(won_row, 5),
                        "loogpad_wtd": self.get_cell(won_row, 6),
                        "loogpad_daily": self.get_cell(won_row, 7)
                    }

                    # Parse due to renew accounts
                    due_start = won_row + 2
                    for acc_row in range(due_start, min(len(self.data), due_start + 10)):
                        account_name = self.get_cell(acc_row, 2)
                        renewal_date = self.get_cell(acc_row, 3)
                        if account_name:
                            result["due_to_renew"].append({
                                "name": account_name,
                                "date": renewal_date
                            })
                    break

        except Exception as e:
            print(f"Error parsing renewal section: {e}", file=sys.stderr)

        return result

    def parse_intl_inbound_section(self) -> Dict[str, Any]:
        """Parse the International Inbound sales section."""
        result = {
            "funnel": [],
            "hot_deals": {},
            "summary": {}
        }

        try:
            # International Inbound section at row 10
            # Agents: Sheronika, Thanom
            # Funnel metrics at rows 13-18 (6 metrics)

            funnel_rows = [13, 14, 15, 16, 17, 18]

            # Column offsets for International Inbound
            label_col = 38         # AM - metric labels
            total_wtd_col = 43     # AR
            total_daily_col = 44   # AS
            total_vs_target_col = 45  # AT
            sheronika_wtd_col = 46    # AU
            sheronika_daily_col = 47  # AV
            sheronika_vs_target_col = 48  # AW
            thanom_wtd_col = 49    # AX
            thanom_daily_col = 50  # AY
            thanom_vs_target_col = 51  # AZ

            # Extract summary
            target_wtd = self.get_cell(10, 38)  # Row 11, col AM
            result["summary"] = {
                "target_wtd": target_wtd,
                "total_wtd": self.get_cell(11, total_wtd_col),
                "total_daily": self.get_cell(11, total_daily_col),
                "total_vs_target": self.get_cell(11, total_vs_target_col)
            }

            # Parse funnel metrics - read metric names dynamically from label column
            for row_idx in funnel_rows:
                metric_name = self.get_cell(row_idx, label_col)
                if not metric_name:
                    metric_name = f"Metric Row {row_idx}"

                metric_data = {
                    "name": metric_name,
                    "total_wtd": self.get_cell(row_idx, total_wtd_col),
                    "total_daily": self.get_cell(row_idx, total_daily_col),
                    "total_vs_target": self.get_cell(row_idx, total_vs_target_col),
                    "sheronika_wtd": self.get_cell(row_idx, sheronika_wtd_col),
                    "sheronika_daily": self.get_cell(row_idx, sheronika_daily_col),
                    "sheronika_vs_target": self.get_cell(row_idx, sheronika_vs_target_col),
                    "thanom_wtd": self.get_cell(row_idx, thanom_wtd_col),
                    "thanom_daily": self.get_cell(row_idx, thanom_daily_col),
                    "thanom_vs_target": self.get_cell(row_idx, thanom_vs_target_col)
                }
                result["funnel"].append(metric_data)

            # Parse Hot Deals section (rows 18-28)
            hot_deals = {
                "hot_deal": {"sheronika": [], "thanom": []},
                "ctp": {"sheronika": [], "thanom": []},
                "won": {"sheronika": [], "thanom": []}
            }

            current_category = None
            for row_idx in range(18, min(len(self.data), 30)):
                sheronika_val = self.get_cell(row_idx, sheronika_wtd_col)
                thanom_val = self.get_cell(row_idx, thanom_wtd_col)

                check_val = sheronika_val.lower().strip() if sheronika_val else ""
                if check_val == "hot deal":
                    current_category = "hot_deal"
                    continue
                elif check_val == "ctp":
                    current_category = "ctp"
                    continue
                elif check_val == "won":
                    current_category = "won"
                    continue

                if current_category:
                    if sheronika_val and sheronika_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["sheronika"].append(sheronika_val)
                    if thanom_val and thanom_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["thanom"].append(thanom_val)

            result["hot_deals"] = hot_deals

        except Exception as e:
            print(f"Error parsing international inbound section: {e}", file=sys.stderr)

        return result

    def parse_intl_outbound_section(self) -> Dict[str, Any]:
        """Parse the International Outbound sales section."""
        result = {
            "funnel": [],
            "summary": {},
            "hot_deals": {}
        }

        try:
            # International Outbound section
            # Agents: Sheronika, Thanom
            # Funnel metrics at rows 13-18 (6 metrics)

            funnel_rows = [13, 14, 15, 16, 17, 18]

            # Column offsets for International Outbound
            label_col = 54         # BC - metric labels
            total_wtd_col = 59     # BH
            total_daily_col = 60   # BI
            total_vs_target_col = 61  # BJ
            sheronika_wtd_col = 62    # BK
            sheronika_daily_col = 63  # BL
            sheronika_vs_target_col = 64  # BM
            thanom_wtd_col = 65    # BN
            thanom_daily_col = 66  # BO
            thanom_vs_target_col = 67  # BP

            # Extract summary
            target_wtd = self.get_cell(10, 54)  # Row 11, col BC
            result["summary"] = {
                "target_wtd": target_wtd,
                "total_wtd": self.get_cell(11, total_wtd_col),
                "total_daily": self.get_cell(11, total_daily_col),
                "total_vs_target": self.get_cell(11, total_vs_target_col)
            }

            # Parse funnel metrics - read metric names dynamically from label column
            for row_idx in funnel_rows:
                metric_name = self.get_cell(row_idx, label_col)
                if not metric_name:
                    metric_name = f"Metric Row {row_idx}"

                metric_data = {
                    "name": metric_name,
                    "total_wtd": self.get_cell(row_idx, total_wtd_col),
                    "total_daily": self.get_cell(row_idx, total_daily_col),
                    "total_vs_target": self.get_cell(row_idx, total_vs_target_col),
                    "sheronika_wtd": self.get_cell(row_idx, sheronika_wtd_col),
                    "sheronika_daily": self.get_cell(row_idx, sheronika_daily_col),
                    "sheronika_vs_target": self.get_cell(row_idx, sheronika_vs_target_col),
                    "thanom_wtd": self.get_cell(row_idx, thanom_wtd_col),
                    "thanom_daily": self.get_cell(row_idx, thanom_daily_col),
                    "thanom_vs_target": self.get_cell(row_idx, thanom_vs_target_col)
                }
                result["funnel"].append(metric_data)

            # Parse Hot Deals section (rows 18-28)
            hot_deals = {
                "hot_deal": {"sheronika": [], "thanom": []},
                "ctp": {"sheronika": [], "thanom": []},
                "won": {"sheronika": [], "thanom": []}
            }

            current_category = None
            for row_idx in range(18, min(len(self.data), 30)):
                sheronika_val = self.get_cell(row_idx, sheronika_wtd_col)
                thanom_val = self.get_cell(row_idx, thanom_wtd_col)

                check_val = sheronika_val.lower().strip() if sheronika_val else ""
                if check_val == "hot deal":
                    current_category = "hot_deal"
                    continue
                elif check_val == "ctp":
                    current_category = "ctp"
                    continue
                elif check_val == "won":
                    current_category = "won"
                    continue

                if current_category:
                    if sheronika_val and sheronika_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["sheronika"].append(sheronika_val)
                    if thanom_val and thanom_val.lower() not in ["hot deal", "ctp", "won"]:
                        hot_deals[current_category]["thanom"].append(thanom_val)

            result["hot_deals"] = hot_deals

        except Exception as e:
            print(f"Error parsing international outbound section: {e}", file=sys.stderr)

        return result


class MarketingSignupsParser:
    """Parses Marketing Sign-ups sheet data."""

    def __init__(self, sheet_data: List[List[str]]):
        self.data = sheet_data

    def get_cell(self, row: int, col: int) -> str:
        """Safely get a cell value."""
        try:
            if row < len(self.data) and col < len(self.data[row]):
                return self.data[row][col].strip()
            return ""
        except:
            return ""

    def parse_data(self) -> Dict[str, Any]:
        """Parse the marketing sign-ups data."""
        result = {
            "date": "",
            "regions": {},
            "total": {}
        }

        try:
            # Row 0: Date
            result["date"] = self.get_cell(0, 0)

            # CSV Row 0: Combined header (date + column names)
            # CSV Row 1: Total, Row 2: TH, Row 3: SEA, Row 4: ROW
            regions_map = {
                2: "TH",
                3: "SEA",
                4: "ROW"
            }

            for row_idx, region in regions_map.items():
                result["regions"][region] = {
                    "target_wtd": self.get_cell(row_idx, 2),
                    "target_daily": self.get_cell(row_idx, 3),
                    "total_wtd": self.get_cell(row_idx, 4),
                    "total_daily": self.get_cell(row_idx, 5),
                    "wtd_vs_target": self.get_cell(row_idx, 6),
                    "qualified_wtd": self.get_cell(row_idx, 7),
                    "qualified_daily": self.get_cell(row_idx, 8),
                    "highly_qualified_wtd": self.get_cell(row_idx, 9),
                    "highly_qualified_daily": self.get_cell(row_idx, 10),
                    "premium_wtd": self.get_cell(row_idx, 11),
                    "premium_daily": self.get_cell(row_idx, 12),
                    "best_wtd": self.get_cell(row_idx, 13),
                    "best_daily": self.get_cell(row_idx, 14)
                }

            # Total row is CSV row 1
            result["total"] = {
                "target_wtd": self.get_cell(1, 2),
                "target_daily": self.get_cell(1, 3),
                "total_wtd": self.get_cell(1, 4),
                "total_daily": self.get_cell(1, 5),
                "wtd_vs_target": self.get_cell(1, 6)
            }

        except Exception as e:
            print(f"Error parsing marketing sign-ups: {e}", file=sys.stderr)

        return result


class HTMLDashboardGenerator:
    """Generates a beautiful self-contained HTML dashboard."""

    ZAAPI_COLORS = {
        "primary": "#1e40af",      # Dark blue
        "secondary": "#0f766e",    # Dark teal
        "success": "#059669",       # Green
        "warning": "#d97706",       # Amber
        "danger": "#dc2626",        # Red
        "bg_dark": "#0f172a",       # Very dark blue
        "bg_card": "#1e293b",       # Dark slate
        "text_primary": "#f1f5f9",  # Light slate
        "text_secondary": "#cbd5e1", # Medium slate
        "border": "#334155"         # Border slate
    }

    def __init__(self):
        self.html_parts = []

    def get_target_color(self, achieved: str, target: str) -> str:
        """Determine color based on achievement percentage."""
        try:
            achieved_val = float(achieved.replace("%", "").replace(",", ""))
            if achieved_val >= 100:
                return self.ZAAPI_COLORS["success"]
            elif achieved_val >= 50:
                return self.ZAAPI_COLORS["warning"]
            else:
                return self.ZAAPI_COLORS["danger"]
        except:
            return self.ZAAPI_COLORS["text_secondary"]

    def safe_number(self, value: str, default: str = "-") -> str:
        """Safely format a number value."""
        if not value or value.strip() == "":
            return default
        return value.strip()

    def generate(self,
                 sales_data: Dict[str, Any],
                 marketing_data: Dict[str, Any],
                 output_path: str = "/tmp/dashboard.html") -> str:
        """Generate the complete HTML dashboard."""

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Zaapi Daily Activity Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, {self.ZAAPI_COLORS['bg_dark']} 0%, #1a2637 100%);
            color: {self.ZAAPI_COLORS['text_primary']};
            min-height: 100vh;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        .header {{
            background: linear-gradient(135deg, {self.ZAAPI_COLORS['primary']} 0%, {self.ZAAPI_COLORS['secondary']} 100%);
            padding: 40px 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }}

        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }}

        .header .date {{
            font-size: 1.1em;
            opacity: 0.95;
            font-weight: 300;
        }}

        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .card {{
            background: {self.ZAAPI_COLORS['bg_card']};
            border: 1px solid {self.ZAAPI_COLORS['border']};
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: all 0.3s ease;
        }}

        .card:hover {{
            border-color: {self.ZAAPI_COLORS['primary']};
            box-shadow: 0 8px 12px rgba(30, 64, 175, 0.15);
        }}

        .card .label {{
            font-size: 0.9em;
            color: {self.ZAAPI_COLORS['text_secondary']};
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 600;
        }}

        .card .value {{
            font-size: 2.2em;
            font-weight: 700;
            color: {self.ZAAPI_COLORS['text_primary']};
            margin-bottom: 5px;
        }}

        .card .subtext {{
            font-size: 0.85em;
            color: {self.ZAAPI_COLORS['text_secondary']};
        }}

        .section {{
            background: {self.ZAAPI_COLORS['bg_card']};
            border: 1px solid {self.ZAAPI_COLORS['border']};
            border-radius: 8px;
            padding: 25px;
            margin-bottom: 25px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}

        .section-title {{
            font-size: 1.5em;
            font-weight: 700;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid {self.ZAAPI_COLORS['primary']};
            color: {self.ZAAPI_COLORS['text_primary']};
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 15px;
        }}

        thead {{
            background: rgba({int(self.ZAAPI_COLORS['primary'][1:3], 16)}, {int(self.ZAAPI_COLORS['primary'][3:5], 16)}, {int(self.ZAAPI_COLORS['primary'][5:], 16)}, 0.1);
            border-bottom: 2px solid {self.ZAAPI_COLORS['border']};
        }}

        th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 0.9em;
            color: {self.ZAAPI_COLORS['text_secondary']};
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid {self.ZAAPI_COLORS['border']};
            font-size: 0.95em;
        }}

        tbody tr:hover {{
            background: rgba({int(self.ZAAPI_COLORS['primary'][1:3], 16)}, {int(self.ZAAPI_COLORS['primary'][3:5], 16)}, {int(self.ZAAPI_COLORS['primary'][5:], 16)}, 0.05);
        }}

        .metric-name {{
            font-weight: 600;
            color: {self.ZAAPI_COLORS['text_primary']};
        }}

        .metric-value {{
            font-weight: 500;
            text-align: right;
            min-width: 60px;
        }}

        .metric-target {{
            color: {self.ZAAPI_COLORS['success']};
            font-weight: 600;
        }}

        .metric-warning {{
            color: {self.ZAAPI_COLORS['warning']};
        }}

        .metric-danger {{
            color: {self.ZAAPI_COLORS['danger']};
            font-weight: 600;
        }}

        .agent-section {{
            background: rgba({int(self.ZAAPI_COLORS['primary'][1:3], 16)}, {int(self.ZAAPI_COLORS['primary'][3:5], 16)}, {int(self.ZAAPI_COLORS['primary'][5:], 16)}, 0.05);
            border-left: 4px solid {self.ZAAPI_COLORS['primary']};
            padding: 15px;
            margin-top: 15px;
            border-radius: 4px;
        }}

        .agent-name {{
            font-weight: 700;
            font-size: 1.1em;
            color: {self.ZAAPI_COLORS['primary']};
            margin-bottom: 10px;
        }}

        .account-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 10px;
        }}

        .account-tag {{
            background: rgba({int(self.ZAAPI_COLORS['secondary'][1:3], 16)}, {int(self.ZAAPI_COLORS['secondary'][3:5], 16)}, {int(self.ZAAPI_COLORS['secondary'][5:], 16)}, 0.2);
            border: 1px solid {self.ZAAPI_COLORS['secondary']};
            color: {self.ZAAPI_COLORS['text_secondary']};
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
        }}

        .unavailable {{
            color: {self.ZAAPI_COLORS['text_secondary']};
            font-style: italic;
            padding: 40px;
            text-align: center;
            background: rgba(0, 0, 0, 0.2);
            border-radius: 8px;
        }}

        .footer {{
            text-align: center;
            padding: 20px;
            color: {self.ZAAPI_COLORS['text_secondary']};
            font-size: 0.85em;
            margin-top: 40px;
            border-top: 1px solid {self.ZAAPI_COLORS['border']};
        }}

        .footer a {{
            color: {self.ZAAPI_COLORS['primary']};
            text-decoration: none;
        }}

        .footer a:hover {{
            text-decoration: underline;
        }}

        @media (max-width: 768px) {{
            .header h1 {{
                font-size: 1.8em;
            }}

            .summary-cards {{
                grid-template-columns: 1fr;
            }}

            table {{
                font-size: 0.85em;
            }}

            th, td {{
                padding: 8px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        {self._generate_header(sales_data)}
        {self._generate_summary_cards(sales_data, marketing_data)}
        {self._generate_marketing_section(marketing_data)}
        {self._generate_sales_sections(sales_data)}
        {self._generate_intl_sales_sections(sales_data)}
        {self._generate_footer()}
    </div>
</body>
</html>
"""

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"Dashboard generated: {output_path}")
            return output_path
        except Exception as e:
            print(f"Error writing HTML file: {e}", file=sys.stderr)
            raise

    def _generate_header(self, sales_data: Dict[str, Any]) -> str:
        """Generate the header section."""
        date_str = sales_data.get("date", datetime.now().strftime("%d/%m/%Y"))
        return f"""
        <div class="header">
            <h1>Zaapi Daily Activity Report</h1>
            <div class="date">{date_str}</div>
        </div>
        """

    def _generate_summary_cards(self, sales_data: Dict[str, Any], marketing_data: Dict[str, Any]) -> str:
        """Generate summary metric cards."""

        # Extract key metrics
        outbound_won = self.safe_number(
            sales_data.get("outbound", {}).get("funnel", [{}] * 5)[-1].get("total_wtd", "-")
        )
        inbound_won = self.safe_number(
            sales_data.get("inbound", {}).get("funnel", [{}] * 5)[-1].get("total_wtd", "-")
        )

        try:
            total_won = int(float(outbound_won.replace(",", ""))) + int(float(inbound_won.replace(",", ""))) if outbound_won != "-" and inbound_won != "-" else "-"
            total_won = str(total_won) if total_won != "-" else "-"
        except:
            total_won = "-"

        outbound_activities = self.safe_number(
            sales_data.get("outbound", {}).get("funnel", [{}])[-1].get("total_wtd", "-")
        )

        marketing_total = self.safe_number(
            marketing_data.get("total", {}).get("total_wtd", "-")
        )

        return f"""
        <div class="summary-cards">
            <div class="card">
                <div class="label">Total Won Deals</div>
                <div class="value">{total_won}</div>
                <div class="subtext">Outbound + Inbound</div>
            </div>
            <div class="card">
                <div class="label">Total Activities</div>
                <div class="value">{outbound_activities}</div>
                <div class="subtext">Outbound + International Activities</div>
            </div>
            <div class="card">
                <div class="label">Total Sign-ups</div>
                <div class="value">{marketing_total}</div>
                <div class="subtext">All regions combined</div>
            </div>
            <div class="card">
                <div class="label">Dashboard Status</div>
                <div class="value">✓</div>
                <div class="subtext">All systems operational</div>
            </div>
        </div>
        """

    def _generate_sales_sections(self, sales_data: Dict[str, Any]) -> str:
        """Generate sales outbound and inbound sections."""
        html = ""

        # Outbound Section
        html += '<div class="section">'
        html += '<div class="section-title">Sales - Outbound</div>'

        outbound = sales_data.get("outbound")
        if outbound and outbound.get("funnel"):
            html += self._generate_funnel_table(
                outbound.get("funnel", []),
                agents=["Yayee", "Toey"],
                agent_keys=["yayee", "toey"]
            )

            # Hot Deals section
            hot_deals = outbound.get("hot_deals", {})
            if hot_deals:
                html += '<table style="margin-top: 20px;">'
                html += '<thead><tr>'
                html += '<th>Hot Deals Category</th>'
                html += '<th style="text-align: right;">Yayee</th>'
                html += '<th style="text-align: right;">Toey</th>'
                html += '</tr></thead>'
                html += '<tbody>'

                for category in ["hot_deal", "ctp", "won"]:
                    if category in hot_deals:
                        cat_data = hot_deals[category]
                        cat_name = "Hot Deal" if category == "hot_deal" else category.upper()
                        yayee_items = cat_data.get("yayee", [])
                        toey_items = cat_data.get("toey", [])
                        yayee_str = ", ".join(yayee_items) if yayee_items else "-"
                        toey_str = ", ".join(toey_items) if toey_items else "-"
                        html += f'<tr><td class="metric-name">{cat_name}</td>'
                        html += f'<td class="metric-value">{yayee_str}</td>'
                        html += f'<td class="metric-value">{toey_str}</td>'
                        html += '</tr>'

                html += '</tbody></table>'
        else:
            html += '<div class="unavailable">Data unavailable</div>'

        html += '</div>'

        # Inbound Section
        html += '<div class="section">'
        html += '<div class="section-title">Sales - Inbound</div>'

        inbound = sales_data.get("inbound")
        if inbound and inbound.get("funnel"):
            html += self._generate_funnel_table(
                inbound.get("funnel", []),
                agents=["Pleum", "Loogpad"],
                agent_keys=["pleum", "loogpad"]
            )

            # Hot Deals section
            hot_deals = inbound.get("hot_deals", {})
            if hot_deals:
                html += '<table style="margin-top: 20px;">'
                html += '<thead><tr>'
                html += '<th>Hot Deals Category</th>'
                html += '<th style="text-align: right;">Pleum</th>'
                html += '<th style="text-align: right;">Loogpad</th>'
                html += '</tr></thead>'
                html += '<tbody>'

                for category in ["hot_deal", "ctp", "won"]:
                    if category in hot_deals:
                        cat_data = hot_deals[category]
                        cat_name = "Hot Deal" if category == "hot_deal" else category.upper()
                        pleum_items = cat_data.get("pleum", [])
                        loogpad_items = cat_data.get("loogpad", [])
                        pleum_str = ", ".join(pleum_items) if pleum_items else "-"
                        loogpad_str = ", ".join(loogpad_items) if loogpad_items else "-"
                        html += f'<tr><td class="metric-name">{cat_name}</td>'
                        html += f'<td class="metric-value">{pleum_str}</td>'
                        html += f'<td class="metric-value">{loogpad_str}</td>'
                        html += '</tr>'

                html += '</tbody></table>'
        else:
            html += '<div class="unavailable">Data unavailable</div>'

        html += '</div>'

        return html

    def _generate_intl_sales_sections(self, sales_data: Dict[str, Any]) -> str:
        """Generate international sales sections (outbound and inbound)."""
        html = ""

        # International Outbound Section
        html += '<div class="section">'
        html += '<div class="section-title">International Sales - Outbound</div>'

        intl_outbound = sales_data.get("intl_outbound")
        if intl_outbound and intl_outbound.get("funnel"):
            html += self._generate_funnel_table(
                intl_outbound.get("funnel", []),
                agents=["Sheronika", "Thanom"],
                agent_keys=["sheronika", "thanom"]
            )

            # Hot Deals section
            hot_deals = intl_outbound.get("hot_deals", {})
            if hot_deals:
                html += '<table style="margin-top: 20px;">'
                html += '<thead><tr>'
                html += '<th>Hot Deals Category</th>'
                html += '<th style="text-align: right;">Sheronika</th>'
                html += '<th style="text-align: right;">Thanom</th>'
                html += '</tr></thead>'
                html += '<tbody>'

                for category in ["hot_deal", "ctp", "won"]:
                    if category in hot_deals:
                        cat_data = hot_deals[category]
                        cat_name = "Hot Deal" if category == "hot_deal" else category.upper()
                        sheronika_items = cat_data.get("sheronika", [])
                        thanom_items = cat_data.get("thanom", [])
                        sheronika_str = ", ".join(sheronika_items) if sheronika_items else "-"
                        thanom_str = ", ".join(thanom_items) if thanom_items else "-"
                        html += f'<tr><td class="metric-name">{cat_name}</td>'
                        html += f'<td class="metric-value">{sheronika_str}</td>'
                        html += f'<td class="metric-value">{thanom_str}</td>'
                        html += '</tr>'

                html += '</tbody></table>'
        else:
            html += '<div class="unavailable">Data unavailable</div>'

        html += '</div>'

        # International Inbound Section
        html += '<div class="section">'
        html += '<div class="section-title">International Sales - Inbound</div>'

        intl_inbound = sales_data.get("intl_inbound")
        if intl_inbound and intl_inbound.get("funnel"):
            html += self._generate_funnel_table(
                intl_inbound.get("funnel", []),
                agents=["Sheronika", "Thanom"],
                agent_keys=["sheronika", "thanom"]
            )

            # Hot Deals section
            hot_deals = intl_inbound.get("hot_deals", {})
            if hot_deals:
                html += '<table style="margin-top: 20px;">'
                html += '<thead><tr>'
                html += '<th>Hot Deals Category</th>'
                html += '<th style="text-align: right;">Sheronika</th>'
                html += '<th style="text-align: right;">Thanom</th>'
                html += '</tr></thead>'
                html += '<tbody>'

                for category in ["hot_deal", "ctp", "won"]:
                    if category in hot_deals:
                        cat_data = hot_deals[category]
                        cat_name = "Hot Deal" if category == "hot_deal" else category.upper()
                        sheronika_items = cat_data.get("sheronika", [])
                        thanom_items = cat_data.get("thanom", [])
                        sheronika_str = ", ".join(sheronika_items) if sheronika_items else "-"
                        thanom_str = ", ".join(thanom_items) if thanom_items else "-"
                        html += f'<tr><td class="metric-name">{cat_name}</td>'
                        html += f'<td class="metric-value">{sheronika_str}</td>'
                        html += f'<td class="metric-value">{thanom_str}</td>'
                        html += '</tr>'

                html += '</tbody></table>'
        else:
            html += '<div class="unavailable">Data unavailable</div>'

        html += '</div>'

        return html

    def _generate_funnel_table(self, funnel: List[Dict[str, str]],
                               agents: List[str], agent_keys: List[str]) -> str:
        """Generate a funnel metrics table."""
        html = '<table>'
        html += '<thead><tr>'
        html += '<th>Metric</th>'
        html += '<th style="text-align: right;">Total WTD</th>'
        html += '<th style="text-align: right;">Total Daily</th>'
        html += '<th style="text-align: right;">WTD vs Target</th>'

        for agent in agents:
            html += f'<th style="text-align: right;">{agent} WTD</th>'
            html += f'<th style="text-align: right;">{agent} Daily</th>'
            html += f'<th style="text-align: right;">{agent} vs Target</th>'

        html += '</tr></thead><tbody>'

        for metric in funnel:
            metric_name = metric.get("name", "")
            html += f'<tr><td class="metric-name">{metric_name}</td>'

            # Total columns
            total_wtd = self.safe_number(metric.get("total_wtd", "-"))
            total_daily = self.safe_number(metric.get("total_daily", "-"))
            total_vs_target = self.safe_number(metric.get("total_vs_target", "-"))

            html += f'<td class="metric-value">{total_wtd}</td>'
            html += f'<td class="metric-value">{total_daily}</td>'

            # Target color
            color = self.get_target_color(total_vs_target, "100%")
            target_class = "metric-target" if color == self.ZAAPI_COLORS["success"] else \
                          "metric-warning" if color == self.ZAAPI_COLORS["warning"] else "metric-danger"
            html += f'<td class="metric-value {target_class}">{total_vs_target}</td>'

            # Agent columns
            for agent_key in agent_keys:
                wtd = self.safe_number(metric.get(f"{agent_key}_wtd", "-"))
                daily = self.safe_number(metric.get(f"{agent_key}_daily", "-"))
                vs_target = self.safe_number(metric.get(f"{agent_key}_vs_target", "-"))

                html += f'<td class="metric-value">{wtd}</td>'
                html += f'<td class="metric-value">{daily}</td>'

                color = self.get_target_color(vs_target, "100%")
                target_class = "metric-target" if color == self.ZAAPI_COLORS["success"] else \
                              "metric-warning" if color == self.ZAAPI_COLORS["warning"] else "metric-danger"
                html += f'<td class="metric-value {target_class}">{vs_target}</td>'

            html += '</tr>'

        html += '</tbody></table>'
        return html

    def _generate_marketing_section(self, marketing_data: Dict[str, Any]) -> str:
        """Generate the marketing sign-ups section."""
        html = '<div class="section">'
        html += '<div class="section-title">Marketing - Lead Overview</div>'

        regions_data = marketing_data.get("regions", {})

        if regions_data:
            html += '<table>'
            html += '<thead><tr>'
            html += '<th>Region</th>'
            html += '<th style="text-align: right;">Target WTD</th>'
            html += '<th style="text-align: right;">Total WTD</th>'
            html += '<th style="text-align: right;">WTD vs Target</th>'
            html += '<th style="text-align: right;">Qualified WTD</th>'
            html += '<th style="text-align: right;">HQ WTD</th>'
            html += '<th style="text-align: right;">Premium WTD</th>'
            html += '<th style="text-align: right;">Best WTD</th>'
            html += '</tr></thead><tbody>'

            for region in ["TH", "SEA", "ROW"]:
                if region in regions_data:
                    data = regions_data[region]
                    html += f'<tr><td class="metric-name">{region}</td>'

                    target = self.safe_number(data.get("target_wtd", "-"))
                    total = self.safe_number(data.get("total_wtd", "-"))
                    vs_target = self.safe_number(data.get("wtd_vs_target", "-"))
                    qualified = self.safe_number(data.get("qualified_wtd", "-"))
                    hq = self.safe_number(data.get("highly_qualified_wtd", "-"))
                    premium = self.safe_number(data.get("premium_wtd", "-"))
                    best = self.safe_number(data.get("best_wtd", "-"))

                    html += f'<td class="metric-value">{target}</td>'
                    html += f'<td class="metric-value">{total}</td>'

                    color = self.get_target_color(vs_target, "100%")
                    target_class = "metric-target" if color == self.ZAAPI_COLORS["success"] else \
                                  "metric-warning" if color == self.ZAAPI_COLORS["warning"] else "metric-danger"
                    html += f'<td class="metric-value {target_class}">{vs_target}</td>'

                    html += f'<td class="metric-value">{qualified}</td>'
                    html += f'<td class="metric-value">{hq}</td>'
                    html += f'<td class="metric-value">{premium}</td>'
                    html += f'<td class="metric-value">{best}</td>'
                    html += '</tr>'

            html += '</tbody></table>'
        else:
            html += '<div class="unavailable">Data unavailable</div>'

        html += '</div>'
        return html

    def _generate_footer(self) -> str:
        """Generate the footer section."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"""
        <div class="footer">
            <p>Last updated: {now}</p>
            <p>Zaapi Daily Activity Report - Automatically Generated</p>
        </div>
        """


class SlackNotifier:
    """Handles posting to Slack."""

    def __init__(self, token: str, channel: str):
        self.token = token
        self.channel = channel
        self.api_url = "https://slack.com/api/chat.postMessage"

    def post_summary(self, sales_data: Dict[str, Any], dashboard_url: str) -> bool:
        """Post a summary message to Slack."""
        try:
            date_str = sales_data.get("date", datetime.now().strftime("%d/%m/%Y"))

            # Extract metrics
            outbound_won = sales_data.get("outbound", {}).get("funnel", [{}] * 5)[-1].get("total_wtd", "N/A")
            inbound_won = sales_data.get("inbound", {}).get("funnel", [{}] * 5)[-1].get("total_wtd", "N/A")

            message = f"""
:chart_with_upwards_trend: *Zaapi Daily Activity Report*
Date: {date_str}

*Sales Metrics*
• Outbound Won: {outbound_won}
• Inbound Won: {inbound_won}

:link: <{dashboard_url}|View Full Dashboard>
            """.strip()

            payload = {
                "channel": self.channel,
                "text": message,
                "mrkdwn": True
            }

            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }

            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=10
            )

            response.raise_for_status()
            result = response.json()

            if result.get("ok"):
                print(f"Slack message posted successfully to {self.channel}")
                return True
            else:
                print(f"Slack API error: {result.get('error')}", file=sys.stderr)
                return False

        except Exception as e:
            print(f"Error posting to Slack: {e}", file=sys.stderr)
            return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch Google Sheets data and generate Zaapi Daily Activity Report"
    )
    parser.add_argument(
        "--output",
        default="/tmp/zaapi_report.html",
        help="Output HTML file path"
    )
    parser.add_argument(
        "--slack-token",
        help="Slack bot token for posting"
    )
    parser.add_argument(
        "--slack-channel",
        default="#daily-reports",
        help="Slack channel to post to"
    )
    parser.add_argument(
        "--github-pages-url",
        help="URL to GitHub Pages hosted dashboard"
    )

    args = parser.parse_args()

    print("Starting Zaapi Daily Activity Report generation...")

    # Fetch data
    fetcher = GoogleSheetsFetcher()

    print("Fetching Sales Huddle data...")
    week_name = fetcher.get_current_week_sheet_name()
    print(f"Current week: {week_name}")

    sales_sheet = fetcher.fetch_sheet(
        "1A33NpnkZlgrwyDSOKn3nwB0u5mFtal2BlVC0nGZL7Xk",
        week_name
    )

    print("Fetching Marketing Sign-ups data...")
    marketing_sheet = fetcher.fetch_sheet(
        "1_0rqXxjO0Ngp8scm2RQgXzw_dgvWPcG9apEkImVpVSY",
        "Daily Report"
    )

    # Parse data
    sales_data = {
        "date": datetime.now().strftime("%d/%m/%Y"),
        "outbound": {},
        "inbound": {},
        "renewal": {},
        "intl_outbound": {},
        "intl_inbound": {}
    }

    if sales_sheet:
        parser = SalesHuddleParser(sales_sheet)
        date_info = parser.get_date_info()
        sales_data["date"] = date_info.get("date", sales_data["date"])
        sales_data["outbound"] = parser.parse_outbound_section()
        sales_data["inbound"] = parser.parse_inbound_section()
        sales_data["renewal"] = parser.parse_renewal_section()
        sales_data["intl_outbound"] = parser.parse_intl_outbound_section()
        sales_data["intl_inbound"] = parser.parse_intl_inbound_section()
    else:
        print("Warning: Sales Huddle data not available", file=sys.stderr)

    marketing_data = {
        "date": datetime.now().strftime("%d/%m/%Y"),
        "regions": {},
        "total": {}
    }

    if marketing_sheet:
        parser = MarketingSignupsParser(marketing_sheet)
        marketing_data = parser.parse_data()
    else:
        print("Warning: Marketing Sign-ups data not available", file=sys.stderr)

    # Generate HTML
    generator = HTMLDashboardGenerator()
    output_path = generator.generate(sales_data, marketing_data, args.output)

    print(f"Dashboard generated successfully: {output_path}")

    # Post to Slack if configured
    if args.slack_token:
        notifier = SlackNotifier(args.slack_token, args.slack_channel)
        dashboard_url = args.github_pages_url or output_path
        notifier.post_summary(sales_data, dashboard_url)

    print("Report generation complete!")


if __name__ == "__main__":
    main()
