#!/usr/bin/env python3
"""
Weekly App Store Analytics synchronization script.

This script downloads all analytics reports from App Store Connect,
saves them to a dated folder, creates a markdown summary, and uploads
the data to Google Sheets.

Designed to run as a weekly cron job.

Usage:
    python weekly_sync.py

Cron (Sunday 21:00):
    0 21 * * 0 cd /path/to/project && .venv/bin/python weekly_sync.py >> logs/weekly_sync.log 2>&1

Environment variables:
    ANALYTICS_REQUEST_ID: The report request ID
    GOOGLE_SPREADSHEET_ID: Google Sheets spreadsheet ID
    GOOGLE_CREDENTIALS_FILE: Path to service account JSON
    APP_NAME: App name for reports (optional)
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from appstore_api import AppStoreClient, get_date_range, format_file_size
from google_sheets import SheetsClient, REPORT_SHEET_NAMES
from firebase_analytics import FirebaseAnalytics, FIREBASE_SHEET_NAMES

load_dotenv()

# Configuration
REQUEST_ID = os.getenv("ANALYTICS_REQUEST_ID", "")
APP_NAME = os.getenv("APP_NAME", "App")
REPORTS_DIR = Path("reports")
LOGS_DIR = Path("logs")

# Reports to download
# (report_id_prefix, granularities, filename_template)
REPORTS = [
    ("r3", ["DAILY"], "downloads_standard_{g}.csv"),
    ("r4", ["DAILY", "WEEKLY", "MONTHLY"], "downloads_detailed_{g}.csv"),
    ("r12", ["DAILY", "WEEKLY", "MONTHLY"], "purchases_standard_{g}.csv"),
    ("r6", ["DAILY", "WEEKLY", "MONTHLY"], "install_delete_standard_{g}.csv"),
    ("r8", ["DAILY", "WEEKLY", "MONTHLY"], "sessions_standard_{g}.csv"),
    ("r9", ["WEEKLY", "MONTHLY"], "sessions_detailed_{g}.csv"),
    ("r14", ["DAILY", "WEEKLY", "MONTHLY"], "discovery_standard_{g}.csv"),
    ("r15", ["DAILY", "WEEKLY", "MONTHLY"], "discovery_detailed_{g}.csv"),
]


class WeeklySyncJob:
    """
    Weekly synchronization job for App Store Analytics.

    Downloads reports, saves to dated folder, creates summary,
    and uploads to Google Sheets.
    """

    def __init__(self):
        """Initialize the sync job."""
        if not REQUEST_ID:
            raise SystemExit(
                "ANALYTICS_REQUEST_ID is not set.\n"
                "Add it to your .env file."
            )

        self.client = AppStoreClient()
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.output_dir = REPORTS_DIR / self.date_str
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.downloaded: List[Dict[str, Any]] = []

    def run(self) -> None:
        """Execute the full sync job."""
        self._print_header("Weekly App Store Analytics Sync")
        print(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Output: {self.output_dir}\n")

        # Step 1: Download App Store reports
        self._download_reports()

        # Step 2: Download Firebase Analytics
        self._download_firebase()

        # Step 3: Create markdown summary
        self._create_summary()

        # Step 4: Upload to Google Sheets
        self._upload_to_sheets()

        # Done
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()

        self._print_header("Sync Complete")
        print(f"Duration: {duration:.0f} seconds")
        print(f"Files: {len(self.downloaded)}")
        print(f"Output: {self.output_dir}")

    def _print_header(self, text: str) -> None:
        """Print formatted header."""
        print()
        print("=" * 60)
        print(f"  {text}")
        print("=" * 60)
        print()

    def _download_reports(self) -> None:
        """Download all configured reports."""
        self._print_header("Downloading Reports")

        self.output_dir.mkdir(parents=True, exist_ok=True)

        for prefix, granularities, filename_tpl in REPORTS:
            report_id = f"{prefix}-{REQUEST_ID}"

            for granularity in granularities:
                filename = filename_tpl.format(g=granularity.lower())
                print(f"Downloading: {filename}...")

                try:
                    instances = self.client.get_instances(report_id, granularity)
                    if not instances:
                        print("  No instances available")
                        continue

                    instance = instances[0]
                    result = self.client.download_instance(
                        instance["id"],
                        self.output_dir,
                        filename
                    )

                    if result:
                        size = result.stat().st_size
                        oldest, newest = get_date_range(result)
                        download_time = datetime.now().strftime("%H:%M")

                        self.downloaded.append({
                            "filename": filename,
                            "path": result,
                            "size": size,
                            "size_fmt": format_file_size(size),
                            "time": download_time,
                            "oldest": oldest,
                            "newest": newest,
                        })

                        print(f"  Saved: {filename} ({format_file_size(size)})")
                    else:
                        print("  No segments available")

                except Exception as e:
                    print(f"  Error: {e}")

        print(f"\nDownloaded {len(self.downloaded)} files")

    def _download_firebase(self) -> None:
        """Download Firebase Analytics data from BigQuery."""
        self._print_header("Downloading Firebase Analytics")

        try:
            firebase = FirebaseAnalytics()
        except SystemExit as e:
            print(f"Skipping Firebase: {e}")
            return

        # Reports to download: (method_name, filename, description, days)
        firebase_reports = [
            ("get_events_summary", "firebase_events_summary.csv", "Events Summary", 30),
            ("get_daily_active_users", "firebase_daily_users.csv", "Daily Active Users", 30),
            ("get_retention", "firebase_retention.csv", "User Retention", 30),
            ("get_screen_views", "firebase_screens.csv", "Screen Views", 30),
            ("get_user_properties", "firebase_user_properties.csv", "User Properties", 30),
        ]

        for method_name, filename, description, days in firebase_reports:
            print(f"Downloading: {description}...")

            try:
                method = getattr(firebase, method_name, None)
                if method is None:
                    # Try alternate method name
                    method = getattr(firebase, f"get_user_retention" if "retention" in method_name else method_name, None)

                if method:
                    data = method(days=days) if "days" in method.__code__.co_varnames else method()

                    if data:
                        result = firebase.export_to_csv(data, self.output_dir, filename)
                        if result:
                            size = result.stat().st_size
                            self.downloaded.append({
                                "filename": filename,
                                "path": result,
                                "size": size,
                                "size_fmt": format_file_size(size),
                                "time": datetime.now().strftime("%H:%M"),
                                "oldest": "-",
                                "newest": "-",
                                "source": "firebase"
                            })
                            print(f"  Saved: {filename} ({format_file_size(size)})")
                    else:
                        print(f"  No data available")
            except Exception as e:
                print(f"  Error: {e}")

        firebase_count = sum(1 for f in self.downloaded if f.get("source") == "firebase")
        print(f"\nDownloaded {firebase_count} Firebase reports")

    def _create_summary(self) -> None:
        """Create markdown summary file."""
        self._print_header("Creating Summary")

        md_path = self.output_dir / f"{self.date_str}-reports.md"

        # Group by category
        categories = {
            "Downloads": [],
            "Purchases": [],
            "Install-Delete": [],
            "Sessions": [],
            "Discovery": [],
            "Firebase": [],
        }

        for f in self.downloaded:
            name = f["filename"]
            if "firebase" in name:
                categories["Firebase"].append(f)
            elif "downloads" in name:
                categories["Downloads"].append(f)
            elif "purchases" in name:
                categories["Purchases"].append(f)
            elif "install_delete" in name:
                categories["Install-Delete"].append(f)
            elif "sessions" in name:
                categories["Sessions"].append(f)
            elif "discovery" in name:
                categories["Discovery"].append(f)

        # Write markdown
        with md_path.open("w", encoding="utf-8") as md:
            md.write("# App Store Analytics Reports\n\n")
            md.write(f"**App:** {APP_NAME}\n\n")

            md.write("## Download Info\n\n")
            md.write("| Parameter | Value |\n")
            md.write("|-----------|-------|\n")
            md.write(f"| Download started | {self.start_time.strftime('%Y-%m-%d %H:%M')} |\n")
            end_str = datetime.now().strftime('%Y-%m-%d %H:%M')
            md.write(f"| Download finished | {end_str} |\n\n")

            md.write("## Downloaded Reports\n\n")
            md.write("| File | Size | Time | Oldest | Newest |\n")
            md.write("|------|------|------|--------|--------|\n")

            for cat_name, files in categories.items():
                if files:
                    md.write(f"| **{cat_name}** | | | | |\n")
                    for f in sorted(files, key=lambda x: x["filename"]):
                        md.write(
                            f"| {f['filename']} | {f['size_fmt']} | "
                            f"{f['time']} | {f['oldest']} | {f['newest']} |\n"
                        )

            md.write(f"\n**Total:** {len(self.downloaded)} files\n")

        print(f"Created: {md_path}")

    def _upload_to_sheets(self) -> None:
        """Upload reports to Google Sheets."""
        self._print_header("Uploading to Google Sheets")

        try:
            sheets = SheetsClient()
            print(f"Connected to: {sheets.spreadsheet_title}\n")
        except SystemExit as e:
            print(f"Skipping Google Sheets upload: {e}")
            return

        for f in self.downloaded:
            filename = f["filename"]
            # Check both App Store and Firebase sheet mappings
            sheet_name = REPORT_SHEET_NAMES.get(filename) or FIREBASE_SHEET_NAMES.get(filename)

            if not sheet_name:
                print(f"  Skipping {filename}: no sheet mapping")
                continue

            print(f"Uploading: {filename} -> '{sheet_name}'...")

            try:
                rows = sheets.upload_csv(f["path"], sheet_name)
                print(f"  Uploaded {rows} rows")
            except Exception as e:
                print(f"  Error: {e}")

        print("\nUpload complete!")


def main() -> None:
    """Main entry point."""
    # Ensure logs directory exists
    LOGS_DIR.mkdir(exist_ok=True)

    # Run sync job
    job = WeeklySyncJob()
    job.run()


if __name__ == "__main__":
    main()
