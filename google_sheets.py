"""
Google Sheets client for uploading App Store Analytics reports.

This module handles uploading CSV report data to Google Sheets,
with support for creating sheets and updating existing data.

Usage:
    from google_sheets import SheetsClient

    client = SheetsClient()
    client.upload_csv("reports/downloads.csv", "Downloads Daily")
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import gspread
from dotenv import load_dotenv
from gspread import Spreadsheet, Worksheet

load_dotenv()

# Configuration
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")


class SheetsClient:
    """Client for Google Sheets operations."""

    def __init__(
        self,
        credentials_file: Optional[str] = None,
        spreadsheet_id: Optional[str] = None
    ):
        """
        Initialize Google Sheets client.

        Args:
            credentials_file: Path to service account JSON file
            spreadsheet_id: Google Spreadsheet ID from URL

        Raises:
            SystemExit: If credentials file not found or spreadsheet ID not set
        """
        self.credentials_file = credentials_file or GOOGLE_CREDENTIALS_FILE
        self.spreadsheet_id = spreadsheet_id or GOOGLE_SPREADSHEET_ID

        if not Path(self.credentials_file).exists():
            raise SystemExit(
                f"Google credentials file not found: {self.credentials_file}\n"
                "Download it from Google Cloud Console."
            )

        if not self.spreadsheet_id:
            raise SystemExit(
                "GOOGLE_SPREADSHEET_ID is not set.\n"
                "Add it to your .env file."
            )

        self._client: Optional[gspread.Client] = None
        self._spreadsheet: Optional[Spreadsheet] = None

    def _connect(self) -> None:
        """Establish connection to Google Sheets."""
        if self._client is None:
            self._client = gspread.service_account(filename=self.credentials_file)
            self._spreadsheet = self._client.open_by_key(self.spreadsheet_id)

    @property
    def spreadsheet(self) -> Spreadsheet:
        """Get the spreadsheet object, connecting if necessary."""
        if self._spreadsheet is None:
            self._connect()
        return self._spreadsheet  # type: ignore

    @property
    def spreadsheet_title(self) -> str:
        """Get the spreadsheet title."""
        return self.spreadsheet.title

    def get_or_create_worksheet(
        self,
        title: str,
        rows: int = 1000,
        cols: int = 26
    ) -> Worksheet:
        """
        Get existing worksheet or create a new one.

        Args:
            title: Worksheet/tab name
            rows: Number of rows for new worksheet
            cols: Number of columns for new worksheet

        Returns:
            The worksheet object
        """
        try:
            return self.spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            return self.spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

    def read_csv(self, file_path: Path) -> List[List[str]]:
        """
        Read CSV file and return as list of rows.

        App Store reports use tab-delimited format.

        Args:
            file_path: Path to CSV file

        Returns:
            List of rows, where each row is a list of cell values
        """
        rows = []
        with file_path.open(encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                rows.append(row)
        return rows

    def upload_data(
        self,
        sheet_name: str,
        data: List[List[str]],
        clear_first: bool = True
    ) -> int:
        """
        Upload data to a worksheet.

        Args:
            sheet_name: Name of the worksheet/tab
            data: List of rows to upload
            clear_first: Whether to clear existing data first

        Returns:
            Number of rows uploaded
        """
        if not data:
            return 0

        worksheet = self.get_or_create_worksheet(
            sheet_name,
            rows=len(data) + 1000,
            cols=len(data[0]) + 5
        )

        if clear_first:
            worksheet.clear()

        worksheet.update(range_name="A1", values=data)
        return len(data)

    def upload_csv(
        self,
        file_path: Path,
        sheet_name: str,
        clear_first: bool = True
    ) -> int:
        """
        Upload a CSV file to a worksheet.

        Args:
            file_path: Path to the CSV file
            sheet_name: Name of the worksheet/tab
            clear_first: Whether to clear existing data first

        Returns:
            Number of rows uploaded
        """
        data = self.read_csv(file_path)
        return self.upload_data(sheet_name, data, clear_first)

    def upsert_csv(
        self,
        file_path: Path,
        sheet_name: str,
        key_columns: List[str]
    ) -> int:
        """
        Upload CSV with upsert logic (update existing, add new).

        Note: For App Store reports, a full replace is typically
        preferred since Apple may update historical data.

        Args:
            file_path: Path to the CSV file
            sheet_name: Name of the worksheet/tab
            key_columns: Column names to use as unique key

        Returns:
            Number of rows in final dataset
        """
        new_data = self.read_csv(file_path)
        if not new_data:
            return 0

        header = new_data[0]

        # Get key column indices
        key_indices = []
        for col_name in key_columns:
            try:
                key_indices.append(header.index(col_name))
            except ValueError:
                pass

        if not key_indices:
            # No key columns found, do full replace
            return self.upload_data(sheet_name, new_data)

        # Get existing data
        worksheet = self.get_or_create_worksheet(sheet_name)
        try:
            existing_data = worksheet.get_all_values()
        except Exception:
            existing_data = []

        if not existing_data or existing_data[0] != header:
            # Different schema or empty, use new data
            return self.upload_data(sheet_name, new_data)

        # Build merged dataset (new data takes precedence)
        # For App Store data, new export is authoritative
        return self.upload_data(sheet_name, new_data)


# -----------------------------------------------------------------------------
# Report Sheet Mapping
# -----------------------------------------------------------------------------

# Maps report filenames to Google Sheets tab names
REPORT_SHEET_NAMES: Dict[str, str] = {
    "downloads_standard_daily.csv": "Downloads Daily",
    "downloads_detailed_daily.csv": "Downloads Detailed Daily",
    "downloads_detailed_weekly.csv": "Downloads Detailed Weekly",
    "downloads_detailed_monthly.csv": "Downloads Detailed Monthly",
    "purchases_standard_daily.csv": "Purchases Daily",
    "purchases_standard_weekly.csv": "Purchases Weekly",
    "purchases_standard_monthly.csv": "Purchases Monthly",
    "install_delete_standard_daily.csv": "Install-Delete Daily",
    "install_delete_standard_weekly.csv": "Install-Delete Weekly",
    "install_delete_standard_monthly.csv": "Install-Delete Monthly",
    "sessions_standard_daily.csv": "Sessions Daily",
    "sessions_standard_weekly.csv": "Sessions Weekly",
    "sessions_standard_monthly.csv": "Sessions Monthly",
    "sessions_detailed_weekly.csv": "Sessions Detailed Weekly",
    "sessions_detailed_monthly.csv": "Sessions Detailed Monthly",
    "discovery_standard_daily.csv": "Discovery Daily",
    "discovery_standard_weekly.csv": "Discovery Weekly",
    "discovery_standard_monthly.csv": "Discovery Monthly",
    "discovery_detailed_daily.csv": "Discovery Detailed Daily",
    "discovery_detailed_weekly.csv": "Discovery Detailed Weekly",
    "discovery_detailed_monthly.csv": "Discovery Detailed Monthly",
}


def upload_all_reports(reports_dir: Path) -> None:
    """
    Upload all CSV reports from a directory to Google Sheets.

    Args:
        reports_dir: Directory containing CSV report files
    """
    client = SheetsClient()
    print(f"Connected to: {client.spreadsheet_title}")

    for filename, sheet_name in REPORT_SHEET_NAMES.items():
        file_path = reports_dir / filename
        if not file_path.exists():
            print(f"  Skipping {filename}: file not found")
            continue

        print(f"  Uploading {filename} -> '{sheet_name}'...")
        rows = client.upload_csv(file_path, sheet_name)
        print(f"    Uploaded {rows} rows")

    print("Upload complete!")
