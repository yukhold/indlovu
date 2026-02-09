"""
App Store Connect Analytics API client.

This module provides functions to interact with the App Store Connect
Analytics API for downloading app analytics reports.

Usage:
    from appstore_api import AppStoreClient

    client = AppStoreClient()
    reports = client.list_report_requests()
"""

from __future__ import annotations

import csv
import gzip
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

from auth import generate_token

load_dotenv()

# API Configuration
API_BASE = "https://api.appstoreconnect.apple.com/v1"
DEFAULT_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 120


class AppStoreClient:
    """Client for App Store Connect Analytics API."""

    def __init__(self, token: Optional[str] = None):
        """
        Initialize the App Store Connect API client.

        Args:
            token: Optional JWT token. If not provided, generates a new one.
        """
        self.token = token or generate_token()
        self.app_id = os.getenv("APP_ID")
        if not self.app_id:
            raise SystemExit("APP_ID is not set in .env file.")

    def _headers(self) -> Dict[str, str]:
        """Get request headers with authorization."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make GET request to API."""
        response = requests.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=DEFAULT_TIMEOUT
        )
        if not response.ok:
            raise Exception(f"API error {response.status_code}: {response.text}")
        return response.json()

    def _post(self, url: str, payload: Dict) -> Dict[str, Any]:
        """Make POST request to API."""
        response = requests.post(
            url,
            headers=self._headers(),
            json=payload,
            timeout=DEFAULT_TIMEOUT
        )
        if not response.ok:
            raise Exception(f"API error {response.status_code}: {response.text}")
        return response.json()

    # -------------------------------------------------------------------------
    # Report Requests
    # -------------------------------------------------------------------------

    def create_report_request(self, access_type: str = "ONE_TIME_SNAPSHOT") -> str:
        """
        Create a new analytics report request.

        Args:
            access_type: "ONE_TIME_SNAPSHOT" or "ONGOING"

        Returns:
            The report request ID
        """
        url = f"{API_BASE}/analyticsReportRequests"
        payload = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": access_type},
                "relationships": {
                    "app": {
                        "data": {"type": "apps", "id": self.app_id}
                    }
                },
            }
        }
        data = self._post(url, payload)
        request_id = data.get("data", {}).get("id")
        if not request_id:
            raise Exception("Report request response did not contain an ID.")
        return request_id

    def list_report_requests(self) -> List[Dict[str, Any]]:
        """
        List all analytics report requests for the app.

        Returns:
            List of report request objects
        """
        url = f"{API_BASE}/apps/{self.app_id}/analyticsReportRequests"
        return self._get(url).get("data", [])

    # -------------------------------------------------------------------------
    # Reports
    # -------------------------------------------------------------------------

    def get_reports(
        self,
        request_id: str,
        category: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get available reports for a report request.

        Args:
            request_id: The report request ID
            category: Optional filter (e.g., "APP_USAGE", "COMMERCE")

        Returns:
            List of report objects
        """
        url = f"{API_BASE}/analyticsReportRequests/{request_id}/reports"
        params = {}
        if category:
            params["filter[category]"] = category
        return self._get(url, params).get("data", [])

    # -------------------------------------------------------------------------
    # Report Instances
    # -------------------------------------------------------------------------

    def get_instances(
        self,
        report_id: str,
        granularity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get report instances (downloadable files) for a report.

        Args:
            report_id: The report ID
            granularity: Optional filter ("DAILY", "WEEKLY", "MONTHLY")

        Returns:
            List of instance objects
        """
        url = f"{API_BASE}/analyticsReports/{report_id}/instances"
        params = {}
        if granularity:
            params["filter[granularity]"] = granularity
        return self._get(url, params).get("data", [])

    def get_segments(self, instance_id: str) -> List[Dict[str, Any]]:
        """
        Get downloadable segments for a report instance.

        Args:
            instance_id: The instance ID

        Returns:
            List of segment objects with download URLs
        """
        url = f"{API_BASE}/analyticsReportInstances/{instance_id}/segments"
        return self._get(url).get("data", [])

    # -------------------------------------------------------------------------
    # Download
    # -------------------------------------------------------------------------

    def download_segment(self, download_url: str, output_path: Path) -> Path:
        """
        Download a report segment to a file.

        Handles gzip decompression automatically.

        Args:
            download_url: The segment download URL
            output_path: Path to save the file (without .gz extension)

        Returns:
            Path to the downloaded file
        """
        response = requests.get(download_url, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()

        # Check if content is gzipped
        is_gzipped = (
            download_url.endswith('.gz') or
            response.headers.get('Content-Type') == 'application/gzip'
        )

        if is_gzipped:
            gz_path = output_path.with_suffix('.csv.gz')
            gz_path.write_bytes(response.content)

            # Decompress
            with gzip.open(gz_path, 'rb') as f_in:
                output_path.write_bytes(f_in.read())
            gz_path.unlink()
        else:
            output_path.write_bytes(response.content)

        return output_path

    def download_instance(
        self,
        instance_id: str,
        output_dir: Path,
        filename: str
    ) -> Optional[Path]:
        """
        Download all segments of a report instance.

        Args:
            instance_id: The instance ID
            output_dir: Directory to save the file
            filename: Output filename

        Returns:
            Path to the downloaded file, or None if no segments
        """
        segments = self.get_segments(instance_id)
        if not segments:
            return None

        output_dir.mkdir(parents=True, exist_ok=True)

        for segment in segments:
            attrs = segment.get("attributes", {})
            download_url = attrs.get("url")
            if not download_url:
                continue

            output_path = output_dir / filename
            return self.download_segment(download_url, output_path)

        return None


# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

def get_date_range(file_path: Path) -> Tuple[str, str]:
    """
    Extract oldest and newest dates from a CSV report file.

    Args:
        file_path: Path to the CSV file

    Returns:
        Tuple of (oldest_date, newest_date) or ("-", "-") if not found
    """
    dates: Set[str] = set()
    try:
        with file_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                date_val = row.get("Date", "")
                if date_val and date_val[0].isdigit():
                    dates.add(date_val)
    except Exception:
        pass

    if dates:
        sorted_dates = sorted(dates)
        return sorted_dates[0], sorted_dates[-1]
    return "-", "-"


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.5M", "256K", "128B")
    """
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}K"
    else:
        return f"{size_bytes / (1024 * 1024):.1f}M"
