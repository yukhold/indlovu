"""
Firebase Analytics client via BigQuery.

This module fetches analytics data from Firebase exports
in BigQuery and provides aggregated metrics.

Usage:
    from firebase_analytics import FirebaseAnalytics

    analytics = FirebaseAnalytics()
    events = analytics.get_events_summary(days=30)
    dau = analytics.get_daily_active_users(days=30)
"""

from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

# Configuration
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "")
FIREBASE_ANALYTICS_DATASET = os.getenv("FIREBASE_ANALYTICS_DATASET", "")


class FirebaseAnalytics:
    """Client for Firebase Analytics data via BigQuery."""

    def __init__(
        self,
        credentials_file: Optional[str] = None,
        project_id: Optional[str] = None,
        dataset: Optional[str] = None
    ):
        """
        Initialize Firebase Analytics client.

        Args:
            credentials_file: Path to service account JSON file
            project_id: Firebase/GCP project ID
            dataset: BigQuery dataset name for analytics
        """
        self.credentials_file = credentials_file or GOOGLE_CREDENTIALS_FILE
        self.project_id = project_id or FIREBASE_PROJECT_ID
        self.dataset = dataset or FIREBASE_ANALYTICS_DATASET

        if not Path(self.credentials_file).exists():
            raise SystemExit(
                f"Google credentials file not found: {self.credentials_file}\n"
                "Download it from Google Cloud Console."
            )

        self._client: Optional[bigquery.Client] = None

    def _connect(self) -> bigquery.Client:
        """Establish connection to BigQuery."""
        if self._client is None:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self._client = bigquery.Client(
                credentials=credentials,
                project=self.project_id
            )
        return self._client

    @property
    def client(self) -> bigquery.Client:
        """Get BigQuery client, connecting if necessary."""
        return self._connect()

    def _run_query(self, query: str) -> List[Dict[str, Any]]:
        """Run a BigQuery query and return results as list of dicts."""
        try:
            result = self.client.query(query).result()
            return [dict(row) for row in result]
        except Exception as e:
            print(f"Query error: {e}")
            return []

    def get_events_summary(
        self,
        days: int = 30,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get summary of Firebase Analytics events.

        Args:
            days: Number of days to look back
            limit: Maximum number of event types to return

        Returns:
            List of event summaries with counts
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = f"""
        SELECT
            event_name,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_pseudo_id) as unique_users,
            MIN(PARSE_DATE('%Y%m%d', event_date)) as first_seen,
            MAX(PARSE_DATE('%Y%m%d', event_date)) as last_seen
        FROM `{self.project_id}.{self.dataset}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN
            FORMAT_DATE('%Y%m%d', DATE('{start_date.strftime('%Y-%m-%d')}'))
            AND FORMAT_DATE('%Y%m%d', DATE('{end_date.strftime('%Y-%m-%d')}'))
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT {limit}
        """
        return self._run_query(query)

    def get_daily_active_users(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get daily active users over time.

        Args:
            days: Number of days to look back

        Returns:
            List of daily user counts
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = f"""
        SELECT
            PARSE_DATE('%Y%m%d', event_date) as date,
            COUNT(DISTINCT user_pseudo_id) as active_users,
            COUNT(*) as total_events
        FROM `{self.project_id}.{self.dataset}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN
            FORMAT_DATE('%Y%m%d', DATE('{start_date.strftime('%Y-%m-%d')}'))
            AND FORMAT_DATE('%Y%m%d', DATE('{end_date.strftime('%Y-%m-%d')}'))
        GROUP BY event_date
        ORDER BY event_date DESC
        """
        return self._run_query(query)

    def get_user_retention(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get user retention by first visit cohort.

        Args:
            days: Number of days to analyze

        Returns:
            List of retention metrics by cohort
        """
        query = f"""
        WITH first_visits AS (
            SELECT
                user_pseudo_id,
                MIN(PARSE_DATE('%Y%m%d', event_date)) as first_visit_date
            FROM `{self.project_id}.{self.dataset}.events_*`
            WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
            GROUP BY user_pseudo_id
        ),
        user_activity AS (
            SELECT
                e.user_pseudo_id,
                fv.first_visit_date,
                PARSE_DATE('%Y%m%d', e.event_date) as activity_date,
                DATE_DIFF(PARSE_DATE('%Y%m%d', e.event_date), fv.first_visit_date, DAY) as days_since_first
            FROM `{self.project_id}.{self.dataset}.events_*` e
            JOIN first_visits fv ON e.user_pseudo_id = fv.user_pseudo_id
            WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
        )
        SELECT
            first_visit_date as cohort_date,
            COUNT(DISTINCT user_pseudo_id) as cohort_size,
            COUNT(DISTINCT CASE WHEN days_since_first = 1 THEN user_pseudo_id END) as day_1,
            COUNT(DISTINCT CASE WHEN days_since_first = 7 THEN user_pseudo_id END) as day_7,
            COUNT(DISTINCT CASE WHEN days_since_first = 14 THEN user_pseudo_id END) as day_14,
            COUNT(DISTINCT CASE WHEN days_since_first = 30 THEN user_pseudo_id END) as day_30
        FROM user_activity
        GROUP BY cohort_date
        ORDER BY cohort_date DESC
        LIMIT {days}
        """
        return self._run_query(query)

    def get_screen_views(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get screen view counts.

        Args:
            days: Number of days to look back

        Returns:
            List of screen views with counts
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = f"""
        SELECT
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'firebase_screen') as screen_name,
            COUNT(*) as view_count,
            COUNT(DISTINCT user_pseudo_id) as unique_users
        FROM `{self.project_id}.{self.dataset}.events_*`
        WHERE event_name = 'screen_view'
            AND _TABLE_SUFFIX BETWEEN
                FORMAT_DATE('%Y%m%d', DATE('{start_date.strftime('%Y-%m-%d')}'))
                AND FORMAT_DATE('%Y%m%d', DATE('{end_date.strftime('%Y-%m-%d')}'))
        GROUP BY screen_name
        HAVING screen_name IS NOT NULL
        ORDER BY view_count DESC
        LIMIT 50
        """
        return self._run_query(query)

    def get_user_properties(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get aggregated user properties.

        Args:
            days: Number of days to look back

        Returns:
            List of user property distributions
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = f"""
        SELECT
            device.category as device_category,
            device.operating_system as os,
            device.operating_system_version as os_version,
            geo.country as country,
            COUNT(DISTINCT user_pseudo_id) as users
        FROM `{self.project_id}.{self.dataset}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN
            FORMAT_DATE('%Y%m%d', DATE('{start_date.strftime('%Y-%m-%d')}'))
            AND FORMAT_DATE('%Y%m%d', DATE('{end_date.strftime('%Y-%m-%d')}'))
        GROUP BY device_category, os, os_version, country
        ORDER BY users DESC
        LIMIT 100
        """
        return self._run_query(query)

    def export_to_csv(
        self,
        data: List[Dict[str, Any]],
        output_path: Path,
        filename: str
    ) -> Optional[Path]:
        """
        Export data to CSV file.

        Args:
            data: List of dictionaries to export
            output_path: Directory to save file
            filename: Name of the CSV file

        Returns:
            Path to created file or None
        """
        if not data:
            return None

        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / filename

        # Convert date objects to strings
        for row in data:
            for key, value in row.items():
                if hasattr(value, 'isoformat'):
                    row[key] = value.isoformat()

        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter="\t")
            writer.writeheader()
            writer.writerows(data)

        return file_path


# Sheet name mappings for Firebase reports
FIREBASE_SHEET_NAMES: Dict[str, str] = {
    "firebase_events_summary.csv": "Firebase Events",
    "firebase_daily_users.csv": "Firebase DAU",
    "firebase_retention.csv": "Firebase Retention",
    "firebase_screens.csv": "Firebase Screens",
    "firebase_user_properties.csv": "Firebase Users",
}
