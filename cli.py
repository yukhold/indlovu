#!/usr/bin/env python3
"""
Command-line interface for App Store Analytics.

This script provides manual access to App Store Connect Analytics
for listing, downloading, and managing reports.

Usage:
    python cli.py --help
    python cli.py --list-requests
    python cli.py --download-all
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from appstore_api import AppStoreClient, get_date_range, format_file_size

load_dotenv()

# Default report request ID
DEFAULT_REQUEST_ID = os.getenv("ANALYTICS_REQUEST_ID", "")

# Reports configuration
REPORTS_CONFIG = [
    # (report_id_prefix, granularities, filename_template, sheet_name_template)
    ("r3", ["DAILY"], "downloads_standard_{granularity}.csv", "Downloads {Granularity}"),
    ("r4", ["DAILY", "WEEKLY", "MONTHLY"], "downloads_detailed_{granularity}.csv", "Downloads Detailed {Granularity}"),
    ("r12", ["DAILY", "WEEKLY", "MONTHLY"], "purchases_standard_{granularity}.csv", "Purchases {Granularity}"),
    ("r6", ["DAILY", "WEEKLY", "MONTHLY"], "install_delete_standard_{granularity}.csv", "Install-Delete {Granularity}"),
    ("r8", ["DAILY", "WEEKLY", "MONTHLY"], "sessions_standard_{granularity}.csv", "Sessions {Granularity}"),
    ("r9", ["WEEKLY", "MONTHLY"], "sessions_detailed_{granularity}.csv", "Sessions Detailed {Granularity}"),
    ("r14", ["DAILY", "WEEKLY", "MONTHLY"], "discovery_standard_{granularity}.csv", "Discovery {Granularity}"),
    ("r15", ["DAILY", "WEEKLY", "MONTHLY"], "discovery_detailed_{granularity}.csv", "Discovery Detailed {Granularity}"),
]


def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}\n")


def list_requests(client: AppStoreClient) -> None:
    """List all analytics report requests."""
    print_header("Analytics Report Requests")

    requests = client.list_report_requests()
    if not requests:
        print("No report requests found.")
        print("Create one with: python cli.py --create-request")
        return

    for req in requests:
        attrs = req.get("attributes", {})
        print(f"  ID: {req.get('id')}")
        print(f"    Access Type: {attrs.get('accessType')}")
        print(f"    Stale: {attrs.get('stale')}")
        print()


def list_reports(client: AppStoreClient, request_id: str, category: Optional[str] = None) -> None:
    """List available reports for a request."""
    print_header(f"Reports for Request: {request_id[:20]}...")

    reports = client.get_reports(request_id, category)
    if not reports:
        print("No reports available.")
        return

    for report in reports:
        attrs = report.get("attributes", {})
        print(f"  ID: {report.get('id')}")
        print(f"    Name: {attrs.get('name')}")
        print(f"    Category: {attrs.get('category')}")
        print()


def list_instances(client: AppStoreClient, report_id: str, granularity: Optional[str] = None) -> None:
    """List instances for a report."""
    print_header(f"Instances for Report: {report_id}")

    instances = client.get_instances(report_id, granularity)
    if not instances:
        print("No instances available.")
        return

    for inst in instances:
        attrs = inst.get("attributes", {})
        print(f"  ID: {inst.get('id')}")
        print(f"    Granularity: {attrs.get('granularity')}")
        print(f"    Processing Date: {attrs.get('processingDate')}")
        print()


def download_instance(client: AppStoreClient, instance_id: str, output_dir: Path) -> None:
    """Download a specific instance."""
    print(f"Downloading instance: {instance_id}")

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"report-{instance_id[:8]}.csv"

    result = client.download_instance(instance_id, output_dir, filename)
    if result:
        size = format_file_size(result.stat().st_size)
        print(f"  Saved to: {result} ({size})")
    else:
        print("  No segments found for this instance.")


def download_all_reports(
    client: AppStoreClient,
    request_id: str,
    output_dir: Path
) -> List[Path]:
    """Download all configured reports."""
    print_header("Downloading All Reports")
    print(f"Output directory: {output_dir}\n")

    downloaded: List[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)

    for prefix, granularities, filename_tpl, _ in REPORTS_CONFIG:
        report_id = f"{prefix}-{request_id}"

        for granularity in granularities:
            filename = filename_tpl.format(granularity=granularity.lower())
            print(f"Downloading: {filename}...")

            try:
                instances = client.get_instances(report_id, granularity)
                if not instances:
                    print("  No instances available")
                    continue

                instance = instances[0]
                result = client.download_instance(
                    instance["id"],
                    output_dir,
                    filename
                )

                if result:
                    size = format_file_size(result.stat().st_size)
                    oldest, newest = get_date_range(result)
                    print(f"  Saved: {filename} ({size}) [{oldest} to {newest}]")
                    downloaded.append(result)
                else:
                    print("  No segments available")

            except Exception as e:
                print(f"  Error: {e}")

    print(f"\nDownloaded {len(downloaded)} files")
    return downloaded


def create_request(client: AppStoreClient, access_type: str) -> None:
    """Create a new report request."""
    print_header("Creating Report Request")

    request_id = client.create_report_request(access_type)
    print(f"Created report request: {request_id}")
    print(f"Access type: {access_type}")
    print()
    print("Add this to your .env file:")
    print(f"ANALYTICS_REQUEST_ID={request_id}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="App Store Analytics CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List report requests
  python cli.py --list-requests

  # Create a new report request
  python cli.py --create-request

  # List reports for a request
  python cli.py --request-id <ID> --list-reports

  # List instances for a report
  python cli.py --report-id <ID> --list-instances

  # Download all reports
  python cli.py --download-all

  # Download specific instance
  python cli.py --instance-id <ID> --download
        """
    )

    # Actions
    parser.add_argument(
        "--list-requests",
        action="store_true",
        help="List all analytics report requests"
    )
    parser.add_argument(
        "--create-request",
        action="store_true",
        help="Create a new analytics report request"
    )
    parser.add_argument(
        "--list-reports",
        action="store_true",
        help="List reports for a request"
    )
    parser.add_argument(
        "--list-instances",
        action="store_true",
        help="List instances for a report"
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download a specific instance"
    )
    parser.add_argument(
        "--download-all",
        action="store_true",
        help="Download all configured reports"
    )

    # Options
    parser.add_argument(
        "--request-id",
        default=DEFAULT_REQUEST_ID,
        help="Analytics report request ID"
    )
    parser.add_argument(
        "--report-id",
        help="Analytics report ID"
    )
    parser.add_argument(
        "--instance-id",
        help="Report instance ID"
    )
    parser.add_argument(
        "--category",
        help="Filter reports by category"
    )
    parser.add_argument(
        "--granularity",
        choices=["DAILY", "WEEKLY", "MONTHLY"],
        help="Filter instances by granularity"
    )
    parser.add_argument(
        "--access-type",
        default="ONE_TIME_SNAPSHOT",
        choices=["ONE_TIME_SNAPSHOT", "ONGOING"],
        help="Access type for new request"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports"),
        help="Output directory for downloads"
    )

    args = parser.parse_args()
    client = AppStoreClient()

    # Execute action
    if args.list_requests:
        list_requests(client)

    elif args.create_request:
        create_request(client, args.access_type)

    elif args.list_reports:
        if not args.request_id:
            parser.error("--request-id is required with --list-reports")
        list_reports(client, args.request_id, args.category)

    elif args.list_instances:
        if not args.report_id:
            parser.error("--report-id is required with --list-instances")
        list_instances(client, args.report_id, args.granularity)

    elif args.download:
        if not args.instance_id:
            parser.error("--instance-id is required with --download")
        download_instance(client, args.instance_id, args.output_dir)

    elif args.download_all:
        if not args.request_id:
            parser.error("--request-id is required with --download-all")
        download_all_reports(client, args.request_id, args.output_dir)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
