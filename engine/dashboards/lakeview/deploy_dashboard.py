#!/usr/bin/env python3
"""Deploy the Watchdog Governance Posture Lakeview dashboard.

Reads the dashboard template, replaces catalog/schema placeholders,
and creates or updates the dashboard via the Lakeview API.

Usage:
    python deploy_dashboard.py --profile fe-stable --catalog my_catalog --schema watchdog
    python deploy_dashboard.py --profile prod --warehouse-id abc123 --update DASHBOARD_ID
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


TEMPLATE = Path(__file__).resolve().parent / "watchdog_governance_posture.json"
DEFAULT_CATALOG = "platform"
DEFAULT_SCHEMA = "watchdog"


def _substitute(dashboard_json: str, catalog: str, schema: str) -> str:
    """Replace catalog.schema references in all dataset queries."""
    # The template uses serverless_stable_s0v155_catalog.watchdog as the
    # original catalog.schema — replace with the target
    original = "serverless_stable_s0v155_catalog.watchdog"
    target = f"{catalog}.{schema}"
    return dashboard_json.replace(original, target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Watchdog Lakeview Dashboard")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help=f"UC catalog (default: {DEFAULT_CATALOG})")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help=f"Watchdog schema (default: {DEFAULT_SCHEMA})")
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse ID")
    parser.add_argument("--parent-path", default=None, help="Workspace path for the dashboard")
    parser.add_argument("--update", metavar="DASHBOARD_ID", default=None, help="Update existing dashboard")
    parser.add_argument("--publish", action="store_true", help="Publish after creating/updating")
    args = parser.parse_args()

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("ERROR: pip install databricks-sdk", file=sys.stderr)
        sys.exit(1)

    # Load and substitute template
    raw = TEMPLATE.read_text()
    substituted = _substitute(raw, args.catalog, args.schema)

    kwargs = {}
    if args.profile:
        kwargs["profile"] = args.profile
    w = WorkspaceClient(**kwargs)

    if args.update:
        body = {"serialized_dashboard": substituted}
        resp = w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{args.update}", body=body)
        dashboard_id = args.update
        print(f"Updated dashboard: {dashboard_id}")
    else:
        body = {
            "display_name": "Watchdog Governance Posture",
            "serialized_dashboard": substituted,
        }
        if args.warehouse_id:
            body["warehouse_id"] = args.warehouse_id
        if args.parent_path:
            body["parent_path"] = args.parent_path
        resp = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body)
        dashboard_id = resp.get("dashboard_id", "unknown")
        print(f"Created dashboard: {dashboard_id}")
        print(f"Path: {resp.get('path', '')}")

    if args.publish:
        pub_body = {"embed_credentials": False}
        if args.warehouse_id:
            pub_body["warehouse_id"] = args.warehouse_id
        w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", body=pub_body)
        print("Published.")

    print(f"\nDashboard ID: {dashboard_id}")


if __name__ == "__main__":
    main()
