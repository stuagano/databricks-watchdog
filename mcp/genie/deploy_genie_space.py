#!/usr/bin/env python3
"""Deploy a Watchdog Governance Genie Space to Databricks.

Reads SQL templates and instructions from this directory, substitutes
catalog/schema placeholders, and creates (or updates) a Genie Space via
the Databricks SDK.

Usage:
    python deploy_genie_space.py --catalog platform --schema watchdog --profile prod
    python deploy_genie_space.py --catalog platform --schema watchdog --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from string import Template

GENIE_DIR = Path(__file__).resolve().parent


def _load_sql_datasets(catalog: str, schema: str) -> list[dict]:
    """Read all .sql files, substitute placeholders, return dataset dicts."""
    datasets = []
    for sql_path in sorted(GENIE_DIR.glob("*.sql")):
        raw = sql_path.read_text()
        # First line comment is the dataset title
        title = raw.strip().splitlines()[0].lstrip("- ").strip()
        # Second comment line is the description
        desc_line = raw.strip().splitlines()[1].lstrip("- ").strip()

        # Replace ${catalog} and ${schema} placeholders
        query = raw.replace("${catalog}", catalog).replace("${schema}", schema)

        datasets.append({
            "name": sql_path.stem,
            "title": title,
            "description": desc_line,
            "query": query,
        })
    return datasets


def _load_instructions() -> str:
    """Read the Genie Space instructions markdown."""
    instructions_path = GENIE_DIR / "instructions.md"
    if not instructions_path.exists():
        print(f"WARNING: {instructions_path} not found, using empty instructions")
        return ""
    return instructions_path.read_text()


def _build_space_config(
    catalog: str,
    schema: str,
    warehouse_id: str | None,
    space_name: str,
) -> dict:
    """Build the Genie Space configuration payload."""
    datasets = _load_sql_datasets(catalog, schema)
    instructions = _load_instructions()

    # Watchdog base tables
    watchdog_tables = [
        "violations",
        "resource_inventory",
        "resource_classifications",
        "policies",
        "exceptions",
        "scan_results",
    ]
    # Watchdog semantic views
    watchdog_views = [
        "v_resource_compliance",
        "v_class_compliance",
        "v_domain_compliance",
        "v_tag_policy_coverage",
        "v_data_classification_summary",
        "v_dq_monitoring_coverage",
    ]
    # UC system tables (Governance Hub data sources)
    system_tables = [
        "system.information_schema.tables",
        "system.information_schema.columns",
        "system.information_schema.table_privileges",
        "system.information_schema.schema_privileges",
        "system.information_schema.column_tags",
        "system.information_schema.table_tags",
        "system.access.audit",
    ]

    table_identifiers = sorted(
        [f"{catalog}.{schema}.{t}" for t in watchdog_tables + watchdog_views]
        + system_tables
    )

    config = {
        "title": space_name,
        "description": (
            "Compliance posture + UC governance — Watchdog violations, "
            "classifications, policies alongside UC system tables for "
            "access, tags, and metadata."
        ),
        "table_identifiers": table_identifiers,
    }

    if warehouse_id:
        config["warehouse_id"] = warehouse_id

    # Build serialized_space.
    # NOTE: The Genie API throws internal errors if instructions are included
    # during CREATE. We create without instructions, then add them via a
    # separate PATCH (without reading first — the etag changes on every PATCH,
    # making read-then-write impossible).
    serialized = {
        "version": 2,
        "data_sources": {
            "tables": sorted(
                [{"identifier": tid} for tid in table_identifiers],
                key=lambda t: t["identifier"],
            )
        },
    }

    config["serialized_space"] = json.dumps(serialized)

    return config


def _add_instructions(w, space_id: str, instructions: str, table_identifiers: list[str]) -> None:
    """Add instructions to an existing Genie Space.

    The Genie API throws internal errors when instructions are included in
    the CREATE payload. So we add them via a separate PATCH.

    Important: the etag changes on every PATCH (including empty ones), making
    read-then-write impossible. We send the full serialized_space from scratch
    without reading first.
    """
    if not instructions:
        return

    serialized = {
        "version": 2,
        "data_sources": {
            "tables": sorted(
                [{"identifier": tid} for tid in table_identifiers],
                key=lambda t: t["identifier"],
            )
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": str(uuid.uuid4()),
                    "content": [line + "\n" for line in instructions.splitlines()],
                }
            ]
        },
    }

    try:
        w.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{space_id}",
            body={
                "serialized_space": json.dumps(serialized),
                "table_identifiers": table_identifiers,
            },
        )
        print("  Added instructions to Genie Space")
    except Exception as exc:
        # Instructions via API are flaky — warn but don't fail
        print(f"  WARNING: Could not add instructions via API: {exc}")
        print("  You can add instructions manually in the Genie Space UI.")


def _deploy(
    config: dict,
    profile: str | None,
    update_space_id: str | None,
    instructions: str = "",
) -> str:
    """Create or update the Genie Space via the Databricks SDK."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print(
            "ERROR: databricks-sdk is required. Install with:\n"
            "  pip install databricks-sdk",
            file=sys.stderr,
        )
        sys.exit(1)

    kwargs = {}
    if profile:
        kwargs["profile"] = profile

    w = WorkspaceClient(**kwargs)

    if update_space_id:
        # Update existing space — send full config without reading first
        print(f"Updating Genie Space {update_space_id} ...")
        resp = w.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{update_space_id}",
            body=config,
        )
        space_id = update_space_id
        print(f"Updated Genie Space: {space_id}")
    else:
        # Create new space (without instructions — added in follow-up PATCH)
        print("Creating Genie Space ...")
        resp = w.api_client.do(
            "POST",
            "/api/2.0/genie/spaces",
            body=config,
        )
        space_id = resp.get("space_id", resp.get("id", "unknown"))
        print(f"Created Genie Space: {space_id}")

        # Add instructions in a separate PATCH
        _add_instructions(w, space_id, instructions, config.get("table_identifiers", []))

    return space_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy a Watchdog Governance Genie Space",
    )
    parser.add_argument(
        "--catalog",
        default="platform",
        help="Unity Catalog catalog name (default: platform)",
    )
    parser.add_argument(
        "--schema",
        default="watchdog",
        help="Schema containing Watchdog tables (default: watchdog)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile to use for authentication",
    )
    parser.add_argument(
        "--warehouse-id",
        default=None,
        help="SQL warehouse ID for the Genie Space",
    )
    parser.add_argument(
        "--space-name",
        default="Watchdog Governance Explorer",
        help="Display name for the Genie Space (default: Watchdog Governance Explorer)",
    )
    parser.add_argument(
        "--update",
        metavar="SPACE_ID",
        default=None,
        help="Update an existing Genie Space instead of creating a new one",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the space configuration without creating it",
    )

    args = parser.parse_args()

    config = _build_space_config(
        catalog=args.catalog,
        schema=args.schema,
        warehouse_id=args.warehouse_id,
        space_name=args.space_name,
    )

    if args.dry_run:
        print("=== Genie Space Configuration (dry run) ===\n")
        # Print config with serialized_space expanded for readability
        display = {**config}
        if "serialized_space" in display:
            display["serialized_space"] = json.loads(display["serialized_space"])
        print(json.dumps(display, indent=2))

        print(f"\n=== SQL Datasets ({args.catalog}.{args.schema}) ===\n")
        datasets = _load_sql_datasets(args.catalog, args.schema)
        for ds in datasets:
            print(f"  {ds['name']}: {ds['title']}")
        print(f"\nTotal datasets: {len(datasets)}")
        print(f"Tables: {', '.join(config['table_identifiers'])}")
        return

    instructions = _load_instructions()
    space_id = _deploy(config, args.profile, args.update, instructions)
    print(f"\nGenie Space is ready. Open it in your workspace to start asking questions.")
    print(f"Space ID: {space_id}")


if __name__ == "__main__":
    main()
