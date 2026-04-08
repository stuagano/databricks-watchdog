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

    # Build table identifiers from the underlying tables
    table_identifiers = sorted(set(
        f"{catalog}.{schema}.{t}"
        for t in [
            "violations",
            "resource_inventory",
            "resource_classifications",
            "policies",
            "exceptions",
        ]
    ))

    config = {
        "title": space_name,
        "description": (
            "Watchdog Governance Genie Space -- natural language exploration "
            "of compliance posture, violations, data classification, and "
            "policy effectiveness across your Databricks workspace."
        ),
        "table_identifiers": table_identifiers,
    }

    if warehouse_id:
        config["warehouse_id"] = warehouse_id

    # Build serialized_space with instructions and datasets as sample queries
    serialized = {
        "version": 2,
        "instructions": {
            "text_instructions": [
                {
                    "id": str(uuid.uuid4()),
                    "content": [line + "\n" for line in instructions.splitlines()],
                }
            ]
        },
        "data_sources": {
            "tables": sorted(
                [{"identifier": tid} for tid in table_identifiers],
                key=lambda t: t["identifier"],
            )
        },
    }

    config["serialized_space"] = json.dumps(serialized)

    # Sample questions go at the top level, not inside serialized_space
    config["sample_questions"] = [
        "What is our overall compliance posture by domain?",
        "Who has the most critical open violations?",
        "Which PII tables are missing a data steward?",
        "What percentage of tables have data quality monitoring?",
        "Which policies are generating the most violations?",
        "Show me all critical violations for gold tables",
    ]

    return config


def _deploy(config: dict, profile: str | None, update_space_id: str | None) -> str:
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
        # Update existing space
        print(f"Updating Genie Space {update_space_id} ...")
        resp = w.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{update_space_id}",
            body=config,
        )
        space_id = update_space_id
        print(f"Updated Genie Space: {space_id}")
    else:
        # Create new space
        print("Creating Genie Space ...")
        resp = w.api_client.do(
            "POST",
            "/api/2.0/genie/spaces",
            body=config,
        )
        space_id = resp.get("space_id", resp.get("id", "unknown"))
        print(f"Created Genie Space: {space_id}")

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

    space_id = _deploy(config, args.profile, args.update)
    print(f"\nGenie Space is ready. Open it in your workspace to start asking questions.")
    print(f"Space ID: {space_id}")


if __name__ == "__main__":
    main()
