"""Generate Watchdog drift detection artifacts from YAML permission declarations.

Reference implementation showing how a permissions-as-code system integrates
with Watchdog's drift_check rule type. Reads permission YAML files and produces:
  - expected_state.json: snapshot Watchdog evaluates against actual UC state
  - permissions_drift_policies.yaml: drift detection policies

Usage:
    python watchdog_generator.py --permissions-dir ./example --env alpha --output-dir ./output
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ---------------------------------------------------------------------------
# Drift policies (static — same for every environment)
# ---------------------------------------------------------------------------

DRIFT_POLICIES = [
    {
        "id": "POL-PERM-001",
        "name": "UC grant drift detection",
        "applies_to": "GrantAsset",
        "domain": "AccessControl",
        "severity": "critical",
        "description": "Detects drift between declared UC grants and actual state.",
        "active": True,
        "rule": {"type": "drift_check", "source": "expected_permissions/expected_state.json", "check": "grants"},
    },
    {
        "id": "POL-PERM-002",
        "name": "Row filter integrity",
        "applies_to": "RowFilterAsset",
        "domain": "AccessControl",
        "severity": "critical",
        "description": "Verifies row-level security filters match expected definitions.",
        "active": True,
        "rule": {"type": "drift_check", "source": "expected_permissions/expected_state.json", "check": "row_filters"},
    },
    {
        "id": "POL-PERM-003",
        "name": "Column mask integrity",
        "applies_to": "ColumnMaskAsset",
        "domain": "AccessControl",
        "severity": "critical",
        "description": "Verifies column masking functions match expected definitions.",
        "active": True,
        "rule": {"type": "drift_check", "source": "expected_permissions/expected_state.json", "check": "column_masks"},
    },
    {
        "id": "POL-PERM-004",
        "name": "Team membership drift",
        "applies_to": "GroupMemberAsset",
        "domain": "AccessControl",
        "severity": "high",
        "description": "Detects drift in group membership and role assignments.",
        "active": True,
        "rule": {"type": "drift_check", "source": "expected_permissions/expected_state.json", "check": "group_membership"},
    },
]


def _checksum(body: str) -> str:
    return "sha256:" + hashlib.sha256(body.encode()).hexdigest()


def load_grants(permissions_dir: Path) -> list[dict]:
    """Load grant declarations from domains/*.yaml."""
    grants = []
    domains_dir = permissions_dir / "domains"
    if not domains_dir.exists():
        return grants
    for f in sorted(domains_dir.glob("*.yaml")):
        if f.name.startswith("_"):
            continue
        with open(f) as fh:
            doc = yaml.safe_load(fh)
        catalog = doc.get("catalog", "")
        for g in doc.get("grants", []):
            grants.append({
                "catalog": catalog,
                "schema": g.get("schema", ""),
                "table": g.get("table", ""),
                "principal": g["principal"],
                "privileges": sorted(g["privileges"]),
            })
    grants.sort(key=lambda r: (r["catalog"], r["schema"], r["table"], r["principal"]))
    return grants


def load_row_filters(permissions_dir: Path) -> list[dict]:
    """Load row filter declarations from abac/row-filters.yaml."""
    path = permissions_dir / "abac" / "row-filters.yaml"
    if not path.exists():
        return []
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    entries = []
    for rf in doc.get("row_filters", []):
        entries.append({
            "table": rf["table"],
            "function": rf["function"],
            "enforcement": rf.get("enforcement", "uc_native"),
            "checksum": _checksum(json.dumps(rf.get("rules", []), sort_keys=True)),
        })
    return entries


def load_column_masks(permissions_dir: Path) -> list[dict]:
    """Load column mask declarations from abac/column-masks.yaml."""
    path = permissions_dir / "abac" / "column-masks.yaml"
    if not path.exists():
        return []
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    entries = []
    for cm in doc.get("column_masks", []):
        entries.append({
            "table": cm["table"],
            "column": cm["column"],
            "function": cm["function"],
            "enforcement": cm.get("enforcement", "uc_native"),
            "checksum": _checksum(cm.get("mask_body", "")),
        })
    return entries


def load_group_membership(permissions_dir: Path) -> list[dict]:
    """Load group membership from teams.yaml."""
    path = permissions_dir / "teams.yaml"
    if not path.exists():
        return []
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    entries = []
    for team in doc.get("teams", []):
        entries.append({
            "group": team["name"],
            "members": sorted(team.get("members", [])),
        })
    return entries


def generate(permissions_dir: Path, env: str, output_dir: Path) -> None:
    """Generate drift detection artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Expected state
    state = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": env,
        "grants": load_grants(permissions_dir),
        "row_filters": load_row_filters(permissions_dir),
        "column_masks": load_column_masks(permissions_dir),
        "group_membership": load_group_membership(permissions_dir),
    }
    (output_dir / "expected_state.json").write_text(json.dumps(state, indent=2) + "\n")

    # Drift policies
    policies_yaml = yaml.dump({"policies": DRIFT_POLICIES}, default_flow_style=False, sort_keys=False)
    (output_dir / "permissions_drift_policies.yaml").write_text(policies_yaml)

    print(f"Generated {len(state['grants'])} grants, {len(state['row_filters'])} row filters, "
          f"{len(state['column_masks'])} column masks, {len(state['group_membership'])} groups")
    print(f"Output: {output_dir}/expected_state.json, {output_dir}/permissions_drift_policies.yaml")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Watchdog drift artifacts from YAML permissions")
    parser.add_argument("--permissions-dir", required=True, help="Directory with domains/, abac/, teams.yaml")
    parser.add_argument("--env", required=True, help="Environment name (alpha, beta, live)")
    parser.add_argument("--output-dir", default="./output", help="Output directory")
    args = parser.parse_args()
    generate(Path(args.permissions_dir), args.env, Path(args.output_dir))
