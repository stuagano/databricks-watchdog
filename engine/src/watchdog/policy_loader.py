"""Policy Loader — loads ontology-aware policies from YAML and syncs to Delta.

Policies are organized by compliance domain in the policies/ directory:
  - cost_governance.yml
  - security_governance.yml
  - data_quality.yml
  - operational.yml
  - regulatory.yml

Each policy file contains a list of policies with:
  - id, name, applies_to, domain, severity, description, remediation, rule

The loader converts YAML policies into PolicyDefinition objects for the engine,
and syncs them to the policies Delta table for dashboards and audit.

Hybrid policy management:
  - YAML policies (origin='yaml') are the SA-managed baseline, version-controlled
    in git and synced to Delta on each deploy.
  - User policies (origin='user') are created directly in the Delta table via
    SQL or notebook by platform admins. The YAML sync never overwrites
    or deactivates user-created policies.
  - At evaluation time, both sources are merged — YAML seeds best practices,
    users tune and extend for their environment.
"""

import json
import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from watchdog.policy_engine import PolicyDefinition


def load_yaml_policies(policies_dir: str | None = None) -> list[PolicyDefinition]:
    """Load all policy definitions from YAML files in the policies directory.

    Returns a list of PolicyDefinition objects ready for the policy engine.
    Skips the legacy starter_policies.yml format (no 'applies_to' field).
    """
    if policies_dir is None:
        policies_dir = str(Path(__file__).parent.parent.parent / "policies")

    policies = []
    policies_path = Path(policies_dir)

    if not policies_path.exists():
        return policies

    for yaml_file in sorted(policies_path.glob("*.yml")):
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if not data or "policies" not in data:
            continue

        for p in data["policies"]:
            rule = p.get("rule", {})
            # Handle shorthand: if rule is a string, treat as primitive ref
            if isinstance(rule, str):
                rule = {"ref": rule}

            # Determine applies_to — use explicit field or convert legacy format
            applies_to = p.get("applies_to", "*")
            if "resource_types" in p and "applies_to" not in p:
                # Legacy format: convert resource_types to applies_to
                rt = p.get("resource_types", ["*"])
                if rt == ["*"] or "*" in rt:
                    applies_to = "*"
                elif rt == ["table"]:
                    applies_to = "DataAsset"
                elif rt == ["job"]:
                    applies_to = "ComputeAsset"
                elif set(rt) <= {"cluster", "warehouse", "job", "pipeline"}:
                    applies_to = "ComputeAsset"
                else:
                    applies_to = "*"

            # Skip policies with no rule — they can't be evaluated
            if not rule:
                continue

            policies.append(PolicyDefinition(
                policy_id=p["id"],
                name=p["name"],
                applies_to=applies_to,
                domain=p.get("domain", "Uncategorized"),
                severity=p.get("severity", "medium"),
                description=p.get("description", ""),
                remediation=p.get("remediation", ""),
                rule=rule,
                active=p.get("active", True),
            ))

    return policies


def load_policies_metadata(policies_dir: str | None = None) -> list[dict]:
    """Load policy metadata for Delta sync (without rule objects).

    Returns flat dicts suitable for DataFrame creation.
    """
    if policies_dir is None:
        policies_dir = str(Path(__file__).parent.parent.parent / "policies")

    rows = []
    policies_path = Path(policies_dir)

    if not policies_path.exists():
        return rows

    for yaml_file in sorted(policies_path.glob("*.yml")):
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if not data or "policies" not in data:
            continue

        for p in data["policies"]:
            # Handle both legacy and ontology formats
            applies_to = p.get("applies_to", "*")
            domain = p.get("domain", "Uncategorized")

            # Legacy format: convert resource_types to applies_to
            if "resource_types" in p and "applies_to" not in p:
                rt = p.get("resource_types", ["*"])
                applies_to = ",".join(rt)
                domain = "Legacy"

            rows.append({
                "policy_id": p["id"],
                "policy_name": p["name"],
                "applies_to": applies_to,
                "domain": domain,
                "severity": p.get("severity", "medium"),
                "description": p.get("description", ""),
                "remediation": p.get("remediation", ""),
                "active": p.get("active", True),
                "rule_json": json.dumps(p.get("rule", {})),
                "source_file": yaml_file.name,
            })

    return rows


def load_delta_policies(spark: SparkSession, catalog: str,
                        schema: str) -> list[PolicyDefinition]:
    """Load user-created policies from the Delta table.

    Returns PolicyDefinition objects for policies with origin='user' that are
    active. These are combined with YAML policies at evaluation time so the
    engine sees both sources.
    """
    policies_table = f"{catalog}.{schema}.policies"

    try:
        rows = spark.sql(f"""
            SELECT policy_id, policy_name, applies_to, domain, severity,
                   description, remediation, rule_json, active
            FROM {policies_table}
            WHERE origin = 'user' AND active = true
        """).collect()
    except Exception:
        return []

    policies = []
    for row in rows:
        rule = {}
        if row.rule_json:
            try:
                rule = json.loads(row.rule_json)
            except json.JSONDecodeError:
                pass

        policies.append(PolicyDefinition(
            policy_id=row.policy_id,
            name=row.policy_name or "",
            applies_to=row.applies_to or "*",
            domain=row.domain or "User",
            severity=row.severity or "medium",
            description=row.description or "",
            remediation=row.remediation or "",
            rule=rule,
            active=True,
        ))

    return policies


def _ensure_policies_history_table(spark: SparkSession, catalog: str,
                                    schema: str) -> str:
    """Create the policies_history table if it doesn't exist.

    Append-only audit trail of every policy change — from YAML deploys and
    user edits alike. Enables "what changed and when" queries that the
    current-state policies table cannot answer.

    Returns the fully qualified table name.
    """
    table = f"{catalog}.{schema}.policies_history"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            policy_id STRING NOT NULL,
            version INT NOT NULL,
            policy_name STRING,
            applies_to STRING,
            domain STRING,
            severity STRING,
            description STRING,
            remediation STRING,
            rule_json STRING,
            active BOOLEAN,
            origin STRING NOT NULL,
            change_type STRING NOT NULL,
            changed_by STRING,
            changed_at TIMESTAMP NOT NULL
        )
        USING DELTA
    """)
    return table


def _record_policy_changes(spark: SparkSession, catalog: str, schema: str,
                            source_rows: list[dict], origin: str,
                            changed_by: str | None = None) -> int:
    """Compare incoming policies against the current policies table and append
    history rows for any that are new or changed.

    Change detection uses md5(rule_json || severity || applies_to || active)
    so cosmetic edits to description/remediation don't generate noise.

    Returns the number of history rows written.
    """
    policies_table = f"{catalog}.{schema}.policies"
    history_table = _ensure_policies_history_table(spark, catalog, schema)

    # Build a map of incoming policies keyed by policy_id
    incoming = {p["policy_id"]: p for p in source_rows}
    if not incoming:
        return 0

    # Read current state for these policy_ids
    id_list = ", ".join(f"'{pid}'" for pid in incoming)
    try:
        current_rows = spark.sql(f"""
            SELECT policy_id, rule_json, severity, applies_to, active
            FROM {policies_table}
            WHERE policy_id IN ({id_list})
        """).collect()
        current = {r.policy_id: r for r in current_rows}
    except Exception:
        current = {}

    # Read max version per policy_id from history
    try:
        version_rows = spark.sql(f"""
            SELECT policy_id, MAX(version) AS max_version
            FROM {history_table}
            WHERE policy_id IN ({id_list})
            GROUP BY policy_id
        """).collect()
        versions = {r.policy_id: r.max_version for r in version_rows}
    except Exception:
        versions = {}

    # Detect changes and build history rows
    history_rows = []
    for pid, p in incoming.items():
        existing = current.get(pid)
        if existing is None:
            change_type = "created"
        else:
            # Compare the fields that matter for evaluation behavior
            changed = (
                existing.rule_json != p["rule_json"]
                or existing.severity != p["severity"]
                or existing.applies_to != p["applies_to"]
                or existing.active != p["active"]
            )
            if not changed:
                continue
            change_type = "updated"

        next_version = versions.get(pid, 0) + 1
        versions[pid] = next_version

        history_rows.append((
            pid, next_version, p["policy_name"], p["applies_to"],
            p["domain"], p["severity"], p["description"], p["remediation"],
            p["rule_json"], p["active"], origin, change_type, changed_by,
        ))

    if not history_rows:
        return 0

    _history_schema = T.StructType([
        T.StructField("policy_id", T.StringType()),
        T.StructField("version", T.IntegerType()),
        T.StructField("policy_name", T.StringType()),
        T.StructField("applies_to", T.StringType()),
        T.StructField("domain", T.StringType()),
        T.StructField("severity", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("remediation", T.StringType()),
        T.StructField("rule_json", T.StringType()),
        T.StructField("active", T.BooleanType()),
        T.StructField("origin", T.StringType()),
        T.StructField("change_type", T.StringType()),
        T.StructField("changed_by", T.StringType()),
    ])
    df = spark.createDataFrame(
        history_rows, schema=_history_schema
    ).withColumn("changed_at", F.current_timestamp())

    df.write.mode("append").saveAsTable(history_table)
    return len(history_rows)


def sync_policies_to_delta(spark: SparkSession, catalog: str, schema: str,
                           policies_dir: str | None = None) -> int:
    """Sync YAML policy definitions to the policies Delta table.

    Uses MERGE to upsert by policy_id. Only touches rows with origin='yaml'.
    User-created policies (origin='user') are never overwritten or deactivated.

    Also writes to policies_history for any new or changed policies so that
    every mutation — from YAML deploys or user edits — has an append-only
    audit trail.

    Returns the number of YAML policies synced.
    """
    policies_table = f"{catalog}.{schema}.policies"
    policy_rows = load_policies_metadata(policies_dir)

    if not policy_rows:
        return 0

    rows = [
        (
            p["policy_id"],
            p["policy_name"],
            p["applies_to"],
            p["domain"],
            p["severity"],
            p["description"],
            p["remediation"],
            p["active"],
            p["rule_json"],
            p["source_file"],
            "yaml",
        )
        for p in policy_rows
    ]

    _policy_schema = T.StructType([
        T.StructField("policy_id", T.StringType()),
        T.StructField("policy_name", T.StringType()),
        T.StructField("applies_to", T.StringType()),
        T.StructField("domain", T.StringType()),
        T.StructField("severity", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("remediation", T.StringType()),
        T.StructField("active", T.BooleanType()),
        T.StructField("rule_json", T.StringType()),
        T.StructField("source_file", T.StringType()),
        T.StructField("origin", T.StringType()),
    ])
    source_df = spark.createDataFrame(rows, schema=_policy_schema)

    # Create table if it doesn't exist (with origin column for hybrid management)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {policies_table} (
            policy_id STRING NOT NULL,
            policy_name STRING,
            applies_to STRING,
            domain STRING,
            resource_types STRING,
            severity STRING,
            description STRING,
            remediation STRING,
            active BOOLEAN,
            rule_json STRING,
            source_file STRING,
            origin STRING NOT NULL,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)

    # Record changes to policies_history BEFORE the MERGE updates current state
    _record_policy_changes(spark, catalog, schema, policy_rows, origin="yaml")

    source_df.createOrReplaceTempView("_watchdog_policies_source")

    # MERGE: upsert YAML policies only — skip rows that have been claimed by users
    spark.sql(f"""
        MERGE INTO {policies_table} AS target
        USING _watchdog_policies_source AS source
        ON target.policy_id = source.policy_id
        WHEN MATCHED AND target.origin = 'yaml' THEN UPDATE SET
            policy_name = source.policy_name,
            applies_to = source.applies_to,
            domain = source.domain,
            severity = source.severity,
            description = source.description,
            remediation = source.remediation,
            active = source.active,
            rule_json = source.rule_json,
            source_file = source.source_file,
            origin = 'yaml',
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            policy_id, policy_name, applies_to, domain, severity,
            description, remediation, active, rule_json, source_file,
            origin, updated_at
        ) VALUES (
            source.policy_id, source.policy_name, source.applies_to,
            source.domain, source.severity, source.description,
            source.remediation, source.active, source.rule_json,
            source.source_file, 'yaml', current_timestamp()
        )
    """)

    # Record deactivations for YAML policies removed from the repo
    yaml_ids = [p["policy_id"] for p in policy_rows]
    if yaml_ids:
        id_list = ", ".join(f"'{pid}'" for pid in yaml_ids)

        # Find policies about to be deactivated so we can log them
        try:
            deactivated = spark.sql(f"""
                SELECT policy_id, policy_name, applies_to, domain, severity,
                       description, remediation, rule_json, active
                FROM {policies_table}
                WHERE policy_id NOT IN ({id_list})
                  AND origin = 'yaml'
                  AND active = true
            """).collect()

            if deactivated:
                deactivated_rows = [
                    {
                        "policy_id": r.policy_id,
                        "policy_name": r.policy_name,
                        "applies_to": r.applies_to,
                        "domain": r.domain,
                        "severity": r.severity,
                        "description": r.description,
                        "remediation": r.remediation,
                        "rule_json": r.rule_json,
                        "active": False,
                    }
                    for r in deactivated
                ]
                _record_policy_changes(
                    spark, catalog, schema, deactivated_rows,
                    origin="yaml",
                )
        except Exception:
            pass  # History is best-effort — don't block the sync

        spark.sql(f"""
            UPDATE {policies_table}
            SET active = false, updated_at = current_timestamp()
            WHERE policy_id NOT IN ({id_list})
              AND origin = 'yaml'
              AND active = true
        """)

    return len(policy_rows)
