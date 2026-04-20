"""Policy Engine — ontology-aware governance evaluation.

Two-pass evaluation:
  1. Classify each resource into ontology classes based on tags
  2. For each policy, check if the resource's classes match the policy's
     applies_to target (including inheritance), then evaluate the rule

Policies are loaded from domain-scoped YAML files in policies/. Each policy
specifies:
  - applies_to: ontology class name or "*" for all resources
  - domain: compliance domain (CostGovernance, SecurityGovernance, etc.)
  - rule: declarative rule tree (or ref to a reusable primitive)
  - severity: critical, high, medium, low

The engine writes results to scan_results (append-only history) and merges
into the violations table (deduplicated, with exception handling).
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk import WorkspaceClient
from pyspark.sql import Row, SparkSession

from watchdog.drift import (
    build_expected_column_masks_lookup,
    build_expected_grants_lookup,
    build_expected_group_membership_lookup,
    build_expected_row_filters_lookup,
    load_expected_state,
)
from watchdog.ontology import OntologyEngine
from watchdog.policies_table import write_policies
from watchdog.rule_engine import RuleEngine
from watchdog.violations import merge_violations, write_classifications, write_scan_summary

# Fallback mapping: ontology class name → resource_types it covers.
# Used when ontology files are absent so policies with applies_to set to a
# class name still apply to the right resource types.
_CLASS_TYPE_FALLBACK: dict[str, set[str]] = {
    "DataAsset": {"table", "volume", "catalog", "schema"},
    "ComputeAsset": {"job", "cluster", "warehouse", "pipeline"},
    "IdentityAsset": {"user", "group", "service_principal"},
    # Derived classes fall back to their base class resource types
    "GoldTable": {"table"},
    "SilverTable": {"table"},
    "BronzeTable": {"table"},
    "PiiTable": {"table"},
    "ConfidentialAsset": {"table", "volume", "catalog", "schema"},
    "ProductionJob": {"job"},
    "InteractiveCluster": {"cluster"},
    "ProductionCluster": {"cluster"},
    "ComputeResource": {"job", "cluster", "warehouse", "pipeline"},
    # Fine-grained access control asset types
    "RowFilterAsset": {"row_filter"},
    "ColumnMaskAsset": {"column_mask"},
    "GroupMemberAsset": {"group_member"},
    "GrantAsset": {"grant"},
}


def ensure_scan_results_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the scan_results table if it doesn't exist.

    Append-only history of every (resource, policy) evaluation. Liquid
    clustering by (scan_id, policy_id) for efficient post-scan queries.
    """
    table = f"{catalog}.{schema}.scan_results"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            scan_id STRING NOT NULL,
            resource_id STRING NOT NULL,
            policy_id STRING NOT NULL,
            result STRING NOT NULL,
            details STRING,
            domain STRING,
            severity STRING,
            resource_classes STRING,
            metastore_id STRING,
            evaluated_at TIMESTAMP NOT NULL
        )
        USING DELTA
        CLUSTER BY (scan_id, policy_id)
        TBLPROPERTIES (
            'delta.appendOnly' = 'true'
        )
    """)


@dataclass
class PolicyDefinition:
    """A governance policy loaded from YAML."""
    policy_id: str
    name: str
    applies_to: str  # ontology class name or "*"
    domain: str
    severity: str
    description: str
    remediation: str
    rule: dict  # rule tree for the rule engine
    active: bool = True
    # Opt-in compile-down: each entry declares a runtime target the policy
    # should be emitted to (e.g. guardrails, uc_abac, uc_tag_policy, sdp).
    # Policies without compile_to remain scan-only.
    compile_to: list[dict] | None = None


@dataclass
class EvaluationSummary:
    """Summary of a full policy evaluation run."""
    policies_run: int
    resources_checked: int
    new_violations: int
    resolved: int
    classes_assigned: int  # how many classification assignments were made


class PolicyEngine:
    """Evaluates ontology-aware governance policies against the resource inventory."""

    def __init__(self, spark: SparkSession, w: WorkspaceClient,
                 catalog: str, schema: str,
                 ontology: OntologyEngine | None = None,
                 rule_engine: RuleEngine | None = None,
                 policies: list[PolicyDefinition] | None = None):
        self.spark = spark
        self.w = w
        self.catalog = catalog
        self.schema = schema
        self.ontology = ontology or OntologyEngine()
        self.rule_engine = rule_engine or RuleEngine()
        self.policies = policies or []
        self.now = datetime.now(timezone.utc)

    @property
    def _scan_results_table(self) -> str:
        return f"{self.catalog}.{self.schema}.scan_results"

    @property
    def _violations_table(self) -> str:
        return f"{self.catalog}.{self.schema}.violations"

    # ------------------------------------------------------------------
    # Drift-check helpers — extracted for testability (no Spark required)
    # ------------------------------------------------------------------

    def _build_drift_lookups(self, expected_state: dict) -> dict:
        """Build lookup tables for all drift-check resource types.

        Returns a dict with four lookup tables:
          - grants: principal → [grant entries]
          - row_filters: table_full_name → {table, function}
          - column_masks: '{table}.{column}' → {table, column, function}
          - group_membership: group_name → set of member values
        """
        return {
            "grants": build_expected_grants_lookup(expected_state.get("grants", [])),
            "row_filters": build_expected_row_filters_lookup(expected_state.get("row_filters", [])),
            "column_masks": build_expected_column_masks_lookup(expected_state.get("column_masks", [])),
            "group_membership": build_expected_group_membership_lookup(
                expected_state.get("group_membership", [])
            ),
        }

    def _inject_drift_metadata(
        self, resource_type: str, metadata: dict, lookups: dict
    ) -> dict:
        """Inject pre-computed expected-state values into resource metadata.

        Called per-resource during evaluate_all. Returns a (possibly augmented)
        copy of metadata — never mutates the input dict.
        """
        if resource_type == "grant":
            grants_lookup = lookups.get("grants", {})
            if grants_lookup:
                grantee = metadata.get("grantee", "")
                if grantee in grants_lookup:
                    metadata = {
                        **metadata,
                        "expected_grants": json.dumps(grants_lookup[grantee]),
                    }

        elif resource_type == "row_filter":
            rf_lookup = lookups.get("row_filters", {})
            if rf_lookup:
                table = metadata.get("table_full_name", "")
                if table in rf_lookup:
                    metadata = {
                        **metadata,
                        "expected_row_filters": json.dumps(rf_lookup[table]),
                    }

        elif resource_type == "column_mask":
            cm_lookup = lookups.get("column_masks", {})
            if cm_lookup:
                table = metadata.get("table_full_name", "")
                col = metadata.get("column_name", "")
                key = f"{table}.{col}"
                if key in cm_lookup:
                    metadata = {
                        **metadata,
                        "expected_column_masks": json.dumps(cm_lookup[key]),
                    }

        elif resource_type == "group_member":
            gm_lookup = lookups.get("group_membership", {})
            if gm_lookup:
                group = metadata.get("group_name", "")
                if group in gm_lookup:
                    metadata = {
                        **metadata,
                        "expected_group_members": json.dumps(
                            list(gm_lookup[group])
                        ),
                    }

        return metadata

    def evaluate_all(self) -> EvaluationSummary:
        """Evaluate all active policies against the latest resource inventory.

        Pipeline:
          1. Load latest scan from resource_inventory
          2. Classify each resource into ontology classes
          3. For each policy, find applicable resources and evaluate rules
          4. Write scan_results (append) and merge violations (dedup)
        """
        inventory_table = f"{self.catalog}.{self.schema}.resource_inventory"

        # Get the latest scan
        latest_scan_id = (
            self.spark.table(inventory_table)
            .select("scan_id")
            .distinct()
            .orderBy(F.col("scan_id").desc())
            .first()
        )

        if not latest_scan_id:
            return EvaluationSummary(0, 0, 0, 0, 0)

        scan_id = latest_scan_id.scan_id
        inventory = (
            self.spark.table(inventory_table)
            .filter(F.col("scan_id") == scan_id)
            .collect()
        )

        # Pass 1: Classify every resource
        resource_classes: dict[str, set[str]] = {}
        total_classifications = 0
        for resource in inventory:
            tags = resource.tags or {}
            metadata = resource.metadata or {}
            # Inject owner into metadata for rule engine access
            if resource.owner:
                metadata = {**metadata, "owner": resource.owner}
            # Inject resource_type into metadata for classifiers
            metadata = {**metadata, "resource_type": resource.resource_type}

            classes = self.ontology.get_all_classes_for_resource(
                resource.resource_type, tags, metadata
            )
            resource_classes[resource.resource_id] = classes
            total_classifications += len(classes)

        # Write classifications to Delta (for Ontos semantic linking)
        classification_rows = []
        for resource in inventory:
            classes = resource_classes.get(resource.resource_id, set())
            for cls_name in classes:
                ancestors = self.ontology.get_ancestor_chain(cls_name)
                root = ancestors[-1] if ancestors else cls_name
                classification_rows.append((
                    resource.resource_id,
                    resource.resource_type,
                    resource.resource_name,
                    resource.owner,
                    cls_name,
                    ",".join(ancestors),
                    root,
                    self.now,
                ))

        # Determine metastore_id from inventory for multi-metastore support
        metastore_id = inventory[0].metastore_id if inventory and hasattr(inventory[0], "metastore_id") else None

        write_classifications(
            self.spark, self.catalog, self.schema,
            scan_id, classification_rows,
            metastore_id=metastore_id,
        )

        # Pass 2: Evaluate policies
        active_policies = [p for p in self.policies if p.active]

        # Persist policy definitions to Delta for v_tag_policy_coverage
        write_policies(self.spark, self.catalog, self.schema, active_policies)

        # Load expected state for drift_check policies
        # Keyed by policy_id so each policy can reference a different source file.
        drift_lookups_by_policy: dict[str, dict] = {}
        for policy in active_policies:
            if policy.rule.get("type") == "drift_check" and "source" in policy.rule:
                source_path = policy.rule["source"]
                volume_path = f"/Volumes/{self.catalog}/{self.schema}/{source_path}"
                data_path = policy.rule.get("data_path")
                state = load_expected_state(volume_path, data_path=data_path)
                drift_lookups_by_policy[policy.policy_id] = self._build_drift_lookups(state)

        scan_results = []

        for policy in active_policies:
            for resource in inventory:
                # Check if this policy applies to this resource
                if not self._policy_applies(policy, resource, resource_classes):
                    continue

                tags = resource.tags or {}
                metadata = resource.metadata or {}
                if resource.owner:
                    metadata = {**metadata, "owner": resource.owner}

                # Inject expected state for drift_check evaluation
                if policy.rule.get("type") == "drift_check":
                    lookups = drift_lookups_by_policy.get(policy.policy_id, {})
                    metadata = self._inject_drift_metadata(
                        resource.resource_type, metadata, lookups
                    )

                # Evaluate the rule
                result = self.rule_engine.evaluate(policy.rule, tags, metadata)

                scan_results.append((
                    scan_id,
                    resource.resource_id,
                    policy.policy_id,
                    "pass" if result.passed else "fail",
                    result.detail,
                    policy.domain,
                    policy.severity,
                    ",".join(sorted(resource_classes.get(resource.resource_id, set()))),
                    metastore_id,
                    self.now,
                ))

        # Write scan results
        if scan_results:
            ensure_scan_results_table(self.spark, self.catalog, self.schema)
            _scan_schema = T.StructType([
                T.StructField("scan_id", T.StringType()),
                T.StructField("resource_id", T.StringType()),
                T.StructField("policy_id", T.StringType()),
                T.StructField("result", T.StringType()),
                T.StructField("details", T.StringType()),
                T.StructField("domain", T.StringType()),
                T.StructField("severity", T.StringType()),
                T.StructField("resource_classes", T.StringType()),
                T.StructField("metastore_id", T.StringType()),
                T.StructField("evaluated_at", T.TimestampType()),
            ])
            df = self.spark.createDataFrame(scan_results, schema=_scan_schema)
            df.write.mode("append").option("mergeSchema", "true").saveAsTable(self._scan_results_table)

        # Merge violations
        violation_summary = merge_violations(
            self.spark, self.catalog, self.schema, scan_id
        )

        # Snapshot posture for trend tracking
        write_scan_summary(
            self.spark, self.catalog, self.schema,
            scan_id=scan_id,
            scanned_at=self.now,
            metastore_id=metastore_id,
            total_resources=len(inventory),
            total_policies_evaluated=len(active_policies),
            total_classifications=total_classifications,
            violation_summary=violation_summary,
        )

        return EvaluationSummary(
            policies_run=len(active_policies),
            resources_checked=len(inventory),
            new_violations=violation_summary["new_this_scan"],
            resolved=violation_summary["resolved"],
            classes_assigned=total_classifications,
        )

    def _policy_applies(self, policy: PolicyDefinition, resource: Row,
                        resource_classes: dict[str, set[str]]) -> bool:
        """Check if a policy applies to a resource based on ontology class.

        When the ontology engine classifies resources, we check class membership.
        When ontology files are absent (MVP mode), we fall back to a static
        class-name → resource_type map so policies still apply correctly.
        """
        # Wildcard — applies to everything
        if policy.applies_to == "*":
            return True

        # Check ontology class membership
        classes = resource_classes.get(resource.resource_id, set())
        if classes:
            return policy.applies_to in classes

        # Fallback: no ontology classes assigned (MVP mode).
        # Match by resource_type using the static mapping.
        fallback_types = _CLASS_TYPE_FALLBACK.get(policy.applies_to)
        if fallback_types:
            return resource.resource_type in fallback_types

        return False

    # ------------------------------------------------------------------
    # Introspection helpers (for MCP tools / dashboards)
    # ------------------------------------------------------------------

    def get_policy_coverage(self) -> list[dict]:
        """Show which ontology classes have policies and which don't."""
        covered_classes = {p.applies_to for p in self.policies if p.active}
        all_classes = self.ontology.list_classes()

        result = []
        for cls in all_classes:
            descendants = self.ontology.get_descendants(cls["name"])
            # A class is covered if it or any ancestor has a policy
            directly_covered = cls["name"] in covered_classes
            inherited_coverage = any(
                a in covered_classes for a in cls["ancestors"]
            )
            result.append({
                "class": cls["name"],
                "parent": cls["parent"],
                "directly_covered": directly_covered,
                "inherited_coverage": inherited_coverage,
                "descendant_count": len(descendants),
                "policy_count": sum(
                    1 for p in self.policies
                    if p.applies_to == cls["name"] and p.active
                ),
            })
        return result

    def classify_resource(self, resource_type: str, tags: dict[str, str],
                          metadata: dict[str, str]) -> dict:
        """Classify a single resource — useful for debugging / MCP tool."""
        classes = self.ontology.get_all_classes_for_resource(
            resource_type, tags, metadata
        )
        applicable_policies = [
            {"id": p.policy_id, "name": p.name, "severity": p.severity}
            for p in self.policies
            if p.active and (p.applies_to == "*" or p.applies_to in classes)
        ]
        return {
            "resource_type": resource_type,
            "tags": tags,
            "classes": sorted(classes),
            "applicable_policies": applicable_policies,
        }
