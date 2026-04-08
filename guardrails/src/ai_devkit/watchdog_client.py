"""Watchdog client — reads governance state from platform.watchdog.* tables.

Provides the guardrails tools with live classification, violation, policy,
and exception data from the Watchdog governance engine. This is the bridge
between Watchdog's offline scan results and the AI DevKit's real-time
pre-flight checks.

All queries run as the calling user (on-behalf-of auth). UC grants on the
watchdog schema govern access — if the user can't read the tables, the
guardrails degrade gracefully to tag-only heuristics.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient

from ai_devkit.config import AiDevkitConfig

logger = logging.getLogger(__name__)


@dataclass
class ResourceGovernanceState:
    """Governance state for a single resource from Watchdog."""

    resource_id: str
    classes: list[str] = field(default_factory=list)
    ancestors: list[str] = field(default_factory=list)
    open_violations: list[dict[str, Any]] = field(default_factory=list)
    active_exceptions: list[dict[str, Any]] = field(default_factory=list)
    policies_applied: list[dict[str, Any]] = field(default_factory=list)
    watchdog_available: bool = True

    @property
    def is_pii(self) -> bool:
        return any("Pii" in c for c in self.classes + self.ancestors)

    @property
    def is_confidential(self) -> bool:
        return any("Confidential" in c for c in self.classes + self.ancestors)

    @property
    def is_export_controlled(self) -> bool:
        return any(
            c in self.classes + self.ancestors
            for c in ("ExportControlledAsset", "ItarAsset", "EarAsset")
        )

    @property
    def is_restricted(self) -> bool:
        return self.is_export_controlled or any(
            "Restricted" in c for c in self.classes + self.ancestors
        )

    @property
    def has_critical_violations(self) -> bool:
        return any(v.get("severity") == "critical" for v in self.open_violations)

    @property
    def has_high_violations(self) -> bool:
        return any(v.get("severity") == "high" for v in self.open_violations)

    @property
    def has_exception(self, policy_id: str | None = None) -> bool:
        if policy_id:
            return any(e.get("policy_id") == policy_id for e in self.active_exceptions)
        return len(self.active_exceptions) > 0

    @property
    def inferred_classification(self) -> str:
        """Infer classification level from ontology classes.

        Uses the class hierarchy to determine the most restrictive
        classification. Falls back to 'unclassified' if no classes match.
        """
        if self.is_export_controlled:
            return "restricted"
        if self.is_restricted:
            return "restricted"
        if self.is_confidential:
            return "confidential"
        if self.is_pii:
            return "confidential"
        if any("Internal" in c for c in self.classes + self.ancestors):
            return "internal"
        if any("Public" in c for c in self.classes + self.ancestors):
            return "public"
        return "unclassified"


def get_resource_governance(
    w: WorkspaceClient,
    config: AiDevkitConfig,
    resource_id: str,
) -> ResourceGovernanceState:
    """Fetch full governance state for a resource from Watchdog tables.

    Queries resource_classifications, violations, and exceptions in
    parallel-ish (sequential SQL, but fast on serverless warehouse).
    Degrades gracefully if watchdog tables are inaccessible.
    """
    state = ResourceGovernanceState(resource_id=resource_id)
    schema = config.watchdog_schema

    # 1. Classifications — what ontology classes does this resource belong to?
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=f"""
                SELECT class_name, class_ancestors
                FROM {schema}.resource_classifications
                WHERE resource_id = '{_esc(resource_id)}'
                ORDER BY class_name
            """,
            wait_timeout="10s",
        )
        if resp.result and resp.result.data_array:
            for row in resp.result.data_array:
                state.classes.append(row[0])
                if row[1]:
                    state.ancestors.extend(
                        a.strip() for a in str(row[1]).split(",") if a.strip()
                    )
    except Exception as e:
        logger.debug(f"Watchdog classifications unavailable for {resource_id}: {e}")
        state.watchdog_available = False
        return state

    # 2. Open violations — what governance rules is this resource failing?
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=f"""
                SELECT violation_id, policy_id, policy_name, severity, domain
                FROM {schema}.violations
                WHERE resource_id = '{_esc(resource_id)}' AND active = true
                ORDER BY
                    CASE severity
                        WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2 ELSE 3
                    END
            """,
            wait_timeout="10s",
        )
        if resp.result and resp.result.data_array:
            cols = [c.name for c in resp.manifest.schema.columns]
            state.open_violations = [dict(zip(cols, row)) for row in resp.result.data_array]
    except Exception as e:
        logger.debug(f"Watchdog violations unavailable for {resource_id}: {e}")

    # 3. Active exceptions — approved waivers for this resource
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=f"""
                SELECT exception_id, policy_id, justification,
                       CAST(expires_at AS STRING) AS expires_at
                FROM {schema}.exceptions
                WHERE resource_id = '{_esc(resource_id)}'
                  AND active = true
                  AND (expires_at IS NULL OR expires_at > current_timestamp())
            """,
            wait_timeout="10s",
        )
        if resp.result and resp.result.data_array:
            cols = [c.name for c in resp.manifest.schema.columns]
            state.active_exceptions = [dict(zip(cols, row)) for row in resp.result.data_array]
    except Exception as e:
        logger.debug(f"Watchdog exceptions unavailable for {resource_id}: {e}")

    return state


def get_policies_for_operation(
    w: WorkspaceClient,
    config: AiDevkitConfig,
    operation: str,
) -> list[dict[str, Any]]:
    """Fetch active policies relevant to an operation type.

    Returns policies from the watchdog policies table that the guardrails
    should evaluate at query time. Useful for showing the user what rules
    apply to their intended operation.
    """
    schema = config.watchdog_schema
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=f"""
                SELECT policy_id, policy_name, applies_to, domain, severity,
                       description, remediation
                FROM {schema}.policies
                WHERE active = true
                ORDER BY
                    CASE severity
                        WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2 ELSE 3
                    END
            """,
            wait_timeout="10s",
        )
        if resp.result and resp.result.data_array:
            cols = [c.name for c in resp.manifest.schema.columns]
            return [dict(zip(cols, row)) for row in resp.result.data_array]
    except Exception as e:
        logger.debug(f"Watchdog policies unavailable: {e}")

    return []


def _esc(value: str) -> str:
    """Escape single quotes for SQL."""
    return value.replace("'", "''")
