"""Watchdog client — reads governance state from Watchdog Delta tables.

Provides live classification, violation, and exception data for
pre-flight governance checks. No class-hierarchy inference: class
names are returned as-is for informational display only. Callers
use violations and exceptions for decisions, not ontology classes.

All queries run as the calling user (on-behalf-of auth). Degrades
gracefully if watchdog tables are inaccessible.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient

from watchdog_guardrails.config import GuardrailsConfig

logger = logging.getLogger(__name__)


@dataclass
class ResourceGovernanceState:
    """Governance state for a single resource from Watchdog."""

    resource_id: str
    classes: list[str] = field(default_factory=list)
    open_violations: list[dict[str, Any]] = field(default_factory=list)
    active_exceptions: list[dict[str, Any]] = field(default_factory=list)
    watchdog_available: bool = True

    @property
    def has_critical_violations(self) -> bool:
        return any(v.get("severity") == "critical" for v in self.open_violations)

    @property
    def has_high_violations(self) -> bool:
        return any(v.get("severity") == "high" for v in self.open_violations)

    def has_exception(self, policy_id: str | None = None) -> bool:
        """Check for active exception, optionally filtered by policy."""
        if policy_id:
            return any(e.get("policy_id") == policy_id for e in self.active_exceptions)
        return len(self.active_exceptions) > 0


def get_resource_governance(
    w: WorkspaceClient,
    config: GuardrailsConfig,
    resource_id: str,
) -> ResourceGovernanceState:
    """Fetch governance state for a resource from Watchdog tables.

    Queries classifications, violations, and exceptions sequentially.
    Returns a degraded state (watchdog_available=False) on any error
    from the first query — assumes tables are inaccessible.
    """
    state = ResourceGovernanceState(resource_id=resource_id)
    schema = config.watchdog_schema

    # 1. Classifications — class names for informational display
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
    except Exception as e:
        logger.debug(f"Watchdog classifications unavailable for {resource_id}: {e}")
        state.watchdog_available = False
        return state

    # 2. Open violations — drives the pre-flight verdict
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
            state.open_violations = [
                dict(zip(cols, row)) for row in resp.result.data_array
            ]
    except Exception as e:
        logger.debug(f"Watchdog violations unavailable for {resource_id}: {e}")

    # 3. Active exceptions — override violation-based blocks
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
            state.active_exceptions = [
                dict(zip(cols, row)) for row in resp.result.data_array
            ]
    except Exception as e:
        logger.debug(f"Watchdog exceptions unavailable for {resource_id}: {e}")

    return state


def _esc(value: str) -> str:
    """Escape single quotes for SQL."""
    return value.replace("'", "''")
