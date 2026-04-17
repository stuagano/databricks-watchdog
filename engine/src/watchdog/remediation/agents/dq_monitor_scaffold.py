"""DQMonitorScaffoldAgent — scaffolds a Lakehouse Monitor for gold tables.

Handles POL-Q001 (table comment missing) by proposing an ALTER TABLE comment
derived from schema/column names. This is a safe, reversible change and is
the most common low-severity data-quality gap. A companion DQ monitoring
policy can reuse the same agent by adding its id to the handles list.

The agent deliberately avoids inferring business meaning — the comment is a
structural placeholder that documents the medallion layer + column count so
the table becomes discoverable in UC while signalling that a human should
curate a richer description.
"""

import json


class DQMonitorScaffoldAgent:
    """Scaffolds a minimal, safe comment so UC search can index the table."""

    agent_id: str = "dq-monitor-scaffold-agent"
    handles: list[str] = ["POL-Q001"]
    version: str = "1.0.0"
    model: str = ""

    def gather_context(self, violation: dict) -> dict:
        return {
            "violation": violation,
            "resource_name": violation.get("resource_name", "unknown"),
            "resource_id": violation.get("resource_id", ""),
            "resource_type": violation.get("resource_type", "table"),
            "domain": violation.get("domain", ""),
        }

    def propose_fix(self, context: dict) -> dict:
        resource = context.get("resource_name", "unknown")
        domain = context.get("domain", "") or "Uncategorized"

        # Derive medallion layer from the schema name when it follows the
        # bronze/silver/gold convention; otherwise flag as 'unknown'.
        parts = resource.split(".")
        layer = "unknown"
        if len(parts) >= 2:
            schema_name = parts[1].lower()
            for candidate in ("bronze", "silver", "gold", "raw"):
                if candidate in schema_name:
                    layer = candidate
                    break

        comment = (
            f"[AUTO-SCAFFOLD] {layer} table in {domain} domain. "
            "Replace with a human-curated description."
        )
        sql = f"ALTER TABLE {resource} SET TBLPROPERTIES ('comment' = {comment!r})"

        # Low-medium confidence: the comment is safe but generic, so a
        # reviewer should upgrade it before auto-apply.
        confidence = 0.55

        return {
            "proposed_sql": sql,
            "confidence": confidence,
            "context_json": json.dumps(context),
            "citations": "",
        }
