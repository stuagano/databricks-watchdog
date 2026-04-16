"""StewardAgent — suggests data stewards for PII tables missing one.

Deterministic agent that uses the resource owner as the suggested steward.
Handles POL-SEC-003 (PII tables must have a data steward). No LLM needed —
the heuristic is: if there's an owner, they're the likely steward.
"""

import json


class StewardAgent:
    """Deterministic steward assignment agent."""

    agent_id: str = "steward-agent"
    handles: list[str] = ["POL-SEC-003"]
    version: str = "1.0.0"
    model: str = ""

    def gather_context(self, violation: dict) -> dict:
        """Extract resource metadata relevant to steward assignment."""
        return {
            "violation": violation,
            "resource_name": violation.get("resource_name", "unknown"),
            "owner": violation.get("owner", ""),
            "resource_type": violation.get("resource_type", "table"),
        }

    def propose_fix(self, context: dict) -> dict:
        """Suggest a data steward based on resource ownership.

        If the resource has an owner, suggest them as steward (high confidence).
        If no owner, suggest 'unassigned' (low confidence — needs human review).
        """
        resource = context.get("resource_name", "unknown")
        owner = context.get("owner", "")

        if owner and owner.strip():
            steward = owner.strip()
            confidence = 0.9
        else:
            steward = "unassigned"
            confidence = 0.3

        return {
            "proposed_sql": f"ALTER TABLE {resource} SET TAGS ('data_steward' = '{steward}')",
            "confidence": confidence,
            "context_json": json.dumps(context),
            "citations": "",
        }
