"""NoOp Agent — stub implementation for testing the dispatch pipeline.

Always proposes a trivial fix with low confidence. Used to validate
that the dispatcher, tables, and protocol work end-to-end without
requiring an LLM or external data sources.
"""

import json


class NoOpAgent:
    """Stub agent that satisfies the RemediationAgent protocol."""

    agent_id: str = "noop-agent"
    handles: list[str] = ["POL-TEST-001"]
    version: str = "1.0.0"
    model: str = ""

    def gather_context(self, violation: dict) -> dict:
        """Returns the violation as-is — no enrichment needed."""
        return {"violation": violation}

    def propose_fix(self, context: dict) -> dict:
        """Proposes a trivial owner-tag fix with low confidence."""
        violation = context.get("violation", {})
        resource = violation.get("resource_name", "unknown_resource")
        return {
            "proposed_sql": f"ALTER TABLE {resource} SET TAGS ('owner' = 'unassigned')",
            "confidence": 0.1,
            "context_json": json.dumps(context),
            "citations": "",
        }
