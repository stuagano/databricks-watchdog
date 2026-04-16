"""RemediationAgent Protocol — contract for pluggable remediation agents.

Agents implement this protocol to participate in the dispatch pipeline.
The dispatcher calls gather_context then propose_fix for each matching
violation. apply and verify are reserved for sub-project 3b.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class RemediationAgent(Protocol):
    """Contract for a pluggable remediation agent.

    Attributes:
        agent_id: Unique identifier for this agent.
        handles: Policy IDs this agent can remediate (e.g., ["POL-S001"]).
        version: Agent version string for reproducibility.
        model: LLM model used, or empty string for deterministic agents.
    """
    agent_id: str
    handles: list[str]
    version: str
    model: str

    def gather_context(self, violation: dict) -> dict:
        """Collect context needed to propose a fix.

        Args:
            violation: Row from the violations table as a dict.

        Returns:
            Context dict with keys relevant to the agent's domain.
        """
        ...

    def propose_fix(self, context: dict) -> dict:
        """Generate a fix proposal from gathered context.

        Args:
            context: Dict returned by gather_context.

        Returns:
            Dict with keys: proposed_sql, confidence (float 0-1),
            context_json (serialized context), citations (optional).
        """
        ...
