# engine/src/watchdog/remediation/dispatcher.py
"""Remediation Dispatcher — routes open violations to registered agents.

Reads violations with status='open' and no active remediation, finds the
first agent whose handles[] matches the violation's policy_id, calls
gather_context + propose_fix, and writes the proposal to Delta.

Idempotent: skips violations that already have a proposal from the same
agent version.
"""

import uuid
from datetime import datetime, timezone


def dispatch_remediations(violations: list[dict], agents: list,
                          existing_proposal_keys: set[tuple] | None = None
                          ) -> dict:
    """Route violations to agents and collect proposals.

    Pure function for testability. Does not read/write Spark tables directly —
    the caller handles that.

    Args:
        violations: List of violation dicts (rows from violations table)
            with keys: violation_id, policy_id, resource_name, etc.
        agents: List of objects satisfying the RemediationAgent protocol.
        existing_proposal_keys: Set of (violation_id, agent_id, agent_version)
            tuples for proposals that already exist. Used for idempotency.

    Returns:
        Dict with keys:
            proposals: list of proposal dicts ready for Delta insertion
            dispatched: count of new proposals created
            skipped: count of violations skipped (already proposed or no agent)
            errors: count of agent failures
    """
    if existing_proposal_keys is None:
        existing_proposal_keys = set()

    # Build policy_id → agent lookup (first match wins)
    policy_agent_map: dict[str, object] = {}
    for agent in agents:
        for policy_id in agent.handles:
            if policy_id not in policy_agent_map:
                policy_agent_map[policy_id] = agent

    proposals = []
    dispatched = 0
    skipped = 0
    errors = 0

    for violation in violations:
        policy_id = violation.get("policy_id", "")
        violation_id = violation.get("violation_id", "")

        # Find matching agent
        agent = policy_agent_map.get(policy_id)
        if agent is None:
            skipped += 1
            continue

        # Idempotency check
        key = (violation_id, agent.agent_id, agent.version)
        if key in existing_proposal_keys:
            skipped += 1
            continue

        # Dispatch
        try:
            context = agent.gather_context(violation)
            fix = agent.propose_fix(context)

            proposal = {
                "proposal_id": str(uuid.uuid4()),
                "violation_id": violation_id,
                "agent_id": agent.agent_id,
                "agent_version": agent.version,
                "status": "pending_review",
                "proposed_sql": fix.get("proposed_sql", ""),
                "confidence": fix.get("confidence", 0.0),
                "context_json": fix.get("context_json", ""),
                "llm_prompt_hash": "",
                "citations": fix.get("citations", ""),
                "created_at": datetime.now(timezone.utc),
            }
            proposals.append(proposal)
            dispatched += 1

        except Exception:
            errors += 1

    return {
        "proposals": proposals,
        "dispatched": dispatched,
        "skipped": skipped,
        "errors": errors,
    }
