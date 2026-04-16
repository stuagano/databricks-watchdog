# engine/src/watchdog/remediation/applier.py
"""Applier — executes approved remediation proposals.

Pure function that takes an approved proposal and returns an apply result.
The caller handles actual SQL execution against Unity Catalog and Delta
persistence. Supports dry-run mode for previewing without executing.
"""

import uuid
from datetime import datetime, timezone


def apply_proposal(proposal: dict, pre_state: str = "",
                   dry_run: bool = False) -> tuple[dict, dict]:
    """Apply an approved proposal.

    Args:
        proposal: Proposal dict with status="approved".
        pre_state: Serialized state before application (for rollback).
        dry_run: If True, create the result but mark as dry_run.

    Returns:
        Tuple of (updated proposal dict, apply_result dict).

    Raises:
        ValueError: If proposal is not in approved status.
    """
    if proposal.get("status") != "approved":
        raise ValueError(
            f"Cannot apply proposal in status '{proposal.get('status')}'. "
            f"Must be 'approved'."
        )

    verify_status = "dry_run" if dry_run else "pending"
    new_status = "applied" if not dry_run else "approved"  # dry_run doesn't change proposal status

    updated_proposal = {**proposal, "status": new_status}
    apply_result = {
        "apply_id": str(uuid.uuid4()),
        "proposal_id": proposal["proposal_id"],
        "executed_sql": proposal.get("proposed_sql", ""),
        "pre_state": pre_state,
        "post_state": "",
        "applied_at": datetime.now(timezone.utc),
        "verify_scan_id": None,
        "verify_status": verify_status,
    }
    return updated_proposal, apply_result
