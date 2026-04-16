# engine/src/watchdog/remediation/review.py
"""Review Queue — state machine for remediation proposal review.

Pure functions that take proposal dicts and return updated state plus
review records. The caller handles Delta persistence.

Valid transitions:
  pending_review → approved  (via approve_proposal)
  pending_review → rejected  (via reject_proposal)
  pending_review → pending_review  (via reassign_proposal, new reviewer logged)
"""

import uuid
from datetime import datetime, timezone


def approve_proposal(proposal: dict, reviewer: str,
                     reasoning: str = "") -> tuple[dict, dict]:
    """Approve a proposal for application.

    Args:
        proposal: Proposal dict with at least proposal_id and status.
        reviewer: Identity of the reviewer.
        reasoning: Optional justification.

    Returns:
        Tuple of (updated proposal dict, review record dict).

    Raises:
        ValueError: If proposal is not in pending_review status.
    """
    if proposal.get("status") != "pending_review":
        raise ValueError(
            f"Cannot approve proposal in status '{proposal.get('status')}'. "
            f"Must be 'pending_review'."
        )

    updated = {**proposal, "status": "approved"}
    review = {
        "review_id": str(uuid.uuid4()),
        "proposal_id": proposal["proposal_id"],
        "reviewer": reviewer,
        "decision": "approved",
        "reasoning": reasoning,
        "reassigned_to": None,
        "reviewed_at": datetime.now(timezone.utc),
    }
    return updated, review


def reject_proposal(proposal: dict, reviewer: str,
                    reasoning: str = "") -> tuple[dict, dict]:
    """Reject a proposal.

    Args:
        proposal: Proposal dict with at least proposal_id and status.
        reviewer: Identity of the reviewer.
        reasoning: Optional justification for rejection.

    Returns:
        Tuple of (updated proposal dict, review record dict).

    Raises:
        ValueError: If proposal is not in pending_review status.
    """
    if proposal.get("status") != "pending_review":
        raise ValueError(
            f"Cannot reject proposal in status '{proposal.get('status')}'. "
            f"Must be 'pending_review'."
        )

    updated = {**proposal, "status": "rejected"}
    review = {
        "review_id": str(uuid.uuid4()),
        "proposal_id": proposal["proposal_id"],
        "reviewer": reviewer,
        "decision": "rejected",
        "reasoning": reasoning,
        "reassigned_to": None,
        "reviewed_at": datetime.now(timezone.utc),
    }
    return updated, review


def reassign_proposal(proposal: dict, reviewer: str,
                      reassigned_to: str,
                      reasoning: str = "") -> tuple[dict, dict]:
    """Reassign a proposal to a different reviewer.

    The proposal stays in pending_review status. A review record is
    created to track the reassignment decision.

    Args:
        proposal: Proposal dict with at least proposal_id and status.
        reviewer: Identity of the current reviewer doing the reassignment.
        reassigned_to: Identity of the new reviewer.
        reasoning: Optional justification.

    Returns:
        Tuple of (proposal dict unchanged in status, review record dict).

    Raises:
        ValueError: If proposal is not in pending_review status.
    """
    if proposal.get("status") != "pending_review":
        raise ValueError(
            f"Cannot reassign proposal in status '{proposal.get('status')}'. "
            f"Must be 'pending_review'."
        )

    updated = {**proposal}  # status stays pending_review
    review = {
        "review_id": str(uuid.uuid4()),
        "proposal_id": proposal["proposal_id"],
        "reviewer": reviewer,
        "decision": "reassigned",
        "reasoning": reasoning,
        "reassigned_to": reassigned_to,
        "reviewed_at": datetime.now(timezone.utc),
    }
    return updated, review
