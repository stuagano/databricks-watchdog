# engine/src/watchdog/remediation/verifier.py
"""Verifier — checks applied proposals against scan results.

Pure functions that take apply results and violation state, returning
updated verification status. The caller handles Delta reads and writes.

The key architectural insight: Watchdog itself is the verification oracle.
The next scan after application is the correctness check — no separate
test harness needed.
"""


def verify_proposal(apply_result: dict,
                    violation_resolved: bool) -> dict:
    """Verify an applied proposal against the latest scan.

    Args:
        apply_result: Dict from apply_proposal with verify_status="pending".
        violation_resolved: Whether the violation is now resolved in the
            latest scan (status changed from 'open' to 'resolved').

    Returns:
        Updated apply_result with verify_status set to 'verified' or
        'verification_failed'.

    Raises:
        ValueError: If apply_result is not in pending status.
    """
    if apply_result.get("verify_status") not in ("pending",):
        raise ValueError(
            f"Cannot verify result in status '{apply_result.get('verify_status')}'. "
            f"Must be 'pending'."
        )

    if violation_resolved:
        return {**apply_result, "verify_status": "verified"}
    else:
        return {**apply_result, "verify_status": "verification_failed"}


def rollback_proposal(apply_result: dict) -> dict:
    """Mark an applied proposal as rolled back.

    Args:
        apply_result: Dict from apply_proposal. Can be in any verify_status
            except already rolled_back.

    Returns:
        Updated apply_result with verify_status="rolled_back".

    Raises:
        ValueError: If already rolled back.
    """
    if apply_result.get("verify_status") == "rolled_back":
        raise ValueError("Proposal is already rolled back.")

    return {**apply_result, "verify_status": "rolled_back"}


def batch_verify(apply_results: list[dict],
                 resolved_violation_ids: set[str],
                 proposal_violations: dict[str, str]) -> dict:
    """Verify a batch of applied proposals against scan results.

    Args:
        apply_results: List of apply_result dicts with verify_status="pending".
        resolved_violation_ids: Set of violation_ids that resolved in latest scan.
        proposal_violations: Mapping of proposal_id → violation_id.

    Returns:
        Dict with verified (count), failed (count), results (list of updated dicts).
    """
    verified = 0
    failed = 0
    results = []

    for result in apply_results:
        if result.get("verify_status") != "pending":
            results.append(result)
            continue

        proposal_id = result.get("proposal_id", "")
        violation_id = proposal_violations.get(proposal_id, "")
        is_resolved = violation_id in resolved_violation_ids

        updated = verify_proposal(result, is_resolved)
        results.append(updated)

        if updated["verify_status"] == "verified":
            verified += 1
        else:
            failed += 1

    return {"verified": verified, "failed": failed, "results": results}
