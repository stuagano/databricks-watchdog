# Remediation Execution Pipeline: Applier, Verifier, Review Queue, Views

**Status:** Design approved
**Date:** 2026-04-16
**Cycle:** 3b of 3 (Hub Integration → Drift Detection → Remediation Agents)
**Sub-project:** Execution Pipeline (3b of 3a/3b/3c)

---

## Problem

Sub-project 3a built the foundation: Protocol, tables, dispatcher. But proposals sit in pending_review with no way to approve, apply, verify, or roll them back. This sub-project completes the closed loop: review → apply → verify → measure.

## Goals

- Full review queue state machine (pending_review → approved/rejected/reassigned → applied → verified/failed → rolled_back)
- Applier that executes approved proposals (with dry-run mode)
- Verifier that checks applied proposals against next scan results
- Rollback support
- 4 compliance views for remediation dashboards
- 2 remaining Delta tables (reviews, applied)

## Non-Goals

- Review UI (separate project)
- Real LLM agents (sub-project 3c)
- ServiceNow/Jira integration
- Auto-approve logic (future)

---

## Design

### 1. Delta Tables

**`remediation_reviews`** — audit trail of review decisions:
- review_id (PK), proposal_id (FK), reviewer, decision (approved/rejected/reassigned), reasoning, reassigned_to (nullable), reviewed_at

**`remediation_applied`** — executed fixes with pre/post state:
- apply_id (PK), proposal_id (FK), executed_sql, pre_state, post_state, applied_at, verify_scan_id (nullable), verify_status (pending/verified/verification_failed/rolled_back)

### 2. Review Queue State Machine

File: `engine/src/watchdog/remediation/review.py`

Pure functions that take proposal dicts and return updated state + review records:

- `approve_proposal(proposal, reviewer, reasoning) -> tuple[dict, dict]` — returns (updated proposal with status=approved, review record)
- `reject_proposal(proposal, reviewer, reasoning) -> tuple[dict, dict]` — returns (updated proposal with status=rejected, review record)
- `reassign_proposal(proposal, reviewer, reassigned_to, reasoning) -> tuple[dict, dict]` — returns (proposal stays pending_review, review record with decision=reassigned)

Transition validation: approve/reject only from pending_review. Reassign only from pending_review. Invalid transitions return error.

### 3. Applier

File: `engine/src/watchdog/remediation/applier.py`

Pure function: `apply_proposal(proposal, pre_state="", dry_run=False) -> dict`

- Takes an approved proposal dict
- Returns an apply_result dict: apply_id, proposal_id, executed_sql, pre_state, post_state="", applied_at, verify_status ("pending" or "dry_run")
- Validates proposal status is "approved" before proceeding
- In dry_run mode: creates the result but sets verify_status="dry_run"
- Updates proposal status to "applied" (returned in updated proposal)

### 4. Verifier

File: `engine/src/watchdog/remediation/verifier.py`

Pure functions:

- `verify_proposal(apply_result, violation_resolved: bool) -> dict` — if resolved, verify_status="verified"; if not, verify_status="verification_failed". Returns updated apply_result.
- `rollback_proposal(apply_result) -> dict` — sets verify_status="rolled_back". Returns updated apply_result.

### 5. Compliance Views

File: `engine/src/watchdog/remediation/views.py`

- `v_remediation_funnel` — counts at each stage: violations → proposed → approved → applied → verified
- `v_remediation_trend` — per-scan compliance delta attributable to remediation vs organic
- `v_agent_effectiveness` — per-agent: proposals created, approved, verified, avg confidence
- `v_reviewer_load` — per-reviewer: pending reviews, approved, rejected counts

---

## Files

| Action | File | What |
|---|---|---|
| Edit | `engine/src/watchdog/remediation/tables.py` | Add reviews + applied table schemas |
| Create | `engine/src/watchdog/remediation/review.py` | Review queue state machine |
| Create | `engine/src/watchdog/remediation/applier.py` | Apply approved proposals |
| Create | `engine/src/watchdog/remediation/verifier.py` | Verify + rollback |
| Create | `engine/src/watchdog/remediation/views.py` | 4 compliance views |
| Create | `tests/unit/test_remediation_pipeline.py` | All pipeline tests |
