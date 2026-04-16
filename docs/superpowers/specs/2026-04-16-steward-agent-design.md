# StewardAgent: First Reference Remediation Agent

**Status:** Design approved
**Date:** 2026-04-16
**Cycle:** 3c of 3 (Hub Integration → Drift Detection → Remediation Agents)
**Sub-project:** First reference agent (3c of 3a/3b/3c)

---

## Problem

The remediation framework (3a + 3b) is built but has no real agent beyond NoOpAgent. This sub-project proves the framework works end-to-end with a deterministic agent that remediates a real policy violation.

## Design

### StewardAgent

**File:** `engine/src/watchdog/remediation/agents/steward.py`

- `agent_id = "steward-agent"`
- `handles = ["POL-SEC-003"]` (PII tables must have a data steward)
- `version = "1.0.0"`
- `model = ""` (deterministic, no LLM)

**gather_context:** Extracts resource_name, owner, existing tags from the violation dict.

**propose_fix:** Suggests `ALTER TABLE {resource} SET TAGS ('data_steward' = '{steward}')` where steward is:
- The resource owner if one exists (confidence 0.9)
- "unassigned" if no owner (confidence 0.3)

### End-to-End Test

Tests prove the full dispatch → review → apply → verify path:
1. Create a violation for POL-SEC-003
2. Dispatch with StewardAgent → proposal created
3. Approve → status changes
4. Apply → apply_result created
5. Verify with resolved=True → verified

### Files

| Action | File | What |
|---|---|---|
| Create | `engine/src/watchdog/remediation/agents/steward.py` | StewardAgent |
| Create | `tests/unit/test_steward_agent.py` | Agent + end-to-end tests |
