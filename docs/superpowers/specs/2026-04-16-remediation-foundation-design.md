# Remediation Foundation: Agent SDK, Data Model, Dispatcher

**Status:** Design approved
**Date:** 2026-04-16
**Cycle:** 3a of 3 (Hub Integration → Drift Detection → Remediation Agents)
**Sub-project:** Foundation (3a of 3a/3b/3c)

---

## Problem

Watchdog detects violations but cannot fix them. The gap between "violation detected" and "violation fixed" is manual. This sub-project builds the foundation layer: the agent protocol, data model, and dispatcher that routes violations to remediation agents.

## Goals

- Define a `RemediationAgent` Protocol that agents implement
- Create 3 Delta tables (agents registry, proposals, reviews placeholder) + 1 new column on violations
- Build a dispatcher that routes open violations to registered agents and writes proposals
- Prove the framework with a stub NoOpAgent
- All pure Python, testable without Spark

## Non-Goals

- Applier, Verifier, review state machine (sub-project 3b)
- Real reference agents with LLM calls (sub-project 3c)
- Review UI (separate project)
- Compliance views for remediation funnel (sub-project 3b)

---

## Design

### 1. RemediationAgent Protocol

File: `engine/src/watchdog/remediation/protocol.py`

Protocol class (not ABC) following the pattern in `ontos-adapter/src/watchdog_governance/provider.py`.

Attributes:
- `agent_id: str` — unique identifier
- `handles: list[str]` — policy IDs this agent can remediate
- `version: str` — agent version for reproducibility
- `model: str` — LLM model used (empty for deterministic agents)

Methods (3a scope):
- `gather_context(violation: dict) -> dict` — collects context needed for a fix proposal
- `propose_fix(context: dict) -> dict` — generates a fix proposal with keys: proposed_sql, confidence, context_json, citations

Methods deferred to 3b:
- `apply(proposal: dict) -> dict`
- `verify(apply_result: dict) -> dict`

### 2. Data Model

**`remediation_agents`** — registry of available agents:
```sql
agent_id STRING NOT NULL,
handles STRING NOT NULL,          -- comma-separated policy IDs
version STRING NOT NULL,
model STRING,
config_json STRING,
permissions STRING,               -- comma-separated allowed operations
active BOOLEAN DEFAULT true,
registered_at TIMESTAMP NOT NULL
```

**`remediation_proposals`** — proposed fixes:
```sql
proposal_id STRING NOT NULL,
violation_id STRING NOT NULL,
agent_id STRING NOT NULL,
agent_version STRING NOT NULL,
status STRING NOT NULL DEFAULT 'pending_review',
proposed_sql STRING,
confidence DOUBLE,
context_json STRING,
llm_prompt_hash STRING,
citations STRING,
created_at TIMESTAMP NOT NULL
```

**New column on `violations`:**
- `remediation_status STRING` — nullable, values: none, proposed, approved, applied, verified, failed

### 3. Dispatcher

File: `engine/src/watchdog/remediation/dispatcher.py`

Function: `dispatch_remediations(spark, catalog, schema, agents: list) -> dict`

Logic:
1. Read open violations where `remediation_status` is null or 'none'
2. For each violation, find first agent whose `handles` contains the violation's `policy_id`
3. Call `agent.gather_context(violation_row)` then `agent.propose_fix(context)`
4. Write proposal to `remediation_proposals` with `status = 'pending_review'`
5. Update violation's `remediation_status` to 'proposed'
6. Idempotent: skip if (violation_id, agent_id, agent_version) already exists in proposals
7. Return summary dict: {dispatched, skipped, errors}

### 4. NoOp Stub Agent

File: `engine/src/watchdog/remediation/agents/noop.py`

Implements the Protocol. Handles `["POL-TEST-001"]`. Always proposes `ALTER TABLE {resource} SET TAGS ('owner' = 'unassigned')` with confidence 0.1. Used for testing the dispatch path.

### 5. Tests

File: `tests/unit/test_remediation.py`

Pure Python with mocked PySpark (same pattern as test_views.py):
- Protocol conformance: NoOpAgent satisfies RemediationAgent
- Dispatcher routes by policy_id correctly
- Dispatcher skips already-proposed violations
- Dispatcher handles no matching agent gracefully
- Proposals have correct schema fields
- Violations table gains remediation_status column

---

## Files

| Action | File | What |
|---|---|---|
| Create | `engine/src/watchdog/remediation/__init__.py` | Package init |
| Create | `engine/src/watchdog/remediation/protocol.py` | RemediationAgent protocol |
| Create | `engine/src/watchdog/remediation/tables.py` | Table schemas + ensure functions |
| Create | `engine/src/watchdog/remediation/dispatcher.py` | Dispatch logic |
| Create | `engine/src/watchdog/remediation/agents/__init__.py` | Agents subpackage |
| Create | `engine/src/watchdog/remediation/agents/noop.py` | Stub test agent |
| Edit | `engine/src/watchdog/violations.py` | Add remediation_status column |
| Create | `tests/unit/test_remediation.py` | All tests |
