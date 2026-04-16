# Remediation Execution Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the remediation closed loop — review queue state machine, applier, verifier with rollback, and 4 compliance views.

**Architecture:** Pure functions for all business logic (review, apply, verify). State transitions on proposal dicts. Delta table schemas for persistence. Compliance views built as SQL in the existing views pattern.

**Tech Stack:** Python, PySpark (mocked in tests), pytest

---

### Task 1: Add reviews and applied tables to tables.py

**Files:**
- Modify: `engine/src/watchdog/remediation/tables.py`

- [ ] **Step 1: Add two ensure functions**

Append to `engine/src/watchdog/remediation/tables.py`:

```python
def ensure_remediation_reviews_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the remediation_reviews table."""
    table = f"{catalog}.{schema}.remediation_reviews"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            review_id STRING NOT NULL,
            proposal_id STRING NOT NULL,
            reviewer STRING NOT NULL,
            decision STRING NOT NULL,
            reasoning STRING,
            reassigned_to STRING,
            reviewed_at TIMESTAMP NOT NULL
        )
        USING DELTA
    """)


def ensure_remediation_applied_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the remediation_applied table."""
    table = f"{catalog}.{schema}.remediation_applied"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            apply_id STRING NOT NULL,
            proposal_id STRING NOT NULL,
            executed_sql STRING,
            pre_state STRING,
            post_state STRING,
            applied_at TIMESTAMP NOT NULL,
            verify_scan_id STRING,
            verify_status STRING NOT NULL DEFAULT 'pending'
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)
```

- [ ] **Step 2: Commit**

```bash
git add engine/src/watchdog/remediation/tables.py
git commit -m "feat: add reviews and applied table schemas"
```

---

### Task 2: Build review queue state machine

**Files:**
- Create: `engine/src/watchdog/remediation/review.py`
- Create: `tests/unit/test_remediation_pipeline.py`

- [ ] **Step 1: Create review.py**

```python
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
```

- [ ] **Step 2: Create test file with review tests**

```python
# tests/unit/test_remediation_pipeline.py
"""Unit tests for remediation execution pipeline — review, apply, verify.

Run with: pytest tests/unit/test_remediation_pipeline.py -v
"""
import pytest
from watchdog.remediation.review import (
    approve_proposal,
    reject_proposal,
    reassign_proposal,
)


def _make_proposal(status="pending_review", proposal_id="prop-001"):
    return {
        "proposal_id": proposal_id,
        "violation_id": "v-001",
        "agent_id": "noop-agent",
        "agent_version": "1.0.0",
        "status": status,
        "proposed_sql": "ALTER TABLE t SET TAGS ('owner' = 'alice')",
        "confidence": 0.85,
    }


class TestApproveProposal:
    def test_approve_changes_status(self):
        proposal = _make_proposal()
        updated, review = approve_proposal(proposal, "reviewer@co.com", "looks good")
        assert updated["status"] == "approved"
        assert review["decision"] == "approved"
        assert review["reviewer"] == "reviewer@co.com"
        assert review["reasoning"] == "looks good"

    def test_approve_preserves_other_fields(self):
        proposal = _make_proposal()
        updated, _ = approve_proposal(proposal, "reviewer@co.com")
        assert updated["proposal_id"] == "prop-001"
        assert updated["proposed_sql"] == proposal["proposed_sql"]

    def test_approve_rejects_wrong_status(self):
        proposal = _make_proposal(status="approved")
        with pytest.raises(ValueError, match="pending_review"):
            approve_proposal(proposal, "reviewer@co.com")

    def test_approve_review_has_required_fields(self):
        proposal = _make_proposal()
        _, review = approve_proposal(proposal, "reviewer@co.com")
        assert "review_id" in review
        assert "proposal_id" in review
        assert "reviewed_at" in review
        assert review["reassigned_to"] is None


class TestRejectProposal:
    def test_reject_changes_status(self):
        proposal = _make_proposal()
        updated, review = reject_proposal(proposal, "reviewer@co.com", "bad SQL")
        assert updated["status"] == "rejected"
        assert review["decision"] == "rejected"

    def test_reject_rejects_wrong_status(self):
        proposal = _make_proposal(status="applied")
        with pytest.raises(ValueError, match="pending_review"):
            reject_proposal(proposal, "reviewer@co.com")


class TestReassignProposal:
    def test_reassign_keeps_pending_status(self):
        proposal = _make_proposal()
        updated, review = reassign_proposal(
            proposal, "reviewer@co.com", "senior@co.com", "needs senior review"
        )
        assert updated["status"] == "pending_review"
        assert review["decision"] == "reassigned"
        assert review["reassigned_to"] == "senior@co.com"

    def test_reassign_rejects_wrong_status(self):
        proposal = _make_proposal(status="rejected")
        with pytest.raises(ValueError, match="pending_review"):
            reassign_proposal(proposal, "a", "b")
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation_pipeline.py -v
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/review.py tests/unit/test_remediation_pipeline.py
git commit -m "feat: add review queue state machine with approve/reject/reassign"
```

---

### Task 3: Build the applier

**Files:**
- Create: `engine/src/watchdog/remediation/applier.py`
- Modify: `tests/unit/test_remediation_pipeline.py`

- [ ] **Step 1: Create applier.py**

```python
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
```

- [ ] **Step 2: Add applier tests**

Append to `tests/unit/test_remediation_pipeline.py`:

```python
from watchdog.remediation.applier import apply_proposal


class TestApplier:
    def test_apply_changes_proposal_status(self):
        proposal = _make_proposal(status="approved")
        updated, result = apply_proposal(proposal, pre_state='{"owner": null}')
        assert updated["status"] == "applied"
        assert result["verify_status"] == "pending"

    def test_apply_result_has_correct_fields(self):
        proposal = _make_proposal(status="approved")
        _, result = apply_proposal(proposal)
        assert "apply_id" in result
        assert result["proposal_id"] == "prop-001"
        assert result["executed_sql"] == proposal["proposed_sql"]
        assert "applied_at" in result
        assert result["verify_scan_id"] is None

    def test_apply_preserves_pre_state(self):
        proposal = _make_proposal(status="approved")
        _, result = apply_proposal(proposal, pre_state='{"owner": "old_value"}')
        assert result["pre_state"] == '{"owner": "old_value"}'

    def test_apply_dry_run_does_not_change_status(self):
        proposal = _make_proposal(status="approved")
        updated, result = apply_proposal(proposal, dry_run=True)
        assert updated["status"] == "approved"  # unchanged
        assert result["verify_status"] == "dry_run"

    def test_apply_rejects_wrong_status(self):
        proposal = _make_proposal(status="pending_review")
        with pytest.raises(ValueError, match="approved"):
            apply_proposal(proposal)
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation_pipeline.py -v
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/applier.py tests/unit/test_remediation_pipeline.py
git commit -m "feat: add applier with dry-run support"
```

---

### Task 4: Build the verifier

**Files:**
- Create: `engine/src/watchdog/remediation/verifier.py`
- Modify: `tests/unit/test_remediation_pipeline.py`

- [ ] **Step 1: Create verifier.py**

```python
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
```

- [ ] **Step 2: Add verifier tests**

Append to `tests/unit/test_remediation_pipeline.py`:

```python
from watchdog.remediation.verifier import verify_proposal, rollback_proposal, batch_verify


def _make_apply_result(proposal_id="prop-001", verify_status="pending"):
    return {
        "apply_id": "apply-001",
        "proposal_id": proposal_id,
        "executed_sql": "ALTER TABLE t SET TAGS ('owner' = 'alice')",
        "pre_state": '{"owner": null}',
        "post_state": "",
        "applied_at": "2026-04-16T10:00:00Z",
        "verify_scan_id": None,
        "verify_status": verify_status,
    }


class TestVerifier:
    def test_verify_resolved_sets_verified(self):
        result = _make_apply_result()
        updated = verify_proposal(result, violation_resolved=True)
        assert updated["verify_status"] == "verified"

    def test_verify_unresolved_sets_failed(self):
        result = _make_apply_result()
        updated = verify_proposal(result, violation_resolved=False)
        assert updated["verify_status"] == "verification_failed"

    def test_verify_rejects_non_pending(self):
        result = _make_apply_result(verify_status="verified")
        with pytest.raises(ValueError, match="pending"):
            verify_proposal(result, violation_resolved=True)

    def test_verify_preserves_other_fields(self):
        result = _make_apply_result()
        updated = verify_proposal(result, violation_resolved=True)
        assert updated["apply_id"] == "apply-001"
        assert updated["proposal_id"] == "prop-001"
        assert updated["pre_state"] == '{"owner": null}'


class TestRollback:
    def test_rollback_sets_status(self):
        result = _make_apply_result()
        updated = rollback_proposal(result)
        assert updated["verify_status"] == "rolled_back"

    def test_rollback_from_failed(self):
        result = _make_apply_result(verify_status="verification_failed")
        updated = rollback_proposal(result)
        assert updated["verify_status"] == "rolled_back"

    def test_rollback_rejects_already_rolled_back(self):
        result = _make_apply_result(verify_status="rolled_back")
        with pytest.raises(ValueError, match="already rolled back"):
            rollback_proposal(result)


class TestBatchVerify:
    def test_batch_counts(self):
        results = [
            _make_apply_result(proposal_id="p1"),
            _make_apply_result(proposal_id="p2"),
            _make_apply_result(proposal_id="p3"),
        ]
        resolved = {"v-001", "v-003"}
        mapping = {"p1": "v-001", "p2": "v-002", "p3": "v-003"}
        outcome = batch_verify(results, resolved, mapping)
        assert outcome["verified"] == 2
        assert outcome["failed"] == 1

    def test_batch_skips_non_pending(self):
        results = [
            _make_apply_result(proposal_id="p1", verify_status="verified"),
            _make_apply_result(proposal_id="p2"),
        ]
        resolved = {"v-002"}
        mapping = {"p2": "v-002"}
        outcome = batch_verify(results, resolved, mapping)
        assert outcome["verified"] == 1
        assert outcome["failed"] == 0
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation_pipeline.py -v
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/verifier.py tests/unit/test_remediation_pipeline.py
git commit -m "feat: add verifier with batch verify and rollback support"
```

---

### Task 5: Build 4 compliance views

**Files:**
- Create: `engine/src/watchdog/remediation/views.py`
- Modify: `tests/unit/test_remediation_pipeline.py`

- [ ] **Step 1: Create views.py**

```python
# engine/src/watchdog/remediation/views.py
"""Remediation Compliance Views — dashboards for the remediation pipeline.

Four views measuring the remediation funnel, trends, agent effectiveness,
and reviewer workload. All are regular views (not materialized).
"""

from pyspark.sql import SparkSession


def ensure_remediation_views(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create or replace all remediation compliance views."""
    _ensure_remediation_funnel_view(spark, catalog, schema)
    _ensure_remediation_trend_view(spark, catalog, schema)
    _ensure_agent_effectiveness_view(spark, catalog, schema)
    _ensure_reviewer_load_view(spark, catalog, schema)


def _ensure_remediation_funnel_view(spark: SparkSession, catalog: str,
                                     schema: str) -> None:
    """v_remediation_funnel: counts at each pipeline stage."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_remediation_funnel AS
        SELECT
            COUNT(DISTINCT v.violation_id) AS total_violations,
            COUNT(DISTINCT CASE WHEN v.remediation_status IS NOT NULL
                AND v.remediation_status != 'none' THEN v.violation_id END)
                AS with_remediation,
            COUNT(DISTINCT CASE WHEN p.status = 'pending_review' THEN p.proposal_id END)
                AS pending_review,
            COUNT(DISTINCT CASE WHEN p.status = 'approved' THEN p.proposal_id END)
                AS approved,
            COUNT(DISTINCT CASE WHEN p.status = 'applied' THEN p.proposal_id END)
                AS applied,
            COUNT(DISTINCT CASE WHEN p.status = 'verified' THEN p.proposal_id END)
                AS verified,
            COUNT(DISTINCT CASE WHEN p.status = 'verification_failed' THEN p.proposal_id END)
                AS verification_failed,
            COUNT(DISTINCT CASE WHEN p.status = 'rejected' THEN p.proposal_id END)
                AS rejected
        FROM {catalog}.{schema}.violations v
        LEFT JOIN {catalog}.{schema}.remediation_proposals p
            ON v.violation_id = p.violation_id
        WHERE v.status = 'open'
    """)


def _ensure_remediation_trend_view(spark: SparkSession, catalog: str,
                                    schema: str) -> None:
    """v_remediation_trend: compliance delta from remediation vs organic."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_remediation_trend AS
        SELECT
            DATE(p.created_at) AS proposal_date,
            COUNT(DISTINCT CASE WHEN p.status = 'verified' THEN p.proposal_id END)
                AS remediation_resolved,
            COUNT(DISTINCT CASE WHEN p.status = 'verification_failed' THEN p.proposal_id END)
                AS remediation_failed,
            COUNT(DISTINCT CASE WHEN p.status IN ('pending_review', 'approved', 'applied')
                THEN p.proposal_id END)
                AS remediation_in_progress
        FROM {catalog}.{schema}.remediation_proposals p
        GROUP BY DATE(p.created_at)
        ORDER BY proposal_date DESC
    """)


def _ensure_agent_effectiveness_view(spark: SparkSession, catalog: str,
                                      schema: str) -> None:
    """v_agent_effectiveness: per-agent scorecard."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_agent_effectiveness AS
        SELECT
            p.agent_id,
            p.agent_version,
            COUNT(*) AS total_proposals,
            COUNT(CASE WHEN p.status = 'verified' THEN 1 END) AS verified,
            COUNT(CASE WHEN p.status = 'verification_failed' THEN 1 END) AS failed,
            COUNT(CASE WHEN p.status = 'rejected' THEN 1 END) AS rejected,
            ROUND(
                COUNT(CASE WHEN p.status = 'verified' THEN 1 END) * 100.0
                / NULLIF(COUNT(CASE WHEN p.status IN ('verified', 'verification_failed') THEN 1 END), 0),
                1
            ) AS precision_pct,
            ROUND(AVG(p.confidence), 3) AS avg_confidence
        FROM {catalog}.{schema}.remediation_proposals p
        GROUP BY p.agent_id, p.agent_version
        ORDER BY total_proposals DESC
    """)


def _ensure_reviewer_load_view(spark: SparkSession, catalog: str,
                                schema: str) -> None:
    """v_reviewer_load: open queue depth per reviewer."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_reviewer_load AS
        SELECT
            r.reviewer,
            COUNT(DISTINCT CASE WHEN p.status = 'pending_review' THEN p.proposal_id END)
                AS pending_reviews,
            COUNT(DISTINCT CASE WHEN r.decision = 'approved' THEN r.review_id END)
                AS total_approved,
            COUNT(DISTINCT CASE WHEN r.decision = 'rejected' THEN r.review_id END)
                AS total_rejected,
            COUNT(DISTINCT CASE WHEN r.decision = 'reassigned' THEN r.review_id END)
                AS total_reassigned,
            COUNT(DISTINCT r.review_id) AS total_reviews
        FROM {catalog}.{schema}.remediation_reviews r
        LEFT JOIN {catalog}.{schema}.remediation_proposals p
            ON r.proposal_id = p.proposal_id
        GROUP BY r.reviewer
        ORDER BY pending_reviews DESC
    """)
```

- [ ] **Step 2: Add view tests**

Append to `tests/unit/test_remediation_pipeline.py`:

```python
import sys
import types
from unittest.mock import MagicMock

# Ensure PySpark mock is available for view imports
if "pyspark" not in sys.modules:
    _pyspark = types.ModuleType("pyspark")
    _pyspark_sql = types.ModuleType("pyspark.sql")
    _pyspark_sql.SparkSession = MagicMock
    _pyspark.sql = _pyspark_sql
    sys.modules["pyspark"] = _pyspark
    sys.modules["pyspark.sql"] = _pyspark_sql

from watchdog.remediation.views import ensure_remediation_views


class TestRemediationViews:
    def _mock_spark(self):
        spark = MagicMock()
        spark.sql_calls = []
        spark.sql.side_effect = lambda s: spark.sql_calls.append(s) or MagicMock()
        return spark

    def test_creates_four_views(self):
        spark = self._mock_spark()
        ensure_remediation_views(spark, "cat", "sch")
        view_sqls = [s for s in spark.sql_calls if "CREATE OR REPLACE VIEW" in s]
        assert len(view_sqls) == 4

    def test_creates_expected_view_names(self):
        spark = self._mock_spark()
        ensure_remediation_views(spark, "cat", "sch")
        all_sql = " ".join(spark.sql_calls)
        assert "v_remediation_funnel" in all_sql
        assert "v_remediation_trend" in all_sql
        assert "v_agent_effectiveness" in all_sql
        assert "v_reviewer_load" in all_sql

    def test_funnel_view_references_correct_tables(self):
        spark = self._mock_spark()
        ensure_remediation_views(spark, "cat", "sch")
        funnel_sql = [s for s in spark.sql_calls if "v_remediation_funnel" in s][0]
        assert "cat.sch.violations" in funnel_sql
        assert "cat.sch.remediation_proposals" in funnel_sql

    def test_effectiveness_view_has_precision(self):
        spark = self._mock_spark()
        ensure_remediation_views(spark, "cat", "sch")
        eff_sql = [s for s in spark.sql_calls if "v_agent_effectiveness" in s][0]
        assert "precision_pct" in eff_sql
        assert "avg_confidence" in eff_sql

    def test_reviewer_load_references_reviews_table(self):
        spark = self._mock_spark()
        ensure_remediation_views(spark, "cat", "sch")
        load_sql = [s for s in spark.sql_calls if "v_reviewer_load" in s][0]
        assert "cat.sch.remediation_reviews" in load_sql
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation_pipeline.py -v
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/views.py tests/unit/test_remediation_pipeline.py
git commit -m "feat: add 4 remediation compliance views"
```

---

### Task 6: Run full test suite and validate

- [ ] **Step 1: Run pipeline tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation_pipeline.py -v
```

- [ ] **Step 2: Run full suite**

```bash
PYTHONPATH=engine/src pytest tests/unit/ --ignore=tests/unit/test_multi_metastore.py -q 2>&1 | tail -5
```

- [ ] **Step 3: Final commit if needed**

```bash
git add -A
git commit -m "fix: address any test failures from remediation pipeline"
```
