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


# ── Applier ──────────────────────────────────────────────────────────────────

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
        assert updated["status"] == "approved"
        assert result["verify_status"] == "dry_run"

    def test_apply_rejects_wrong_status(self):
        proposal = _make_proposal(status="pending_review")
        with pytest.raises(ValueError, match="approved"):
            apply_proposal(proposal)


# ── Verifier ─────────────────────────────────────────────────────────────────

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


# ── Remediation Views ────────────────────────────────────────────────────────

import sys
import types
from unittest.mock import MagicMock

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
