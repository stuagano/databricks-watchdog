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
