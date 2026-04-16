# tests/unit/test_remediation_router.py
"""Unit tests for remediation review API — provider methods + router.

Run with: pytest tests/unit/test_remediation_router.py -v
"""
import uuid
from unittest.mock import MagicMock

import pytest


# ── Fixtures ────────────────────────────────────────────────────────────────

def _make_mock_provider():
    """Create a mock provider with canned remediation data."""
    provider = MagicMock()

    provider.remediation_funnel.return_value = {
        "total_violations": 247,
        "with_remediation": 84,
        "pending_review": 12,
        "approved": 8,
        "applied": 56,
        "verified": 48,
        "verification_failed": 3,
        "rejected": 5,
    }

    provider.agent_effectiveness.return_value = [
        {
            "agent_id": "steward-agent",
            "agent_version": "1.0.0",
            "total_proposals": 84,
            "verified": 48,
            "failed": 3,
            "rejected": 5,
            "precision_pct": 94.1,
            "avg_confidence": 0.87,
        }
    ]

    provider.reviewer_load.return_value = [
        {
            "reviewer": "alice@co.com",
            "pending_reviews": 7,
            "total_approved": 20,
            "total_rejected": 3,
            "total_reassigned": 1,
            "total_reviews": 24,
        }
    ]

    provider.list_proposals.return_value = [
        {
            "proposal_id": "prop-001",
            "violation_id": "v-001",
            "resource_id": "catalog.schema.table_a",
            "resource_name": "table_a",
            "resource_type": "table",
            "policy_id": "POL-SEC-003",
            "policy_name": "PII tables must have a data steward",
            "severity": "critical",
            "domain": "Security",
            "agent_id": "steward-agent",
            "agent_version": "1.0.0",
            "status": "pending_review",
            "confidence": 0.9,
            "proposed_sql": "ALTER TABLE catalog.schema.table_a SET TAGS ('data_steward' = 'jane')",
            "created_at": "2026-04-16T10:00:00Z",
        }
    ]

    provider.get_proposal.return_value = {
        "proposal_id": "prop-001",
        "violation_id": "v-001",
        "resource_id": "catalog.schema.table_a",
        "resource_name": "table_a",
        "resource_type": "table",
        "policy_id": "POL-SEC-003",
        "policy_name": "PII tables must have a data steward",
        "severity": "critical",
        "domain": "Security",
        "agent_id": "steward-agent",
        "agent_version": "1.0.0",
        "status": "pending_review",
        "confidence": 0.9,
        "proposed_sql": "ALTER TABLE catalog.schema.table_a SET TAGS ('data_steward' = 'jane')",
        "created_at": "2026-04-16T10:00:00Z",
        "context_json": '{"owner": "jane", "reason": "table owner is jane"}',
        "citations": "",
        "pre_state": '{"data_steward": null}',
        "review_history": [],
    }

    provider.submit_review.return_value = {
        "review_id": "rev-001",
        "proposal_id": "prop-001",
        "decision": "approved",
        "status": "approved",
    }

    return provider


# ── Provider method tests ───────────────────────────────────────────────────


class TestRemediationFunnel:
    def test_funnel_returns_all_stages(self):
        provider = _make_mock_provider()
        result = provider.remediation_funnel()
        assert result["total_violations"] == 247
        assert result["pending_review"] == 12
        assert result["verified"] == 48


class TestAgentEffectiveness:
    def test_returns_agent_list(self):
        provider = _make_mock_provider()
        result = provider.agent_effectiveness()
        assert len(result) == 1
        assert result[0]["agent_id"] == "steward-agent"
        assert result[0]["precision_pct"] == 94.1


class TestListProposals:
    def test_returns_enriched_proposals(self):
        provider = _make_mock_provider()
        result = provider.list_proposals(MagicMock())
        assert len(result) == 1
        assert result[0]["severity"] == "critical"
        assert result[0]["policy_name"] == "PII tables must have a data steward"

    def test_proposal_has_required_fields(self):
        provider = _make_mock_provider()
        result = provider.list_proposals(MagicMock())
        p = result[0]
        required = [
            "proposal_id", "violation_id", "resource_id", "resource_name",
            "policy_id", "policy_name", "severity", "agent_id", "status",
            "confidence", "proposed_sql", "created_at",
        ]
        for field in required:
            assert field in p, f"Missing field: {field}"


class TestGetProposal:
    def test_detail_includes_context_and_history(self):
        provider = _make_mock_provider()
        result = provider.get_proposal("prop-001")
        assert "context_json" in result
        assert "pre_state" in result
        assert "review_history" in result


class TestSubmitReview:
    def test_approve_returns_review_record(self):
        provider = _make_mock_provider()
        result = provider.submit_review(
            "prop-001", "approved", "looks good", reviewer="alice@co.com"
        )
        assert result["decision"] == "approved"

    def test_submit_review_called_with_correct_args(self):
        provider = _make_mock_provider()
        provider.submit_review("prop-001", "rejected", "bad SQL", reviewer="bob@co.com")
        provider.submit_review.assert_called_once_with(
            "prop-001", "rejected", "bad SQL", reviewer="bob@co.com"
        )
