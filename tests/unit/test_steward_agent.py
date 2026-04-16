"""Unit tests for StewardAgent — protocol conformance and end-to-end pipeline.

Run with: pytest tests/unit/test_steward_agent.py -v
"""
import json

import pytest
from watchdog.remediation.protocol import RemediationAgent
from watchdog.remediation.agents.steward import StewardAgent
from watchdog.remediation.dispatcher import dispatch_remediations
from watchdog.remediation.review import approve_proposal
from watchdog.remediation.applier import apply_proposal
from watchdog.remediation.verifier import verify_proposal


def _make_steward_violation(violation_id="v-steward-001", owner="alice@co.com"):
    return {
        "violation_id": violation_id,
        "policy_id": "POL-SEC-003",
        "resource_id": "gold.pii.customer_records",
        "resource_name": "gold.pii.customer_records",
        "resource_type": "table",
        "severity": "high",
        "owner": owner,
        "status": "open",
        "remediation_status": None,
    }


# ── Protocol conformance ─────────────────────────────────────────────────────

class TestStewardProtocol:
    def test_is_remediation_agent(self):
        assert isinstance(StewardAgent(), RemediationAgent)

    def test_handles_correct_policy(self):
        agent = StewardAgent()
        assert "POL-SEC-003" in agent.handles

    def test_agent_attributes(self):
        agent = StewardAgent()
        assert agent.agent_id == "steward-agent"
        assert agent.version == "1.0.0"
        assert agent.model == ""


# ── Gather context ───────────────────────────────────────────────────────────

class TestStewardGatherContext:
    def test_extracts_resource_info(self):
        agent = StewardAgent()
        violation = _make_steward_violation()
        context = agent.gather_context(violation)
        assert context["resource_name"] == "gold.pii.customer_records"
        assert context["owner"] == "alice@co.com"

    def test_handles_missing_owner(self):
        agent = StewardAgent()
        violation = _make_steward_violation(owner="")
        context = agent.gather_context(violation)
        assert context["owner"] == ""


# ── Propose fix ──────────────────────────────────────────────────────────────

class TestStewardProposeFix:
    def test_suggests_owner_as_steward(self):
        agent = StewardAgent()
        context = agent.gather_context(_make_steward_violation())
        proposal = agent.propose_fix(context)
        assert "alice@co.com" in proposal["proposed_sql"]
        assert "data_steward" in proposal["proposed_sql"]
        assert proposal["confidence"] == 0.9

    def test_low_confidence_without_owner(self):
        agent = StewardAgent()
        context = agent.gather_context(_make_steward_violation(owner=""))
        proposal = agent.propose_fix(context)
        assert "unassigned" in proposal["proposed_sql"]
        assert proposal["confidence"] == 0.3

    def test_proposal_has_required_fields(self):
        agent = StewardAgent()
        context = agent.gather_context(_make_steward_violation())
        proposal = agent.propose_fix(context)
        assert "proposed_sql" in proposal
        assert "confidence" in proposal
        assert "context_json" in proposal
        assert "citations" in proposal


# ── End-to-end pipeline ─────────────────────────────────────────────────────

class TestStewardEndToEnd:
    """Prove the full dispatch → review → apply → verify path."""

    def test_full_pipeline(self):
        agent = StewardAgent()
        violation = _make_steward_violation()

        # Step 1: Dispatch
        result = dispatch_remediations([violation], [agent])
        assert result["dispatched"] == 1
        proposal = result["proposals"][0]
        assert proposal["agent_id"] == "steward-agent"
        assert proposal["status"] == "pending_review"
        assert "data_steward" in proposal["proposed_sql"]

        # Step 2: Approve
        approved, review = approve_proposal(proposal, "senior@co.com", "owner is correct steward")
        assert approved["status"] == "approved"
        assert review["decision"] == "approved"

        # Step 3: Apply
        applied_proposal, apply_result = apply_proposal(approved, pre_state='{"data_steward": null}')
        assert applied_proposal["status"] == "applied"
        assert apply_result["verify_status"] == "pending"

        # Step 4: Verify (violation resolved in next scan)
        verified = verify_proposal(apply_result, violation_resolved=True)
        assert verified["verify_status"] == "verified"

    def test_full_pipeline_verification_fails(self):
        agent = StewardAgent()
        violation = _make_steward_violation()

        result = dispatch_remediations([violation], [agent])
        proposal = result["proposals"][0]
        approved, _ = approve_proposal(proposal, "reviewer@co.com")
        _, apply_result = apply_proposal(approved)

        # Violation still open after next scan
        failed = verify_proposal(apply_result, violation_resolved=False)
        assert failed["verify_status"] == "verification_failed"

    def test_dispatch_skips_non_matching_policy(self):
        agent = StewardAgent()
        violation = _make_steward_violation()
        violation["policy_id"] = "POL-COST-001"  # Not handled by StewardAgent

        result = dispatch_remediations([violation], [agent])
        assert result["dispatched"] == 0
        assert result["skipped"] == 1
