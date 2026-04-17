"""Unit tests for the newly-added remediation agents.

Covers:
  - ClusterTaggerAgent (POL-C002/C003/C004)
  - JobOwnerAgent (POL-C001)
  - DQMonitorScaffoldAgent (POL-Q001)
"""

import json

import pytest
from watchdog.remediation.agents.cluster_tagger import (
    ClusterTaggerAgent,
    _infer_business_unit,
    _infer_environment,
)
from watchdog.remediation.agents.dq_monitor_scaffold import DQMonitorScaffoldAgent
from watchdog.remediation.agents.job_owner import JobOwnerAgent
from watchdog.remediation.dispatcher import dispatch_remediations
from watchdog.remediation.protocol import RemediationAgent


def _violation(policy_id, resource_name, resource_type="cluster",
               owner="alice@co.com", **extra):
    base = {
        "violation_id": f"v-{policy_id}-{resource_name}",
        "policy_id": policy_id,
        "resource_id": f"id-{resource_name}",
        "resource_name": resource_name,
        "resource_type": resource_type,
        "severity": "high",
        "owner": owner,
        "status": "open",
    }
    base.update(extra)
    return base


# ─────────────────────────────────────────────────────────────
# ClusterTaggerAgent
# ─────────────────────────────────────────────────────────────

class TestClusterTaggerProtocol:
    def test_conforms_to_protocol(self):
        assert isinstance(ClusterTaggerAgent(), RemediationAgent)

    def test_claims_three_cost_policies(self):
        agent = ClusterTaggerAgent()
        assert set(agent.handles) == {"POL-C002", "POL-C003", "POL-C004"}


class TestInferEnvironment:
    @pytest.mark.parametrize("name,expected", [
        ("analytics_prod", "prod"),
        ("prod-cluster", "prod"),
        ("team/production/etl", "prod"),
        ("staging_wh", "staging"),
        ("qa-cluster", "test"),
        ("dev-scratch", "dev"),
        ("unrelated-name", "dev"),  # fallback
    ])
    def test_env_inference(self, name, expected):
        value, _confidence = _infer_environment(name)
        assert value == expected


class TestInferBusinessUnit:
    def test_derives_from_email_local_part(self):
        # "alice.data-platform" → "data-platform"
        value, conf = _infer_business_unit("alice.data-platform@co.com")
        assert value == "data-platform"
        assert conf == pytest.approx(0.6)

    def test_unassigned_without_signal(self):
        value, conf = _infer_business_unit("")
        assert value == "UNASSIGNED"
        assert conf <= 0.3


class TestClusterTaggerProposals:
    def test_proposes_environment_for_pol_c004(self):
        agent = ClusterTaggerAgent()
        ctx = agent.gather_context(_violation("POL-C004", "prod_etl"))
        fix = agent.propose_fix(ctx)
        assert "environment" in fix["proposed_sql"]
        assert "prod" in fix["proposed_sql"]
        assert fix["confidence"] >= 0.5

    def test_proposes_business_unit_for_pol_c003(self):
        agent = ClusterTaggerAgent()
        ctx = agent.gather_context(
            _violation("POL-C003", "some-table", resource_type="table",
                       owner="bob.platform@co.com"),
        )
        fix = agent.propose_fix(ctx)
        # Table-typed resources get SQL ALTER TABLE syntax.
        assert "ALTER TABLE some-table SET TAGS" in fix["proposed_sql"]
        assert "business_unit" in fix["proposed_sql"]
        assert "platform" in fix["proposed_sql"]

    def test_cluster_resource_emits_api_hint(self):
        agent = ClusterTaggerAgent()
        ctx = agent.gather_context(
            _violation("POL-C002", "cluster-x", resource_type="cluster"),
        )
        fix = agent.propose_fix(ctx)
        assert "cost_center" in fix["proposed_sql"]
        # API hint appears as a SQL comment so `spark.sql` can parse it (the
        # applier short-circuits on comments).
        assert fix["proposed_sql"].startswith("--")

    def test_unknown_policy_produces_empty_sql(self):
        agent = ClusterTaggerAgent()
        ctx = agent.gather_context(_violation("POL-UNKNOWN", "x"))
        fix = agent.propose_fix(ctx)
        assert fix["proposed_sql"] == ""
        assert fix["confidence"] == 0.0

    def test_cost_center_has_low_confidence(self):
        # cost_center cannot be inferred from a name/owner alone, so the
        # proposal must be low-confidence to force human review.
        agent = ClusterTaggerAgent()
        ctx = agent.gather_context(_violation("POL-C002", "prod_cluster"))
        fix = agent.propose_fix(ctx)
        assert fix["confidence"] <= 0.3

    def test_context_json_round_trips(self):
        agent = ClusterTaggerAgent()
        ctx = agent.gather_context(_violation("POL-C004", "prod_etl"))
        fix = agent.propose_fix(ctx)
        # context_json must be valid JSON (the applier deserialises it).
        json.loads(fix["context_json"])


# ─────────────────────────────────────────────────────────────
# JobOwnerAgent
# ─────────────────────────────────────────────────────────────

class TestJobOwnerAgent:
    def test_conforms_to_protocol(self):
        assert isinstance(JobOwnerAgent(), RemediationAgent)

    def test_handles_pol_c001(self):
        assert JobOwnerAgent().handles == ["POL-C001"]

    def test_uses_owner_hint_high_confidence(self):
        agent = JobOwnerAgent()
        violation = _violation("POL-C001", "etl-job", resource_type="job",
                                owner_hint="carol@co.com")
        ctx = agent.gather_context(violation)
        fix = agent.propose_fix(ctx)
        assert "carol@co.com" in fix["proposed_sql"]
        assert fix["confidence"] >= 0.8

    def test_without_hint_falls_back(self):
        agent = JobOwnerAgent()
        ctx = agent.gather_context(_violation("POL-C001", "etl", resource_type="job"))
        fix = agent.propose_fix(ctx)
        assert "platform-admin@company.com" in fix["proposed_sql"]
        assert fix["confidence"] <= 0.4


# ─────────────────────────────────────────────────────────────
# DQMonitorScaffoldAgent
# ─────────────────────────────────────────────────────────────

class TestDQMonitorScaffoldAgent:
    def test_conforms_to_protocol(self):
        assert isinstance(DQMonitorScaffoldAgent(), RemediationAgent)

    def test_handles_pol_q001(self):
        assert DQMonitorScaffoldAgent().handles == ["POL-Q001"]

    @pytest.mark.parametrize("fqn,layer", [
        ("cat.bronze_raw.events", "bronze"),
        ("cat.silver_curated.events", "silver"),
        ("cat.gold_marts.events", "gold"),
        ("cat.raw_ingest.events", "raw"),
        ("cat.other.events", "unknown"),
    ])
    def test_layer_inference(self, fqn, layer):
        agent = DQMonitorScaffoldAgent()
        ctx = agent.gather_context(
            _violation("POL-Q001", fqn, resource_type="table"),
        )
        fix = agent.propose_fix(ctx)
        assert layer in fix["proposed_sql"]

    def test_sql_targets_correct_table(self):
        agent = DQMonitorScaffoldAgent()
        ctx = agent.gather_context(
            _violation("POL-Q001", "cat.gold.orders", resource_type="table"),
        )
        fix = agent.propose_fix(ctx)
        assert fix["proposed_sql"].startswith("ALTER TABLE cat.gold.orders")

    def test_moderate_confidence(self):
        agent = DQMonitorScaffoldAgent()
        ctx = agent.gather_context(
            _violation("POL-Q001", "cat.gold.orders", resource_type="table"),
        )
        fix = agent.propose_fix(ctx)
        # Generic comment is safe but must be reviewed — flag as moderate.
        assert 0.3 <= fix["confidence"] <= 0.75


# ─────────────────────────────────────────────────────────────
# Dispatch integration across all new agents
# ─────────────────────────────────────────────────────────────

class TestDispatchAcrossAgents:
    def test_each_agent_picks_up_its_policy(self):
        agents = [
            ClusterTaggerAgent(), JobOwnerAgent(), DQMonitorScaffoldAgent(),
        ]
        violations = [
            _violation("POL-C004", "prod_cluster"),
            _violation("POL-C001", "etl", resource_type="job"),
            _violation("POL-Q001", "cat.gold.x", resource_type="table"),
        ]
        result = dispatch_remediations(violations, agents)
        assert result["dispatched"] == 3
        assert result["errors"] == 0
        by_agent = {p["agent_id"] for p in result["proposals"]}
        assert by_agent == {
            "cluster-tagger-agent", "job-owner-agent", "dq-monitor-scaffold-agent",
        }

    def test_idempotency_skips_existing_proposal(self):
        agent = ClusterTaggerAgent()
        violation = _violation("POL-C004", "prod_cluster")
        # Seed the existing set so dispatcher treats this as already handled.
        existing = {(violation["violation_id"], agent.agent_id, agent.version)}
        result = dispatch_remediations([violation], [agent], existing)
        assert result["dispatched"] == 0
        assert result["skipped"] == 1
