# tests/unit/test_remediation.py
"""Unit tests for remediation framework — protocol, dispatcher, tables.

Run with: pytest tests/unit/test_remediation.py -v
"""
import json
import sys
import types
from unittest.mock import MagicMock

import pytest

# ── Mock PySpark before importing watchdog modules ───────────────────────────

_pyspark = types.ModuleType("pyspark")
_pyspark_sql = types.ModuleType("pyspark.sql")
_pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
_pyspark_sql_types = types.ModuleType("pyspark.sql.types")

_pyspark_sql.SparkSession = MagicMock
_pyspark_sql.DataFrame = MagicMock
_pyspark_sql.Row = MagicMock
_pyspark_sql_functions.col = MagicMock

def _dummy_type(*args, **kwargs):
    return f"type({args})"

_pyspark_sql_types.StructType = _dummy_type
_pyspark_sql_types.StructField = _dummy_type
_pyspark_sql_types.StringType = _dummy_type
_pyspark_sql_types.BooleanType = _dummy_type
_pyspark_sql_types.IntegerType = _dummy_type
_pyspark_sql_types.TimestampType = _dummy_type
_pyspark_sql_types.MapType = _dummy_type
_pyspark_sql_types.DoubleType = _dummy_type

_pyspark.sql = _pyspark_sql

sys.modules.setdefault("pyspark", _pyspark)
sys.modules.setdefault("pyspark.sql", _pyspark_sql)
sys.modules.setdefault("pyspark.sql.functions", _pyspark_sql_functions)
sys.modules.setdefault("pyspark.sql.types", _pyspark_sql_types)

_databricks = types.ModuleType("databricks")
_databricks_sdk = types.ModuleType("databricks.sdk")
_databricks_sdk.WorkspaceClient = MagicMock
_databricks.sdk = _databricks_sdk

sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)

from watchdog.remediation.protocol import RemediationAgent
from watchdog.remediation.agents.noop import NoOpAgent
from watchdog.remediation.dispatcher import dispatch_remediations


# ── Protocol conformance ─────────────────────────────────────────────────────

class TestProtocolConformance:
    def test_noop_agent_is_remediation_agent(self):
        agent = NoOpAgent()
        assert isinstance(agent, RemediationAgent)

    def test_noop_agent_has_required_attributes(self):
        agent = NoOpAgent()
        assert agent.agent_id == "noop-agent"
        assert agent.handles == ["POL-TEST-001"]
        assert agent.version == "1.0.0"
        assert agent.model == ""

    def test_noop_agent_gather_context(self):
        agent = NoOpAgent()
        violation = {
            "violation_id": "v-001",
            "resource_id": "gold.finance.gl_balances",
            "resource_name": "gold.finance.gl_balances",
            "policy_id": "POL-TEST-001",
            "severity": "high",
            "owner": "stuart.gano@company.com",
        }
        context = agent.gather_context(violation)
        assert "violation" in context
        assert context["violation"] == violation

    def test_noop_agent_propose_fix(self):
        agent = NoOpAgent()
        context = {
            "violation": {
                "resource_name": "gold.finance.gl_balances",
                "policy_id": "POL-TEST-001",
            }
        }
        proposal = agent.propose_fix(context)
        assert "proposed_sql" in proposal
        assert "confidence" in proposal
        assert proposal["confidence"] == 0.1
        assert "gold.finance.gl_balances" in proposal["proposed_sql"]


# ── Table schema validation ──────────────────────────────────────────────────

class TestRemediationTables:
    def test_agents_table_has_correct_columns(self):
        from watchdog.remediation.tables import ensure_remediation_agents_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_remediation_agents_table(spark, "cat", "sch")

        sql = sql_calls[0]
        for col in ["agent_id", "handles", "version", "model", "active", "registered_at"]:
            assert col in sql, f"Missing column: {col}"

    def test_proposals_table_has_correct_columns(self):
        from watchdog.remediation.tables import ensure_remediation_proposals_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_remediation_proposals_table(spark, "cat", "sch")

        sql = sql_calls[0]
        for col in ["proposal_id", "violation_id", "agent_id", "agent_version",
                     "status", "proposed_sql", "confidence", "context_json",
                     "llm_prompt_hash", "citations", "created_at"]:
            assert col in sql, f"Missing column: {col}"

    def test_violations_table_has_remediation_status(self):
        from watchdog.violations import ensure_violations_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_violations_table(spark, "cat", "sch")

        sql = sql_calls[0]
        assert "remediation_status" in sql


# ── Dispatcher ───────────────────────────────────────────────────────────────

class TestDispatcher:
    def _make_violation(self, violation_id="v-001", policy_id="POL-TEST-001"):
        return {
            "violation_id": violation_id,
            "policy_id": policy_id,
            "resource_id": "gold.finance.gl_balances",
            "resource_name": "gold.finance.gl_balances",
            "severity": "high",
            "owner": "stuart.gano@company.com",
            "status": "open",
            "remediation_status": None,
        }

    def test_dispatches_to_matching_agent(self):
        agent = NoOpAgent()
        violations = [self._make_violation()]
        result = dispatch_remediations(violations, [agent])
        assert result["dispatched"] == 1
        assert result["skipped"] == 0
        assert len(result["proposals"]) == 1

    def test_proposal_has_correct_fields(self):
        agent = NoOpAgent()
        violations = [self._make_violation()]
        result = dispatch_remediations(violations, [agent])
        proposal = result["proposals"][0]
        assert proposal["violation_id"] == "v-001"
        assert proposal["agent_id"] == "noop-agent"
        assert proposal["agent_version"] == "1.0.0"
        assert proposal["status"] == "pending_review"
        assert "proposed_sql" in proposal
        assert "confidence" in proposal
        assert "created_at" in proposal

    def test_skips_when_no_matching_agent(self):
        agent = NoOpAgent()  # handles POL-TEST-001
        violations = [self._make_violation(policy_id="POL-UNKNOWN-999")]
        result = dispatch_remediations(violations, [agent])
        assert result["dispatched"] == 0
        assert result["skipped"] == 1

    def test_skips_already_proposed_violations(self):
        agent = NoOpAgent()
        violations = [self._make_violation()]
        existing = {("v-001", "noop-agent", "1.0.0")}
        result = dispatch_remediations(violations, [agent], existing)
        assert result["dispatched"] == 0
        assert result["skipped"] == 1

    def test_dispatches_multiple_violations(self):
        agent = NoOpAgent()
        violations = [
            self._make_violation(violation_id="v-001"),
            self._make_violation(violation_id="v-002"),
            self._make_violation(violation_id="v-003", policy_id="POL-OTHER"),
        ]
        result = dispatch_remediations(violations, [agent])
        assert result["dispatched"] == 2  # v-001 and v-002
        assert result["skipped"] == 1    # v-003 (no matching agent)

    def test_handles_agent_error_gracefully(self):
        class FailingAgent:
            agent_id = "failing-agent"
            handles = ["POL-TEST-001"]
            version = "1.0.0"
            model = ""
            def gather_context(self, v):
                raise RuntimeError("LLM unavailable")
            def propose_fix(self, c):
                return {}

        violations = [self._make_violation()]
        result = dispatch_remediations(violations, [FailingAgent()])
        assert result["dispatched"] == 0
        assert result["errors"] == 1
