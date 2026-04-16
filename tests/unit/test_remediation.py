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
