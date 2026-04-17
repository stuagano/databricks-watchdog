"""Unit tests for governance tools — validate_ai_query logic and agent session management.

Run with: pytest tests/unit/test_guardrails_governance.py -v
"""
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# Stub databricks.sdk and mcp before import
_db = types.ModuleType("databricks")
_sdk = types.ModuleType("databricks.sdk")
_sdk.WorkspaceClient = MagicMock
sys.modules.setdefault("databricks", _db)
sys.modules.setdefault("databricks.sdk", _sdk)

_mcp = types.ModuleType("mcp")
_mcp_types = types.ModuleType("mcp.types")


class _TextContent:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _Tool:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


_mcp_types.TextContent = _TextContent
_mcp_types.Tool = _Tool
_mcp.types = _mcp_types
sys.modules.setdefault("mcp", _mcp)
sys.modules.setdefault("mcp.types", _mcp_types)

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "guardrails" / "src"))

import json
import pytest
from watchdog_guardrails.tools.governance import (
    TOOLS,
    handle,
    _init_agent_session,
    _calculate_risk_level,
    _agent_sessions,
)
from watchdog_guardrails.config import GuardrailsConfig


def _mock_config():
    cfg = GuardrailsConfig()
    cfg.watchdog_schema = "platform.watchdog"
    cfg.warehouse_id = "wh-abc"
    cfg.catalog = "gold"
    return cfg


def _make_table_info(classification=None, pii=False, phi=False):
    """Build a mock w.tables.get() result."""
    info = MagicMock()
    info.owner = "owner@co.com"
    info.comment = "Test table"
    info.table_type = "MANAGED"
    info.storage_location = None
    info.columns = []
    props = {}
    if classification:
        props["classification"] = classification
    if pii:
        props["pii"] = "true"
    if phi:
        props["phi"] = "true"
    info.properties = props
    return info


def _make_governance_state(violations=None, exceptions=None):
    """Build a mock ResourceGovernanceState."""
    from watchdog_guardrails.watchdog_client import ResourceGovernanceState
    return ResourceGovernanceState(
        resource_id="gold.finance.gl",
        classes=[],
        open_violations=violations or [],
        active_exceptions=exceptions or [],
        watchdog_available=True,
    )


class TestToolsList:
    def test_has_13_tools(self):
        assert len(TOOLS) == 13

    def test_tool_names(self):
        names = {t.name for t in TOOLS}
        required = {
            "get_table_lineage", "get_table_permissions", "describe_table",
            "search_tables_by_tag", "validate_ai_query", "suggest_safe_tables",
            "preview_data", "safe_columns", "estimate_cost", "check_before_access",
            "log_agent_action", "get_agent_compliance", "report_agent_execution",
        }
        assert names == required


class TestValidateAiQuery:
    @pytest.mark.asyncio
    async def test_public_table_query_proceeds(self):
        w = MagicMock()
        config = _mock_config()
        w.tables.get.return_value = _make_table_info(classification="public")

        import watchdog_guardrails.tools.governance as gov_module
        original = gov_module.get_resource_governance

        def mock_gov(w, config, resource_id):
            return _make_governance_state()

        gov_module.get_resource_governance = mock_gov
        try:
            result = await handle("validate_ai_query", {
                "tables": ["gold.finance.gl"],
                "operation": "query",
            }, w, config)
        finally:
            gov_module.get_resource_governance = original

        data = json.loads(result[0].text)
        assert data["verdict"] == "proceed"

    @pytest.mark.asyncio
    async def test_restricted_table_train_blocked(self):
        # train(4) + restricted(3) = 7 >= 7 → blocked
        w = MagicMock()
        config = _mock_config()
        w.tables.get.return_value = _make_table_info(classification="restricted")

        import watchdog_guardrails.tools.governance as gov_module
        original = gov_module.get_resource_governance

        def mock_gov(w, config, resource_id):
            return _make_governance_state()

        gov_module.get_resource_governance = mock_gov
        try:
            result = await handle("validate_ai_query", {
                "tables": ["gold.finance.gl"],
                "operation": "train",
            }, w, config)
        finally:
            gov_module.get_resource_governance = original

        data = json.loads(result[0].text)
        assert data["verdict"] == "blocked"

    @pytest.mark.asyncio
    async def test_restricted_table_embed_warning(self):
        # embed(3) + restricted(3) = 6 >= 5 but < 7 → warning
        w = MagicMock()
        config = _mock_config()
        w.tables.get.return_value = _make_table_info(classification="restricted")

        import watchdog_guardrails.tools.governance as gov_module
        original = gov_module.get_resource_governance

        def mock_gov(w, config, resource_id):
            return _make_governance_state()

        gov_module.get_resource_governance = mock_gov
        try:
            result = await handle("validate_ai_query", {
                "tables": ["gold.finance.gl"],
                "operation": "embed",
            }, w, config)
        finally:
            gov_module.get_resource_governance = original

        data = json.loads(result[0].text)
        assert data["verdict"] == "warning"

    @pytest.mark.asyncio
    async def test_critical_violation_blocks(self):
        w = MagicMock()
        config = _mock_config()
        w.tables.get.return_value = _make_table_info(classification="public")

        import watchdog_guardrails.tools.governance as gov_module
        original = gov_module.get_resource_governance

        def mock_gov(w, config, resource_id):
            return _make_governance_state(violations=[
                {"severity": "critical", "policy_id": "POL-001", "policy_name": "Must have steward"}
            ])

        gov_module.get_resource_governance = mock_gov
        try:
            result = await handle("validate_ai_query", {
                "tables": ["gold.finance.gl"],
                "operation": "query",
            }, w, config)
        finally:
            gov_module.get_resource_governance = original

        data = json.loads(result[0].text)
        assert data["verdict"] == "blocked"

    @pytest.mark.asyncio
    async def test_no_tables_blocked(self):
        w = MagicMock()
        config = _mock_config()
        result = await handle("validate_ai_query", {
            "tables": [],
            "operation": "query",
        }, w, config)
        data = json.loads(result[0].text)
        assert data["verdict"] == "blocked"


class TestAgentSession:
    def setup_method(self):
        # Clean up any stale sessions
        _agent_sessions.clear()

    def test_init_agent_session(self):
        session = _init_agent_session("agent-001")
        assert session["agent_id"] == "agent-001"
        assert session["checks_passed"] == 0
        assert session["risk_level"] == "low"

    def test_calculate_risk_level_clean(self):
        session = {"checks_denied": 0, "checks_warned": 0}
        assert _calculate_risk_level(session) == "low"

    def test_calculate_risk_level_warned(self):
        session = {"checks_denied": 0, "checks_warned": 1}
        assert _calculate_risk_level(session) == "medium"

    def test_calculate_risk_level_denied(self):
        session = {"checks_denied": 1, "checks_warned": 0}
        assert _calculate_risk_level(session) == "high"

    @pytest.mark.asyncio
    async def test_report_cleans_session(self):
        w = MagicMock()
        config = _mock_config()

        _agent_sessions["agent-cleanup"] = {
            "agent_id": "agent-cleanup",
            "checks_passed": 5, "checks_denied": 0, "checks_warned": 0,
            "tables_accessed": ["gold.finance.gl"], "actions_logged": 2,
            "risk_level": "low", "session_start": "2026-04-16T00:00:00+00:00",
        }

        result = await handle("report_agent_execution", {
            "agent_id": "agent-cleanup",
            "execution_summary": "Ran 5 queries",
        }, w, config)

        data = json.loads(result[0].text)
        assert data["compliance_status"] == "compliant"
        assert "agent-cleanup" not in _agent_sessions  # Session cleaned up
