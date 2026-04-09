"""Tests for Phase 5D runtime governance tools.

Validates tool registration, schema correctness, session state management,
decision logic, risk level calculation, compliance reporting, and session
cleanup. No Spark or Databricks connection required.
"""

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

import pytest

# ── Mock MCP types before importing governance ─────────────────────────
# The real `mcp` package may not be installed in the test environment, and
# the repo's local `mcp/` directory shadows it. We create minimal stubs.

_mcp_types = types.ModuleType("mcp.types")
_mcp_server = types.ModuleType("mcp.server")
_mcp_server_sse = types.ModuleType("mcp.server.sse")


@dataclass
class _TextContent:
    type: str
    text: str


@dataclass
class _Tool:
    name: str
    description: str
    inputSchema: dict


_mcp_types.TextContent = _TextContent
_mcp_types.Tool = _Tool
_mcp_server.Server = MagicMock
_mcp_server_sse.SseServerTransport = MagicMock

# Patch sys.modules so ai_devkit imports resolve
sys.modules.setdefault("mcp", types.ModuleType("mcp"))
sys.modules["mcp.types"] = _mcp_types
sys.modules["mcp.server"] = _mcp_server
sys.modules["mcp.server.sse"] = _mcp_server_sse

# Ensure guardrails package is importable
GUARDRAILS_SRC = Path(__file__).parent.parent.parent / "guardrails" / "src"
sys.path.insert(0, str(GUARDRAILS_SRC))

from ai_devkit.tools.governance import (
    TOOLS,
    _agent_sessions,
    _calculate_risk_level,
    _init_agent_session,
    _check_before_access,
    _log_agent_action,
    _get_agent_compliance,
    _report_agent_execution,
)
from ai_devkit.watchdog_client import ResourceGovernanceState
from ai_devkit.config import AiDevkitConfig


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clear_sessions():
    """Clear agent sessions before and after each test."""
    _agent_sessions.clear()
    yield
    _agent_sessions.clear()


@pytest.fixture
def mock_ws():
    """Mock WorkspaceClient."""
    return MagicMock()


@pytest.fixture
def mock_config():
    """Mock AiDevkitConfig."""
    return AiDevkitConfig(
        host="https://test.cloud.databricks.com",
        catalog="test_catalog",
        watchdog_schema="platform.watchdog",
        warehouse_id="test-warehouse-id",
    )


def _make_gov_state(**kwargs) -> ResourceGovernanceState:
    """Create a ResourceGovernanceState with overrides."""
    defaults = {
        "resource_id": "test.schema.table",
        "classes": [],
        "ancestors": [],
        "open_violations": [],
        "active_exceptions": [],
        "policies_applied": [],
        "watchdog_available": True,
    }
    defaults.update(kwargs)
    return ResourceGovernanceState(**defaults)


# ── Tool registration ──────────────────────────────────────────────────


class TestRuntimeToolRegistration:
    """Verify runtime tools are registered with correct names and schemas."""

    def test_total_tool_count(self):
        assert len(TOOLS) == 13, (
            f"Expected 13 registered tools (9 build-time + 4 runtime), got {len(TOOLS)}: "
            f"{[t.name for t in TOOLS]}"
        )

    def test_runtime_tool_names_present(self):
        names = {t.name for t in TOOLS}
        runtime_tools = {
            "check_before_access",
            "log_agent_action",
            "get_agent_compliance",
            "report_agent_execution",
        }
        assert runtime_tools.issubset(names), (
            f"Missing runtime tools: {runtime_tools - names}"
        )

    def test_check_before_access_schema(self):
        tool = next(t for t in TOOLS if t.name == "check_before_access")
        props = tool.inputSchema["properties"]
        assert "agent_id" in props
        assert "table" in props
        assert "operation" in props
        assert "columns" in props
        assert props["agent_id"]["type"] == "string"
        assert props["table"]["type"] == "string"
        assert props["operation"]["enum"] == ["SELECT", "INSERT", "UPDATE", "DELETE"]
        assert props["columns"]["type"] == "array"
        assert tool.inputSchema["required"] == ["agent_id", "table"]

    def test_log_agent_action_schema(self):
        tool = next(t for t in TOOLS if t.name == "log_agent_action")
        props = tool.inputSchema["properties"]
        assert "agent_id" in props
        assert "action" in props
        assert "target" in props
        assert "details" in props
        assert "classification" in props
        assert props["action"]["enum"] == [
            "data_access", "data_export", "external_api_call",
            "model_invocation", "tool_call",
        ]
        assert tool.inputSchema["required"] == ["agent_id", "action", "target"]

    def test_get_agent_compliance_schema(self):
        tool = next(t for t in TOOLS if t.name == "get_agent_compliance")
        assert tool.inputSchema["required"] == ["agent_id"]

    def test_report_agent_execution_schema(self):
        tool = next(t for t in TOOLS if t.name == "report_agent_execution")
        props = tool.inputSchema["properties"]
        assert "agent_id" in props
        assert "execution_summary" in props
        assert tool.inputSchema["required"] == ["agent_id"]

    def test_every_runtime_tool_has_input_schema(self):
        runtime_names = {
            "check_before_access", "log_agent_action",
            "get_agent_compliance", "report_agent_execution",
        }
        for tool in TOOLS:
            if tool.name in runtime_names:
                assert tool.inputSchema is not None, f"{tool.name} missing inputSchema"
                assert tool.inputSchema["type"] == "object"


# ── Session state management ───────────────────────────────────────────


class TestSessionState:
    """Verify _agent_sessions init, update, and cleanup."""

    def test_init_creates_session(self):
        session = _init_agent_session("agent-1")
        assert session["agent_id"] == "agent-1"
        assert session["checks_passed"] == 0
        assert session["checks_denied"] == 0
        assert session["checks_warned"] == 0
        assert session["tables_accessed"] == []
        assert session["pii_tables_accessed"] == []
        assert isinstance(session["classifications_seen"], set)
        assert session["actions_logged"] == 0
        assert session["risk_level"] == "low"
        assert "session_start" in session
        assert "agent-1" in _agent_sessions

    def test_init_returns_existing_session(self):
        session1 = _init_agent_session("agent-2")
        session1["checks_passed"] = 5
        session2 = _init_agent_session("agent-2")
        assert session2["checks_passed"] == 5
        assert session1 is session2

    def test_multiple_agents_independent(self):
        s1 = _init_agent_session("agent-a")
        s2 = _init_agent_session("agent-b")
        s1["checks_passed"] = 10
        assert s2["checks_passed"] == 0


# ── Risk level calculation ─────────────────────────────────────────────


class TestRiskLevelCalculation:
    """Verify risk level logic: low/medium/high/critical."""

    def test_low_risk_all_passed(self):
        session = {"pii_tables_accessed": [], "checks_denied": 0, "checks_warned": 0}
        assert _calculate_risk_level(session) == "low"

    def test_medium_risk_warnings_only(self):
        session = {"pii_tables_accessed": [], "checks_denied": 0, "checks_warned": 3}
        assert _calculate_risk_level(session) == "medium"

    def test_high_risk_denied_no_pii(self):
        session = {"pii_tables_accessed": [], "checks_denied": 1, "checks_warned": 0}
        assert _calculate_risk_level(session) == "high"

    def test_high_risk_pii_with_warnings(self):
        session = {"pii_tables_accessed": ["t1"], "checks_denied": 0, "checks_warned": 1}
        assert _calculate_risk_level(session) == "high"

    def test_critical_risk_pii_with_denied(self):
        session = {"pii_tables_accessed": ["t1"], "checks_denied": 1, "checks_warned": 0}
        assert _calculate_risk_level(session) == "critical"


# ── check_before_access decision logic ─────────────────────────────────


class TestCheckBeforeAccess:
    """Verify decision logic: deny for PII/critical, warn for high, allow for clean."""

    @pytest.mark.asyncio
    async def test_allow_clean_table(self, mock_ws, mock_config):
        gov = _make_gov_state(classes=["DataAsset"])
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a1", "table": "cat.sch.clean_table"},
            )
        data = json.loads(result[0].text)
        assert data["decision"] == "allow"
        assert data["reasons"] == []
        assert _agent_sessions["a1"]["checks_passed"] == 1

    @pytest.mark.asyncio
    async def test_deny_pii_table(self, mock_ws, mock_config):
        gov = _make_gov_state(classes=["PiiAsset"])
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a2", "table": "cat.sch.pii_table"},
            )
        data = json.loads(result[0].text)
        assert data["decision"] == "deny"
        assert any("PII" in r for r in data["reasons"])
        assert any("masked" in a for a in data["alternatives"])
        assert _agent_sessions["a2"]["checks_denied"] == 1
        assert "cat.sch.pii_table" in _agent_sessions["a2"]["pii_tables_accessed"]

    @pytest.mark.asyncio
    async def test_deny_critical_violations(self, mock_ws, mock_config):
        gov = _make_gov_state(
            open_violations=[{"severity": "critical", "policy_id": "POL-1"}],
        )
        mock_ws.tables.get.return_value = MagicMock(owner="data-team@corp.com")
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a3", "table": "cat.sch.bad_table"},
            )
        data = json.loads(result[0].text)
        assert data["decision"] == "deny"
        assert data["critical_violations"] is True
        assert any("data-team@corp.com" in a for a in data["alternatives"])

    @pytest.mark.asyncio
    async def test_warn_high_violations(self, mock_ws, mock_config):
        gov = _make_gov_state(
            open_violations=[{"severity": "high", "policy_id": "POL-2"}],
        )
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a4", "table": "cat.sch.warn_table"},
            )
        data = json.loads(result[0].text)
        assert data["decision"] == "warn"
        assert _agent_sessions["a4"]["checks_warned"] == 1

    @pytest.mark.asyncio
    async def test_warn_confidential_sensitive_columns(self, mock_ws, mock_config):
        gov = _make_gov_state(classes=["ConfidentialAsset"], ancestors=["Confidential"])
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {
                    "agent_id": "a5",
                    "table": "cat.sch.conf_table",
                    "columns": ["name", "ssn", "email"],
                },
            )
        data = json.loads(result[0].text)
        assert data["decision"] == "warn"
        assert any("ssn" in r for r in data["reasons"])

    @pytest.mark.asyncio
    async def test_deny_restricted_table(self, mock_ws, mock_config):
        gov = _make_gov_state(classes=["RestrictedAsset"], ancestors=["Restricted"])
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a6", "table": "cat.sch.restricted_table"},
            )
        data = json.loads(result[0].text)
        assert data["decision"] == "deny"
        assert any("restricted" in r.lower() for r in data["reasons"])

    @pytest.mark.asyncio
    async def test_session_tracks_classifications(self, mock_ws, mock_config):
        gov = _make_gov_state(classes=["DataAsset", "InternalAsset"])
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a7", "table": "cat.sch.table1"},
            )
        assert "DataAsset" in _agent_sessions["a7"]["classifications_seen"]
        assert "InternalAsset" in _agent_sessions["a7"]["classifications_seen"]

    @pytest.mark.asyncio
    async def test_default_operation_is_select(self, mock_ws, mock_config):
        gov = _make_gov_state()
        with patch("ai_devkit.tools.governance.get_resource_governance", return_value=gov):
            result = await _check_before_access(
                mock_ws, mock_config,
                {"agent_id": "a8", "table": "cat.sch.t"},
            )
        data = json.loads(result[0].text)
        assert data["operation"] == "SELECT"


# ── log_agent_action ───────────────────────────────────────────────────


class TestLogAgentAction:
    """Verify audit logging and session state updates."""

    @pytest.mark.asyncio
    async def test_log_returns_event_id(self, mock_ws, mock_config):
        result = await _log_agent_action(
            mock_ws, mock_config,
            {"agent_id": "a1", "action": "data_access", "target": "cat.sch.t"},
        )
        data = json.loads(result[0].text)
        assert data["status"] == "logged"
        assert "event_id" in data
        assert data["agent_id"] == "a1"
        assert data["action"] == "data_access"

    @pytest.mark.asyncio
    async def test_log_increments_actions_logged(self, mock_ws, mock_config):
        await _log_agent_action(
            mock_ws, mock_config,
            {"agent_id": "log-test", "action": "data_access", "target": "t1"},
        )
        await _log_agent_action(
            mock_ws, mock_config,
            {"agent_id": "log-test", "action": "external_api_call", "target": "api.example.com"},
        )
        assert _agent_sessions["log-test"]["actions_logged"] == 2


# ── get_agent_compliance ───────────────────────────────────────────────


class TestGetAgentCompliance:
    """Verify compliance status retrieval."""

    @pytest.mark.asyncio
    async def test_new_agent_returns_clean_state(self, mock_ws, mock_config):
        result = await _get_agent_compliance(
            mock_ws, mock_config,
            {"agent_id": "new-agent"},
        )
        data = json.loads(result[0].text)
        assert data["agent_id"] == "new-agent"
        assert data["checks_passed"] == 0
        assert data["risk_level"] == "low"

    @pytest.mark.asyncio
    async def test_returns_accumulated_state(self, mock_ws, mock_config):
        session = _init_agent_session("existing-agent")
        session["checks_passed"] = 3
        session["checks_warned"] = 1
        session["tables_accessed"] = ["t1", "t2", "t3"]
        session["risk_level"] = "medium"

        result = await _get_agent_compliance(
            mock_ws, mock_config,
            {"agent_id": "existing-agent"},
        )
        data = json.loads(result[0].text)
        assert data["checks_passed"] == 3
        assert data["checks_warned"] == 1
        assert data["risk_level"] == "medium"
        assert len(data["tables_accessed"]) == 3


# ── report_agent_execution ─────────────────────────────────────────────


class TestReportAgentExecution:
    """Verify compliance report generation and session cleanup."""

    @pytest.mark.asyncio
    async def test_compliant_report(self, mock_ws, mock_config):
        session = _init_agent_session("compliant-agent")
        session["checks_passed"] = 5

        result = await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "compliant-agent", "execution_summary": "Ran queries."},
        )
        data = json.loads(result[0].text)
        assert data["compliance_status"] == "compliant"
        assert data["risk_level"] == "low"
        assert data["governance_checks"]["total"] == 5
        assert data["governance_checks"]["passed"] == 5
        assert data["execution_summary"] == "Ran queries."
        # Session should be cleared
        assert "compliant-agent" not in _agent_sessions

    @pytest.mark.asyncio
    async def test_non_compliant_report(self, mock_ws, mock_config):
        session = _init_agent_session("bad-agent")
        session["checks_passed"] = 2
        session["checks_denied"] = 1
        session["risk_level"] = "high"

        result = await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "bad-agent"},
        )
        data = json.loads(result[0].text)
        assert data["compliance_status"] == "non_compliant"
        assert data["risk_level"] == "high"
        assert data["governance_checks"]["denied"] == 1
        assert any("denied" in r.lower() for r in data["recommendations"])
        assert "bad-agent" not in _agent_sessions

    @pytest.mark.asyncio
    async def test_needs_review_report(self, mock_ws, mock_config):
        session = _init_agent_session("warn-agent")
        session["checks_passed"] = 3
        session["checks_warned"] = 2
        session["risk_level"] = "medium"

        result = await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "warn-agent"},
        )
        data = json.loads(result[0].text)
        assert data["compliance_status"] == "needs_review"
        assert "warn-agent" not in _agent_sessions

    @pytest.mark.asyncio
    async def test_report_no_session_returns_minimal(self, mock_ws, mock_config):
        result = await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "unknown-agent", "execution_summary": "Did nothing."},
        )
        data = json.loads(result[0].text)
        assert data["compliance_status"] == "compliant"
        assert data["governance_checks"]["total"] == 0
        assert data["recommendations"] == ["No governance activity recorded for this agent."]

    @pytest.mark.asyncio
    async def test_report_includes_pii_recommendation(self, mock_ws, mock_config):
        session = _init_agent_session("pii-agent")
        session["checks_passed"] = 1
        session["pii_tables_accessed"] = ["cat.sch.customers"]
        session["risk_level"] = "medium"
        session["checks_warned"] = 1

        result = await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "pii-agent"},
        )
        data = json.loads(result[0].text)
        assert any("PII" in r for r in data["recommendations"])

    @pytest.mark.asyncio
    async def test_session_cleanup_after_report(self, mock_ws, mock_config):
        _init_agent_session("cleanup-agent")
        assert "cleanup-agent" in _agent_sessions

        await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "cleanup-agent"},
        )
        assert "cleanup-agent" not in _agent_sessions

    @pytest.mark.asyncio
    async def test_report_has_duration(self, mock_ws, mock_config):
        _init_agent_session("duration-agent")

        result = await _report_agent_execution(
            mock_ws, mock_config,
            {"agent_id": "duration-agent"},
        )
        data = json.loads(result[0].text)
        assert "duration" in data
        assert data["duration"].endswith("s")
