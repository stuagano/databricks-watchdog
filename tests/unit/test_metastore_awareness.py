"""Tests for Phase 3 metastore-awareness across all downstream consumers.

Verifies that metastore_id support is properly wired into:
- MCP server tools and config
- Guardrails watchdog client
- Ontos adapter provider protocol and WatchdogProvider
- Genie SQL dashboard templates
"""

import importlib
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent

# Ensure sub-packages are importable
sys.path.insert(0, str(REPO_ROOT / "mcp" / "src"))
sys.path.insert(0, str(REPO_ROOT / "guardrails" / "src"))
sys.path.insert(0, str(REPO_ROOT / "ontos-adapter" / "src"))


# ── Helpers for mocking missing packages ───────────────────────────────────

def _ensure_mcp_importable():
    """Stub out the mcp package if it's not installed."""
    if "mcp" not in sys.modules:
        mcp_mod = ModuleType("mcp")
        mcp_types = ModuleType("mcp.types")

        class _TextContent:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.text = kwargs.get("text", "")

        class _Tool:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        mcp_types.TextContent = _TextContent
        mcp_types.Tool = _Tool
        mcp_mod.types = mcp_types

        mcp_server_mod = ModuleType("mcp.server")
        mcp_mod.server = mcp_server_mod

        mcp_sse = ModuleType("mcp.server.sse")
        mcp_server_mod.sse = mcp_sse

        sys.modules["mcp"] = mcp_mod
        sys.modules["mcp.types"] = mcp_types
        sys.modules["mcp.server"] = mcp_server_mod
        sys.modules["mcp.server.sse"] = mcp_sse


def _ensure_databricks_sql_importable():
    """Stub out databricks.sql if not installed."""
    if "databricks.sql" not in sys.modules:
        db_sql = ModuleType("databricks.sql")
        db_sql.connect = MagicMock()
        sys.modules["databricks.sql"] = db_sql

        if "databricks" in sys.modules:
            sys.modules["databricks"].sql = db_sql


# Set up stubs before importing tested modules
_ensure_mcp_importable()
_ensure_databricks_sql_importable()

# Force re-import of modules that may have failed before stubs were set up
for mod_name in list(sys.modules.keys()):
    if mod_name.startswith(("watchdog_mcp", "watchdog_governance")):
        del sys.modules[mod_name]


# ── MCP Server ─────────────────────────────────────────────────────────────


class TestMcpConfig:
    def test_default_metastore_id_empty(self):
        from watchdog_mcp.config import WatchdogMcpConfig

        config = WatchdogMcpConfig()
        assert config.default_metastore_id == ""

    def test_default_metastore_id_from_env(self):
        from watchdog_mcp.config import WatchdogMcpConfig

        with patch.dict("os.environ", {"WATCHDOG_DEFAULT_METASTORE_ID": "ms-abc123"}):
            config = WatchdogMcpConfig()
            assert config.default_metastore_id == "ms-abc123"

    def test_server_version_bumped(self):
        from watchdog_mcp.config import WatchdogMcpConfig

        config = WatchdogMcpConfig()
        assert config.server_version == "0.4.0"


class TestMcpTools:
    def test_list_metastores_tool_registered(self):
        from watchdog_mcp.tools.governance import TOOLS

        tool_names = [t.name for t in TOOLS]
        assert "list_metastores" in tool_names

    def test_all_tools_accept_metastore_parameter(self):
        """Every tool except list_metastores should accept the metastore param."""
        from watchdog_mcp.tools.governance import TOOLS

        tools_needing_metastore = [
            "get_violations",
            "get_governance_summary",
            "get_policies",
            "get_scan_history",
            "get_resource_violations",
            "get_exceptions",
        ]

        for tool in TOOLS:
            if tool.name in tools_needing_metastore:
                props = tool.inputSchema.get("properties", {})
                assert "metastore" in props, (
                    f"Tool '{tool.name}' missing 'metastore' property in inputSchema"
                )

    def test_list_metastores_has_no_required_params(self):
        from watchdog_mcp.tools.governance import TOOLS

        tool = next(t for t in TOOLS if t.name == "list_metastores")
        assert "required" not in tool.inputSchema or tool.inputSchema.get("required", []) == []

    @pytest.mark.asyncio
    async def test_handle_routes_list_metastores(self):
        """Verify the handle function dispatches list_metastores."""
        from watchdog_mcp.tools.governance import handle

        mock_w = MagicMock()
        mock_config = MagicMock()
        mock_config.qualified_schema = "platform.watchdog"

        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"

        # MagicMock(name=...) sets the mock's repr, not .name attr
        col_ms = MagicMock()
        col_ms.name = "metastore_id"
        col_scan = MagicMock()
        col_scan.name = "latest_scan"
        col_count = MagicMock()
        col_count.name = "resource_count"
        col_last = MagicMock()
        col_last.name = "last_scanned"
        mock_response.manifest.schema.columns = [col_ms, col_scan, col_count, col_last]
        mock_response.result.data_array = [
            ["ms-001", "scan-99", "150", "2026-04-01T00:00:00"]
        ]
        mock_w.statement_execution.execute_statement.return_value = mock_response

        result = await handle("list_metastores", {}, mock_w, mock_config)
        assert len(result) == 1
        assert "ms-001" in result[0].text

    def test_resolve_metastore_prefers_args(self):
        from watchdog_mcp.tools.governance import _resolve_metastore
        from watchdog_mcp.config import WatchdogMcpConfig

        config = WatchdogMcpConfig()
        config.default_metastore_id = "default-ms"

        assert _resolve_metastore({"metastore": "arg-ms"}, config) == "arg-ms"
        assert _resolve_metastore({}, config) == "default-ms"

    def test_resolve_metastore_empty_when_no_default(self):
        from watchdog_mcp.tools.governance import _resolve_metastore
        from watchdog_mcp.config import WatchdogMcpConfig

        config = WatchdogMcpConfig()
        assert _resolve_metastore({}, config) == ""


# ── Guardrails ─────────────────────────────────────────────────────────────


class TestGuardrailsMetastore:
    def test_resource_governance_state_has_metastore_id(self):
        from ai_devkit.watchdog_client import ResourceGovernanceState

        state = ResourceGovernanceState(resource_id="r-1")
        assert state.metastore_id == ""

        state2 = ResourceGovernanceState(resource_id="r-2", metastore_id="ms-abc")
        assert state2.metastore_id == "ms-abc"

    def test_get_resource_governance_accepts_metastore_id(self):
        import inspect
        from ai_devkit.watchdog_client import get_resource_governance

        sig = inspect.signature(get_resource_governance)
        assert "metastore_id" in sig.parameters
        assert sig.parameters["metastore_id"].default is None

    def test_get_policies_for_operation_accepts_metastore_id(self):
        import inspect
        from ai_devkit.watchdog_client import get_policies_for_operation

        sig = inspect.signature(get_policies_for_operation)
        assert "metastore_id" in sig.parameters
        assert sig.parameters["metastore_id"].default is None

    def test_get_resource_governance_propagates_metastore_id(self):
        """When metastore_id is provided, it appears in SQL queries."""
        from ai_devkit.watchdog_client import get_resource_governance

        mock_w = MagicMock()
        mock_config = MagicMock()
        mock_config.watchdog_schema = "platform.watchdog"
        mock_config.warehouse_id = "wh-001"

        mock_resp = MagicMock()
        mock_resp.result.data_array = []
        mock_w.statement_execution.execute_statement.return_value = mock_resp

        state = get_resource_governance(
            mock_w, mock_config, "resource-1", metastore_id="ms-test"
        )
        assert state.metastore_id == "ms-test"

        call_args = mock_w.statement_execution.execute_statement.call_args_list
        assert len(call_args) >= 1
        first_sql = call_args[0].kwargs.get("statement", "")
        assert "metastore_id = 'ms-test'" in first_sql

    def test_get_resource_governance_no_metastore_no_filter(self):
        """When metastore_id is omitted, no metastore filter in SQL."""
        from ai_devkit.watchdog_client import get_resource_governance

        mock_w = MagicMock()
        mock_config = MagicMock()
        mock_config.watchdog_schema = "platform.watchdog"
        mock_config.warehouse_id = "wh-001"

        mock_resp = MagicMock()
        mock_resp.result.data_array = []
        mock_w.statement_execution.execute_statement.return_value = mock_resp

        get_resource_governance(mock_w, mock_config, "resource-1")

        call_args = mock_w.statement_execution.execute_statement.call_args_list
        first_sql = call_args[0].kwargs.get("statement", "")
        assert "metastore_id" not in first_sql


# ── Ontos Adapter ──────────────────────────────────────────────────────────


class TestOntosMetastoreInfo:
    def test_metastore_info_model(self):
        from watchdog_governance.models import MetastoreInfo

        info = MetastoreInfo(metastore_id="ms-abc")
        assert info.metastore_id == "ms-abc"
        assert info.latest_scan is None
        assert info.resource_count == 0
        assert info.last_scanned is None

    def test_metastore_info_with_all_fields(self):
        from watchdog_governance.models import MetastoreInfo

        info = MetastoreInfo(
            metastore_id="ms-abc",
            latest_scan="scan-42",
            resource_count=100,
            last_scanned="2026-04-01T12:00:00",
        )
        assert info.resource_count == 100
        assert info.latest_scan == "scan-42"


class TestOntosProviderProtocol:
    def test_protocol_has_list_metastores(self):
        from watchdog_governance.provider import GovernanceProvider
        import inspect

        assert hasattr(GovernanceProvider, "list_metastores")
        sig = inspect.signature(GovernanceProvider.list_metastores)
        assert "self" in sig.parameters

    def test_protocol_has_set_active_metastore(self):
        from watchdog_governance.provider import GovernanceProvider
        import inspect

        assert hasattr(GovernanceProvider, "set_active_metastore")
        sig = inspect.signature(GovernanceProvider.set_active_metastore)
        assert "metastore_id" in sig.parameters

    def test_violations_summary_has_metastore_param(self):
        from watchdog_governance.provider import GovernanceProvider
        import inspect

        sig = inspect.signature(GovernanceProvider.violations_summary)
        assert "metastore_id" in sig.parameters
        assert sig.parameters["metastore_id"].default is None

    def test_list_violations_has_metastore_param(self):
        from watchdog_governance.provider import GovernanceProvider
        import inspect

        sig = inspect.signature(GovernanceProvider.list_violations)
        assert "metastore_id" in sig.parameters

    def test_list_resources_has_metastore_param(self):
        from watchdog_governance.provider import GovernanceProvider
        import inspect

        sig = inspect.signature(GovernanceProvider.list_resources)
        assert "metastore_id" in sig.parameters


class TestWatchdogProviderMetastore:
    def test_set_active_metastore(self):
        from watchdog_governance.providers.watchdog import WatchdogProvider

        provider = WatchdogProvider(
            server_hostname="host", http_path="/path", access_token="tok"
        )
        assert provider._active_metastore is None

        provider.set_active_metastore("ms-abc")
        assert provider._active_metastore == "ms-abc"

        provider.set_active_metastore(None)
        assert provider._active_metastore is None

    def test_resolve_metastore_priority(self):
        from watchdog_governance.providers.watchdog import WatchdogProvider

        provider = WatchdogProvider(
            server_hostname="host", http_path="/path", access_token="tok"
        )

        assert provider._resolve_metastore() is None

        provider.set_active_metastore("ms-active")
        assert provider._resolve_metastore() == "ms-active"

        assert provider._resolve_metastore("ms-param") == "ms-param"

    def test_metastore_clause_generation(self):
        from watchdog_governance.providers.watchdog import WatchdogProvider

        provider = WatchdogProvider(
            server_hostname="host", http_path="/path", access_token="tok"
        )

        assert provider._metastore_clause() == ""

        provider.set_active_metastore("ms-abc")
        clause = provider._metastore_clause()
        assert "metastore_id = 'ms-abc'" in clause
        assert clause.startswith("AND")

        clause_where = provider._metastore_clause(prefix="WHERE")
        assert clause_where.startswith("WHERE")


# ── Genie SQL Templates ───────────────────────────────────────────────────


class TestGenieSqlTemplates:
    @pytest.fixture
    def dashboards_dir(self):
        return REPO_ROOT / "engine" / "dashboards"

    def test_compliance_summary_includes_metastore_id(self, dashboards_dir):
        sql = (dashboards_dir / "v2_compliance_summary.sql").read_text()
        assert "metastore_id" in sql

    def test_violations_detail_includes_metastore_id(self, dashboards_dir):
        sql = (dashboards_dir / "v2_violations_detail.sql").read_text()
        assert "v.metastore_id" in sql

    def test_violations_by_owner_includes_metastore_id(self, dashboards_dir):
        sql = (dashboards_dir / "v2_violations_by_owner.sql").read_text()
        assert "metastore_id" in sql

    def test_exceptions_active_includes_metastore_id(self, dashboards_dir):
        sql = (dashboards_dir / "v2_exceptions_active.sql").read_text()
        assert "e.metastore_id" in sql

    def test_exceptions_history_includes_metastore_id(self, dashboards_dir):
        sql = (dashboards_dir / "v2_exceptions_history.sql").read_text()
        assert "e.metastore_id" in sql

    def test_compliance_summary_metastore_aware_scan_subquery(self, dashboards_dir):
        """The latest-scan subquery should be metastore-aware."""
        sql = (dashboards_dir / "v2_compliance_summary.sql").read_text()
        assert "WHERE metastore_id = ri.metastore_id" in sql
