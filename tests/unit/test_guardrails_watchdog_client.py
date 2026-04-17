"""Unit tests for guardrails Watchdog client.

Run with: pytest tests/unit/test_guardrails_watchdog_client.py -v
"""
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Stub databricks.sdk before import
_db = types.ModuleType("databricks")
_sdk = types.ModuleType("databricks.sdk")
_sdk.WorkspaceClient = MagicMock
sys.modules.setdefault("databricks", _db)
sys.modules.setdefault("databricks.sdk", _sdk)

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "guardrails" / "src"))

from watchdog_guardrails.watchdog_client import (
    ResourceGovernanceState,
    get_resource_governance,
)
from watchdog_guardrails.config import GuardrailsConfig


def _mock_config():
    cfg = GuardrailsConfig()
    cfg.watchdog_schema = "platform.watchdog"
    cfg.warehouse_id = "wh-abc123"
    return cfg


def _make_sdk_result(rows: list[list], columns: list[str]) -> MagicMock:
    """Build a mocked statement_execution result."""
    col_mocks = []
    for c in columns:
        m = MagicMock()
        m.name = c
        col_mocks.append(m)
    manifest = MagicMock()
    manifest.schema.columns = col_mocks
    result = MagicMock()
    result.data_array = rows
    resp = MagicMock()
    resp.result = result
    resp.manifest = manifest
    return resp


class TestResourceGovernanceState:
    def test_has_critical_violations_true(self):
        state = ResourceGovernanceState(
            resource_id="gold.finance.gl",
            open_violations=[{"severity": "critical", "policy_id": "POL-001"}],
        )
        assert state.has_critical_violations

    def test_has_critical_violations_false_when_only_high(self):
        state = ResourceGovernanceState(
            resource_id="gold.finance.gl",
            open_violations=[{"severity": "high", "policy_id": "POL-001"}],
        )
        assert not state.has_critical_violations

    def test_has_high_violations_true(self):
        state = ResourceGovernanceState(
            resource_id="gold.finance.gl",
            open_violations=[{"severity": "high", "policy_id": "POL-001"}],
        )
        assert state.has_high_violations

    def test_has_exception_by_policy_id(self):
        state = ResourceGovernanceState(
            resource_id="gold.finance.gl",
            active_exceptions=[{"policy_id": "POL-001", "exception_id": "exc-1"}],
        )
        assert state.has_exception("POL-001")
        assert not state.has_exception("POL-999")

    def test_has_exception_any(self):
        state = ResourceGovernanceState(
            resource_id="gold.finance.gl",
            active_exceptions=[{"policy_id": "POL-001"}],
        )
        assert state.has_exception()

    def test_no_class_hierarchy_properties(self):
        state = ResourceGovernanceState(resource_id="t")
        assert not hasattr(state, "is_pii")
        assert not hasattr(state, "is_confidential")
        assert not hasattr(state, "is_export_controlled")
        assert not hasattr(state, "inferred_classification")


class TestGetResourceGovernance:
    def test_returns_populated_state(self):
        w = MagicMock()
        config = _mock_config()

        # classifications query → 1 class
        class_resp = _make_sdk_result(
            [["GoldTable", "DataAsset"]],
            ["class_name", "class_ancestors"],
        )
        # violations query → 1 critical violation
        viol_resp = _make_sdk_result(
            [["v-001", "POL-001", "Must have steward", "critical", "DataQuality"]],
            ["violation_id", "policy_id", "policy_name", "severity", "domain"],
        )
        # exceptions query → empty
        exc_resp = _make_sdk_result([], ["exception_id", "policy_id", "justification", "expires_at"])

        w.statement_execution.execute_statement.side_effect = [
            class_resp, viol_resp, exc_resp
        ]

        state = get_resource_governance(w, config, "gold.finance.gl")

        assert state.resource_id == "gold.finance.gl"
        assert "GoldTable" in state.classes
        assert state.has_critical_violations
        assert not state.active_exceptions
        assert state.watchdog_available

    def test_degrades_gracefully_on_sdk_error(self):
        w = MagicMock()
        config = _mock_config()
        w.statement_execution.execute_statement.side_effect = Exception("access denied")

        state = get_resource_governance(w, config, "gold.finance.gl")

        assert not state.watchdog_available
        assert state.classes == []
        assert state.open_violations == []

    def test_empty_result_no_crash(self):
        w = MagicMock()
        config = _mock_config()

        empty = _make_sdk_result([], ["class_name", "class_ancestors"])
        empty2 = _make_sdk_result([], ["violation_id", "policy_id", "policy_name", "severity", "domain"])
        empty3 = _make_sdk_result([], ["exception_id", "policy_id", "justification", "expires_at"])

        w.statement_execution.execute_statement.side_effect = [empty, empty2, empty3]

        state = get_resource_governance(w, config, "gold.finance.gl")

        assert state.watchdog_available
        assert state.classes == []
        assert not state.has_critical_violations
