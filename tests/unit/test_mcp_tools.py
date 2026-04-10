"""Tests for Watchdog MCP governance tools.

Validates tool registration, schema correctness, remediation step
generation, and simulation query building. No Spark or Databricks
connection required.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Ensure watchdog_mcp package is importable
MCP_SRC = Path(__file__).parent.parent.parent / "mcp" / "src"
sys.path.insert(0, str(MCP_SRC))

# Mock the mcp package so tests run without installing the MCP server deps.
# governance.py uses Tool (a dataclass-like with name/inputSchema) and
# TextContent (a dataclass-like with type/text). We provide lightweight
# stand-ins that support attribute access.
_mcp_types = MagicMock()


class _StubTool:
    """Minimal stand-in for mcp.types.Tool."""
    def __init__(self, *, name, description="", inputSchema=None):
        self.name = name
        self.description = description
        self.inputSchema = inputSchema or {}


class _StubTextContent:
    """Minimal stand-in for mcp.types.TextContent."""
    def __init__(self, *, type="text", text=""):
        self.type = type
        self.text = text


_mcp_types.Tool = _StubTool
_mcp_types.TextContent = _StubTextContent

_mcp_mock = MagicMock()
_mcp_mock.types = _mcp_types

sys.modules.setdefault("mcp", _mcp_mock)
sys.modules.setdefault("mcp.types", _mcp_types)

import pytest

from watchdog_mcp.tools.governance import (
    TOOLS,
    _build_failure_condition,
    _build_remediation_steps,
)


# ── Tool registration ───────────────────────────────────────────────


class TestToolRegistration:
    """Verify all tools are registered with correct names and schemas."""

    def test_total_tool_count(self):
        assert len(TOOLS) == 13, (
            f"Expected 13 registered tools, got {len(TOOLS)}: "
            f"{[t.name for t in TOOLS]}"
        )

    def test_all_tool_names(self):
        names = {t.name for t in TOOLS}
        expected = {
            "get_violations",
            "get_governance_summary",
            "get_policies",
            "get_scan_history",
            "get_resource_violations",
            "get_exceptions",
            "explain_violation",
            "what_if_policy",
            "list_metastores",
            "suggest_policies",
            "policy_impact_analysis",
            "explore_governance",
            "suggest_classification",
        }
        assert names == expected

    def test_explain_violation_schema(self):
        tool = next(t for t in TOOLS if t.name == "explain_violation")
        props = tool.inputSchema["properties"]
        assert "violation_id" in props
        assert "resource_id" in props
        assert "policy_id" in props
        assert props["violation_id"]["type"] == "string"
        # No required fields — either violation_id or resource_id+policy_id
        assert "required" not in tool.inputSchema

    def test_what_if_policy_schema(self):
        tool = next(t for t in TOOLS if t.name == "what_if_policy")
        props = tool.inputSchema["properties"]
        assert "applies_to" in props
        assert "rule_type" in props
        assert "rule_key" in props
        assert "rule_value" in props
        assert "severity" in props
        assert props["rule_type"]["enum"] == [
            "tag_exists", "tag_equals", "tag_in", "metadata_equals", "metadata_not_empty",
        ]
        assert props["severity"]["enum"] == ["critical", "high", "medium", "low"]
        assert tool.inputSchema["required"] == ["rule_type", "rule_key"]

    def test_every_tool_has_input_schema(self):
        for tool in TOOLS:
            assert tool.inputSchema is not None, f"{tool.name} missing inputSchema"
            assert tool.inputSchema["type"] == "object", f"{tool.name} schema type not object"


# ── Remediation step builder ─────────────────────────────────────────


class TestBuildRemediationSteps:
    """Verify _build_remediation_steps produces actionable output."""

    def test_tag_exists_rule(self):
        violation = {
            "resource_type": "table",
            "resource_name": "sales.orders",
            "resource_id": "r-1",
        }
        policy = {
            "policy_id": "POL-D-001",
            "rule_json": '{"type": "tag_exists", "key": "data_owner"}',
        }
        steps = _build_remediation_steps(violation, policy, tags=None)
        assert len(steps) >= 1
        assert "data_owner" in steps[0]
        assert "sales.orders" in steps[0]
        # Should include an example SQL command
        assert any("ALTER" in s or "SET TAGS" in s for s in steps)

    def test_tag_equals_rule(self):
        violation = {
            "resource_type": "table",
            "resource_name": "finance.ledger",
            "resource_id": "r-2",
        }
        policy = {
            "policy_id": "POL-D-002",
            "rule_json": '{"type": "tag_equals", "key": "classification", "value": "confidential"}',
        }
        tags = {"classification": "internal"}
        steps = _build_remediation_steps(violation, policy, tags)
        assert any("confidential" in s for s in steps)
        assert any("internal" in s for s in steps)  # shows current value

    def test_access_governance_rule(self):
        violation = {
            "resource_type": "endpoint",
            "resource_name": "prod-serving",
            "resource_id": "r-3",
        }
        policy = {
            "policy_id": "POL-A-001",
            "rule_json": None,
            "remediation": "Restrict endpoint access.",
        }
        steps = _build_remediation_steps(violation, policy, tags=None)
        # POL-A* triggers access governance steps
        assert any("SHOW GRANTS" in s for s in steps)

    def test_fallback_to_remediation_text(self):
        violation = {
            "resource_type": "cluster",
            "resource_name": "my-cluster",
            "resource_id": "r-4",
            "remediation": "Contact security team.",
        }
        policy = {"policy_id": "POL-X-001", "rule_json": None}
        steps = _build_remediation_steps(violation, policy, tags=None)
        assert "Contact security team." in steps

    def test_fallback_to_policy_remediation(self):
        violation = {
            "resource_type": "cluster",
            "resource_name": "my-cluster",
            "resource_id": "r-4",
        }
        policy = {
            "policy_id": "POL-X-002",
            "rule_json": None,
            "remediation": "See runbook.",
        }
        steps = _build_remediation_steps(violation, policy, tags=None)
        assert "See runbook." in steps

    def test_ultimate_fallback(self):
        violation = {"resource_type": "table", "resource_name": "t", "resource_id": "r-5"}
        policy = {"policy_id": "POL-Z-001", "rule_json": None}
        steps = _build_remediation_steps(violation, policy, tags=None)
        assert any("platform admin" in s.lower() for s in steps)

    def test_metadata_rule(self):
        violation = {
            "resource_type": "volume",
            "resource_name": "raw_data",
            "resource_id": "r-6",
        }
        policy = {
            "policy_id": "POL-M-001",
            "rule_json": '{"type": "metadata_not_empty", "key": "retention_days"}',
        }
        steps = _build_remediation_steps(violation, policy, tags=None)
        assert any("retention_days" in s for s in steps)


# ── Simulation query builder ─────────────────────────────────────────


class TestBuildFailureCondition:
    """Verify _build_failure_condition produces correct WHERE clauses."""

    def test_tag_exists(self):
        clause = _build_failure_condition("tag_exists", "owner", None)
        assert clause == "tags['owner'] IS NULL"

    def test_tag_equals(self):
        clause = _build_failure_condition("tag_equals", "env", "production")
        assert clause == "COALESCE(tags['env'], '') != 'production'"

    def test_tag_in(self):
        clause = _build_failure_condition("tag_in", "env", "dev,staging,prod")
        assert "NOT IN" in clause
        assert "'dev'" in clause
        assert "'staging'" in clause
        assert "'prod'" in clause

    def test_metadata_equals(self):
        clause = _build_failure_condition("metadata_equals", "format", "delta")
        assert clause == "COALESCE(metadata['format'], '') != 'delta'"

    def test_metadata_not_empty(self):
        clause = _build_failure_condition("metadata_not_empty", "description", None)
        assert clause == "COALESCE(metadata['description'], '') = ''"

    def test_unsupported_rule_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported rule_type"):
            _build_failure_condition("invalid_type", "key", None)
