"""Unit tests for PolicyEngine drift-check injection helpers.

Tests focus on the two pure helpers extracted from evaluate_all:
  - _build_drift_lookups(expected_state) — builds all four lookup tables
  - _inject_drift_metadata(resource_type, metadata, lookups) — injects per-resource

No Spark or Databricks connection is needed.

Run with: pytest tests/unit/test_policy_engine.py -v
"""
import json
import sys
from unittest.mock import MagicMock

import pytest

# Mock heavyweight runtime dependencies so tests run without pyspark/databricks installed.
_mock_modules = {}
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "databricks", "databricks.sdk",
]:
    _mock_modules[_mod] = MagicMock()

# StructType/StructField stubs used in policy_engine module-level code
_types = _mock_modules["pyspark.sql.types"]
_types.StructType = list
_types.StructField = lambda name, typ, nullable=True: name
_types.StringType = MagicMock
_types.TimestampType = MagicMock

with __import__("unittest.mock", fromlist=["patch"]).patch.dict(sys.modules, _mock_modules):
    from watchdog.policy_engine import PolicyEngine


@pytest.fixture
def engine():
    """Minimal PolicyEngine — Spark and WorkspaceClient are None (not needed)."""
    return PolicyEngine(
        spark=None,  # type: ignore[arg-type]
        w=None,  # type: ignore[arg-type]
        catalog="test_catalog",
        schema="test_schema",
    )


# ── _build_drift_lookups ──────────────────────────────────────────────────────


class TestBuildDriftLookups:
    def test_empty_state_returns_empty_lookups(self, engine):
        lookups = engine._build_drift_lookups({})
        assert lookups["grants"] == {}
        assert lookups["row_filters"] == {}
        assert lookups["column_masks"] == {}
        assert lookups["group_membership"] == {}

    def test_builds_grants_lookup(self, engine):
        state = {
            "grants": [
                {"principal": "alice", "catalog": "gold", "schema": "fin", "table": None, "privileges": ["SELECT"]}
            ]
        }
        lookups = engine._build_drift_lookups(state)
        assert "alice" in lookups["grants"]
        assert lookups["grants"]["alice"][0]["privileges"] == ["SELECT"]

    def test_builds_row_filters_lookup(self, engine):
        state = {
            "row_filters": [
                {"table": "gold.finance.gl_balances", "function": "gold.sec.filter_gl"}
            ]
        }
        lookups = engine._build_drift_lookups(state)
        assert "gold.finance.gl_balances" in lookups["row_filters"]
        assert lookups["row_filters"]["gold.finance.gl_balances"]["function"] == "gold.sec.filter_gl"

    def test_builds_column_masks_lookup(self, engine):
        state = {
            "column_masks": [
                {"table": "gold.finance.gl_balances", "column": "cost_center_owner", "function": "gold.sec.mask_cc"}
            ]
        }
        lookups = engine._build_drift_lookups(state)
        key = "gold.finance.gl_balances.cost_center_owner"
        assert key in lookups["column_masks"]
        assert lookups["column_masks"][key]["function"] == "gold.sec.mask_cc"

    def test_builds_group_membership_lookup(self, engine):
        state = {
            "group_membership": [
                {"group": "finance-analysts", "members": ["alice@co.com", "bob@co.com"]}
            ]
        }
        lookups = engine._build_drift_lookups(state)
        assert "finance-analysts" in lookups["group_membership"]
        assert "alice@co.com" in lookups["group_membership"]["finance-analysts"]

    def test_missing_sections_produce_empty_sublookups(self, engine):
        state = {"grants": [{"principal": "alice", "privileges": ["SELECT"]}]}
        lookups = engine._build_drift_lookups(state)
        assert lookups["row_filters"] == {}
        assert lookups["column_masks"] == {}
        assert lookups["group_membership"] == {}


# ── _inject_drift_metadata ────────────────────────────────────────────────────


class TestInjectDriftMetadata:
    """Tests for the per-resource metadata injection helper."""

    # ── grants ────────────────────────────────────────────────────────────────

    def test_grant_injects_expected_grants(self, engine):
        lookups = {
            "grants": {"alice": [{"principal": "alice", "privileges": ["SELECT"]}]},
            "row_filters": {},
            "column_masks": {},
            "group_membership": {},
        }
        metadata = {"grantee": "alice", "privilege": "SELECT"}
        result = engine._inject_drift_metadata("grant", metadata, lookups)
        assert "expected_grants" in result
        parsed = json.loads(result["expected_grants"])
        assert parsed[0]["principal"] == "alice"

    def test_grant_no_injection_when_grantee_not_in_lookup(self, engine):
        lookups = {
            "grants": {"bob": [{"principal": "bob", "privileges": ["SELECT"]}]},
            "row_filters": {},
            "column_masks": {},
            "group_membership": {},
        }
        metadata = {"grantee": "alice", "privilege": "SELECT"}
        result = engine._inject_drift_metadata("grant", metadata, lookups)
        assert "expected_grants" not in result

    def test_grant_no_injection_when_lookup_empty(self, engine):
        lookups = {"grants": {}, "row_filters": {}, "column_masks": {}, "group_membership": {}}
        metadata = {"grantee": "alice", "privilege": "SELECT"}
        result = engine._inject_drift_metadata("grant", metadata, lookups)
        assert "expected_grants" not in result

    # ── row_filter ────────────────────────────────────────────────────────────

    def test_row_filter_injects_expected(self, engine):
        lookups = {
            "grants": {},
            "row_filters": {"gold.finance.gl_balances": {"table": "gold.finance.gl_balances", "function": "gold.sec.f"}},
            "column_masks": {},
            "group_membership": {},
        }
        metadata = {"table_full_name": "gold.finance.gl_balances", "filter_function": "gold.sec.f"}
        result = engine._inject_drift_metadata("row_filter", metadata, lookups)
        assert "expected_row_filters" in result
        parsed = json.loads(result["expected_row_filters"])
        assert parsed["function"] == "gold.sec.f"

    def test_row_filter_no_injection_when_table_not_in_lookup(self, engine):
        lookups = {
            "grants": {},
            "row_filters": {"gold.finance.other_table": {"table": "gold.finance.other_table", "function": "gold.sec.f"}},
            "column_masks": {},
            "group_membership": {},
        }
        metadata = {"table_full_name": "gold.finance.gl_balances", "filter_function": "gold.sec.f"}
        result = engine._inject_drift_metadata("row_filter", metadata, lookups)
        assert "expected_row_filters" not in result

    def test_row_filter_does_not_mutate_input_metadata(self, engine):
        lookups = {
            "grants": {},
            "row_filters": {"gold.finance.gl_balances": {"table": "gold.finance.gl_balances", "function": "gold.sec.f"}},
            "column_masks": {},
            "group_membership": {},
        }
        metadata = {"table_full_name": "gold.finance.gl_balances", "filter_function": "gold.sec.f"}
        original_keys = set(metadata.keys())
        engine._inject_drift_metadata("row_filter", metadata, lookups)
        assert set(metadata.keys()) == original_keys

    # ── column_mask ───────────────────────────────────────────────────────────

    def test_column_mask_injects_expected(self, engine):
        lookups = {
            "grants": {},
            "row_filters": {},
            "column_masks": {
                "gold.finance.gl_balances.cost_center_owner": {
                    "table": "gold.finance.gl_balances",
                    "column": "cost_center_owner",
                    "function": "gold.sec.mask_cc",
                }
            },
            "group_membership": {},
        }
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center_owner",
            "mask_function": "gold.sec.mask_cc",
        }
        result = engine._inject_drift_metadata("column_mask", metadata, lookups)
        assert "expected_column_masks" in result
        parsed = json.loads(result["expected_column_masks"])
        assert parsed["function"] == "gold.sec.mask_cc"

    def test_column_mask_key_is_table_dot_column(self, engine):
        """Key must be '{table}.{column}' — wrong table or column misses the lookup."""
        lookups = {
            "grants": {},
            "row_filters": {},
            "column_masks": {
                "gold.finance.gl_balances.other_col": {
                    "table": "gold.finance.gl_balances",
                    "column": "other_col",
                    "function": "gold.sec.mask_other",
                }
            },
            "group_membership": {},
        }
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center_owner",
            "mask_function": "gold.sec.mask_cc",
        }
        result = engine._inject_drift_metadata("column_mask", metadata, lookups)
        assert "expected_column_masks" not in result

    # ── group_member ──────────────────────────────────────────────────────────

    def test_group_member_injects_expected_members(self, engine):
        lookups = {
            "grants": {},
            "row_filters": {},
            "column_masks": {},
            "group_membership": {"finance-analysts": {"alice@co.com", "bob@co.com"}},
        }
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "alice@co.com",
            "member_type": "user",
        }
        result = engine._inject_drift_metadata("group_member", metadata, lookups)
        assert "expected_group_members" in result
        members = json.loads(result["expected_group_members"])
        assert isinstance(members, list)
        assert "alice@co.com" in members

    def test_group_member_injected_as_json_list_not_set(self, engine):
        """Sets are not JSON-serializable — must be converted to list."""
        lookups = {
            "grants": {},
            "row_filters": {},
            "column_masks": {},
            "group_membership": {"g1": {"a@x.com"}},
        }
        metadata = {"group_name": "g1", "member_value": "a@x.com", "member_type": "user"}
        result = engine._inject_drift_metadata("group_member", metadata, lookups)
        # Must not raise — json.loads must succeed
        parsed = json.loads(result["expected_group_members"])
        assert isinstance(parsed, list)

    def test_group_member_no_injection_when_group_not_in_lookup(self, engine):
        lookups = {
            "grants": {},
            "row_filters": {},
            "column_masks": {},
            "group_membership": {"other-group": {"alice@co.com"}},
        }
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "alice@co.com",
            "member_type": "user",
        }
        result = engine._inject_drift_metadata("group_member", metadata, lookups)
        assert "expected_group_members" not in result

    # ── unknown resource type — no injection ──────────────────────────────────

    def test_unknown_resource_type_returns_metadata_unchanged(self, engine):
        lookups = {
            "grants": {"alice": [{"principal": "alice", "privileges": ["SELECT"]}]},
            "row_filters": {"t": {"table": "t", "function": "f"}},
            "column_masks": {},
            "group_membership": {},
        }
        metadata = {"key": "value"}
        result = engine._inject_drift_metadata("table", metadata, lookups)
        assert result == {"key": "value"}


# ── TestDriftCheckPolicyEngineV2 — integration via helpers ────────────────────


class TestDriftCheckPolicyEngineV2:
    """End-to-end tests: _build_drift_lookups + _inject_drift_metadata → RuleEngine.

    Validates that the injected metadata is accepted by the RuleEngine and
    produces correct pass/fail results — without touching Spark.
    """

    @pytest.fixture
    def rule_engine(self, tmp_path):
        from watchdog.rule_engine import RuleEngine
        return RuleEngine(primitives_dir=str(tmp_path))

    def _evaluate(self, engine, rule_engine, expected_state, resource_type, metadata, rule):
        """Build lookups, inject metadata, then call rule_engine.evaluate."""
        lookups = engine._build_drift_lookups(expected_state)
        enriched = engine._inject_drift_metadata(resource_type, metadata, lookups)
        return rule_engine.evaluate(rule, {}, enriched)

    def test_row_filter_expected_injected_and_passes(self, engine, rule_engine):
        """Expected row filter matches actual — should pass."""
        expected_state = {
            "row_filters": [
                {"table": "gold.finance.gl_balances", "function": "gold.sec.filter_gl"}
            ]
        }
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.filter_gl",
        }
        rule = {"type": "drift_check", "check": "row_filters"}
        result = self._evaluate(engine, rule_engine, expected_state, "row_filter", metadata, rule)
        assert result.passed

    def test_row_filter_mismatch_fails(self, engine, rule_engine):
        """Expected row filter differs from actual — should fail."""
        expected_state = {
            "row_filters": [
                {"table": "gold.finance.gl_balances", "function": "gold.sec.expected_filter"}
            ]
        }
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.unauthorized_filter",
        }
        rule = {"type": "drift_check", "check": "row_filters"}
        result = self._evaluate(engine, rule_engine, expected_state, "row_filter", metadata, rule)
        assert not result.passed
        assert "unauthorized_filter" in result.detail

    def test_column_mask_expected_injected_and_passes(self, engine, rule_engine):
        """Expected column mask matches actual — should pass."""
        expected_state = {
            "column_masks": [
                {"table": "gold.finance.gl_balances", "column": "cost_center_owner", "function": "gold.sec.mask_cc"}
            ]
        }
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center_owner",
            "mask_function": "gold.sec.mask_cc",
        }
        rule = {"type": "drift_check", "check": "column_masks"}
        result = self._evaluate(engine, rule_engine, expected_state, "column_mask", metadata, rule)
        assert result.passed

    def test_column_mask_mismatch_fails(self, engine, rule_engine):
        """Expected column mask differs from actual — should fail."""
        expected_state = {
            "column_masks": [
                {"table": "gold.finance.gl_balances", "column": "cost_center_owner", "function": "gold.sec.expected_mask"}
            ]
        }
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center_owner",
            "mask_function": "gold.sec.unauthorized_mask",
        }
        rule = {"type": "drift_check", "check": "column_masks"}
        result = self._evaluate(engine, rule_engine, expected_state, "column_mask", metadata, rule)
        assert not result.passed
        assert "unauthorized_mask" in result.detail

    def test_group_member_expected_injected_and_passes(self, engine, rule_engine):
        """Expected group member is in expected list — should pass."""
        expected_state = {
            "group_membership": [
                {"group": "finance-analysts", "members": ["alice@co.com", "bob@co.com"]}
            ]
        }
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "alice@co.com",
            "member_type": "user",
        }
        rule = {"type": "drift_check", "check": "group_membership"}
        result = self._evaluate(engine, rule_engine, expected_state, "group_member", metadata, rule)
        assert result.passed

    def test_group_member_not_in_expected_fails(self, engine, rule_engine):
        """Member not in expected list — should fail."""
        expected_state = {
            "group_membership": [
                {"group": "finance-analysts", "members": ["alice@co.com"]}
            ]
        }
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "unauthorized@co.com",
            "member_type": "user",
        }
        rule = {"type": "drift_check", "check": "group_membership"}
        result = self._evaluate(engine, rule_engine, expected_state, "group_member", metadata, rule)
        assert not result.passed
        assert "unauthorized@co.com" in result.detail

    def test_data_path_navigates_to_nested_key(self, engine, rule_engine, tmp_path):
        """data_path in the rule navigates into nested expected state."""
        import json as _json
        from watchdog.drift import load_expected_state

        expected_state_file = {
            "permissions": {
                "row_filters": [
                    {"table": "gold.finance.t1", "function": "gold.sec.f"}
                ]
            }
        }
        f = tmp_path / "expected_state.json"
        f.write_text(_json.dumps(expected_state_file))

        # Simulate what evaluate_all does: load with data_path, build lookups, inject
        state = load_expected_state(str(f), data_path="permissions")
        lookups = engine._build_drift_lookups(state)

        metadata = {
            "table_full_name": "gold.finance.t1",
            "filter_function": "gold.sec.f",
        }
        enriched = engine._inject_drift_metadata("row_filter", metadata, lookups)
        rule = {"type": "drift_check", "check": "row_filters"}
        result = rule_engine.evaluate(rule, {}, enriched)
        assert result.passed

    def test_data_path_missing_key_returns_empty_lookups(self, engine, rule_engine, tmp_path):
        """Missing data_path key → empty lookups → vacuous pass (no expected state)."""
        import json as _json
        from watchdog.drift import load_expected_state

        expected_state_file = {"other_section": {"row_filters": []}}
        f = tmp_path / "expected_state.json"
        f.write_text(_json.dumps(expected_state_file))

        state = load_expected_state(str(f), data_path="permissions")  # key absent
        lookups = engine._build_drift_lookups(state)

        metadata = {
            "table_full_name": "gold.finance.t1",
            "filter_function": "gold.sec.f",
        }
        enriched = engine._inject_drift_metadata("row_filter", metadata, lookups)
        # No expected_row_filters injected → vacuous pass
        assert "expected_row_filters" not in enriched
        rule = {"type": "drift_check", "check": "row_filters"}
        result = rule_engine.evaluate(rule, {}, enriched)
        assert result.passed
