"""Unit tests for drift_check rule type and expected state loading.

Run with: pytest tests/unit/test_drift.py -v
"""
import io
import json
import tarfile as _tarfile_mod
from pathlib import Path

import pytest
from watchdog.rule_engine import RuleEngine
from watchdog.drift import (
    load_expected_state,
    build_expected_grants_lookup,
    build_expected_row_filters_lookup,
    build_expected_column_masks_lookup,
    build_expected_group_membership_lookup,
)


@pytest.fixture(scope="module")
def engine(ontology_dir):
    return RuleEngine(primitives_dir=ontology_dir)


@pytest.fixture(scope="module")
def bare(tmp_path_factory):
    d = tmp_path_factory.mktemp("empty_ontology")
    return RuleEngine(primitives_dir=str(d))


DRIFT_RULE = {"type": "drift_check", "check": "grants"}


# ── drift_check rule type ────────────────────────────────────────────────────


class TestDriftCheckPass:
    def test_pass_no_expected_state_in_metadata(self, bare):
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed

    def test_pass_empty_expected_grants(self, bare):
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": "[]",
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed

    def test_pass_actual_matches_expected(self, bare):
        expected = json.dumps([{
            "catalog": "gold", "schema": "finance", "table": None,
            "principal": "finance-analysts",
            "privileges": ["SELECT", "USE_CATALOG", "USE_SCHEMA"],
        }])
        metadata = {
            "securable_type": "schema",
            "securable_full_name": "gold.finance",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed

    def test_pass_no_matching_principal_in_expected(self, bare):
        expected = json.dumps([{
            "catalog": "gold", "schema": "finance", "table": None,
            "principal": "data-engineers",
            "privileges": ["SELECT"],
        }])
        metadata = {
            "securable_type": "schema",
            "securable_full_name": "gold.finance",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed


class TestDriftCheckFail:
    def test_fail_extra_privilege(self, bare):
        expected = json.dumps([{
            "catalog": "gold", "schema": "finance", "table": None,
            "principal": "finance-analysts",
            "privileges": ["SELECT", "USE_CATALOG"],
        }])
        metadata = {
            "securable_type": "schema",
            "securable_full_name": "gold.finance",
            "grantee": "finance-analysts",
            "privilege": "MODIFY",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert not result.passed
        assert "MODIFY" in result.detail
        assert "not in expected state" in result.detail

    def test_fail_detail_includes_resource_info(self, bare):
        expected = json.dumps([{
            "catalog": "gold", "schema": "finance", "table": "gl_balances",
            "principal": "finance-analysts",
            "privileges": ["SELECT"],
        }])
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "MODIFY",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert not result.passed
        assert "finance-analysts" in result.detail
        assert "gold.finance.gl_balances" in result.detail

    def test_fail_malformed_json(self, bare):
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": "not valid json{{{",
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


class TestDriftCheckRuleType:
    def test_rule_type_in_dispatch(self, bare):
        result = bare.evaluate(DRIFT_RULE, {}, {})
        assert result.rule_type == "drift_check"

    def test_unknown_check_type_fails(self, bare):
        rule = {"type": "drift_check", "check": "unsupported_type"}
        metadata = {"expected_grants": "[]"}
        result = bare.evaluate(rule, {}, metadata)
        assert not result.passed
        assert "unsupported_type" in result.detail


# ── Expected state loader ────────────────────────────────────────────────────


class TestLoadExpectedState:
    def test_load_valid_json(self, tmp_path):
        state = {
            "generated_at": "2026-04-14T10:00:00Z",
            "environment": "production",
            "grants": [
                {
                    "catalog": "gold",
                    "schema": "finance",
                    "table": None,
                    "principal": "finance-analysts",
                    "privileges": ["SELECT", "USE_CATALOG"],
                }
            ],
        }
        f = tmp_path / "expected_state.json"
        f.write_text(json.dumps(state))
        result = load_expected_state(str(f))
        assert result == state
        assert len(result["grants"]) == 1

    def test_load_missing_file(self, tmp_path):
        result = load_expected_state(str(tmp_path / "nonexistent.json"))
        assert result == {}

    def test_load_malformed_json(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json {{{")
        result = load_expected_state(str(f))
        assert result == {}

    def test_load_no_grants_section(self, tmp_path):
        state = {"generated_at": "2026-04-14T10:00:00Z", "environment": "prod"}
        f = tmp_path / "expected_state.json"
        f.write_text(json.dumps(state))
        result = load_expected_state(str(f))
        assert "grants" not in result


class TestBuildExpectedGrantsLookup:
    def test_build_lookup_from_grants(self):
        grants = [
            {"catalog": "gold", "schema": "finance", "table": None,
             "principal": "finance-analysts", "privileges": ["SELECT", "USE_CATALOG"]},
            {"catalog": "gold", "schema": "finance", "table": "gl_balances",
             "principal": "data-engineers", "privileges": ["SELECT", "MODIFY"]},
        ]
        lookup = build_expected_grants_lookup(grants)
        assert "finance-analysts" in lookup
        assert "data-engineers" in lookup
        assert len(lookup["finance-analysts"]) == 1
        assert lookup["finance-analysts"][0]["privileges"] == ["SELECT", "USE_CATALOG"]

    def test_build_lookup_empty(self):
        lookup = build_expected_grants_lookup([])
        assert lookup == {}

    def test_build_lookup_multiple_entries_same_principal(self):
        grants = [
            {"catalog": "gold", "schema": "finance", "table": None,
             "principal": "analysts", "privileges": ["SELECT"]},
            {"catalog": "silver", "schema": "raw", "table": None,
             "principal": "analysts", "privileges": ["USE_CATALOG"]},
        ]
        lookup = build_expected_grants_lookup(grants)
        assert len(lookup["analysts"]) == 2

    def test_skips_entry_missing_principal_key(self):
        grants = [
            {"catalog": "gold", "schema": "finance", "table": None,
             "principal": "analysts", "privileges": ["SELECT"]},
            {"catalog": "gold", "schema": "finance", "table": None,
             "privileges": ["SELECT"]},  # missing "principal"
        ]
        lookup = build_expected_grants_lookup(grants)
        assert len(lookup) == 1
        assert "analysts" in lookup


# ── row_filters drift_check ───────────────────────────────────────────────────

ROW_FILTER_RULE = {"type": "drift_check", "check": "row_filters"}


class TestDriftCheckRowFilters:
    def test_pass_no_expected_state(self, bare):
        """No expected_row_filters in metadata — vacuously true."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.my_filter",
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert result.passed

    def test_pass_function_matches_expected(self, bare):
        """Actual filter function matches expected — pass."""
        expected = json.dumps({"table": "gold.finance.gl_balances", "function": "gold.sec.my_filter"})
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.my_filter",
            "expected_row_filters": expected,
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert result.passed

    def test_fail_function_mismatch(self, bare):
        """Actual filter function differs from expected — fail."""
        expected = json.dumps({"table": "gold.finance.gl_balances", "function": "gold.sec.expected_filter"})
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.unauthorized_filter",
            "expected_row_filters": expected,
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert not result.passed
        assert "unauthorized_filter" in result.detail
        assert "gold.finance.gl_balances" in result.detail

    def test_fail_malformed_json(self, bare):
        """Malformed expected_row_filters JSON — fail gracefully."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.my_filter",
            "expected_row_filters": "not-json{{{",
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


# ── column_masks drift_check ──────────────────────────────────────────────────

COLUMN_MASK_RULE = {"type": "drift_check", "check": "column_masks"}


class TestDriftCheckColumnMasks:
    def test_pass_no_expected_state(self, bare):
        """No expected_column_masks in metadata — vacuously true."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.my_mask",
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert result.passed

    def test_pass_function_matches_expected(self, bare):
        """Actual mask function matches expected — pass."""
        expected = json.dumps({
            "table": "gold.finance.gl_balances",
            "column": "cost_center",
            "function": "gold.sec.my_mask",
        })
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.my_mask",
            "expected_column_masks": expected,
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert result.passed

    def test_fail_function_mismatch(self, bare):
        """Actual mask function differs from expected — fail."""
        expected = json.dumps({
            "table": "gold.finance.gl_balances",
            "column": "cost_center",
            "function": "gold.sec.expected_mask",
        })
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.unauthorized_mask",
            "expected_column_masks": expected,
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert not result.passed
        assert "unauthorized_mask" in result.detail
        assert "cost_center" in result.detail

    def test_fail_malformed_json(self, bare):
        """Malformed expected_column_masks JSON — fail gracefully."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.my_mask",
            "expected_column_masks": "not-json{{{",
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


# ── group_membership drift_check ──────────────────────────────────────────────

GROUP_MEMBER_RULE = {"type": "drift_check", "check": "group_membership"}


class TestDriftCheckGroupMembership:
    def test_pass_no_expected_state(self, bare):
        """No expected_group_members in metadata — vacuously true."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert result.passed

    def test_pass_member_in_expected(self, bare):
        """Actual member is in the expected members list — pass."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
            "expected_group_members": json.dumps(["user@company.com", "other@company.com"]),
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert result.passed

    def test_fail_member_not_in_expected(self, bare):
        """Actual member not in expected list — fail with detail."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "unauthorized@company.com",
            "member_type": "user",
            "expected_group_members": json.dumps(["alice@company.com", "bob@company.com"]),
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert not result.passed
        assert "unauthorized@company.com" in result.detail
        assert "finance-analysts" in result.detail

    def test_fail_empty_expected_list(self, bare):
        """Empty expected_group_members list — any actual member is unauthorized."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
            "expected_group_members": json.dumps([]),
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert not result.passed

    def test_fail_malformed_json(self, bare):
        """Malformed expected_group_members JSON — fail gracefully."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
            "expected_group_members": "not-json{{{",
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


# ── OPA bundle loader ─────────────────────────────────────────────────────────


def _make_bundle(data: dict, inner_path: str = "data.json") -> bytes:
    """Build a minimal in-memory .tar.gz bundle with a single JSON file."""
    content = json.dumps(data).encode()
    buf = io.BytesIO()
    with _tarfile_mod.open(fileobj=buf, mode="w:gz") as tf:
        info = _tarfile_mod.TarInfo(name=inner_path)
        info.size = len(content)
        tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


class TestLoadExpectedStateBundleLoader:
    def test_load_from_plain_json(self, tmp_path):
        """Existing JSON behaviour is unchanged."""
        state = {"grants": [{"principal": "alice", "privilege": "SELECT", "resource": "t"}]}
        p = tmp_path / "state.json"
        p.write_text(json.dumps(state))
        result = load_expected_state(str(p))
        assert result["grants"] == state["grants"]

    def test_load_from_tar_gz_bundle(self, tmp_path):
        """Bundle .tar.gz: extracts data.json from root."""
        state = {"grants": [], "row_filters": [{"table": "t", "function": "f"}]}
        bundle_bytes = _make_bundle(state)
        p = tmp_path / "bundle.tar.gz"
        p.write_bytes(bundle_bytes)
        result = load_expected_state(str(p))
        assert result["row_filters"] == state["row_filters"]

    def test_load_from_bundle_with_data_path(self, tmp_path):
        """data_path navigates to a nested key before reading sections."""
        state = {"permissions": {"grants": [], "group_membership": [{"group": "g", "members": ["u@x.com"]}]}}
        bundle_bytes = _make_bundle(state)
        p = tmp_path / "bundle.tar.gz"
        p.write_bytes(bundle_bytes)
        result = load_expected_state(str(p), data_path="permissions")
        assert result["group_membership"][0]["group"] == "g"

    def test_load_from_bundle_missing_data_json(self, tmp_path):
        """Bundle missing data.json returns empty dict (graceful)."""
        state = {"some": "value"}
        bundle_bytes = _make_bundle(state, inner_path="other.json")
        p = tmp_path / "bundle.tar.gz"
        p.write_bytes(bundle_bytes)
        result = load_expected_state(str(p))
        assert result == {}


# ── Lookup builders ───────────────────────────────────────────────────────────


class TestBuildExpectedRowFiltersLookup:
    def test_empty_input(self):
        assert build_expected_row_filters_lookup([]) == {}

    def test_single_entry(self):
        entry = {"table": "gold.finance.gl_balances", "function": "gold.sec.f"}
        result = build_expected_row_filters_lookup([entry])
        assert "gold.finance.gl_balances" in result
        assert result["gold.finance.gl_balances"]["function"] == "gold.sec.f"

    def test_multiple_entries(self):
        entries = [
            {"table": "gold.finance.t1", "function": "gold.sec.f1"},
            {"table": "gold.finance.t2", "function": "gold.sec.f2"},
        ]
        result = build_expected_row_filters_lookup(entries)
        assert len(result) == 2
        assert result["gold.finance.t2"]["function"] == "gold.sec.f2"

    def test_skips_entry_missing_table_key(self):
        entries = [
            {"table": "gold.finance.t1", "function": "gold.sec.f1"},
            {"function": "gold.sec.orphan"},  # missing "table"
        ]
        result = build_expected_row_filters_lookup(entries)
        assert len(result) == 1
        assert "gold.finance.t1" in result


class TestBuildExpectedColumnMasksLookup:
    def test_empty_input(self):
        assert build_expected_column_masks_lookup([]) == {}

    def test_single_entry(self):
        entry = {"table": "gold.finance.gl_balances", "column": "cost_center_owner", "function": "gold.sec.m"}
        result = build_expected_column_masks_lookup([entry])
        key = "gold.finance.gl_balances.cost_center_owner"
        assert key in result
        assert result[key]["function"] == "gold.sec.m"

    def test_multiple_columns_same_table(self):
        entries = [
            {"table": "gold.finance.t1", "column": "col_a", "function": "gold.sec.f1"},
            {"table": "gold.finance.t1", "column": "col_b", "function": "gold.sec.f2"},
        ]
        result = build_expected_column_masks_lookup(entries)
        assert len(result) == 2
        assert "gold.finance.t1.col_b" in result

    def test_skips_entry_missing_column_key(self):
        entries = [
            {"table": "gold.finance.t1", "column": "col_a", "function": "gold.sec.f1"},
            {"table": "gold.finance.t1", "function": "gold.sec.f2"},  # missing "column"
        ]
        result = build_expected_column_masks_lookup(entries)
        assert len(result) == 1
        assert "gold.finance.t1.col_a" in result


class TestBuildExpectedGroupMembershipLookup:
    def test_empty_input(self):
        assert build_expected_group_membership_lookup([]) == {}

    def test_single_group(self):
        entries = [{"group": "finance-analysts", "members": ["alice@co.com", "bob@co.com"]}]
        result = build_expected_group_membership_lookup(entries)
        assert "finance-analysts" in result
        assert "alice@co.com" in result["finance-analysts"]
        assert isinstance(result["finance-analysts"], set)

    def test_multiple_groups(self):
        entries = [
            {"group": "g1", "members": ["a@x.com"]},
            {"group": "g2", "members": ["b@x.com", "c@x.com"]},
        ]
        result = build_expected_group_membership_lookup(entries)
        assert len(result) == 2
        assert len(result["g2"]) == 2

    def test_skips_entry_missing_group_key(self):
        entries = [
            {"group": "g1", "members": ["a@x.com"]},
            {"members": ["b@x.com"]},  # missing "group"
        ]
        result = build_expected_group_membership_lookup(entries)
        assert len(result) == 1
        assert "g1" in result
