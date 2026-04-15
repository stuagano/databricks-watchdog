"""Unit tests for drift_check rule type and expected state loading.

Run with: pytest tests/unit/test_drift.py -v
"""
import json
from pathlib import Path

import pytest
from watchdog.rule_engine import RuleEngine


@pytest.fixture(scope="module")
def engine(ontology_dir):
    return RuleEngine(primitives_dir=ontology_dir)


@pytest.fixture(scope="module")
def bare(tmp_path_factory):
    d = tmp_path_factory.mktemp("empty_ontology")
    return RuleEngine(primitives_dir=str(d))


DRIFT_RULE = {"type": "drift_check", "check": "grants"}


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
        rule = {"type": "drift_check", "check": "row_filters"}
        metadata = {"expected_grants": "[]"}
        result = bare.evaluate(rule, {}, metadata)
        assert not result.passed
        assert "row_filters" in result.detail
