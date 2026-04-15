"""Unit tests for RuleEngine — declarative rule evaluation.

Tests are organized by rule type. Each group covers both the pass and fail
path, plus any non-obvious edge cases documented in the engine's docstrings.

Run with: pytest tests/unit/test_rule_engine.py -v
"""
from pathlib import Path

import pytest
from watchdog.rule_engine import RuleEngine


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def engine(ontology_dir):
    """RuleEngine loaded with the live rule_primitives.yml."""
    return RuleEngine(primitives_dir=ontology_dir)


@pytest.fixture(scope="module")
def bare(tmp_path_factory):
    """RuleEngine with no primitives — for testing inline rules only."""
    d = tmp_path_factory.mktemp("empty_ontology")
    return RuleEngine(primitives_dir=str(d))


# ── tag_exists ────────────────────────────────────────────────────────────────

class TestTagExists:
    RULE = {"type": "tag_exists", "keys": ["owner", "env"]}

    def test_pass_all_keys_present(self, bare):
        result = bare.evaluate(self.RULE, {"owner": "alice", "env": "prod"}, {})
        assert result.passed

    def test_fail_one_key_missing(self, bare):
        result = bare.evaluate(self.RULE, {"owner": "alice"}, {})
        assert not result.passed
        assert "env" in result.detail

    def test_fail_all_keys_missing(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed
        assert "owner" in result.detail
        assert "env" in result.detail


# ── tag_equals ────────────────────────────────────────────────────────────────

class TestTagEquals:
    RULE = {"type": "tag_equals", "key": "environment", "value": "prod"}

    def test_pass_exact_match(self, bare):
        result = bare.evaluate(self.RULE, {"environment": "prod"}, {})
        assert result.passed

    def test_fail_wrong_value(self, bare):
        result = bare.evaluate(self.RULE, {"environment": "dev"}, {})
        assert not result.passed
        assert "'dev'" in result.detail
        assert "'prod'" in result.detail

    def test_fail_key_absent(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed


# ── tag_in ────────────────────────────────────────────────────────────────────

class TestTagIn:
    RULE = {"type": "tag_in", "key": "environment",
            "allowed": ["dev", "test", "staging", "prod"]}

    def test_pass_value_in_allowed_set(self, bare):
        for val in ("dev", "prod", "staging"):
            result = bare.evaluate(self.RULE, {"environment": val}, {})
            assert result.passed, f"Expected pass for environment={val}"

    def test_fail_value_not_in_set(self, bare):
        result = bare.evaluate(self.RULE, {"environment": "production"}, {})
        assert not result.passed
        assert "'production'" in result.detail

    def test_fail_key_absent(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed


# ── tag_not_in ────────────────────────────────────────────────────────────────

class TestTagNotIn:
    RULE = {"type": "tag_not_in", "key": "status", "disallowed": ["deprecated", "deleted"]}

    def test_pass_value_not_in_disallowed(self, bare):
        result = bare.evaluate(self.RULE, {"status": "active"}, {})
        assert result.passed

    def test_fail_value_in_disallowed(self, bare):
        result = bare.evaluate(self.RULE, {"status": "deprecated"}, {})
        assert not result.passed
        assert "'deprecated'" in result.detail


# ── tag_matches ───────────────────────────────────────────────────────────────

class TestTagMatches:
    RULE = {"type": "tag_matches", "key": "owner", "pattern": r"@example\.com$"}

    def test_pass_pattern_matches(self, bare):
        result = bare.evaluate(self.RULE, {"owner": "alice@example.com"}, {})
        assert result.passed

    def test_fail_pattern_not_matching(self, bare):
        result = bare.evaluate(self.RULE, {"owner": "alice@other.com"}, {})
        assert not result.passed
        assert "pattern" in result.detail or "alice@other.com" in result.detail

    def test_fail_key_absent(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed


# ── metadata_equals ───────────────────────────────────────────────────────────

class TestMetadataEquals:
    RULE = {"type": "metadata_equals", "field": "region", "value": "eastus2"}

    def test_pass_field_matches(self, bare):
        result = bare.evaluate(self.RULE, {}, {"region": "eastus2"})
        assert result.passed

    def test_fail_wrong_value(self, bare):
        result = bare.evaluate(self.RULE, {}, {"region": "westus"})
        assert not result.passed

    def test_fail_field_absent(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed


# ── metadata_not_empty ────────────────────────────────────────────────────────

class TestMetadataNotEmpty:
    def test_pass_field_has_value(self, bare):
        rule = {"type": "metadata_not_empty", "field": "comment"}
        result = bare.evaluate(rule, {}, {"comment": "Sales orders for Q1"})
        assert result.passed

    def test_fail_field_empty_string(self, bare):
        rule = {"type": "metadata_not_empty", "field": "comment"}
        result = bare.evaluate(rule, {}, {"comment": ""})
        assert not result.passed

    def test_fail_field_absent(self, bare):
        rule = {"type": "metadata_not_empty", "field": "comment"}
        result = bare.evaluate(rule, {}, {})
        assert not result.passed

    def test_owner_dual_source_metadata_wins(self, bare):
        """owner field: metadata takes priority over tags."""
        rule = {"type": "metadata_not_empty", "field": "owner"}
        result = bare.evaluate(rule, {}, {"owner": "alice@example.com"})
        assert result.passed

    def test_owner_dual_source_tag_fallback(self, bare):
        """owner field: falls back to tags when metadata.owner is absent."""
        rule = {"type": "metadata_not_empty", "field": "owner"}
        result = bare.evaluate(rule, {"owner": "alice@example.com"}, {})
        assert result.passed

    def test_owner_fails_when_both_absent(self, bare):
        rule = {"type": "metadata_not_empty", "field": "owner"}
        result = bare.evaluate(rule, {}, {})
        assert not result.passed


# ── metadata_gte ──────────────────────────────────────────────────────────────

class TestMetadataGte:
    """Verify version-aware comparison for Databricks runtime version policies."""

    RULE = {"type": "metadata_gte", "field": "spark_version", "threshold": "15.4"}

    def test_pass_exact_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"spark_version": "15.4.x-scala2.12"})
        assert result.passed

    def test_pass_higher_version(self, bare):
        result = bare.evaluate(self.RULE, {}, {"spark_version": "16.0.x-scala2.12"})
        assert result.passed

    def test_fail_lower_version(self, bare):
        result = bare.evaluate(self.RULE, {}, {"spark_version": "10.4.x-scala2.12"})
        assert not result.passed
        assert "10.4" in result.detail

    def test_fail_lts_below_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"spark_version": "13.3.x-scala2.12"})
        assert not result.passed

    def test_fail_empty_field(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed


# ── metadata_lte ─────────────────────────────────────────────────────────────

class TestMetadataLte:
    """metadata_lte: fail if metadata field exceeds threshold."""

    RULE = {"type": "metadata_lte", "field": "freshness_hours", "threshold": "48"}

    def test_pass_below_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "12"})
        assert result.passed

    def test_pass_exact_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "48"})
        assert result.passed

    def test_fail_above_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "72"})
        assert not result.passed
        assert "72" in result.detail

    def test_fail_empty_field(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed

    def test_fail_non_numeric(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "unknown"})
        assert not result.passed


# ── has_owner ─────────────────────────────────────────────────────────────────

class TestHasOwner:
    """has_owner is a composite shorthand: checks metadata.owner OR tags.owner."""

    RULE = {"type": "has_owner"}

    def test_pass_owner_in_metadata(self, bare):
        result = bare.evaluate(self.RULE, {}, {"owner": "alice@example.com"})
        assert result.passed

    def test_pass_owner_in_tags(self, bare):
        result = bare.evaluate(self.RULE, {"owner": "alice@example.com"}, {})
        assert result.passed

    def test_fail_no_owner_anywhere(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed
        assert "owner" in result.detail.lower()


# ── Composite: all_of ─────────────────────────────────────────────────────────

class TestAllOf:
    def test_pass_all_rules_pass(self, bare):
        rule = {"type": "all_of", "rules": [
            {"type": "tag_exists", "keys": ["owner"]},
            {"type": "tag_exists", "keys": ["env"]},
        ]}
        result = bare.evaluate(rule, {"owner": "alice", "env": "prod"}, {})
        assert result.passed

    def test_fail_one_rule_fails(self, bare):
        rule = {"type": "all_of", "rules": [
            {"type": "tag_exists", "keys": ["owner"]},
            {"type": "tag_exists", "keys": ["env"]},
        ]}
        result = bare.evaluate(rule, {"owner": "alice"}, {})
        assert not result.passed
        assert "env" in result.detail

    def test_collects_all_failures_no_short_circuit(self, bare):
        """all_of evaluates EVERY sub-rule and reports all failures."""
        rule = {"type": "all_of", "rules": [
            {"type": "tag_exists", "keys": ["owner"]},
            {"type": "tag_exists", "keys": ["env"]},
            {"type": "tag_exists", "keys": ["cost_center"]},
        ]}
        result = bare.evaluate(rule, {}, {})
        assert not result.passed
        # All three missing keys should appear in the combined detail
        assert "owner" in result.detail
        assert "env" in result.detail
        assert "cost_center" in result.detail


# ── Composite: any_of ─────────────────────────────────────────────────────────

class TestAnyOf:
    def test_pass_first_rule_passes(self, bare):
        rule = {"type": "any_of", "rules": [
            {"type": "tag_exists", "keys": ["owner"]},
            {"type": "tag_exists", "keys": ["env"]},
        ]}
        result = bare.evaluate(rule, {"owner": "alice"}, {})
        assert result.passed

    def test_pass_second_rule_passes(self, bare):
        rule = {"type": "any_of", "rules": [
            {"type": "tag_exists", "keys": ["owner"]},
            {"type": "tag_exists", "keys": ["env"]},
        ]}
        result = bare.evaluate(rule, {"env": "prod"}, {})
        assert result.passed

    def test_fail_no_rules_pass(self, bare):
        rule = {"type": "any_of", "rules": [
            {"type": "tag_exists", "keys": ["owner"]},
            {"type": "tag_exists", "keys": ["env"]},
        ]}
        result = bare.evaluate(rule, {}, {})
        assert not result.passed
        assert "None of the alternatives" in result.detail


# ── Composite: none_of ────────────────────────────────────────────────────────

class TestNoneOf:
    def test_pass_nothing_matches(self, bare):
        """none_of passes when no sub-rule matches — resource is not prohibited."""
        rule = {"type": "none_of", "rules": [
            {"type": "tag_equals", "key": "status", "value": "deprecated"},
        ]}
        result = bare.evaluate(rule, {"status": "active"}, {})
        assert result.passed

    def test_fail_one_matches(self, bare):
        rule = {"type": "none_of", "rules": [
            {"type": "tag_equals", "key": "status", "value": "deprecated"},
        ]}
        result = bare.evaluate(rule, {"status": "deprecated"}, {})
        assert not result.passed


# ── Conditional: if_then ──────────────────────────────────────────────────────

class TestIfThen:
    def test_vacuously_true_when_condition_not_met(self, bare):
        """The rule is silent — vacuously true — when condition doesn't match.

        Example: 'if env=prod then require owner' — non-prod resources are
        not checked for owner at all, rather than failing.
        """
        rule = {
            "type": "if_then",
            "condition": {"type": "tag_equals", "key": "environment", "value": "prod"},
            "then": {"type": "tag_exists", "keys": ["owner"]},
        }
        # dev resource, no owner — condition doesn't match → vacuously true
        result = bare.evaluate(rule, {"environment": "dev"}, {})
        assert result.passed

    def test_evaluates_then_when_condition_matches(self, bare):
        """When condition matches, the then-clause is enforced."""
        rule = {
            "type": "if_then",
            "condition": {"type": "tag_equals", "key": "environment", "value": "prod"},
            "then": {"type": "tag_exists", "keys": ["owner"]},
        }
        # prod resource with owner — passes
        result = bare.evaluate(rule, {"environment": "prod", "owner": "alice"}, {})
        assert result.passed

    def test_fail_when_condition_matches_then_fails(self, bare):
        rule = {
            "type": "if_then",
            "condition": {"type": "tag_equals", "key": "environment", "value": "prod"},
            "then": {"type": "tag_exists", "keys": ["owner"]},
        }
        # prod resource, no owner — condition matches, then fails
        result = bare.evaluate(rule, {"environment": "prod"}, {})
        assert not result.passed
        assert "owner" in result.detail


# ── Ref to named primitives ───────────────────────────────────────────────────

class TestPrimitiveRefs:
    """These tests use the live rule_primitives.yml via the engine fixture."""

    def test_has_owner_via_ref(self, engine):
        rule = {"ref": "has_owner"}
        assert engine.evaluate(rule, {}, {"owner": "alice@example.com"}).passed
        assert not engine.evaluate(rule, {}, {}).passed

    def test_runtime_current_via_ref(self, engine):
        rule = {"ref": "runtime_current"}
        assert engine.evaluate(rule, {}, {"spark_version": "15.4.x-scala2.12"}).passed
        assert engine.evaluate(rule, {}, {"spark_version": "16.0.x-scala2.12"}).passed
        assert not engine.evaluate(rule, {}, {"spark_version": "10.4.x-scala2.12"}).passed
        assert not engine.evaluate(rule, {}, {"spark_version": "13.3.x-scala2.12"}).passed

    def test_pii_has_steward_condition_not_met(self, engine):
        """pii_has_steward is vacuously true for non-PII resources."""
        rule = {"ref": "pii_has_steward"}
        # data_classification=internal, no steward — condition doesn't fire
        result = engine.evaluate(rule, {"data_classification": "internal"}, {})
        assert result.passed

    def test_pii_has_steward_condition_met_then_fails(self, engine):
        """pii_has_steward fires when data_classification=pii and steward is absent."""
        rule = {"ref": "pii_has_steward"}
        result = engine.evaluate(rule, {"data_classification": "pii"}, {})
        assert not result.passed

    def test_pii_has_steward_full_pass(self, engine):
        rule = {"ref": "pii_has_steward"}
        tags = {
            "data_classification": "pii",
            "data_steward": "dpo@example.com",
            "retention_days": "365",
        }
        result = engine.evaluate(rule, tags, {})
        assert result.passed

    def test_unknown_ref_fails_gracefully(self, engine):
        rule = {"ref": "nonexistent_primitive"}
        result = engine.evaluate(rule, {}, {})
        assert not result.passed
        assert "Unknown rule primitive" in result.detail

    def test_pii_has_steward(self, engine):
        """Condition: data_classification=pii. Then: steward + retention."""
        rule = {"ref": "pii_has_steward"}
        # PII with no steward → fails
        result = engine.evaluate(rule, {"data_classification": "pii"}, {})
        assert not result.passed
        # PII with all required tags → passes
        tags = {
            "data_classification": "pii",
            "data_steward": "privacy@example.com",
            "retention_days": "365",
        }
        result = engine.evaluate(rule, tags, {})
        assert result.passed
        # Non-PII → vacuously true (condition doesn't match)
        result = engine.evaluate(rule, {"data_classification": "internal"}, {})
        assert result.passed


# ── Access governance primitives ─────────────────────────────────────────────

class TestAccessGovernancePrimitives:
    """Tests for grant-related rule primitives added in Sprint 1-2."""

    def test_no_all_privileges_pass(self, engine):
        """no_all_privileges passes when privilege is not ALL PRIVILEGES."""
        rule = {"ref": "no_all_privileges"}
        result = engine.evaluate(rule, {}, {"privilege": "SELECT"})
        assert result.passed

    def test_no_all_privileges_fail(self, engine):
        """no_all_privileges fails when privilege is ALL PRIVILEGES."""
        rule = {"ref": "no_all_privileges"}
        result = engine.evaluate(rule, {}, {"privilege": "ALL PRIVILEGES"})
        assert not result.passed

    def test_grant_uses_groups_pass(self, engine):
        """grant_uses_groups passes for group grantee."""
        rule = {"ref": "grant_uses_groups"}
        result = engine.evaluate(rule, {}, {"grantee": "group:data_engineers"})
        assert result.passed

    def test_grant_uses_groups_account_group_pass(self, engine):
        """grant_uses_groups passes for account group grantee."""
        rule = {"ref": "grant_uses_groups"}
        result = engine.evaluate(rule, {}, {"grantee": "account group:admins"})
        assert result.passed

    def test_grant_uses_groups_fail(self, engine):
        """grant_uses_groups fails for direct user grantee."""
        rule = {"ref": "grant_uses_groups"}
        result = engine.evaluate(rule, {}, {"grantee": "alice@example.com"})
        assert not result.passed

    def test_sp_not_workspace_admin_pass(self, engine):
        """sp_not_workspace_admin passes when entitlements don't include workspace-admin."""
        rule = {"ref": "sp_not_workspace_admin"}
        result = engine.evaluate(rule, {}, {"entitlements": "cluster-create"})
        assert result.passed

    def test_sp_not_workspace_admin_fail(self, engine):
        """sp_not_workspace_admin fails when entitlements include workspace-admin."""
        rule = {"ref": "sp_not_workspace_admin"}
        result = engine.evaluate(rule, {}, {"entitlements": "workspace-admin"})
        assert not result.passed

    def test_group_has_multiple_members_pass(self, engine):
        """group_has_multiple_members passes when member_count >= 2."""
        rule = {"ref": "group_has_multiple_members"}
        result = engine.evaluate(rule, {}, {"member_count": "3"})
        assert result.passed

    def test_group_has_multiple_members_fail(self, engine):
        """group_has_multiple_members fails when member_count < 2."""
        rule = {"ref": "group_has_multiple_members"}
        result = engine.evaluate(rule, {}, {"member_count": "1"})
        assert not result.passed


# ── Policy loading ───────────────────────────────────────────────────────────

class TestPolicyLoading:
    """Verify that all policy YAML files load without errors.

    Uses raw YAML parsing to avoid pyspark dependency in unit tests.
    """

    @staticmethod
    def _load_policies_yaml(policies_dir):
        """Load all policies from YAML files without pyspark dependency."""
        import yaml
        policies = []
        for yaml_file in sorted(Path(policies_dir).glob("*.yml")):
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
            if data and "policies" in data:
                for p in data["policies"]:
                    policies.append(p)
        return policies

    def test_access_governance_policies_load(self, ontology_dir):
        """access_governance.yml loads and contains all expected policy IDs."""
        policies_dir = str(Path(ontology_dir).parent / "policies")
        policies = self._load_policies_yaml(policies_dir)
        access_ids = [p["id"] for p in policies if p["id"].startswith("POL-A")]
        assert "POL-A001" in access_ids
        assert "POL-A002" in access_ids
        assert "POL-A003" in access_ids
        assert "POL-A004" in access_ids

    def test_access_governance_policies_have_required_fields(self, ontology_dir):
        """Every access governance policy has all required fields populated."""
        policies_dir = str(Path(ontology_dir).parent / "policies")
        policies = self._load_policies_yaml(policies_dir)
        required_keys = ["id", "name", "applies_to", "domain", "severity",
                         "description", "remediation", "rule"]
        for p in policies:
            if not p["id"].startswith("POL-A"):
                continue
            for key in required_keys:
                assert key in p and p[key], f"{p['id']} missing or empty: {key}"
            assert p.get("active") is True, f"{p['id']} should be active"

    def test_all_policy_files_load_without_errors(self, ontology_dir):
        """Smoke test: all YAML policy files load without exceptions."""
        policies_dir = str(Path(ontology_dir).parent / "policies")
        policies = self._load_policies_yaml(policies_dir)
        assert len(policies) > 0, "No policies loaded"
