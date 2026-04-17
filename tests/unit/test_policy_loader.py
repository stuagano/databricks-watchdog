"""Unit tests for watchdog.policy_loader — YAML parsing & metadata extraction.

Spark-dependent paths (load_delta_policies, sync_policies_to_delta) are
exercised by integration tests. These unit tests cover the pure-Python
file-system functions.
"""

import textwrap
from pathlib import Path

import pytest
from watchdog.policy_loader import load_policies_metadata, load_yaml_policies


@pytest.fixture
def policies_root(tmp_path):
    """Temporary policies/ directory the loader can read from."""
    root = tmp_path / "policies"
    root.mkdir()
    return root


def _write(policies_root: Path, name: str, body: str) -> None:
    (policies_root / name).write_text(textwrap.dedent(body))


class TestLoadYamlPolicies:
    def test_returns_empty_for_missing_dir(self, tmp_path):
        # Non-existent dir must return [] — never raise.
        assert load_yaml_policies(str(tmp_path / "nope")) == []

    def test_parses_minimal_policy(self, policies_root):
        _write(policies_root, "example.yml", """
            policies:
              - id: POL-UNIT-001
                name: "Example"
                applies_to: "*"
                domain: "Test"
                severity: "medium"
                description: "desc"
                remediation: "fix"
                rule:
                  ref: has_owner
        """)
        policies = load_yaml_policies(str(policies_root))
        assert len(policies) == 1
        p = policies[0]
        assert p.policy_id == "POL-UNIT-001"
        assert p.applies_to == "*"
        assert p.rule == {"ref": "has_owner"}

    def test_shorthand_rule_string_becomes_ref(self, policies_root):
        _write(policies_root, "shorthand.yml", """
            policies:
              - id: POL-S1
                name: "S"
                applies_to: "*"
                rule: has_owner
        """)
        policies = load_yaml_policies(str(policies_root))
        assert policies[0].rule == {"ref": "has_owner"}

    def test_legacy_resource_types_all_maps_to_star(self, policies_root):
        _write(policies_root, "legacy.yml", """
            policies:
              - id: POL-L1
                name: "Legacy any"
                resource_types: ["*"]
                rule:
                  ref: has_owner
        """)
        policies = load_yaml_policies(str(policies_root))
        assert policies[0].applies_to == "*"

    def test_legacy_resource_types_table_maps_to_data_asset(self, policies_root):
        _write(policies_root, "legacy_table.yml", """
            policies:
              - id: POL-L2
                name: "Legacy table"
                resource_types: ["table"]
                rule:
                  ref: has_owner
        """)
        assert load_yaml_policies(str(policies_root))[0].applies_to == "DataAsset"

    def test_legacy_resource_types_compute_set_maps_to_compute_asset(self, policies_root):
        _write(policies_root, "legacy_compute.yml", """
            policies:
              - id: POL-L3
                name: "Legacy compute"
                resource_types: ["cluster", "warehouse", "job"]
                rule:
                  ref: has_owner
        """)
        assert load_yaml_policies(str(policies_root))[0].applies_to == "ComputeAsset"

    def test_skips_policy_without_rule(self, policies_root):
        _write(policies_root, "ruleless.yml", """
            policies:
              - id: POL-X
                name: "No rule"
                applies_to: "*"
        """)
        assert load_yaml_policies(str(policies_root)) == []

    def test_skips_starter_format_without_applies_to_and_resource_types(self, policies_root):
        # A policy with neither applies_to nor resource_types defaults applies_to="*"
        # and is still loaded — this asserts that path behaves predictably.
        _write(policies_root, "starter.yml", """
            policies:
              - id: POL-STARTER
                name: "Starter"
                rule:
                  ref: has_owner
        """)
        assert load_yaml_policies(str(policies_root))[0].applies_to == "*"

    def test_ignores_empty_file(self, policies_root):
        _write(policies_root, "empty.yml", "")
        assert load_yaml_policies(str(policies_root)) == []

    def test_file_without_policies_key_ignored(self, policies_root):
        _write(policies_root, "other.yml", "foo: bar\n")
        assert load_yaml_policies(str(policies_root)) == []

    def test_active_defaults_true(self, policies_root):
        _write(policies_root, "defaults.yml", """
            policies:
              - id: POL-D1
                name: "defaults"
                applies_to: "*"
                rule:
                  ref: has_owner
        """)
        assert load_yaml_policies(str(policies_root))[0].active is True

    def test_inactive_policy_preserved(self, policies_root):
        _write(policies_root, "inactive.yml", """
            policies:
              - id: POL-I1
                name: "off"
                applies_to: "*"
                active: false
                rule:
                  ref: has_owner
        """)
        assert load_yaml_policies(str(policies_root))[0].active is False


class TestLoadPoliciesMetadata:
    def test_rule_json_serialized(self, policies_root):
        _write(policies_root, "a.yml", """
            policies:
              - id: POL-M1
                name: "m"
                applies_to: "*"
                domain: "D"
                severity: "high"
                description: "desc"
                remediation: "r"
                rule:
                  ref: has_owner
        """)
        rows = load_policies_metadata(str(policies_root))
        assert len(rows) == 1
        assert rows[0]["rule_json"] == '{"ref": "has_owner"}'
        assert rows[0]["source_file"] == "a.yml"

    def test_legacy_applies_to_preserves_original_list(self, policies_root):
        _write(policies_root, "legacy.yml", """
            policies:
              - id: POL-MLEG
                name: "m"
                resource_types: ["table", "volume"]
                rule:
                  ref: has_owner
        """)
        rows = load_policies_metadata(str(policies_root))
        assert rows[0]["applies_to"] == "table,volume"
        assert rows[0]["domain"] == "Legacy"
