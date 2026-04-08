"""Unit tests for the Healthcare (HIPAA) policy pack.

Validates YAML structure, cross-references between policies, rule primitives,
and ontology classes, and checks dashboard SQL for unresolved placeholders.

Run with: pytest tests/unit/test_policy_pack_healthcare.py -v
"""
import re
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).parent.parent.parent
HEALTHCARE_DIR = REPO_ROOT / "library" / "healthcare"


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def ontology_classes():
    path = HEALTHCARE_DIR / "ontology_classes.yml"
    with open(path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def rule_primitives():
    path = HEALTHCARE_DIR / "rule_primitives.yml"
    with open(path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def policies():
    path = HEALTHCARE_DIR / "policies.yml"
    with open(path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def base_ontology():
    """Load the base engine ontology for parent class validation."""
    path = REPO_ROOT / "engine" / "ontologies" / "resource_classes.yml"
    with open(path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def dashboard_sql():
    path = HEALTHCARE_DIR / "dashboard_queries.sql"
    return path.read_text()


# ── YAML parsing ────────────────────────────────────────────────────────────


class TestYamlParsing:
    def test_ontology_classes_parses(self, ontology_classes):
        assert ontology_classes is not None
        assert "derived_classes" in ontology_classes

    def test_rule_primitives_parses(self, rule_primitives):
        assert rule_primitives is not None
        assert "primitives" in rule_primitives

    def test_policies_parses(self, policies):
        assert policies is not None
        assert "policies" in policies


# ── Policy structure ────────────────────────────────────────────────────────


REQUIRED_POLICY_FIELDS = {"id", "name", "applies_to", "domain", "severity", "rule", "active"}


class TestPolicyStructure:
    def test_all_policies_have_required_fields(self, policies):
        for policy in policies["policies"]:
            missing = REQUIRED_POLICY_FIELDS - set(policy.keys())
            assert not missing, (
                f"Policy {policy.get('id', '???')} missing fields: {missing}"
            )

    def test_policy_ids_are_unique(self, policies):
        ids = [p["id"] for p in policies["policies"]]
        assert len(ids) == len(set(ids)), f"Duplicate policy IDs: {ids}"

    def test_policy_ids_follow_hipaa_format(self, policies):
        pattern = re.compile(r"^POL-HIPAA-\d{3}$")
        for policy in policies["policies"]:
            assert pattern.match(policy["id"]), (
                f"Policy ID '{policy['id']}' does not match POL-HIPAA-NNN format"
            )

    def test_severity_values_are_valid(self, policies):
        valid_severities = {"critical", "high", "medium", "low"}
        for policy in policies["policies"]:
            assert policy["severity"] in valid_severities, (
                f"Policy {policy['id']} has invalid severity: {policy['severity']}"
            )

    def test_domain_values_are_valid(self, policies):
        valid_domains = {
            "CostGovernance", "SecurityGovernance", "DataQuality",
            "OperationalGovernance", "RegulatoryCompliance", "DataClassification",
        }
        for policy in policies["policies"]:
            assert policy["domain"] in valid_domains, (
                f"Policy {policy['id']} has invalid domain: {policy['domain']}"
            )

    def test_all_policies_are_active(self, policies):
        for policy in policies["policies"]:
            assert policy["active"] is True, (
                f"Policy {policy['id']} is not active"
            )


# ── Cross-references: policy rules → rule primitives ────────────────────────


def _collect_refs(rule):
    """Recursively collect all 'ref' values from a rule tree."""
    refs = set()
    if isinstance(rule, dict):
        if "ref" in rule:
            refs.add(rule["ref"])
        for value in rule.values():
            refs |= _collect_refs(value)
    elif isinstance(rule, list):
        for item in rule:
            refs |= _collect_refs(item)
    return refs


class TestPolicyRuleRefs:
    def test_all_rule_refs_exist_in_primitives(self, policies, rule_primitives):
        primitive_names = set(rule_primitives["primitives"].keys())
        for policy in policies["policies"]:
            refs = _collect_refs(policy["rule"])
            missing = refs - primitive_names
            assert not missing, (
                f"Policy {policy['id']} references undefined primitives: {missing}"
            )


# ── Cross-references: policy applies_to → ontology classes ──────────────────


class TestPolicyAppliesTo:
    def test_all_applies_to_classes_exist(self, policies, ontology_classes, base_ontology):
        # Collect all known class names from both base and healthcare ontologies
        known_classes = set()
        if base_ontology.get("base_classes"):
            known_classes.update(base_ontology["base_classes"].keys())
        if base_ontology.get("derived_classes"):
            known_classes.update(base_ontology["derived_classes"].keys())
        if ontology_classes.get("derived_classes"):
            known_classes.update(ontology_classes["derived_classes"].keys())
        # Wildcard is always valid
        known_classes.add("*")

        for policy in policies["policies"]:
            applies_to = policy["applies_to"]
            assert applies_to in known_classes, (
                f"Policy {policy['id']} applies_to '{applies_to}' "
                f"is not a known ontology class"
            )


# ── Ontology class structure ─────────────────────────────────────────────────


class TestOntologyClasses:
    def test_all_classes_have_parent(self, ontology_classes):
        for name, cls in ontology_classes["derived_classes"].items():
            assert "parent" in cls, f"Class '{name}' has no parent"

    def test_all_classes_have_description(self, ontology_classes):
        for name, cls in ontology_classes["derived_classes"].items():
            assert "description" in cls, f"Class '{name}' has no description"

    def test_all_classes_have_classifier(self, ontology_classes):
        for name, cls in ontology_classes["derived_classes"].items():
            assert "classifier" in cls, f"Class '{name}' has no classifier"

    def test_parent_references_are_valid(self, ontology_classes, base_ontology):
        known_classes = set()
        if base_ontology.get("base_classes"):
            known_classes.update(base_ontology["base_classes"].keys())
        if base_ontology.get("derived_classes"):
            known_classes.update(base_ontology["derived_classes"].keys())
        # Healthcare classes can reference each other
        if ontology_classes.get("derived_classes"):
            known_classes.update(ontology_classes["derived_classes"].keys())

        for name, cls in ontology_classes["derived_classes"].items():
            parent = cls["parent"]
            assert parent in known_classes, (
                f"Class '{name}' references unknown parent '{parent}'"
            )

    def test_expected_classes_present(self, ontology_classes):
        expected = {"PhiAsset", "EphiAsset", "HipaaAuditAsset", "DeIdentifiedDataset"}
        actual = set(ontology_classes["derived_classes"].keys())
        missing = expected - actual
        assert not missing, f"Missing expected classes: {missing}"


# ── Rule primitives structure ────────────────────────────────────────────────


class TestRulePrimitives:
    def test_all_primitives_have_description(self, rule_primitives):
        for name, prim in rule_primitives["primitives"].items():
            assert "description" in prim, f"Primitive '{name}' has no description"

    def test_all_primitives_have_type(self, rule_primitives):
        for name, prim in rule_primitives["primitives"].items():
            assert "type" in prim, f"Primitive '{name}' has no type"

    def test_expected_primitives_present(self, rule_primitives):
        expected = {
            "has_phi_steward",
            "has_hipaa_retention_policy",
            "has_encryption_at_rest",
            "has_access_logging",
            "has_baa_reference",
            "has_minimum_necessary",
            "phi_not_in_dev",
            "has_breach_notification_plan",
            "has_de_identification_method",
            "audit_trail_immutable",
        }
        actual = set(rule_primitives["primitives"].keys())
        missing = expected - actual
        assert not missing, f"Missing expected primitives: {missing}"


# ── Dashboard SQL ────────────────────────────────────────────────────────────


class TestDashboardSql:
    def test_no_unresolved_placeholders(self, dashboard_sql):
        """Only ${catalog} and ${schema} are allowed as placeholders."""
        allowed = {"${catalog}", "${schema}"}
        # Find all ${...} placeholders
        found = set(re.findall(r"\$\{[^}]+\}", dashboard_sql))
        unexpected = found - allowed
        assert not unexpected, (
            f"Dashboard SQL has unresolved placeholders: {unexpected}"
        )

    def test_sql_is_not_empty(self, dashboard_sql):
        assert len(dashboard_sql.strip()) > 0

    def test_sql_has_multiple_queries(self, dashboard_sql):
        """Should contain at least 3 queries (separated by semicolons)."""
        # Count semicolons that end statements (not in strings)
        statements = [s.strip() for s in dashboard_sql.split(";") if s.strip()]
        assert len(statements) >= 3, (
            f"Expected at least 3 SQL queries, found {len(statements)}"
        )
