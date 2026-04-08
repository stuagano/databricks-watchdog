"""Unit tests for the Defense industry policy pack.

Validates YAML structure, unique IDs, rule references, applies_to class
references, and required fields for NIST 800-171, CMMC, and ITAR policies.

Run with: pytest tests/unit/test_policy_pack_defense.py -v
"""
import pytest
import yaml
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
PACK_DIR = REPO_ROOT / "library" / "defense"


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def ontology_classes():
    with open(PACK_DIR / "ontology_classes.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def rule_primitives():
    with open(PACK_DIR / "rule_primitives.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def policies():
    with open(PACK_DIR / "policies.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def primitive_ids(rule_primitives):
    return set(rule_primitives["primitives"].keys())


@pytest.fixture(scope="module")
def class_names(ontology_classes):
    """All class names defined in the defense ontology pack."""
    names = set()
    if "base_classes" in ontology_classes:
        names.update(ontology_classes["base_classes"].keys())
    if "derived_classes" in ontology_classes:
        names.update(ontology_classes["derived_classes"].keys())
    return names


@pytest.fixture(scope="module")
def base_class_names():
    """Base class names from the core engine ontology."""
    engine_path = REPO_ROOT / "engine" / "ontologies" / "resource_classes.yml"
    with open(engine_path) as f:
        data = yaml.safe_load(f)
    names = set()
    if "base_classes" in data:
        names.update(data["base_classes"].keys())
    if "derived_classes" in data:
        names.update(data["derived_classes"].keys())
    return names


@pytest.fixture(scope="module")
def base_primitive_ids():
    """Primitive IDs from the core engine rule_primitives.yml."""
    engine_path = REPO_ROOT / "engine" / "ontologies" / "rule_primitives.yml"
    with open(engine_path) as f:
        data = yaml.safe_load(f)
    return set(data["primitives"].keys())


# ── YAML Parsing ─────────────────────────────────────────────────────────────


class TestYamlParsing:
    def test_ontology_classes_loads(self, ontology_classes):
        assert ontology_classes is not None
        assert "derived_classes" in ontology_classes

    def test_rule_primitives_loads(self, rule_primitives):
        assert rule_primitives is not None
        assert "primitives" in rule_primitives

    def test_policies_loads(self, policies):
        assert policies is not None
        assert "policies" in policies


# ── Unique IDs ───────────────────────────────────────────────────────────────


class TestUniqueIds:
    def test_policy_ids_unique(self, policies):
        ids = [p["id"] for p in policies["policies"]]
        assert len(ids) == len(set(ids)), f"Duplicate policy IDs: {[x for x in ids if ids.count(x) > 1]}"

    def test_primitive_ids_unique(self, rule_primitives):
        ids = list(rule_primitives["primitives"].keys())
        assert len(ids) == len(set(ids)), "Duplicate primitive IDs found"

    def test_class_names_unique(self, ontology_classes):
        names = list(ontology_classes["derived_classes"].keys())
        assert len(names) == len(set(names)), "Duplicate class names found"


# ── Required Fields ──────────────────────────────────────────────────────────


REQUIRED_POLICY_FIELDS = ["id", "name", "applies_to", "domain", "severity", "description", "remediation", "active", "rule"]
REQUIRED_PRIMITIVE_FIELDS_BY_TYPE = {
    "tag_exists": ["keys"],
    "tag_equals": ["key", "value"],
    "none_of": ["rules"],
    "any_of": ["rules"],
}
REQUIRED_CLASS_FIELDS = ["parent", "description", "classifier"]


class TestRequiredFields:
    def test_policies_have_required_fields(self, policies):
        for policy in policies["policies"]:
            for field in REQUIRED_POLICY_FIELDS:
                assert field in policy, f"Policy {policy.get('id', '???')} missing field: {field}"

    def test_primitives_have_description(self, rule_primitives):
        for name, prim in rule_primitives["primitives"].items():
            assert "description" in prim, f"Primitive '{name}' missing description"
            assert "type" in prim, f"Primitive '{name}' missing type"

    def test_classes_have_required_fields(self, ontology_classes):
        for name, cls in ontology_classes["derived_classes"].items():
            for field in REQUIRED_CLASS_FIELDS:
                assert field in cls, f"Class '{name}' missing field: {field}"


# ── Rule References ──────────────────────────────────────────────────────────


def _collect_refs(rule):
    """Recursively collect all 'ref' values from a rule tree."""
    refs = set()
    if isinstance(rule, dict):
        if "ref" in rule:
            refs.add(rule["ref"])
        for key in ("rules", "then", "condition"):
            if key in rule:
                refs.update(_collect_refs(rule[key]))
        for key in ("all_of", "any_of", "none_of"):
            if key in rule:
                refs.update(_collect_refs(rule[key]))
    elif isinstance(rule, list):
        for item in rule:
            refs.update(_collect_refs(item))
    return refs


class TestRuleReferences:
    def test_policy_rule_refs_exist(self, policies, primitive_ids, base_primitive_ids):
        all_primitives = primitive_ids | base_primitive_ids
        for policy in policies["policies"]:
            refs = _collect_refs(policy["rule"])
            for ref in refs:
                assert ref in all_primitives, (
                    f"Policy {policy['id']} references unknown primitive: '{ref}'"
                )


# ── Class References ─────────────────────────────────────────────────────────


class TestClassReferences:
    def test_applies_to_classes_exist(self, policies, class_names, base_class_names):
        all_classes = class_names | base_class_names | {"*"}
        for policy in policies["policies"]:
            applies_to = policy["applies_to"]
            assert applies_to in all_classes, (
                f"Policy {policy['id']} applies_to unknown class: '{applies_to}'"
            )

    def test_parent_classes_exist(self, ontology_classes, base_class_names):
        all_classes = set(ontology_classes["derived_classes"].keys()) | base_class_names
        for name, cls in ontology_classes["derived_classes"].items():
            assert cls["parent"] in all_classes, (
                f"Class '{name}' has unknown parent: '{cls['parent']}'"
            )


# ── Policy Content Checks ───────────────────────────────────────────────────


class TestPolicyContent:
    def test_expected_policy_count(self, policies):
        assert len(policies["policies"]) == 8

    def test_severity_values(self, policies):
        valid = {"critical", "high", "medium", "low"}
        for policy in policies["policies"]:
            assert policy["severity"] in valid, (
                f"Policy {policy['id']} has invalid severity: {policy['severity']}"
            )

    def test_all_policies_active(self, policies):
        for policy in policies["policies"]:
            assert policy["active"] is True, f"Policy {policy['id']} is not active"

    def test_expected_policy_ids(self, policies):
        expected = {
            "POL-NIST-001", "POL-NIST-002", "POL-NIST-003", "POL-NIST-004", "POL-NIST-005",
            "POL-CMMC-001", "POL-CMMC-002",
            "POL-ITAR-001",
        }
        actual = {p["id"] for p in policies["policies"]}
        assert actual == expected

    def test_expected_class_count(self, ontology_classes):
        assert len(ontology_classes["derived_classes"]) == 5

    def test_expected_primitive_count(self, rule_primitives):
        assert len(rule_primitives["primitives"]) >= 9
