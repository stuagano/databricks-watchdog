"""Unit tests for the Financial industry policy pack (SOX/PCI-DSS/GLBA).

Validates YAML structure, unique IDs, rule reference integrity, class
references, and required fields — same validation pattern as healthcare.

Run with: pytest tests/unit/test_policy_pack_financial.py -v
"""
import pytest
import yaml
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
LIBRARY_DIR = REPO_ROOT / "library" / "financial"


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def policies():
    """Load and parse policies.yml."""
    path = LIBRARY_DIR / "policies.yml"
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["policies"]


@pytest.fixture(scope="module")
def ontology_classes():
    """Load and parse ontology_classes.yml."""
    path = LIBRARY_DIR / "ontology_classes.yml"
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["derived_classes"]


@pytest.fixture(scope="module")
def rule_primitives():
    """Load and parse rule_primitives.yml."""
    path = LIBRARY_DIR / "rule_primitives.yml"
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["primitives"]


@pytest.fixture(scope="module")
def core_ontology_classes():
    """Load class names from the core engine ontology."""
    path = REPO_ROOT / "engine" / "ontologies" / "resource_classes.yml"
    with open(path) as f:
        data = yaml.safe_load(f)
    classes = set(data.get("base_classes", {}).keys())
    classes.update(data.get("derived_classes", {}).keys())
    return classes


@pytest.fixture(scope="module")
def core_rule_primitives():
    """Load primitive names from the core engine ontology."""
    path = REPO_ROOT / "engine" / "ontologies" / "rule_primitives.yml"
    with open(path) as f:
        data = yaml.safe_load(f)
    return set(data.get("primitives", {}).keys())


# ── YAML Parsing ─────────────────────────────────────────────────────────────

class TestYamlParsing:
    def test_policies_yml_parses(self):
        path = LIBRARY_DIR / "policies.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert "policies" in data
        assert isinstance(data["policies"], list)

    def test_ontology_classes_yml_parses(self):
        path = LIBRARY_DIR / "ontology_classes.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert "derived_classes" in data
        assert isinstance(data["derived_classes"], dict)

    def test_rule_primitives_yml_parses(self):
        path = LIBRARY_DIR / "rule_primitives.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert "primitives" in data
        assert isinstance(data["primitives"], dict)


# ── Unique IDs ───────────────────────────────────────────────────────────────

class TestUniqueIds:
    def test_policy_ids_are_unique(self, policies):
        ids = [p["id"] for p in policies]
        assert len(ids) == len(set(ids)), f"Duplicate policy IDs: {[x for x in ids if ids.count(x) > 1]}"

    def test_rule_primitive_names_are_unique(self, rule_primitives):
        names = list(rule_primitives.keys())
        assert len(names) == len(set(names)), f"Duplicate primitive names found"

    def test_ontology_class_names_are_unique(self, ontology_classes):
        names = list(ontology_classes.keys())
        assert len(names) == len(set(names)), f"Duplicate class names found"


# ── Policy Count ─────────────────────────────────────────────────────────────

class TestPolicyCounts:
    def test_total_policy_count(self, policies):
        assert len(policies) == 12, f"Expected 12 policies, got {len(policies)}"

    def test_sox_policy_count(self, policies):
        sox = [p for p in policies if p["id"].startswith("POL-SOX-")]
        assert len(sox) == 5, f"Expected 5 SOX policies, got {len(sox)}"

    def test_pci_policy_count(self, policies):
        pci = [p for p in policies if p["id"].startswith("POL-PCI-")]
        assert len(pci) == 4, f"Expected 4 PCI-DSS policies, got {len(pci)}"

    def test_glba_policy_count(self, policies):
        glba = [p for p in policies if p["id"].startswith("POL-GLBA-")]
        assert len(glba) == 3, f"Expected 3 GLBA policies, got {len(glba)}"


# ── Required Fields ──────────────────────────────────────────────────────────

REQUIRED_POLICY_FIELDS = ["id", "name", "applies_to", "domain", "severity",
                          "description", "remediation", "active", "rule"]


class TestRequiredFields:
    @pytest.mark.parametrize("field", REQUIRED_POLICY_FIELDS)
    def test_all_policies_have_required_field(self, policies, field):
        for policy in policies:
            assert field in policy, (
                f"Policy {policy.get('id', '???')} missing required field: {field}"
            )

    def test_all_policies_are_active(self, policies):
        for policy in policies:
            assert policy["active"] is True, (
                f"Policy {policy['id']} is not active"
            )

    def test_severity_values_are_valid(self, policies):
        valid = {"critical", "high", "medium", "low"}
        for policy in policies:
            assert policy["severity"] in valid, (
                f"Policy {policy['id']} has invalid severity: {policy['severity']}"
            )

    def test_domain_is_regulatory_compliance(self, policies):
        for policy in policies:
            assert policy["domain"] == "RegulatoryCompliance", (
                f"Policy {policy['id']} has unexpected domain: {policy['domain']}"
            )


# ── Rule References ──────────────────────────────────────────────────────────

class TestRuleReferences:
    def test_all_rule_refs_exist_in_primitives(self, policies, rule_primitives,
                                                core_rule_primitives):
        """Every 'ref' in a policy rule must resolve to a known primitive."""
        all_primitives = set(rule_primitives.keys()) | core_rule_primitives

        for policy in policies:
            rule = policy["rule"]
            refs = _extract_refs(rule)
            for ref in refs:
                assert ref in all_primitives, (
                    f"Policy {policy['id']} references unknown primitive: {ref}"
                )


# ── Applies-To Classes ───────────────────────────────────────────────────────

class TestAppliesToClasses:
    def test_all_applies_to_classes_exist(self, policies, ontology_classes,
                                          core_ontology_classes):
        """Every applies_to value must be a known class or wildcard."""
        all_classes = set(ontology_classes.keys()) | core_ontology_classes | {"*"}

        for policy in policies:
            target = policy["applies_to"]
            assert target in all_classes, (
                f"Policy {policy['id']} applies_to unknown class: {target}"
            )


# ── Ontology Class Structure ─────────────────────────────────────────────────

class TestOntologyClassStructure:
    def test_all_classes_have_parent(self, ontology_classes):
        for name, cls in ontology_classes.items():
            assert "parent" in cls, f"Class {name} missing 'parent'"

    def test_all_classes_have_description(self, ontology_classes):
        for name, cls in ontology_classes.items():
            assert "description" in cls, f"Class {name} missing 'description'"

    def test_all_classes_have_classifier(self, ontology_classes):
        for name, cls in ontology_classes.items():
            assert "classifier" in cls, f"Class {name} missing 'classifier'"

    def test_parent_classes_exist(self, ontology_classes, core_ontology_classes):
        """Parent references must resolve to a known class."""
        all_classes = set(ontology_classes.keys()) | core_ontology_classes
        for name, cls in ontology_classes.items():
            assert cls["parent"] in all_classes, (
                f"Class {name} has unknown parent: {cls['parent']}"
            )


# ── Rule Primitive Structure ─────────────────────────────────────────────────

VALID_RULE_TYPES = {"tag_exists", "tag_equals", "tag_in", "tag_not_in",
                    "tag_matches", "metadata_equals", "metadata_matches",
                    "metadata_not_empty", "metadata_gte", "has_owner",
                    "all_of", "any_of", "none_of", "if_then"}


class TestRulePrimitiveStructure:
    def test_all_primitives_have_type(self, rule_primitives):
        for name, prim in rule_primitives.items():
            assert "type" in prim, f"Primitive {name} missing 'type'"

    def test_all_primitives_have_description(self, rule_primitives):
        for name, prim in rule_primitives.items():
            assert "description" in prim, f"Primitive {name} missing 'description'"

    def test_all_primitive_types_are_valid(self, rule_primitives):
        for name, prim in rule_primitives.items():
            assert prim["type"] in VALID_RULE_TYPES, (
                f"Primitive {name} has invalid type: {prim['type']}"
            )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_refs(rule: dict) -> list[str]:
    """Recursively extract all 'ref' values from a rule tree."""
    refs = []
    if "ref" in rule:
        refs.append(rule["ref"])
    if "rules" in rule:
        for sub in rule["rules"]:
            refs.extend(_extract_refs(sub))
    if "condition" in rule:
        refs.extend(_extract_refs(rule["condition"]))
    if "then" in rule:
        refs.extend(_extract_refs(rule["then"]))
    return refs
