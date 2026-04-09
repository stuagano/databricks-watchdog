"""Unit tests for agent governance — ontology classes, rule primitives, and policies.

Validates Phase 5A/B/C: AgentAsset base class, derived agent classes,
agent rule primitives, agent/execution policies, and AgentGovernance domain.

Run with: pytest tests/unit/test_agent_governance.py -v
"""
import re

import pytest
import yaml
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
ENGINE_ROOT = REPO_ROOT / "engine"


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def resource_classes():
    with open(ENGINE_ROOT / "ontologies" / "resource_classes.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def rule_primitives():
    with open(ENGINE_ROOT / "ontologies" / "rule_primitives.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def compliance_domains():
    with open(ENGINE_ROOT / "ontologies" / "compliance_domains.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def agent_policies():
    with open(ENGINE_ROOT / "policies" / "agent_governance.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def primitive_ids(rule_primitives):
    return set(rule_primitives["primitives"].keys())


@pytest.fixture(scope="module")
def all_class_names(resource_classes):
    names = set()
    if "base_classes" in resource_classes:
        names.update(resource_classes["base_classes"].keys())
    if "derived_classes" in resource_classes:
        names.update(resource_classes["derived_classes"].keys())
    return names


# ── AgentAsset base class ────────────────────────────────────────────────────


class TestAgentAssetBaseClass:
    def test_agent_asset_exists_in_base_classes(self, resource_classes):
        assert "AgentAsset" in resource_classes["base_classes"]

    def test_agent_asset_matches_agent_type(self, resource_classes):
        agent_asset = resource_classes["base_classes"]["AgentAsset"]
        assert "agent" in agent_asset["matches_resource_types"]

    def test_agent_asset_matches_agent_execution_type(self, resource_classes):
        agent_asset = resource_classes["base_classes"]["AgentAsset"]
        assert "agent_execution" in agent_asset["matches_resource_types"]

    def test_agent_asset_has_description(self, resource_classes):
        agent_asset = resource_classes["base_classes"]["AgentAsset"]
        assert "description" in agent_asset


# ── Derived agent classes ────────────────────────────────────────────────────


AGENT_DERIVED_CLASSES = [
    "AgentWithPiiAccess",
    "AgentWithExternalAccess",
    "AgentWithDataExport",
    "UngovernedAgent",
    "HighRiskExecution",
    "ProductionAgent",
]


class TestDerivedAgentClasses:
    @pytest.mark.parametrize("class_name", AGENT_DERIVED_CLASSES)
    def test_derived_class_exists(self, resource_classes, class_name):
        assert class_name in resource_classes["derived_classes"]

    @pytest.mark.parametrize("class_name", AGENT_DERIVED_CLASSES)
    def test_derived_class_has_parent(self, resource_classes, class_name):
        cls = resource_classes["derived_classes"][class_name]
        assert cls["parent"] == "AgentAsset"

    @pytest.mark.parametrize("class_name", AGENT_DERIVED_CLASSES)
    def test_derived_class_has_description(self, resource_classes, class_name):
        cls = resource_classes["derived_classes"][class_name]
        assert "description" in cls
        assert len(cls["description"]) > 10

    @pytest.mark.parametrize("class_name", AGENT_DERIVED_CLASSES)
    def test_derived_class_has_classifier(self, resource_classes, class_name):
        cls = resource_classes["derived_classes"][class_name]
        assert "classifier" in cls
        assert isinstance(cls["classifier"], dict)

    def test_agent_derived_class_count(self, resource_classes):
        agent_classes = [
            name for name, cls in resource_classes["derived_classes"].items()
            if cls.get("parent") == "AgentAsset"
        ]
        assert len(agent_classes) == 6


# ── Agent rule primitives ───────────────────────────────────────────────────


AGENT_PRIMITIVES = [
    "agent_has_audit_logging",
    "has_agent_owner",
    "has_data_export_approval",
    "has_external_access_registration",
    "not_accessing_production",
    "execution_has_trace",
    "pii_table_count_under_threshold",
    "agent_has_model_governance",
    "agent_execution_under_duration_limit",
    "agent_has_error_handling",
]


class TestAgentRulePrimitives:
    @pytest.mark.parametrize("primitive_name", AGENT_PRIMITIVES)
    def test_primitive_exists(self, rule_primitives, primitive_name):
        assert primitive_name in rule_primitives["primitives"]

    @pytest.mark.parametrize("primitive_name", AGENT_PRIMITIVES)
    def test_primitive_has_description(self, rule_primitives, primitive_name):
        prim = rule_primitives["primitives"][primitive_name]
        assert "description" in prim
        assert len(prim["description"]) > 10

    @pytest.mark.parametrize("primitive_name", AGENT_PRIMITIVES)
    def test_primitive_has_type(self, rule_primitives, primitive_name):
        prim = rule_primitives["primitives"][primitive_name]
        assert "type" in prim

    def test_primitive_types_valid(self, rule_primitives):
        valid_types = {
            "tag_exists", "tag_equals", "tag_in", "tag_not_in", "tag_matches",
            "metadata_equals", "metadata_matches", "metadata_not_empty",
            "metadata_gte", "has_owner", "all_of", "any_of", "none_of", "if_then",
        }
        for name in AGENT_PRIMITIVES:
            prim = rule_primitives["primitives"][name]
            assert prim["type"] in valid_types, (
                f"Primitive '{name}' has invalid type: {prim['type']}"
            )


# ── Agent policies ──────────────────────────────────────────────────────────


REQUIRED_POLICY_FIELDS = [
    "id", "name", "applies_to", "domain", "severity",
    "description", "remediation", "active", "rule",
]


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


class TestAgentPolicies:
    def test_policies_load(self, agent_policies):
        assert agent_policies is not None
        assert "policies" in agent_policies

    def test_policy_count(self, agent_policies):
        assert len(agent_policies["policies"]) == 9

    @pytest.mark.parametrize("field", REQUIRED_POLICY_FIELDS)
    def test_policies_have_required_fields(self, agent_policies, field):
        for policy in agent_policies["policies"]:
            assert field in policy, (
                f"Policy {policy.get('id', '???')} missing field: {field}"
            )

    def test_policy_ids_unique(self, agent_policies):
        ids = [p["id"] for p in agent_policies["policies"]]
        assert len(ids) == len(set(ids)), (
            f"Duplicate policy IDs: {[x for x in ids if ids.count(x) > 1]}"
        )

    def test_policy_ids_follow_pattern(self, agent_policies):
        agent_pattern = re.compile(r"^POL-AGENT-\d{3}$")
        exec_pattern = re.compile(r"^POL-EXEC-\d{3}$")
        for policy in agent_policies["policies"]:
            pid = policy["id"]
            assert agent_pattern.match(pid) or exec_pattern.match(pid), (
                f"Policy ID '{pid}' does not follow POL-AGENT-NNN or POL-EXEC-NNN pattern"
            )

    def test_expected_agent_policy_ids(self, agent_policies):
        expected_agent = {f"POL-AGENT-{i:03d}" for i in range(1, 8)}
        expected_exec = {f"POL-EXEC-{i:03d}" for i in range(1, 3)}
        actual = {p["id"] for p in agent_policies["policies"]}
        assert actual == expected_agent | expected_exec

    def test_severity_values_valid(self, agent_policies):
        valid = {"critical", "high", "medium", "low"}
        for policy in agent_policies["policies"]:
            assert policy["severity"] in valid, (
                f"Policy {policy['id']} has invalid severity: {policy['severity']}"
            )

    def test_all_policies_active(self, agent_policies):
        for policy in agent_policies["policies"]:
            assert policy["active"] is True, (
                f"Policy {policy['id']} is not active"
            )

    def test_rule_refs_exist_in_primitives(self, agent_policies, primitive_ids):
        for policy in agent_policies["policies"]:
            refs = _collect_refs(policy["rule"])
            for ref in refs:
                assert ref in primitive_ids, (
                    f"Policy {policy['id']} references unknown primitive: '{ref}'"
                )

    def test_applies_to_classes_exist(self, agent_policies, all_class_names):
        for policy in agent_policies["policies"]:
            applies_to = policy["applies_to"]
            assert applies_to in all_class_names, (
                f"Policy {policy['id']} applies_to unknown class: '{applies_to}'"
            )

    def test_domains_valid(self, agent_policies, compliance_domains):
        valid_domains = set(compliance_domains["domains"].keys())
        for policy in agent_policies["policies"]:
            assert policy["domain"] in valid_domains, (
                f"Policy {policy['id']} has unknown domain: {policy['domain']}"
            )


# ── AgentGovernance domain ──────────────────────────────────────────────────


class TestAgentGovernanceDomain:
    def test_domain_exists(self, compliance_domains):
        assert "AgentGovernance" in compliance_domains["domains"]

    def test_domain_has_description(self, compliance_domains):
        domain = compliance_domains["domains"]["AgentGovernance"]
        assert "description" in domain
        assert len(domain["description"]) > 10

    def test_domain_has_owner_role(self, compliance_domains):
        domain = compliance_domains["domains"]["AgentGovernance"]
        assert "owner_role" in domain

    def test_domain_has_severity_floor(self, compliance_domains):
        domain = compliance_domains["domains"]["AgentGovernance"]
        assert domain["severity_floor"] == "high"

    def test_domain_has_sub_domains(self, compliance_domains):
        domain = compliance_domains["domains"]["AgentGovernance"]
        assert "sub_domains" in domain
        expected = {"AgentAccessControl", "AgentAuditCompliance", "AgentDataExportControl"}
        assert set(domain["sub_domains"]) == expected

    def test_domain_has_escalation(self, compliance_domains):
        domain = compliance_domains["domains"]["AgentGovernance"]
        assert "escalation" in domain


# ── OntologyEngine integration ──────────────────────────────────────────────


class TestAgentOntologyEngineIntegration:
    """Test agent classes through the OntologyEngine (if available)."""

    @pytest.fixture(scope="class")
    def engine(self, ontology_dir):
        from watchdog.ontology import OntologyEngine
        return OntologyEngine(ontology_dir=ontology_dir)

    def test_agent_is_agent_asset(self, engine):
        result = engine.classify("agent", {}, {})
        assert "AgentAsset" in result.classes

    def test_agent_execution_is_agent_asset(self, engine):
        result = engine.classify("agent_execution", {}, {})
        assert "AgentAsset" in result.classes

    def test_agent_not_data_asset(self, engine):
        result = engine.classify("agent", {}, {})
        assert "DataAsset" not in result.classes
        assert "ComputeAsset" not in result.classes

    def test_agent_with_pii_access(self, engine):
        result = engine.classify("agent", {"accessed_pii": "true"}, {})
        assert "AgentWithPiiAccess" in result.classes
        assert "AgentAsset" in result.classes

    def test_agent_with_external_access(self, engine):
        result = engine.classify("agent", {"used_external_tool": "true"}, {})
        assert "AgentWithExternalAccess" in result.classes

    def test_agent_with_data_export(self, engine):
        result = engine.classify("agent", {"exported_data": "true"}, {})
        assert "AgentWithDataExport" in result.classes

    def test_ungoverned_agent(self, engine):
        result = engine.classify("agent", {}, {})
        assert "UngovernedAgent" in result.classes

    def test_governed_agent_not_ungoverned(self, engine):
        result = engine.classify("agent", {"agent_owner": "alice@co.com", "audit_logging_enabled": "true"}, {})
        assert "UngovernedAgent" not in result.classes

    def test_high_risk_execution(self, engine):
        result = engine.classify("agent_execution", {"accessed_pii": "true"}, {})
        assert "HighRiskExecution" in result.classes

    def test_non_pii_execution_not_high_risk(self, engine):
        result = engine.classify("agent_execution", {}, {})
        assert "HighRiskExecution" not in result.classes

    def test_production_agent(self, engine):
        result = engine.classify("agent", {"environment": "prod"}, {})
        assert "ProductionAgent" in result.classes

    def test_non_prod_agent_not_production(self, engine):
        result = engine.classify("agent", {"environment": "dev"}, {})
        assert "ProductionAgent" not in result.classes

    def test_agent_asset_ancestor_chain(self, engine):
        chain = engine.get_ancestor_chain("AgentWithPiiAccess")
        assert chain == ["AgentWithPiiAccess", "AgentAsset"]
