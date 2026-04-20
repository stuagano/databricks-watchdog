"""Unit tests for watchdog.compiler — compile-down and drift detection."""

import json
import textwrap
from pathlib import Path

import pytest
from watchdog.compiler import (
    DEFAULT_REGISTRY,
    GuardrailsTarget,
    artifact_hash,
    check_drift,
    compile_policies,
    load_manifest,
    write_artifacts,
    write_manifest,
)
from watchdog.policy_engine import PolicyDefinition
from watchdog.policy_loader import load_yaml_policies


def _policy(policy_id: str, **overrides) -> PolicyDefinition:
    base = dict(
        policy_id=policy_id,
        name="Example",
        applies_to="*",
        domain="Security",
        severity="high",
        description="desc",
        remediation="fix",
        rule={"ref": "has_owner"},
    )
    base.update(overrides)
    return PolicyDefinition(**base)


class TestCompilePolicies:
    def test_skips_policies_without_compile_to(self):
        assert compile_policies([_policy("POL-1")]) == []

    def test_emits_one_artifact_per_target(self):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        assert len(artifacts) == 1
        assert artifacts[0].policy_id == "POL-1"
        assert artifacts[0].target == "guardrails"
        assert artifacts[0].artifact_id == "guardrails/POL-1.json"

    def test_emits_multiple_targets_for_one_policy(self):
        # Two guardrails entries so the test doesn't require a second target.
        p = _policy("POL-1", compile_to=[
            {"target": "guardrails", "kind": "advisory"},
            {"target": "guardrails", "kind": "blocking"},
        ])
        artifacts = compile_policies([p])
        assert len(artifacts) == 2
        kinds = sorted(json.loads(a.content)["kind"] for a in artifacts)
        assert kinds == ["advisory", "blocking"]

    def test_unknown_target_raises(self):
        p = _policy("POL-1", compile_to=[{"target": "sdp_expectation"}])
        with pytest.raises(ValueError, match="unknown compile_to target"):
            compile_policies([p])

    def test_missing_target_field_raises(self):
        p = _policy("POL-1", compile_to=[{"kind": "advisory"}])
        with pytest.raises(ValueError, match="missing 'target'"):
            compile_policies([p])

    def test_custom_registry_can_extend(self):
        class FakeTarget:
            name = "fake"
            def compile(self, policy, config):
                from watchdog.compiler import EmittedArtifact
                return EmittedArtifact(
                    policy_id=policy.policy_id,
                    target="fake",
                    artifact_id=f"fake/{policy.policy_id}",
                    content="x",
                    emitted_at="2026-04-19T00:00:00+00:00",
                )

        registry = {**DEFAULT_REGISTRY, "fake": FakeTarget()}
        p = _policy("POL-1", compile_to=[{"target": "fake"}])
        artifacts = compile_policies([p], registry=registry)
        assert artifacts[0].target == "fake"


class TestGuardrailsTarget:
    def test_artifact_is_deterministic_for_same_input(self):
        # emitted_at changes, but content hash must not — the check body
        # must be independent of emission time.
        target = GuardrailsTarget()
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        a1 = target.compile(p, {"target": "guardrails", "kind": "advisory"})
        a2 = target.compile(p, {"target": "guardrails", "kind": "advisory"})
        assert artifact_hash(a1.content) == artifact_hash(a2.content)

    def test_invalid_kind_raises(self):
        target = GuardrailsTarget()
        p = _policy("POL-1")
        with pytest.raises(ValueError, match="must be 'advisory' or 'blocking'"):
            target.compile(p, {"target": "guardrails", "kind": "warn"})

    def test_content_includes_policy_metadata(self):
        target = GuardrailsTarget()
        p = _policy("POL-PII-001", name="PII must be masked", severity="critical")
        artifact = target.compile(
            p, {"target": "guardrails", "kind": "blocking", "block_when": "classified=pii"}
        )
        check = json.loads(artifact.content)
        assert check["policy_id"] == "POL-PII-001"
        assert check["severity"] == "critical"
        assert check["kind"] == "blocking"
        assert check["block_when"] == "classified=pii"


class TestManifestAndDrift:
    def test_manifest_roundtrip(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        manifest_path = tmp_path / "manifest.json"
        write_manifest(artifacts, manifest_path)

        entries = load_manifest(manifest_path)
        assert len(entries) == 1
        assert entries[0]["policy_id"] == "POL-1"
        assert entries[0]["content_hash"] == artifact_hash(artifacts[0].content)

    def test_load_manifest_missing_returns_empty(self, tmp_path):
        assert load_manifest(tmp_path / "nope.json") == []

    def test_drift_in_sync_after_emit(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        results = check_drift(manifest, out)
        assert [r.state for r in results] == ["in_sync"]

    def test_drift_detects_out_of_band_edit(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        # Simulate an out-of-band edit.
        (out / artifacts[0].artifact_id).write_text("tampered\n")
        results = check_drift(manifest, out)
        assert [r.state for r in results] == ["drifted"]

    def test_drift_detects_missing_artifact(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        (out / artifacts[0].artifact_id).unlink()
        results = check_drift(manifest, out)
        assert [r.state for r in results] == ["missing"]

    def test_empty_manifest_produces_empty_drift(self, tmp_path):
        assert check_drift(tmp_path / "nope.json", tmp_path) == []


class TestPolicyLoaderCompileTo:
    def test_compile_to_absent_is_none(self, tmp_path):
        policies_dir = tmp_path / "policies"
        policies_dir.mkdir()
        (policies_dir / "a.yml").write_text(textwrap.dedent("""
            policies:
              - id: POL-1
                name: no-compile
                applies_to: "*"
                rule: has_owner
        """))
        policies = load_yaml_policies(str(policies_dir))
        assert policies[0].compile_to is None

    def test_compile_to_list_is_parsed(self, tmp_path):
        policies_dir = tmp_path / "policies"
        policies_dir.mkdir()
        (policies_dir / "a.yml").write_text(textwrap.dedent("""
            policies:
              - id: POL-1
                name: compiled
                applies_to: "*"
                rule: has_owner
                compile_to:
                  - target: guardrails
                    kind: advisory
        """))
        policies = load_yaml_policies(str(policies_dir))
        assert policies[0].compile_to == [{"target": "guardrails", "kind": "advisory"}]

    def test_compile_to_single_entry_normalized_to_list(self, tmp_path):
        policies_dir = tmp_path / "policies"
        policies_dir.mkdir()
        (policies_dir / "a.yml").write_text(textwrap.dedent("""
            policies:
              - id: POL-1
                name: compiled
                applies_to: "*"
                rule: has_owner
                compile_to:
                  target: guardrails
                  kind: advisory
        """))
        policies = load_yaml_policies(str(policies_dir))
        assert policies[0].compile_to == [{"target": "guardrails", "kind": "advisory"}]

    def test_compile_to_flows_end_to_end_from_yaml(self, tmp_path):
        # The real seam: YAML → loader → compiler → artifact on disk.
        policies_dir = tmp_path / "policies"
        policies_dir.mkdir()
        (policies_dir / "a.yml").write_text(textwrap.dedent("""
            policies:
              - id: POL-E2E
                name: End-to-end
                applies_to: "*"
                severity: high
                domain: Security
                description: end-to-end smoke
                remediation: none
                rule: has_owner
                compile_to:
                  - target: guardrails
                    kind: blocking
        """))
        policies = load_yaml_policies(str(policies_dir))
        artifacts = compile_policies(policies)
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        emitted = json.loads((out / "guardrails/POL-E2E.json").read_text())
        assert emitted["kind"] == "blocking"
        assert [r.state for r in check_drift(manifest, out)] == ["in_sync"]
