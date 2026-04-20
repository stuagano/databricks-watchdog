"""Policy compile-down — emit runtime artifacts from Watchdog policies.

A policy that declares a `compile_to:` block is emitted to one or more
runtime substrates (Guardrails MCP, UC tag policy, UC ABAC, SDP
expectations). The compiler is one-way: policy → artifact. Drift in the
deployed artifact is detected by `check_drift`, not silently reconciled.

v1 scope: Guardrails target only, plus the manifest + drift loop. The
substrate-specific targets (UC ABAC, UC tag policy, SDP expectations)
plug in through the same CompileTarget protocol.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from watchdog.policy_engine import PolicyDefinition


@dataclass(frozen=True)
class EmittedArtifact:
    """A single artifact produced for one (policy, target) pair.

    content is the canonical serialized form — identical content must
    produce an identical hash regardless of emission time.
    """
    policy_id: str
    target: str
    artifact_id: str
    content: str
    emitted_at: str


@dataclass(frozen=True)
class DriftResult:
    """Outcome of comparing a manifest entry against the on-disk artifact."""
    policy_id: str
    target: str
    artifact_id: str
    state: str  # "in_sync" | "drifted" | "missing"


class CompileTarget(Protocol):
    name: str
    def compile(self, policy: PolicyDefinition, config: dict) -> EmittedArtifact: ...


class GuardrailsTarget:
    """Compile a policy into a Guardrails MCP check definition.

    The artifact is a JSON object the Guardrails MCP server can load at
    startup and register alongside its hardcoded SQL/chat/embedding
    checks. Compile-down here does not replace `guardrails.py` — it
    extends it with policy-driven checks.

    The compiler does not invent enforcement semantics: each policy must
    specify `kind` (advisory or blocking) and optionally a `block_when`
    predicate describing when the check fires. That keeps the substrate
    honest — the policy author decides what enforcement means.
    """
    name = "guardrails"

    def compile(self, policy: PolicyDefinition, config: dict) -> EmittedArtifact:
        kind = config.get("kind", "advisory")
        if kind not in ("advisory", "blocking"):
            raise ValueError(
                f"{policy.policy_id}: compile_to.guardrails.kind must be "
                f"'advisory' or 'blocking', got {kind!r}"
            )

        check = {
            "policy_id": policy.policy_id,
            "name": policy.name,
            "severity": policy.severity,
            "domain": policy.domain,
            "reason": policy.description.strip() or policy.name,
            "remediation": policy.remediation,
            "kind": kind,
            "block_when": config.get("block_when"),
            "applies_to": policy.applies_to,
        }
        content = json.dumps(check, sort_keys=True, indent=2) + "\n"
        return EmittedArtifact(
            policy_id=policy.policy_id,
            target=self.name,
            artifact_id=f"guardrails/{policy.policy_id}.json",
            content=content,
            emitted_at=datetime.now(timezone.utc).isoformat(),
        )


class UCTagPolicyTarget:
    """Compile a policy into a Unity Catalog tag policy spec.

    UC tag policies are purely declarative: they state which tag keys
    are required on which resource types, or which values a tag may
    take. The platform rejects writes that violate the policy at
    tag-set time — that's the runtime enforcement point.

    Compile-down here is straightforward because the substrate has a
    first-class API and no runtime code. The artifact is a JSON spec
    the deployer turns into an API call; drift detection watches the
    deployed object for out-of-band edits.

    Config shape::

        compile_to:
          - target: uc_tag_policy
            tag_key: data_steward
            policy_type: required          # or 'allowed_values'
            allowed_values: [dev, prod]    # required iff policy_type=allowed_values
            resource_types: [table]        # defaults to [table]
            scope:                         # optional — defaults to workspace-wide
              catalog: main
              schema: governance

    Two policy_type options to start — 'required' covers the common
    "prod tables must have a steward tag" intent, and 'allowed_values'
    covers the "environment tag must be one of …" intent. Adding a
    third (forbidden values) is trivial when a real policy needs it;
    not speculatively built.
    """
    name = "uc_tag_policy"
    _ALLOWED_TYPES = ("required", "allowed_values")

    def compile(self, policy: PolicyDefinition, config: dict) -> EmittedArtifact:
        tag_key = config.get("tag_key")
        if not tag_key:
            raise ValueError(
                f"{policy.policy_id}: compile_to.uc_tag_policy.tag_key is required"
            )

        policy_type = config.get("policy_type", "required")
        if policy_type not in self._ALLOWED_TYPES:
            raise ValueError(
                f"{policy.policy_id}: compile_to.uc_tag_policy.policy_type must be "
                f"one of {self._ALLOWED_TYPES}, got {policy_type!r}"
            )

        allowed_values = config.get("allowed_values")
        if policy_type == "allowed_values":
            if not allowed_values or not isinstance(allowed_values, list):
                raise ValueError(
                    f"{policy.policy_id}: compile_to.uc_tag_policy.allowed_values "
                    f"must be a non-empty list when policy_type=allowed_values"
                )
        elif allowed_values is not None:
            # Guard against silently weaker emission: author probably meant
            # policy_type=allowed_values but forgot to set it.
            raise ValueError(
                f"{policy.policy_id}: compile_to.uc_tag_policy.allowed_values is "
                f"only valid when policy_type=allowed_values"
            )

        spec = {
            "policy_id": policy.policy_id,
            "name": policy.name,
            "tag_key": tag_key,
            "policy_type": policy_type,
            "resource_types": sorted(config.get("resource_types", ["table"])),
            "scope": config.get("scope"),
            "severity": policy.severity,
            "domain": policy.domain,
            "description": policy.description.strip() or policy.name,
        }
        if policy_type == "allowed_values":
            spec["allowed_values"] = sorted(allowed_values)

        content = json.dumps(spec, sort_keys=True, indent=2) + "\n"
        return EmittedArtifact(
            policy_id=policy.policy_id,
            target=self.name,
            artifact_id=f"uc_tag_policy/{policy.policy_id}.json",
            content=content,
            emitted_at=datetime.now(timezone.utc).isoformat(),
        )


DEFAULT_REGISTRY: dict[str, CompileTarget] = {
    "guardrails": GuardrailsTarget(),
    "uc_tag_policy": UCTagPolicyTarget(),
}


def compile_policies(
    policies: list[PolicyDefinition],
    registry: dict[str, CompileTarget] | None = None,
) -> list[EmittedArtifact]:
    """Emit artifacts for every policy with a compile_to block.

    Policies without compile_to are silently skipped — they remain
    scan-only. Unknown target names raise ValueError so typos fail
    loudly instead of producing a silent compliance gap.
    """
    reg = registry if registry is not None else DEFAULT_REGISTRY
    artifacts: list[EmittedArtifact] = []

    for policy in policies:
        compile_to = policy.compile_to or []
        for entry in compile_to:
            target_name = entry.get("target")
            if not target_name:
                raise ValueError(
                    f"{policy.policy_id}: compile_to entry missing 'target'"
                )
            target = reg.get(target_name)
            if target is None:
                raise ValueError(
                    f"{policy.policy_id}: unknown compile_to target {target_name!r} "
                    f"(known: {sorted(reg)})"
                )
            artifacts.append(target.compile(policy, entry))

    return artifacts


def artifact_hash(content: str) -> str:
    """Stable content hash used by the manifest and drift detector."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def write_artifacts(artifacts: list[EmittedArtifact], output_dir: str | Path) -> None:
    """Materialize artifacts on disk under output_dir.

    The output directory is the filesystem stand-in for the workspace —
    for the Guardrails target it is a real path the MCP server reads.
    For future substrates (UC ABAC, tag policies) the 'output' becomes
    an API call; the disk layout here mirrors the deployment shape.
    """
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    for a in artifacts:
        path = base / a.artifact_id
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(a.content)


def write_manifest(artifacts: list[EmittedArtifact], manifest_path: str | Path) -> None:
    """Record what was emitted, where, and with what content hash.

    The manifest is the only durable record of compile-down state. Drift
    detection reads it on every scan; the scanner uses it to score
    posture.
    """
    entries = [
        {
            "policy_id": a.policy_id,
            "target": a.target,
            "artifact_id": a.artifact_id,
            "content_hash": artifact_hash(a.content),
            "emitted_at": a.emitted_at,
        }
        for a in artifacts
    ]
    path = Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"entries": entries}, indent=2) + "\n")


def load_manifest(manifest_path: str | Path) -> list[dict]:
    path = Path(manifest_path)
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    return data.get("entries", [])


def check_drift(
    manifest_path: str | Path,
    output_dir: str | Path,
) -> list[DriftResult]:
    """Compare each manifest entry to the artifact on disk.

    Three outcomes per entry:
      - in_sync:  artifact present, hash matches manifest
      - drifted:  artifact present, hash differs (modified out-of-band)
      - missing:  artifact absent from output_dir

    The compiler never silently re-emits a drifted artifact. The caller
    decides whether to re-emit, accept the drift, or remove the
    compile_to block.
    """
    entries = load_manifest(manifest_path)
    base = Path(output_dir)
    results: list[DriftResult] = []

    for e in entries:
        artifact_path = base / e["artifact_id"]
        if not artifact_path.exists():
            state = "missing"
        else:
            actual = artifact_hash(artifact_path.read_text())
            state = "in_sync" if actual == e["content_hash"] else "drifted"
        results.append(DriftResult(
            policy_id=e["policy_id"],
            target=e["target"],
            artifact_id=e["artifact_id"],
            state=state,
        ))

    return results
