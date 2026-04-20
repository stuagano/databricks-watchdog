# UC ABAC Column Mask Compile Target Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `UCAbacTarget` compile target that emits declarative JSON artifacts for UC column mask rules, following the same pattern as the existing `GuardrailsTarget` and `UCTagPolicyTarget`.

**Architecture:** A single new class `UCAbacTarget` in `engine/src/watchdog/compiler.py` implementing the `CompileTarget` protocol. Validates `mask_function` as a three-part dotted identifier, emits a JSON artifact at `uc_abac/{policy_id}.json`. Registered in `DEFAULT_REGISTRY`. No SDK calls — pure compile.

**Tech Stack:** Python, pytest

**Spec:** `docs/superpowers/specs/2026-04-20-uc-abac-compile-target-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/compiler.py` | Modify | Add `UCAbacTarget` class, register in `DEFAULT_REGISTRY` |
| `tests/unit/test_compiler.py` | Modify | Add `TestUCAbacTarget` test class |

---

### Task 1: Write failing tests for UCAbacTarget

**Files:**
- Modify: `tests/unit/test_compiler.py`

- [ ] **Step 1: Add UCAbacTarget import**

In `tests/unit/test_compiler.py`, update the import block to include `UCAbacTarget`:

```python
from watchdog.compiler import (
    DEFAULT_REGISTRY,
    GuardrailsTarget,
    UCAbacTarget,
    UCTagPolicyTarget,
    artifact_hash,
    check_drift,
    compile_policies,
    load_manifest,
    write_artifacts,
    write_manifest,
)
```

- [ ] **Step 2: Write the test class**

Append the following test class to `tests/unit/test_compiler.py`, before the `TestManifestAndDrift` class:

```python
class TestUCAbacTarget:
    def test_valid_mask_function_emits_artifact(self):
        target = UCAbacTarget()
        p = _policy("POL-PII-001", name="PII must be masked", severity="critical",
                     applies_to="PIIColumn")
        artifact = target.compile(p, {
            "target": "uc_abac",
            "mask_function": "main.governance.redact_pii",
            "apply_when": "environment = prod",
        })
        spec = json.loads(artifact.content)
        assert spec["policy_id"] == "POL-PII-001"
        assert spec["name"] == "PII must be masked"
        assert spec["mask_function"] == "main.governance.redact_pii"
        assert spec["apply_when"] == "environment = prod"
        assert spec["applies_to"] == "PIIColumn"
        assert spec["severity"] == "critical"
        assert artifact.artifact_id == "uc_abac/POL-PII-001.json"
        assert artifact.target == "uc_abac"

    def test_apply_when_omitted(self):
        target = UCAbacTarget()
        p = _policy("POL-1")
        artifact = target.compile(p, {
            "target": "uc_abac",
            "mask_function": "cat.sch.fn",
        })
        spec = json.loads(artifact.content)
        assert "apply_when" not in spec
        assert spec["mask_function"] == "cat.sch.fn"

    def test_missing_mask_function_raises(self):
        target = UCAbacTarget()
        with pytest.raises(ValueError, match="mask_function is required"):
            target.compile(_policy("POL-1"), {"target": "uc_abac"})

    def test_malformed_mask_function_one_part_raises(self):
        target = UCAbacTarget()
        with pytest.raises(ValueError, match="three-part"):
            target.compile(_policy("POL-1"), {
                "target": "uc_abac",
                "mask_function": "redact_pii",
            })

    def test_malformed_mask_function_two_parts_raises(self):
        target = UCAbacTarget()
        with pytest.raises(ValueError, match="three-part"):
            target.compile(_policy("POL-1"), {
                "target": "uc_abac",
                "mask_function": "schema.redact_pii",
            })

    def test_malformed_mask_function_four_parts_raises(self):
        target = UCAbacTarget()
        with pytest.raises(ValueError, match="three-part"):
            target.compile(_policy("POL-1"), {
                "target": "uc_abac",
                "mask_function": "a.b.c.d",
            })

    def test_deterministic_hash_for_same_input(self):
        target = UCAbacTarget()
        p = _policy("POL-1")
        config = {"target": "uc_abac", "mask_function": "cat.sch.fn"}
        a1 = target.compile(p, config)
        a2 = target.compile(p, config)
        assert artifact_hash(a1.content) == artifact_hash(a2.content)

    def test_end_to_end_through_registry(self, tmp_path):
        p = _policy("POL-ABAC", compile_to=[{
            "target": "uc_abac",
            "mask_function": "main.governance.redact_pii",
            "apply_when": "environment = prod",
        }])
        artifacts = compile_policies([p])
        assert len(artifacts) == 1
        assert artifacts[0].target == "uc_abac"

        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        emitted = json.loads((out / "uc_abac/POL-ABAC.json").read_text())
        assert emitted["mask_function"] == "main.governance.redact_pii"
        assert [r.state for r in check_drift(manifest, out)] == ["in_sync"]
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_compiler.py::TestUCAbacTarget -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'UCAbacTarget' from 'watchdog.compiler'`

---

### Task 2: Implement UCAbacTarget and register it

**Files:**
- Modify: `engine/src/watchdog/compiler.py`

- [ ] **Step 1: Add UCAbacTarget class**

In `engine/src/watchdog/compiler.py`, add the following class after `UCTagPolicyTarget` and before `DEFAULT_REGISTRY`:

```python
class UCAbacTarget:
    """Compile a policy into a UC ABAC column mask spec.

    UC column masks transform column values at query time via a UDF.
    The artifact is a JSON spec the deployer turns into
    ALTER TABLE ... SET COLUMN MASK API calls.

    The compiler validates the mask function name is well-formed but
    does not verify it exists — that is the deployer's responsibility.
    The compiler stays pure: no SDK calls, deterministic output.

    Config shape::

        compile_to:
          - target: uc_abac
            mask_function: main.governance.redact_pii
            apply_when: environment = prod

    mask_function is a three-part UDF reference (catalog.schema.function).
    apply_when is an optional human-readable scope note stored in the
    artifact for deployer context.
    """
    name = "uc_abac"
    _MASK_FUNCTION_RE = r"^\w+\.\w+\.\w+$"

    def compile(self, policy: PolicyDefinition, config: dict) -> EmittedArtifact:
        import re as _re

        mask_function = config.get("mask_function")
        if not mask_function:
            raise ValueError(
                f"{policy.policy_id}: compile_to.uc_abac.mask_function is required"
            )

        if not _re.match(self._MASK_FUNCTION_RE, mask_function):
            raise ValueError(
                f"{policy.policy_id}: compile_to.uc_abac.mask_function must be a "
                f"three-part name (catalog.schema.function), got {mask_function!r}"
            )

        spec: dict = {
            "policy_id": policy.policy_id,
            "name": policy.name,
            "mask_function": mask_function,
            "applies_to": policy.applies_to,
            "severity": policy.severity,
            "domain": policy.domain,
            "description": policy.description.strip() or policy.name,
        }

        apply_when = config.get("apply_when")
        if apply_when:
            spec["apply_when"] = apply_when

        content = json.dumps(spec, sort_keys=True, indent=2) + "\n"
        return EmittedArtifact(
            policy_id=policy.policy_id,
            target=self.name,
            artifact_id=f"uc_abac/{policy.policy_id}.json",
            content=content,
            emitted_at=datetime.now(timezone.utc).isoformat(),
        )
```

- [ ] **Step 2: Move the `import re` to the module level**

The module already uses `re` indirectly through other code paths. Move the import to the top of the file alongside the existing imports. In the existing import block at the top of `compiler.py`, add `re`:

```python
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol
```

Then update the `compile` method to use `re.match` directly instead of `import re as _re`:

```python
        if not re.match(self._MASK_FUNCTION_RE, mask_function):
```

(Remove the `import re as _re` line from inside the method.)

- [ ] **Step 3: Register in DEFAULT_REGISTRY**

Update the `DEFAULT_REGISTRY` dict to include the new target:

```python
DEFAULT_REGISTRY: dict[str, CompileTarget] = {
    "guardrails": GuardrailsTarget(),
    "uc_abac": UCAbacTarget(),
    "uc_tag_policy": UCTagPolicyTarget(),
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_compiler.py -v
```

Expected: All tests PASS (27 existing + 8 new = 35 total).

- [ ] **Step 5: Run the full unit test suite to confirm no regressions**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=guardrails/src:engine/src pytest tests/unit/ -v --tb=short 2>&1 | tail -10
```

Expected: All tests pass, no regressions.

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/compiler.py tests/unit/test_compiler.py
git commit -m "feat(compiler): add UC ABAC column mask compile target"
```

---

## Self-Review

**Spec coverage:**
- ✅ `UCAbacTarget` class implementing `CompileTarget` protocol
- ✅ `mask_function` required, validated as three-part dotted identifier
- ✅ `apply_when` optional, omitted from artifact when not provided
- ✅ Artifact at `uc_abac/{policy_id}.json` with sorted-key JSON
- ✅ Registered in `DEFAULT_REGISTRY`
- ✅ Deterministic hash test
- ✅ End-to-end through registry (YAML-shaped policy → compile → write → drift)
- ✅ Missing mask_function raises
- ✅ Malformed mask_function (1, 2, 4 parts) raises

**Placeholder scan:** No TBDs, TODOs, or vague steps. All code blocks complete.

**Type consistency:**
- `UCAbacTarget.compile(policy: PolicyDefinition, config: dict) -> EmittedArtifact` — matches `CompileTarget` protocol
- `_MASK_FUNCTION_RE` regex `^\w+\.\w+\.\w+$` — correctly validates three dot-separated identifier parts
- `artifact_id` format `uc_abac/{policy_id}.json` — consistent with `guardrails/{id}.json` and `uc_tag_policy/{id}.json` patterns
- Test helper `_policy()` used correctly — `applies_to` override used in first test
