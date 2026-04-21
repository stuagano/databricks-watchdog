# Meta-Violations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit meta-violation scan results for drifted/missing compile-down artifacts so they surface as actionable violations in the existing pipeline.

**Architecture:** A new static method `_build_meta_violation()` on `PolicyEngine` builds the scan_result tuple. Called once per policy (not per resource) in `evaluate_all()` when artifact state is drifted or missing. Flows through existing `merge_violations` for dedup and auto-resolution.

**Tech Stack:** Python, pytest

**Spec:** `docs/superpowers/specs/2026-04-21-meta-violations-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/policy_engine.py` | Modify | Add `_build_meta_violation()`, emit in `evaluate_all()` |
| `tests/unit/test_policy_engine.py` | Modify | Add `TestMetaViolations` test class |

---

### Task 1: `_build_meta_violation()` + tests

**Files:**
- Modify: `engine/src/watchdog/policy_engine.py`
- Modify: `tests/unit/test_policy_engine.py`

- [ ] **Step 1: Write failing tests**

Append the following test class to `tests/unit/test_policy_engine.py`:

```python
class TestMetaViolations:
    """Tests for _build_meta_violation — compile-down drift as violations."""

    def test_not_emitted_for_scan_only(self, engine):
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-1",
            artifact_state=None,
            compile_to=[{"target": "guardrails", "kind": "advisory"}],
            metastore_id=None,
        )
        assert result is None

    def test_not_emitted_for_in_sync(self, engine):
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-1",
            artifact_state="in_sync",
            compile_to=[{"target": "guardrails", "kind": "advisory"}],
            metastore_id=None,
        )
        assert result is None

    def test_emitted_for_drifted(self, engine):
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-1",
            artifact_state="drifted",
            compile_to=[{"target": "uc_abac", "mask_function": "cat.sch.fn"}],
            metastore_id="ms-1",
        )
        assert result is not None
        scan_id, resource_id, policy_id, result_str, details, domain, severity, classes, ms_id, ts = result
        assert scan_id == "scan-1"
        assert policy_id == "META-DRIFT-POL-1"
        assert result_str == "fail"
        assert severity == "medium"
        assert domain == "CompileDown"
        assert "drifted" in details
        assert ms_id == "ms-1"

    def test_emitted_for_missing(self, engine):
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-1",
            artifact_state="missing",
            compile_to=[{"target": "uc_tag_policy", "tag_key": "owner"}],
            metastore_id=None,
        )
        assert result is not None
        _, _, _, _, _, _, severity, _, _, _ = result
        assert severity == "high"

    def test_policy_id_format(self, engine):
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-PII-001",
            artifact_state="drifted",
            compile_to=[{"target": "guardrails"}],
            metastore_id=None,
        )
        _, _, policy_id, _, _, _, _, _, _, _ = result
        assert policy_id == "META-DRIFT-POL-PII-001"

    def test_resource_id_uses_first_artifact_id(self, engine):
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-1",
            artifact_state="missing",
            compile_to=[
                {"target": "guardrails", "kind": "advisory"},
                {"target": "uc_tag_policy", "tag_key": "owner"},
            ],
            metastore_id=None,
        )
        _, resource_id, _, _, _, _, _, _, _, _ = result
        assert resource_id == "compile-artifact:POL-1"

    def test_emitted_once_per_policy(self, engine):
        # _build_meta_violation returns a single tuple, not one per resource.
        # This test confirms the return type is a single tuple (not a list).
        result = engine._build_meta_violation(
            scan_id="scan-1",
            policy_id="POL-1",
            artifact_state="drifted",
            compile_to=[{"target": "guardrails"}],
            metastore_id=None,
        )
        assert isinstance(result, tuple)
        assert len(result) == 10  # same shape as scan_results tuples
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_policy_engine.py::TestMetaViolations -v 2>&1 | head -10
```

Expected: `AttributeError: 'PolicyEngine' object has no attribute '_build_meta_violation'`

- [ ] **Step 3: Implement `_build_meta_violation()`**

In `engine/src/watchdog/policy_engine.py`, add the following method to `PolicyEngine`, after `_enrich_result()` and before `evaluate_all()`:

```python
    def _build_meta_violation(
        self,
        scan_id: str,
        policy_id: str,
        artifact_state: str | None,
        compile_to: list[dict],
        metastore_id: str | None,
    ) -> tuple | None:
        """Build a meta-violation scan result for a drifted/missing artifact.

        Returns None for scan-only policies or in-sync artifacts.
        Returns a single scan_results tuple for drifted or missing artifacts.
        """
        if artifact_state is None or artifact_state == "in_sync":
            return None

        severity = "high" if artifact_state == "missing" else "medium"
        targets = ", ".join(e.get("target", "unknown") for e in compile_to)
        details = (
            f"Compile-down artifact for {policy_id} is {artifact_state} "
            f"(targets: {targets}). Runtime enforcement not "
            f"{'deployed' if artifact_state == 'missing' else 'in sync'}."
        )

        return (
            scan_id,
            f"compile-artifact:{policy_id}",
            f"META-DRIFT-{policy_id}",
            "fail",
            details,
            "CompileDown",
            severity,
            "",  # resource_classes — not applicable for meta-violations
            metastore_id,
            self.now,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_policy_engine.py -v
```

Expected: All tests pass (existing 34 + 7 new = 41).

- [ ] **Step 5: Wire into `evaluate_all()`**

In `engine/src/watchdog/policy_engine.py`, inside `evaluate_all()`, after the artifact_state computation block (around line 343) and before the `for resource in inventory` loop, add:

```python
            # Emit meta-violation for drifted/missing artifacts (once per policy)
            if artifact_state and artifact_state != "in_sync" and policy.compile_to:
                meta = self._build_meta_violation(
                    scan_id, policy.policy_id, artifact_state,
                    policy.compile_to, metastore_id,
                )
                if meta:
                    scan_results.append(meta)
```

- [ ] **Step 6: Run all tests**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_policy_engine.py tests/unit/test_compiler.py -v --tb=short
```

Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add engine/src/watchdog/policy_engine.py tests/unit/test_policy_engine.py
git commit -m "feat(policy-engine): emit meta-violations for drifted/missing compile-down artifacts"
```

---

## Self-Review

**Spec coverage:**
- ✅ Meta-violation emitted for drifted (severity=medium) and missing (severity=high)
- ✅ Not emitted for scan-only or in-sync
- ✅ policy_id format: `META-DRIFT-{original}`
- ✅ resource_id: `compile-artifact:{policy_id}`
- ✅ domain: `CompileDown`
- ✅ result: `"fail"`
- ✅ One per policy, not per resource
- ✅ Flows through existing merge_violations (no special handling needed)
- ✅ Auto-resolves when drift is fixed (next scan won't emit → merge_violations resolves)

**Placeholder scan:** No TBDs or vague steps. All code complete.

**Type consistency:**
- `_build_meta_violation()` returns `tuple | None` — matches scan_results tuple shape (10 fields)
- `artifact_state` parameter type `str | None` — matches `get_policy_artifact_state()` return type
- Meta-violation tuple fields match `_scan_schema` in `evaluate_all()`
