# `watchdog compile` Entrypoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `watchdog-compile` CLI entrypoint that compiles policies to runtime artifacts, writes a manifest, runs drift detection, and prints a terse summary.

**Architecture:** A `compile()` function in `entrypoints.py` wires together existing compiler functions. A pure `format_compile_summary()` helper handles output formatting (testable without Spark). Registered as `watchdog-compile` in `setup.py` console_scripts.

**Tech Stack:** Python, pytest

**Spec:** `docs/superpowers/specs/2026-04-21-watchdog-compile-entrypoint-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/entrypoints.py` | Modify | Add `compile()` entrypoint + `format_compile_summary()` helper |
| `engine/setup.py` | Modify | Register `watchdog-compile` console_scripts entry |
| `tests/unit/test_compile_entrypoint.py` | Create | Tests for `format_compile_summary()` |

---

### Task 1: `format_compile_summary()` — testable summary formatter

**Files:**
- Modify: `engine/src/watchdog/entrypoints.py`
- Create: `tests/unit/test_compile_entrypoint.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_compile_entrypoint.py`:

```python
"""Unit tests for compile entrypoint summary formatting.

Run with: pytest tests/unit/test_compile_entrypoint.py -v
"""
import sys
from unittest.mock import MagicMock

# Mock heavyweight runtime dependencies so tests run without pyspark/databricks.
_mock_modules = {}
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "databricks", "databricks.sdk",
]:
    _mock_modules[_mod] = MagicMock()

_types = _mock_modules["pyspark.sql.types"]
_types.StructType = list
_types.StructField = lambda name, typ, nullable=True: name
_types.StringType = MagicMock
_types.TimestampType = MagicMock

with __import__("unittest.mock", fromlist=["patch"]).patch.dict(sys.modules, _mock_modules):
    from watchdog.entrypoints import format_compile_summary


class TestFormatCompileSummary:
    def test_basic_summary(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-2", "guardrails"),
            _artifact("POL-3", "uc_tag_policy"),
        ]
        drift = [
            _drift("POL-1", "guardrails", "in_sync"),
            _drift("POL-2", "guardrails", "in_sync"),
            _drift("POL-3", "uc_tag_policy", "in_sync"),
        ]
        result = format_compile_summary(artifacts, drift)
        assert "3 artifacts" in result
        assert "2 guardrails" in result
        assert "1 uc_tag_policy" in result
        assert "3 in_sync" in result

    def test_empty_artifacts(self):
        result = format_compile_summary([], [])
        assert "Nothing to compile" in result

    def test_mixed_drift_states(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-2", "uc_abac"),
            _artifact("POL-3", "uc_tag_policy"),
        ]
        drift = [
            _drift("POL-1", "guardrails", "in_sync"),
            _drift("POL-2", "uc_abac", "drifted"),
            _drift("POL-3", "uc_tag_policy", "missing"),
        ]
        result = format_compile_summary(artifacts, drift)
        assert "1 in_sync" in result
        assert "1 drifted" in result
        assert "1 missing" in result

    def test_multiple_targets_per_policy(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-1", "uc_abac"),
            _artifact("POL-2", "guardrails"),
        ]
        drift = [
            _drift("POL-1", "guardrails", "in_sync"),
            _drift("POL-1", "uc_abac", "in_sync"),
            _drift("POL-2", "guardrails", "drifted"),
        ]
        result = format_compile_summary(artifacts, drift)
        assert "3 artifacts" in result
        assert "2 guardrails" in result
        assert "1 uc_abac" in result

    def test_counts_unique_policies(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-1", "uc_abac"),
        ]
        drift = []
        result = format_compile_summary(artifacts, drift)
        assert "1 policies" in result or "1 policy" in result


def _artifact(policy_id, target):
    """Minimal artifact-like object for summary formatting."""
    return type("A", (), {"policy_id": policy_id, "target": target})()


def _drift(policy_id, target, state):
    """Minimal drift-result-like object for summary formatting."""
    return type("D", (), {"policy_id": policy_id, "target": target, "state": state})()
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_compile_entrypoint.py -v 2>&1 | head -10
```

Expected: `ImportError: cannot import name 'format_compile_summary'`

- [ ] **Step 3: Implement `format_compile_summary()`**

In `engine/src/watchdog/entrypoints.py`, add the following function before the `_build_engine()` function (near the top of the file, after the imports):

```python
def format_compile_summary(
    artifacts: list, drift_results: list
) -> str:
    """Format a terse compile summary for CLI output.

    Args:
        artifacts: List of EmittedArtifact from compile_policies().
        drift_results: List of DriftResult from check_drift().
    """
    if not artifacts:
        return "No policies with compile_to found. Nothing to compile."

    policy_ids = {a.policy_id for a in artifacts}
    target_counts: dict[str, int] = {}
    for a in artifacts:
        target_counts[a.target] = target_counts.get(a.target, 0) + 1
    target_str = ", ".join(f"{count} {target}" for target, count in sorted(target_counts.items()))

    drift_counts: dict[str, int] = {"in_sync": 0, "drifted": 0, "missing": 0}
    for d in drift_results:
        drift_counts[d.state] = drift_counts.get(d.state, 0) + 1
    drift_str = ", ".join(f"{count} {state}" for state, count in drift_counts.items())

    return (
        f"Compiled {len(policy_ids)} policies → {len(artifacts)} artifacts "
        f"({target_str}). Drift: {drift_str}."
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_compile_entrypoint.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/watchdog/entrypoints.py tests/unit/test_compile_entrypoint.py
git commit -m "feat(entrypoints): add format_compile_summary() helper"
```

---

### Task 2: `compile()` entrypoint + setup.py registration

**Files:**
- Modify: `engine/src/watchdog/entrypoints.py`
- Modify: `engine/setup.py`

- [ ] **Step 1: Add the `compile()` function**

In `engine/src/watchdog/entrypoints.py`, add the following function. Place it after `format_compile_summary()` and before `_build_engine()`:

```python
def compile():
    """Entrypoint: compile policies to runtime enforcement artifacts.

    Loads policies, runs the compiler for any with compile_to blocks,
    writes artifacts + manifest to compile_output/, runs drift detection,
    and prints a summary.
    """
    import os
    from pathlib import Path

    from watchdog.compiler import (
        check_drift,
        compile_policies,
        write_artifacts,
        write_manifest,
    )
    from watchdog.policy_loader import load_delta_policies, load_yaml_policies

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    # Load policies
    yaml_policies = load_yaml_policies()
    user_policies = load_delta_policies(spark, args.catalog, args.schema)
    policies = yaml_policies + user_policies

    compilable = [p for p in policies if p.compile_to]
    print(f"Loaded {len(policies)} policies ({len(compilable)} with compile_to)")

    # Compile
    artifacts = compile_policies(policies)

    if not artifacts:
        print(format_compile_summary([], []))
        return

    # Resolve output directory
    try:
        compile_dir = Path(__file__).parent.parent.parent / "compile_output"
    except NameError:
        compile_dir = Path(os.getcwd()) / "compile_output"

    # Write artifacts + manifest
    write_artifacts(artifacts, compile_dir)
    manifest_path = compile_dir / "manifest.json"
    write_manifest(artifacts, manifest_path)

    # Drift detection
    drift_results = check_drift(manifest_path, compile_dir)

    # Summary
    print(format_compile_summary(artifacts, drift_results))
```

- [ ] **Step 2: Register in setup.py**

In `engine/setup.py`, add the `watchdog-compile` entry to `console_scripts`. Update the list to include:

```python
            "watchdog-compile=watchdog.entrypoints:compile",
```

Add it after `"watchdog-crawl-all-metastores=watchdog.entrypoints:crawl_all_metastores",` and before `"watchdog-evaluate=watchdog.entrypoints:evaluate",` to maintain alphabetical-ish order.

- [ ] **Step 3: Run the full test suite to confirm no regressions**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=guardrails/src:engine/src pytest tests/unit/ -v --tb=short --ignore=tests/unit/test_merge_pack.py 2>&1 | tail -10
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/entrypoints.py engine/setup.py
git commit -m "feat(entrypoints): add watchdog-compile CLI entrypoint"
```

---

## Self-Review

**Spec coverage:**
- ✅ `compile()` entrypoint with `--catalog` and `--schema` args (Task 2)
- ✅ Loads YAML + Delta policies (Task 2)
- ✅ `compile_policies()` → `write_artifacts()` → `write_manifest()` pipeline (Task 2)
- ✅ `check_drift()` after compile (Task 2)
- ✅ Terse summary output (Task 1)
- ✅ "Nothing to compile" when no artifacts (Task 1)
- ✅ `compile_output/` relative to engine root with serverless fallback (Task 2)
- ✅ `watchdog-compile` registered in `setup.py` (Task 2)

**Placeholder scan:** No TBDs, TODOs, or vague steps. All code blocks complete.

**Type consistency:**
- `format_compile_summary(artifacts: list, drift_results: list) -> str` — signature consistent between Task 1 definition and Task 2 usage
- `artifacts` accessed via `.policy_id` and `.target` — matches `EmittedArtifact` dataclass fields
- `drift_results` accessed via `.state` — matches `DriftResult` dataclass field
- Test helpers `_artifact()` and `_drift()` create objects with matching attributes
