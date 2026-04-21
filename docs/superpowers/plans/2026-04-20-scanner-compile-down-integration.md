# Scanner Compile-Down Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate artifact drift checking into `PolicyEngine.evaluate_all()` so compile-down policies produce enriched scan results (`pass_drifted`, `pass_missing`) and the posture score reflects enforcement state.

**Architecture:** Add a pure helper `get_policy_artifact_state()` to `compiler.py` that checks manifest entries for a single policy. Call it from `evaluate_all()` to enrich `pass` results for policies with `compile_to`. Update `write_scan_summary()` to weight partial results in `compliance_pct`. Backward compatible — when no manifest/output paths are configured, artifact checking is skipped.

**Tech Stack:** Python, pytest

**Spec:** `docs/superpowers/specs/2026-04-20-scanner-compile-down-integration-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/compiler.py` | Modify | Add `get_policy_artifact_state()` helper |
| `engine/src/watchdog/policy_engine.py` | Modify | Add `compile_manifest_path`/`compile_output_dir` to `__init__`, enrich results in `evaluate_all()` |
| `engine/src/watchdog/violations.py` | Modify | Update `compliance_pct` weighting in `write_scan_summary()` |
| `engine/src/watchdog/entrypoints.py` | Modify | Pass manifest/output paths to `PolicyEngine` in `_build_engine()` |
| `tests/unit/test_compiler.py` | Modify | Add `TestGetPolicyArtifactState` |
| `tests/unit/test_policy_engine.py` | Modify | Add `TestEnrichResult` |

---

### Task 1: `get_policy_artifact_state()` — pure helper in compiler.py

**Files:**
- Modify: `engine/src/watchdog/compiler.py`
- Modify: `tests/unit/test_compiler.py`

- [ ] **Step 1: Write failing tests**

Append the following test class to `tests/unit/test_compiler.py` at the end of the file:

```python
class TestGetPolicyArtifactState:
    """Tests for get_policy_artifact_state — per-policy artifact drift check."""

    def test_no_compile_to_returns_none(self):
        state = get_policy_artifact_state("POL-1", None, "/fake/manifest.json", "/fake/out")
        assert state is None

    def test_empty_compile_to_returns_none(self):
        state = get_policy_artifact_state("POL-1", [], "/fake/manifest.json", "/fake/out")
        assert state is None

    def test_all_in_sync(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        state = get_policy_artifact_state(
            "POL-1", p.compile_to, str(manifest), str(out)
        )
        assert state == "in_sync"

    def test_drifted_artifact(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        # Tamper with the artifact
        (out / artifacts[0].artifact_id).write_text("tampered\n")

        state = get_policy_artifact_state(
            "POL-1", p.compile_to, str(manifest), str(out)
        )
        assert state == "drifted"

    def test_missing_artifact(self, tmp_path):
        p = _policy("POL-1", compile_to=[{"target": "guardrails", "kind": "advisory"}])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        # Delete the artifact
        (out / artifacts[0].artifact_id).unlink()

        state = get_policy_artifact_state(
            "POL-1", p.compile_to, str(manifest), str(out)
        )
        assert state == "missing"

    def test_policy_not_in_manifest_returns_missing(self, tmp_path):
        # compile_to is declared but policy was never compiled (not in manifest)
        manifest = tmp_path / "manifest.json"
        manifest.write_text('{"entries": []}\n')
        out = tmp_path / "out"
        out.mkdir()

        state = get_policy_artifact_state(
            "POL-NEVER-COMPILED",
            [{"target": "guardrails", "kind": "advisory"}],
            str(manifest),
            str(out),
        )
        assert state == "missing"

    def test_worst_case_missing_trumps_drifted(self, tmp_path):
        # Policy with two targets: one drifted, one missing → missing wins
        p = _policy("POL-1", compile_to=[
            {"target": "guardrails", "kind": "advisory"},
            {"target": "uc_tag_policy", "tag_key": "owner"},
        ])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        # Tamper with guardrails artifact (drifted), delete uc_tag_policy (missing)
        (out / "guardrails/POL-1.json").write_text("tampered\n")
        (out / "uc_tag_policy/POL-1.json").unlink()

        state = get_policy_artifact_state(
            "POL-1", p.compile_to, str(manifest), str(out)
        )
        assert state == "missing"

    def test_worst_case_drifted_trumps_in_sync(self, tmp_path):
        p = _policy("POL-1", compile_to=[
            {"target": "guardrails", "kind": "advisory"},
            {"target": "uc_tag_policy", "tag_key": "owner"},
        ])
        artifacts = compile_policies([p])
        out = tmp_path / "out"
        manifest = tmp_path / "manifest.json"
        write_artifacts(artifacts, out)
        write_manifest(artifacts, manifest)

        # Tamper with guardrails artifact only
        (out / "guardrails/POL-1.json").write_text("tampered\n")

        state = get_policy_artifact_state(
            "POL-1", p.compile_to, str(manifest), str(out)
        )
        assert state == "drifted"
```

- [ ] **Step 2: Add `get_policy_artifact_state` to the imports in test_compiler.py**

Update the import block:

```python
from watchdog.compiler import (
    DEFAULT_REGISTRY,
    GuardrailsTarget,
    UCAbacTarget,
    UCTagPolicyTarget,
    artifact_hash,
    check_drift,
    compile_policies,
    get_policy_artifact_state,
    load_manifest,
    write_artifacts,
    write_manifest,
)
```

- [ ] **Step 3: Run to verify they fail**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_compiler.py::TestGetPolicyArtifactState -v 2>&1 | head -10
```

Expected: `ImportError: cannot import name 'get_policy_artifact_state'`

- [ ] **Step 4: Implement `get_policy_artifact_state()`**

Add the following function to `engine/src/watchdog/compiler.py`, after the `check_drift()` function (at the end of the file):

```python
def get_policy_artifact_state(
    policy_id: str,
    compile_to: list[dict] | None,
    manifest_path: str | Path,
    output_dir: str | Path,
) -> str | None:
    """Check artifact drift for a single policy's compile targets.

    Returns the worst-case state across all targets for efficiency during
    evaluate_all (avoids processing the entire manifest for every policy).

    Returns:
        None if compile_to is empty/None (scan-only policy).
        "in_sync" if all targets are present and match the manifest hash.
        "drifted" if any target is present but modified out-of-band.
        "missing" if any target is absent or was never emitted.
    """
    if not compile_to:
        return None

    entries = load_manifest(manifest_path)
    policy_entries = [e for e in entries if e["policy_id"] == policy_id]

    if not policy_entries:
        return "missing"

    base = Path(output_dir)
    worst = "in_sync"
    _SEVERITY = {"in_sync": 0, "drifted": 1, "missing": 2}

    for entry in policy_entries:
        artifact_path = base / entry["artifact_id"]
        if not artifact_path.exists():
            state = "missing"
        else:
            actual = artifact_hash(artifact_path.read_text())
            state = "in_sync" if actual == entry["content_hash"] else "drifted"
        if _SEVERITY[state] > _SEVERITY[worst]:
            worst = state

    return worst
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_compiler.py -v
```

Expected: All 43 tests pass (35 existing + 8 new).

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/compiler.py tests/unit/test_compiler.py
git commit -m "feat(compiler): add get_policy_artifact_state() for per-policy drift check"
```

---

### Task 2: Enrich results in `PolicyEngine.evaluate_all()`

**Files:**
- Modify: `engine/src/watchdog/policy_engine.py`
- Modify: `tests/unit/test_policy_engine.py`

- [ ] **Step 1: Write failing tests**

Append the following test class to `tests/unit/test_policy_engine.py`:

```python
class TestEnrichResult:
    """Tests for _enrich_result — compile-down artifact state enrichment."""

    def test_scan_only_pass_unchanged(self, engine):
        result = engine._enrich_result("pass", None)
        assert result == "pass"

    def test_scan_only_fail_unchanged(self, engine):
        result = engine._enrich_result("fail", None)
        assert result == "fail"

    def test_compile_down_pass_in_sync(self, engine):
        result = engine._enrich_result("pass", "in_sync")
        assert result == "pass"

    def test_compile_down_pass_drifted(self, engine):
        result = engine._enrich_result("pass", "drifted")
        assert result == "pass_drifted"

    def test_compile_down_pass_missing(self, engine):
        result = engine._enrich_result("pass", "missing")
        assert result == "pass_missing"

    def test_compile_down_fail_in_sync(self, engine):
        result = engine._enrich_result("fail", "in_sync")
        assert result == "fail"

    def test_compile_down_fail_drifted(self, engine):
        result = engine._enrich_result("fail", "drifted")
        assert result == "fail"

    def test_compile_down_fail_missing(self, engine):
        result = engine._enrich_result("fail", "missing")
        assert result == "fail"
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_policy_engine.py::TestEnrichResult -v 2>&1 | head -10
```

Expected: `AttributeError: 'PolicyEngine' object has no attribute '_enrich_result'`

- [ ] **Step 3: Add `_enrich_result` method and constructor params**

In `engine/src/watchdog/policy_engine.py`, update `PolicyEngine.__init__()` to accept optional compile paths:

```python
    def __init__(self, spark: SparkSession, w: WorkspaceClient,
                 catalog: str, schema: str,
                 ontology: OntologyEngine | None = None,
                 rule_engine: RuleEngine | None = None,
                 policies: list[PolicyDefinition] | None = None,
                 compile_manifest_path: str | None = None,
                 compile_output_dir: str | None = None):
        self.spark = spark
        self.w = w
        self.catalog = catalog
        self.schema = schema
        self.ontology = ontology or OntologyEngine()
        self.rule_engine = rule_engine or RuleEngine()
        self.policies = policies or []
        self.now = datetime.now(timezone.utc)
        self.compile_manifest_path = compile_manifest_path
        self.compile_output_dir = compile_output_dir
```

Add the `_enrich_result` method after `_inject_drift_metadata` and before `evaluate_all`:

```python
    @staticmethod
    def _enrich_result(result: str, artifact_state: str | None) -> str:
        """Enrich a scan result with compile-down artifact state.

        fail always trumps artifact state. pass is enriched to pass_drifted
        or pass_missing when the runtime artifact is not in sync.
        """
        if result != "pass" or artifact_state is None:
            return result
        if artifact_state == "in_sync":
            return "pass"
        return f"pass_{artifact_state}"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_policy_engine.py -v
```

Expected: All tests pass (existing + 8 new).

- [ ] **Step 5: Wire enrichment into `evaluate_all()`**

In `engine/src/watchdog/policy_engine.py`, inside `evaluate_all()`, add the import and artifact state logic. After the line `result = self.rule_engine.evaluate(policy.rule, tags, metadata)` (around line 335), and before the `scan_results.append(...)` block, add the enrichment:

Replace this block (lines 337-348):

```python
                scan_results.append((
                    scan_id,
                    resource.resource_id,
                    policy.policy_id,
                    "pass" if result.passed else "fail",
                    result.detail,
                    policy.domain,
                    policy.severity,
                    ",".join(sorted(resource_classes.get(resource.resource_id, set()))),
                    metastore_id,
                    self.now,
                ))
```

With:

```python
                result_str = "pass" if result.passed else "fail"

                # Enrich with compile-down artifact state
                if policy.compile_to and self.compile_manifest_path and self.compile_output_dir:
                    from watchdog.compiler import get_policy_artifact_state
                    artifact_state = get_policy_artifact_state(
                        policy.policy_id,
                        policy.compile_to,
                        self.compile_manifest_path,
                        self.compile_output_dir,
                    )
                    result_str = self._enrich_result(result_str, artifact_state)

                scan_results.append((
                    scan_id,
                    resource.resource_id,
                    policy.policy_id,
                    result_str,
                    result.detail,
                    policy.domain,
                    policy.severity,
                    ",".join(sorted(resource_classes.get(resource.resource_id, set()))),
                    metastore_id,
                    self.now,
                ))
```

- [ ] **Step 6: Run all tests to verify no regressions**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_policy_engine.py tests/unit/test_compiler.py -v
```

Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add engine/src/watchdog/policy_engine.py tests/unit/test_policy_engine.py
git commit -m "feat(policy-engine): enrich scan results with compile-down artifact state"
```

---

### Task 3: Update `compliance_pct` weighting in `write_scan_summary()`

**Files:**
- Modify: `engine/src/watchdog/violations.py`

This change is in Spark SQL that runs against Delta tables, so it cannot be unit-tested without Spark. The change is small and surgical — a SQL `CASE` expression update.

- [ ] **Step 1: Update the compliance query**

In `engine/src/watchdog/violations.py`, inside `write_scan_summary()`, replace the compliance calculation block (lines 378-392):

```python
    # Compliance %: resources with zero open violations / total resources
    inventory_table = f"{catalog}.{schema}.resource_inventory"
    compliance_row = spark.sql(f"""
        SELECT
            COUNT(DISTINCT ri.resource_id) AS total,
            COUNT(DISTINCT CASE WHEN v.resource_id IS NOT NULL THEN ri.resource_id END) AS with_violations
        FROM {inventory_table} ri
        LEFT JOIN {violations_table} v
            ON ri.resource_id = v.resource_id AND v.status = 'open'
        WHERE ri.scan_id = '{scan_id}'
    """).first()

    total = compliance_row.total or 0
    with_violations = compliance_row.with_violations or 0
    compliance_pct = round((total - with_violations) * 100.0 / total, 1) if total > 0 else 100.0
```

With:

```python
    # Compliance %: weighted by scan result, accounting for compile-down partial credit.
    # pass = 1.0, pass_drifted = 0.5, pass_missing/fail = 0.0.
    # Per-resource weight = minimum weight across all its scan results.
    scan_results_table = f"{catalog}.{schema}.scan_results"
    inventory_table = f"{catalog}.{schema}.resource_inventory"
    compliance_row = spark.sql(f"""
        SELECT
            COUNT(DISTINCT ri.resource_id) AS total,
            COALESCE(SUM(resource_weight), 0) AS weighted_sum
        FROM {inventory_table} ri
        LEFT JOIN (
            SELECT resource_id, MIN(
                CASE result
                    WHEN 'pass' THEN 1.0
                    WHEN 'pass_drifted' THEN 0.5
                    ELSE 0.0
                END
            ) AS resource_weight
            FROM {scan_results_table}
            WHERE scan_id = '{scan_id}'
            GROUP BY resource_id
        ) sw ON ri.resource_id = sw.resource_id
        WHERE ri.scan_id = '{scan_id}'
    """).first()

    total = compliance_row.total or 0
    weighted_sum = compliance_row.weighted_sum or 0.0
    compliance_pct = round(weighted_sum * 100.0 / total, 1) if total > 0 else 100.0
```

- [ ] **Step 2: Run the full unit test suite to confirm no import errors or regressions**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=guardrails/src:engine/src pytest tests/unit/ -v --tb=short 2>&1 | tail -15
```

Expected: All tests pass (violations.py changes are SQL-only, no unit test coverage needed — the existing tests don't exercise `write_scan_summary` which requires Spark).

- [ ] **Step 3: Commit**

```bash
git add engine/src/watchdog/violations.py
git commit -m "feat(violations): weight compliance_pct by compile-down artifact state"
```

---

### Task 4: Wire manifest/output paths through entrypoints

**Files:**
- Modify: `engine/src/watchdog/entrypoints.py`

- [ ] **Step 1: Update `_build_engine()` to detect and pass compile paths**

In `engine/src/watchdog/entrypoints.py`, update `_build_engine()`. After the `has_primitives` check (around line 42), add compile-down path detection:

```python
    # Detect compile-down manifest presence
    try:
        compile_dir = Path(__file__).parent.parent.parent / "compile_output"
    except NameError:
        compile_dir = Path(os.getcwd()) / "compile_output"
    compile_manifest = compile_dir / "manifest.json"
    has_compile = compile_manifest.exists()
```

Then update the `PolicyEngine` constructor call (around line 65):

```python
    engine_kwargs = dict(
        ontology=ontology,
        rule_engine=rule_engine,
        policies=policies,
    )
    if has_compile:
        engine_kwargs["compile_manifest_path"] = str(compile_manifest)
        engine_kwargs["compile_output_dir"] = str(compile_dir)
        print(f"Watchdog: compile-down enabled — manifest at {compile_manifest}")

    return PolicyEngine(spark, w, catalog, schema, **engine_kwargs)
```

Replace the existing two print blocks (full mode / MVP mode, lines 51-63) with:

```python
    if has_ontology and has_primitives:
        print(f"Watchdog: full mode — ontology ({len(ontology.classes)} classes), "
              f"rule engine ({len(rule_engine.primitives)} primitives), "
              f"{len(policies)} policies ({len(yaml_policies)} YAML + {len(user_policies)} user)")
    else:
        missing = []
        if not has_ontology:
            missing.append("resource_classes.yml")
        if not has_primitives:
            missing.append("rule_primitives.yml")
        print(f"Watchdog: MVP mode — missing {', '.join(missing)}. "
              f"Using resource_type fallback. "
              f"{len(policies)} policies ({len(yaml_policies)} YAML + {len(user_policies)} user)")
```

(This block is unchanged — just confirming it stays in place before the new compile-down code.)

- [ ] **Step 2: Run the full test suite**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=guardrails/src:engine/src pytest tests/unit/ -v --tb=short 2>&1 | tail -15
```

Expected: All tests pass. Entrypoints are not unit-tested (they require Spark/CLI), so this confirms no import-time regressions.

- [ ] **Step 3: Commit**

```bash
git add engine/src/watchdog/entrypoints.py
git commit -m "feat(entrypoints): wire compile-down manifest into PolicyEngine"
```

---

## Self-Review

**Spec coverage:**
- ✅ `get_policy_artifact_state()` — pure helper, per-policy, worst-case across targets (Task 1)
- ✅ Enriched result values: `pass`, `pass_drifted`, `pass_missing`, `fail` (Task 2)
- ✅ `fail` always trumps artifact state (Task 2, `_enrich_result`)
- ✅ Backward compatible — no manifest = no enrichment (Task 2, conditional in `evaluate_all`)
- ✅ `compliance_pct` weights: `pass` = 1.0, `pass_drifted` = 0.5, `pass_missing`/`fail` = 0.0 (Task 3)
- ✅ Manifest/output paths wired through `_build_engine()` (Task 4)
- ✅ No new tables, no schema changes, no new views

**Placeholder scan:** No TBDs, TODOs, or vague steps. All code blocks complete.

**Type consistency:**
- `get_policy_artifact_state(policy_id: str, compile_to: list[dict] | None, manifest_path: str | Path, output_dir: str | Path) -> str | None` — signature consistent between Task 1 definition and Task 2 usage
- `_enrich_result(result: str, artifact_state: str | None) -> str` — signature consistent between Task 2 definition and tests
- `compile_manifest_path: str | None` and `compile_output_dir: str | None` — consistent between Task 2 (`__init__`) and Task 4 (entrypoint wiring)
- Return values `"in_sync"`, `"drifted"`, `"missing"` — consistent between Task 1 helper and Task 2 enrichment
- SQL result values `'pass'`, `'pass_drifted'`, `'pass_missing'` — consistent between Task 2 enrichment and Task 3 compliance query
