# Scanner Integration for Compile-Down Posture

**Status:** Approved
**Date:** 2026-04-20

---

## Problem

The compile-down framework emits runtime enforcement artifacts and detects drift, but the scanner does not check artifact state during evaluation. The posture score treats "policy rule passes" and "policy rule passes AND runtime artifact is deployed" identically. The spec requires the posture score to distinguish these — a passing rule with a missing artifact should not get full compliance credit.

## Goal

Integrate artifact drift checking into `PolicyEngine.evaluate_all()` so that compile-down policies produce enriched scan results reflecting both rule outcome and enforcement state. Update the compliance percentage to weight partial enforcement appropriately.

## Non-Goals

- **New tables.** Reuses the existing `scan_results.result` column with enriched values.
- **Schema migration.** No new columns on `scan_results`.
- **Deploy step.** Only checks artifacts already on disk. Does not deploy or re-emit.
- **New views.** Existing views work because `result LIKE 'pass%'` still means "rule passes."

---

## Enriched Result Values

| Rule result | Artifact state | `scan_results.result` | Posture weight |
|-------------|---------------|----------------------|----------------|
| pass | no `compile_to` | `pass` | 1.0 |
| pass | in_sync | `pass` | 1.0 |
| pass | drifted | `pass_drifted` | 0.5 |
| pass | missing | `pass_missing` | 0.0 |
| fail | any | `fail` | 0.0 |

- Scan-only policies are unchanged — `pass` or `fail`.
- `fail` always trumps artifact state. If the rule fails, enforcement state is irrelevant.
- A policy with multiple `compile_to` targets uses the **worst-case** state across all targets: `missing` > `drifted` > `in_sync`.

---

## Changes

### 1. `compiler.py` — add `get_policy_artifact_state()`

A pure function that, given a policy's `compile_to` list, the manifest entries, and the output directory, returns the worst-case artifact state across all that policy's targets.

```python
def get_policy_artifact_state(
    policy_id: str,
    compile_to: list[dict],
    manifest_path: str | Path,
    output_dir: str | Path,
) -> str | None:
```

Returns:
- `None` if `compile_to` is empty/None (scan-only policy)
- `"in_sync"` if all targets are in sync
- `"drifted"` if any target is drifted (and none missing)
- `"missing"` if any target is missing

Uses the existing `load_manifest()` and `artifact_hash()` internally. Does not call `check_drift()` (which processes the entire manifest) — instead checks only the entries for the given policy_id for efficiency.

### 2. `policy_engine.py` — enrich results in `evaluate_all()`

After the rule evaluation loop produces a `pass` result for a policy with `compile_to`, call `get_policy_artifact_state()` and enrich the result:

- `pass` + `in_sync` → `"pass"`
- `pass` + `drifted` → `"pass_drifted"`
- `pass` + `missing` → `"pass_missing"`
- `fail` + anything → `"fail"`

The manifest path and output directory are passed to `PolicyEngine.__init__()` as optional parameters (`compile_manifest_path` and `compile_output_dir`). When not provided, artifact checking is skipped (backward compatible).

### 3. `violations.py` — update `compliance_pct` in `write_scan_summary()`

Today's compliance calculation:

```
compliance_pct = (resources with zero open violations) / total_resources
```

Updated calculation accounts for partial credit. A resource's compliance weight is:

- 1.0 if all its scan results are `pass`
- 0.5 if any scan result is `pass_drifted` (and none are `fail` or `pass_missing`)
- 0.0 if any scan result is `fail` or `pass_missing`

The SQL query in `write_scan_summary()` is updated to compute a weighted compliance score using `CASE` on the result values.

---

## Tests

### `test_compiler.py` — `get_policy_artifact_state()`

| Test | Asserts |
|------|---------|
| No compile_to returns None | Scan-only policy returns None |
| All in_sync returns "in_sync" | All artifacts present and matching |
| One drifted returns "drifted" | Worst-case across targets |
| One missing returns "missing" | Missing trumps drifted |
| Policy not in manifest returns "missing" | Compile_to declared but never emitted |

### `test_policy_engine.py` — enriched results

These tests require mocking the rule engine and artifact state. They verify:

| Test | Asserts |
|------|---------|
| Scan-only policy pass → `"pass"` | No enrichment when no compile_to |
| Compile-down pass + in_sync → `"pass"` | Full credit |
| Compile-down pass + drifted → `"pass_drifted"` | Partial credit |
| Compile-down pass + missing → `"pass_missing"` | No runtime credit |
| Compile-down fail + any → `"fail"` | Fail always trumps |
| No manifest/output paths → skip enrichment | Backward compatible |

### `test_violations.py` — weighted compliance_pct

These tests verify the updated SQL logic in `write_scan_summary()`. Since this runs Spark SQL, they may need integration-level testing or SQL logic extraction for unit testing.

| Test | Asserts |
|------|---------|
| All pass → 100% | Unchanged behavior |
| Mix of pass and fail → correct % | Unchanged behavior |
| pass_drifted counts as 0.5 | Partial credit applied |
| pass_missing counts as 0.0 | No credit for missing enforcement |
