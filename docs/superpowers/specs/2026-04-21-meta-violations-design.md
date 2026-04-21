# Meta-Violations — Surface Drifted/Missing Artifacts as Violations

**Status:** Approved
**Date:** 2026-04-21

---

## Problem

The scanner enriches scan results with `pass_drifted` and `pass_missing` for compile-down policies, but these only affect `compliance_pct`. Stewards have no actionable violation to remediate. A drifted artifact should surface as a violation the same way a failing policy rule does — visible in dashboards, triggerable by notifications, resolvable when the artifact is re-compiled or re-deployed.

## Goal

Emit meta-violation scan results during `evaluate_all()` when compile-down artifacts are drifted or missing. These flow through the existing `merge_violations` pipeline and appear as actionable violations that auto-resolve when drift is fixed.

## Non-Goals

- **New tables.** Meta-violations use existing `scan_results` and `violations`.
- **YAML policy definitions for meta-policies.** Synthetic policy IDs are generated at runtime.
- **Special UI treatment.** Meta-violations appear alongside regular violations.

---

## How It Works

In `evaluate_all()`, after enriching a result to `pass_drifted` or `pass_missing`, emit an additional `scan_results` tuple:

| Field | Value |
|-------|-------|
| `resource_id` | The artifact_id (e.g., `uc_abac/POL-PII-001.json`) |
| `policy_id` | `META-DRIFT-{original_policy_id}` |
| `result` | `"fail"` |
| `severity` | `"high"` for missing, `"medium"` for drifted |
| `domain` | `"CompileDown"` |
| `details` | Human-readable: `"Artifact {artifact_id} is {state} — runtime enforcement not {deployed/in sync}"` |
| `resource_classes` | Empty string (meta-violations are not resource-typed) |

### Auto-Resolution

Meta-violations flow through `merge_violations` like any other violation:
- When a drifted artifact is re-compiled and becomes in_sync, the next scan does NOT emit a meta-violation for it → `merge_violations` marks the existing one as `resolved`.
- No special resolution logic needed — the existing pipeline handles it.

### Deduplication

Meta-violations use `resource_id = artifact_id` and `policy_id = META-DRIFT-{policy_id}`. This composite key is unique per (artifact, policy) pair. The `merge_violations` MERGE ON clause (`resource_id, policy_id`) handles deduplication automatically.

### One Meta-Violation Per Policy (Not Per Resource)

The artifact state is per-policy (computed once in the outer loop). The meta-violation is emitted once per policy with a drifted/missing artifact — NOT once per resource that policy applies to. This is correct because the drift is about the enforcement artifact, not about individual resources.

## Changes

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/policy_engine.py` | Modify | Emit meta-violation scan results in `evaluate_all()` |
| `tests/unit/test_policy_engine.py` | Modify | Tests for meta-violation emission |

## Tests

| Test | Asserts |
|------|---------|
| `test_meta_violation_not_emitted_for_scan_only` | No meta-violation when policy has no compile_to |
| `test_meta_violation_not_emitted_for_in_sync` | No meta-violation when artifact is in_sync |
| `test_meta_violation_emitted_for_drifted` | Meta-violation with severity=medium, domain=CompileDown |
| `test_meta_violation_emitted_for_missing` | Meta-violation with severity=high |
| `test_meta_violation_policy_id_format` | policy_id is `META-DRIFT-{original}` |
| `test_meta_violation_resource_id_is_artifact_id` | resource_id matches an artifact_id pattern |
| `test_meta_violation_emitted_once_per_policy` | Only one meta-violation even if policy applies to many resources |
